//! The Postgres metadata backend of [`Meta`](crate::meta::Meta)
//! ([#47](https://github.com/bnsd55/open-scion/issues/47)).
//!
//! One shared database takes the place of one data directory: many
//! processes can hold clients, and Postgres sorts the writers out, so
//! there is no directory lock, one write connection, or reader pool of a
//! fixed set of pages. A small pool of clients stands in for the read
//! connections, and one client for writes, in the shape of
//! [`Store`](crate::store::Store).
//!
//! The rules that a backend must keep hold here too:
//!
//! - `synchronous_commit = on` on every client, so a commit is durable
//!   when the statement returns. `flush` is a no-op, `read` need not wait,
//!   and `read_counted` returns no count, because there is nothing to wait
//!   for.
//! - Reads and writes run at READ COMMITTED. Rows that content addresses
//!   never change once written, and refs and lands move only through
//!   compare-and-swap predicates (`version = $n`, `state = 'pending'`), so
//!   a lost swap is visible to the caller, as on SQLite.
//! - Land IDs come from an identity sequence, so no ID is handed out
//!   twice, and the newest land holds the largest one.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use gix::ObjectId;
use postgres::types::ToSql;

/// PROBE BRANCH ONLY: a breadcrumb for the hang hunt. Prints one line per
/// call when SCION_PROBE is set; silent otherwise.
fn probe(site: &str, msg: impl std::fmt::Display) {
    if std::env::var_os("SCION_PROBE").is_some() {
        let t = std::thread::current();
        eprintln!("[probe] {site} tid={:?} name={:?} {msg}", t.id(), t.name());
    }
}
use postgres::{Client, Config, NoTls};

/// Shorthand for the owned query parameter.
type P = Param;
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};
use scion_proto::metrics::Metrics;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::auth::Caller;
use crate::backend::Marker;
use crate::error::{Error, Result};
use crate::history::NodeId;
use crate::import::SHA1;
use crate::land::{
    EXPIRE_PER_LAND, LAND_RETENTION_SECS, LandOutcome, LandRequest, LandState, decode_paths,
    encode_paths,
};
use crate::meta::{FsckDerivedRow, Meta, MetaTx, RefState, RepoChangeset, RepoId};
use crate::store::Blobs;

/// The schema version this build makes and reads.
pub(crate) const PG_SCHEMA_VERSION: u32 = 4;

/// How long a call may wait for a Postgres reader before the server
/// answers [`Error::Unavailable`]. Well below the 600 s call deadline:
/// a wait this long means the pool is stuck in long reads, and a retry
/// after the pool drains is kinder than blocking to the deadline.
pub(crate) const READER_WAIT: Duration = Duration::from_secs(30);

/// The bound on one statement of a read transaction, below the 600 s
/// call deadline (`scn`'s), so one stuck query cannot hold a reader to
/// the end of a stream.
const READ_STATEMENT_TIMEOUT: Duration = Duration::from_secs(300);

/// How long a read transaction may sit idle between statements, so an
/// abandoned walk (its call gone, its reply unread) gives the reader
/// back to the pool.
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The advisory lock of the one server process of a database.
const SERVER_LOCK: i64 = 0x5343_494F_4E53_5256;
/// The advisory lock of one migration run of a database.
const SCHEMA_LOCK: i64 = 0x5343_494F_4E53_4348;

/// Schema migrations in order. Migration `n`, from 1, moves the database
/// from version `n - 1` to version `n`. A released migration never changes.
const PG_MIGRATIONS: [&str; PG_SCHEMA_VERSION as usize] = [
    include_str!("../migrations/postgres/0001_init.sql"),
    include_str!("../migrations/postgres/0002_view_roots.sql"),
    include_str!("../migrations/postgres/0003_view_trees.sql"),
    include_str!("../migrations/postgres/0004_store_uuid.sql"),
];

/// The fence of a process that lost the one-server lock. The worker of
/// the exclusive client trips it when a setup that once held the lock no
/// longer does: another process may serve the database now, while this
/// one still answers reads from its reader pool and its local blobs.
/// A tripped fence fails every read and write of the backend, the health
/// service flips to `NOT_SERVING`, and the server process exits.
struct Fence {
    tripped: Arc<AtomicBool>,
}

impl Fence {
    fn new() -> Self {
        Self {
            tripped: Arc::new(AtomicBool::new(false)),
        }
    }

    fn tripped(&self) -> bool {
        self.tripped.load(Ordering::SeqCst)
    }

    /// Trips the fence once: the log line is the operator's answer to
    /// "why did every call start failing".
    fn trip(&self) {
        if !self.tripped.swap(true, Ordering::SeqCst) {
            tracing::error!(
                "this server process lost the database to another server \
                 process; it serves nothing from here on, and the health \
                 service answers NOT_SERVING until the process exits"
            );
        }
    }
}

/// Handle to one Postgres database. Cheap to clone.
#[derive(Clone)]
pub struct Pg {
    inner: Arc<PgInner>,
}

struct PgInner {
    /// The write client, for every commit and [`Pg::read_unflushed`].
    writer: Mutex<PgClient>,
    /// Set when this process lost the one-server lock: every read and
    /// write of this backend fails from then on, and the server that
    /// holds the backend exits (the [`Fence`]).
    fence: Arc<Fence>,
    /// The UUID of this database, written at the first open and carried
    /// by the marker file of the data directory.
    store_uuid: [u8; 16],
    /// Clients for [`Pg::read`], at most [`READERS`](crate::store::READERS)
    /// at once, like the readers of a SQLite store.
    readers: Readers,
    /// The blob folder: blob files stay on local disk in every backend.
    blobs: Blobs,
    /// Writers that wait for the write client: the derivation walk waits
    /// while they wait, so a steady stream of writers delays a cold read
    /// up to its deadline, never a land ([#142]).
    writers: Mutex<usize>,
    write_woken: Condvar,
    /// The walks in flight of this backend's derivations.
    derive: crate::derived::DeriveFlights,
    /// The import's test hooks and busy-retry count, as the SQLite store
    /// keeps its own.
    #[cfg(any(test, feature = "test-hooks"))]
    hooks: crate::store::ImportHooks,
}

/// Clients for [`Pg::read`], so at most [`READERS`](crate::store::READERS)
/// reads run at once. A caller takes one out and returns it when it is
/// done.
struct Readers {
    idle: Mutex<Vec<PgClient>>,
    returned: Condvar,
    /// The start time of every live read, so the gauges can say how old
    /// the oldest open read transaction is.
    open_since: Mutex<Vec<Instant>>,
    /// How many clients the pool holds, for the busy gauge.
    size: usize,
    /// The registry the pool records into. Set once, by the server,
    /// through [`Backend::set_metrics`](crate::backend::Backend::set_metrics);
    /// a second set is a no-op. [`None`] until then: import, fsck, gc,
    /// and tests without a registry record nothing.
    metrics: OnceLock<Arc<Metrics>>,
}

impl Readers {
    /// [`Readers::new`], with libpq `options` on every client. The
    /// schema is the search path each reader sets up on its own worker:
    /// without it a reader reads the database's default, which holds no
    /// tables of this store.
    fn new_with(
        size: usize,
        url: &str,
        ca: Option<&Path>,
        options: &str,
        schema: Option<&str>,
    ) -> Result<Self> {
        // The open is sequential, so every client sees the same schema.
        let idle = (0..size)
            .map(|_| {
                let client = connect_with(url, ca, options, schema)?;
                let on: String = client.query("SHOW synchronous_commit", &[])?[0].get(0);
                if on != "on" {
                    return Err(Error::FailedPrecondition(format!(
                        "the database runs synchronous_commit={on}; this \
                         backend needs synchronous_commit=on"
                    )));
                }
                Ok(client)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            idle: Mutex::new(idle),
            returned: Condvar::new(),
            open_since: Mutex::new(Vec::new()),
            size,
            metrics: OnceLock::new(),
        })
    }

    /// Takes a reader, or waits at most `wait` for one. A full pool means
    /// the readers are stuck in long reads: the caller hears
    /// [`Error::Unavailable`] and may retry, instead of waiting forever
    /// behind a transaction that never ends.
    fn take(&self, wait: Duration) -> Result<ReaderGuard<'_>> {
        let started = Instant::now();
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        probe(
            "pg.take",
            format!("enter idle={} wait={wait:?}", idle.len()),
        );
        let client = loop {
            if let Some(client) = idle.pop() {
                break client;
            }
            let left = wait.checked_sub(started.elapsed());
            let Some(left) = left else {
                probe("pg.take", "TIMEOUT -> Unavailable");
                return Err(Error::Unavailable(format!(
                    "no Postgres reader free after {wait:?}; the readers are stuck in long reads"
                )));
            };
            let (guard, _woke) = self
                .returned
                .wait_timeout(idle, left)
                .unwrap_or_else(PoisonError::into_inner);
            idle = guard;
            // A timed-out wait falls through to the top of the loop, so a
            // reader given back in this window is taken before refusing.
        };
        probe("pg.take", "got a reader");
        let started_at = Instant::now();
        self.open_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(started_at);
        self.report_busy();
        if let Some(metrics) = self.metrics() {
            metrics.pg_pool_wait(started.elapsed());
        }
        Ok(ReaderGuard {
            readers: self,
            client: Some(client),
            started: started_at,
        })
    }

    /// The start time of the oldest live read, for the tx-age gauge.
    fn oldest_open(&self) -> Option<Instant> {
        self.open_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .min()
    }

    /// A read gave its reader back: forget its start time.
    fn closed(&self, started: Instant) {
        let mut open = self
            .open_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(at) = open.iter().position(|known| *known == started) {
            open.swap_remove(at);
        }
    }

    /// How many readers are out of the pool, for the busy gauge.
    fn report_busy(&self) {
        let idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        let busy = self.size.saturating_sub(idle.len());
        drop(idle);
        if let Some(metrics) = self.metrics() {
            metrics.pg_readers_busy(u64::try_from(busy).unwrap_or(u64::MAX));
        }
    }

    /// The metrics to record into, if the server gave the backend its
    /// registry.
    fn metrics(&self) -> Option<Arc<Metrics>> {
        self.metrics.get().cloned()
    }

    /// Gives the pool the server's metrics registry. Set once: a second
    /// set is a no-op, and says so in the debug log.
    fn set_metrics(&self, metrics: Arc<Metrics>) {
        if self.metrics.set(metrics).is_err() {
            tracing::debug!("the metrics registry was already set");
        }
        self.report_busy();
    }
}

/// A reader that is out of the pool. Returns it when it drops.
struct ReaderGuard<'a> {
    readers: &'a Readers,
    client: Option<PgClient>,
    started: Instant,
}

impl ReaderGuard<'_> {
    fn client(&mut self) -> &mut PgClient {
        self.client.as_mut().expect("a taken reader has a client")
    }
}

impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.readers.closed(self.started);
            self.readers
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(client);
            // The gauge reads the pool after the client is back in it: a
            // report before the push would leave one reader counted busy
            // until the next take.
            self.readers.report_busy();
            self.readers.returned.notify_one();
        }
    }
}

/// A value that a query binds, owned, so it can cross to the worker
/// thread. The parameters of this schema are integers, text, and bytes.
#[derive(Debug, Clone)]
enum Param {
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
    OptBytes(Option<Vec<u8>>),
    /// A list of byte strings, for a `= ANY($n)` over `bytea[]`.
    ByteaArray(Vec<Vec<u8>>),
}

impl Param {
    /// An integer parameter.
    fn i64(v: i64) -> Param {
        Param::I64(v)
    }

    /// A text parameter.
    fn text(v: impl Into<String>) -> Param {
        Param::Text(v.into())
    }

    /// A byte parameter.
    fn bytes(v: &[u8]) -> Param {
        Param::Bytes(v.to_vec())
    }

    /// An optional byte parameter.
    fn opt_bytes(v: Option<Vec<u8>>) -> Param {
        Param::OptBytes(v)
    }

    /// A list of byte strings, as one `bytea[]` parameter.
    fn bytea_array(v: Vec<Vec<u8>>) -> Param {
        Param::ByteaArray(v)
    }
}

impl ToSql for Param {
    fn to_sql(
        &self,
        t: &postgres::types::Type,
        out: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>>
    {
        match self {
            Self::I64(v) => v.to_sql(t, out),
            Self::Text(v) => v.to_sql(t, out),
            Self::Bytes(v) => v.to_sql(t, out),
            Self::OptBytes(v) => v.to_sql(t, out),
            Self::ByteaArray(v) => v.to_sql(t, out),
        }
    }

    fn accepts(_: &postgres::types::Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        t: &postgres::types::Type,
        out: &mut postgres::types::private::BytesMut,
    ) -> std::result::Result<postgres::types::IsNull, Box<dyn std::error::Error + Send + Sync>>
    {
        match self {
            Self::I64(v) => v.to_sql(t, out),
            Self::Text(v) => v.to_sql(t, out),
            Self::Bytes(v) => v.to_sql(t, out),
            Self::OptBytes(v) => v.to_sql(t, out),
            Self::ByteaArray(v) => v.to_sql(t, out),
        }
    }
}

/// The reply of one statement.
enum Reply {
    Rows(Vec<postgres::Row>),
    Count(u64),
}

/// What a statement returns.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    /// All rows of a SELECT.
    Rows,
    /// The affected-row count of an INSERT, UPDATE or DELETE.
    Count,
    /// A batch of statements, as one transaction, such as a migration.
    Batch,
    /// No statement: the job's closure runs the client itself.
    Raw,
}

/// One statement to run on the worker thread.
struct Job {
    sql: String,
    params: Vec<Param>,
    kind: Kind,
    reply: std::sync::mpsc::Sender<Result<Reply>>,
    /// A setup closure for [`Kind::Raw`]: it runs the client on the
    /// worker thread, where the sync client belongs.
    raw: Option<RawFn>,
}

type RawFn = Box<dyn FnOnce(&mut Client) -> Result<()> + Send>;

/// A client of the database, driven by one worker thread. The sync
/// `postgres` client runs its own tokio runtime, which must not start
/// inside the server's, so a statement is sent to the worker and the
/// caller blocks for its reply. The client itself never crosses threads.
struct PgClient {
    jobs: std::sync::mpsc::Sender<Job>,
}

impl PgClient {
    /// Puts `client` on its own worker thread.
    fn spawn(cfg: ClientConfig) -> PgClient {
        let (jobs, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("postgres-client".into())
            .spawn(move || {
                let ClientConfig {
                    mut client,
                    conn,
                    fence,
                } = cfg;
                // The schema of the connection, named again after every
                // reconnect; the one-server lock, taken again after every
                // reconnect. A lock the database does not give back fails
                // every statement that follows: another server process may
                // hold it, and this one must not land beside it.
                let mut setup = Setup::new(conn.schema.as_deref(), conn.lock_key, &mut client);
                // BEGIN opens one; COMMIT and ROLLBACK end it. A
                // transaction a reconnect lost fails every statement until
                // its rollback: on the new connection, autocommit would
                // answer COMMIT with Ok.
                let mut tx = TxState::Idle;
                // Whether a setup of this worker ever held the lock: the
                // fence trips on a lock that was held and is gone, never
                // on a first setup that never got one. The flag lives for
                // the worker's life, so a wedge of dead transactions
                // cannot hide a takeover from the fence.
                let mut ever_held = setup.ok();
                for job in rx {
                    // A transaction the reconnect dropped fails every
                    // statement until its caller ends it. A new BEGIN may
                    // pass: the reconnect ended the old tx, so a BEGIN
                    // starts a fresh one on the fresh session, and nothing
                    // of the old tx ever runs outside a tx.
                    if tx.lost()
                        && !job.sql.starts_with("ROLLBACK")
                        && !job.sql.starts_with("BEGIN")
                    {
                        // The caller may be gone, which is fine: the reply
                        // fails, and the job's outcome is the caller's
                        // business. The worker goes on to the next job.
                        let _ = job.reply.send(Err(Error::Corrupt(TX_LOST.into())));
                        continue;
                    }
                    // A client the server closed, as after a restart of
                    // the database, connects again: the caller sees one
                    // error at most. A reconnect inside a transaction
                    // loses the transaction.
                    let lost_tx = tx.in_progress();
                    let mut reconnected = false;
                    if client.is_closed() {
                        match reconnect(&mut client, &conn) {
                            Ok(fresh) => {
                                setup = fresh;
                                if lost_tx {
                                    tx = TxState::Lost;
                                }
                            }
                            Err(error) => {
                                if job.reply.send(Err(error)).is_err() {
                                    break;
                                }
                                continue;
                            }
                        }
                        reconnected = true;
                        // The lock state of the fresh session is checked
                        // here, before any lost-tx reply: a lock lost
                        // across a wedge of dead transactions still fences
                        // the process.
                        if !setup.ok() {
                            if ever_held && matches!(setup.outcome, SetupOutcome::LockRefused) {
                                trip_fence(fence.as_ref());
                            }
                            // A database that cannot be reached is no
                            // proof of a takeover: the caller retries.
                            let _ = job.reply.send(Err(setup_error(&setup, ever_held)));
                            continue;
                        }
                    }
                    // The setup failed on a client that still lives: a
                    // network break left the old session of this very
                    // connection holding the lock, and the session dies
                    // some while after the connection did. One retry a
                    // job, so the recovery needs no process restart.
                    if !setup.ok() {
                        setup = Setup::new(conn.schema.as_deref(), conn.lock_key, &mut client);
                    }
                    if !setup.ok() {
                        if ever_held && matches!(setup.outcome, SetupOutcome::LockRefused) {
                            trip_fence(fence.as_ref());
                        }
                        let _ = job.reply.send(Err(setup_error(&setup, ever_held)));
                        continue;
                    }
                    ever_held = true;
                    // A transaction the reconnect dropped refuses this job
                    // too: it was asked while the old connection held the
                    // transaction, and the new one holds none (its COMMIT
                    // would answer Ok about a transaction that is gone).
                    // A new BEGIN passes: the reconnect ended the old tx,
                    // so the BEGIN opens a fresh one on the fresh session.
                    // As above: a gone caller is fine, the worker goes on.
                    if reconnected
                        && lost_tx
                        && !job.sql.starts_with("ROLLBACK")
                        && !job.sql.starts_with("BEGIN")
                    {
                        let _ = job.reply.send(Err(Error::Corrupt(TX_LOST.into())));
                        continue;
                    }
                    if job.kind == Kind::Raw {
                        let mut job = job;
                        let f = job.raw.take().expect("a raw job carries its closure");
                        let _ = job.reply.send(f(&mut client).map(|(): ()| Reply::Count(0)));
                        continue;
                    }
                    let out = run_with_retry(
                        &mut client,
                        &mut setup,
                        &conn,
                        &job,
                        &mut tx,
                        ever_held,
                        fence.as_ref(),
                    );
                    if setup.ok() {
                        ever_held = true;
                    }
                    tx = tx.after(&job.sql, out.is_ok());
                    if job.reply.send(out).is_err() {
                        break;
                    }
                }
            })
            .expect("spawn the postgres worker");
        Self { jobs }
    }

    /// Runs one closure with the client on the worker thread. The sync
    /// client must never be touched from a tokio thread, which this
    /// keeps true for the few setup statements a caller needs.
    fn as_raw(&self, f: impl FnOnce(&mut Client) -> Result<()> + Send + 'static) -> Result<()> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Job {
                sql: String::new(),
                params: Vec::new(),
                kind: Kind::Raw,
                reply,
                raw: Some(Box::new(f)),
            })
            .map_err(|_| Error::Corrupt("the database worker stopped".into()))?;
        match rx.recv() {
            Ok(Ok(Reply::Count(_))) => Ok(()),
            Ok(Ok(_)) => unreachable!("a raw job replies with a count"),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Corrupt("the database worker stopped".into())),
        }
    }

    /// Runs one statement and waits for its reply.
    fn ask(&self, sql: &str, params: Vec<Param>, kind: Kind) -> Result<Reply> {
        let (reply, rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Job {
                sql: sql.to_owned(),
                params,
                kind,
                reply,
                raw: None,
            })
            .map_err(|_| Error::Corrupt("the database worker stopped".into()))?;
        rx.recv()
            .map_err(|_| Error::Corrupt("the database worker stopped".into()))?
    }

    /// The rows of one query.
    fn query(&self, sql: &str, params: &[Param]) -> Result<Vec<postgres::Row>> {
        match self.ask(sql, params.to_vec(), Kind::Rows)? {
            Reply::Rows(rows) => Ok(rows),
            Reply::Count(_) => unreachable!("a query returns rows"),
        }
    }

    /// The first row of a query, or [`None`] when it has none.
    fn query_opt(&self, sql: &str, params: &[Param]) -> Result<Option<postgres::Row>> {
        Ok(self.query(sql, params)?.into_iter().next())
    }

    /// The affected-row count of one statement.
    fn execute(&self, sql: &str, params: &[Param]) -> Result<u64> {
        match self.ask(sql, params.to_vec(), Kind::Count)? {
            Reply::Count(n) => Ok(n),
            Reply::Rows(_) => unreachable!("an execute returns a count"),
        }
    }

    /// A batch of statements, as one transaction.
    fn batch(&self, sql: &str) -> Result<()> {
        match self.ask(sql, Vec::new(), Kind::Batch)? {
            Reply::Count(_) => Ok(()),
            Reply::Rows(_) => unreachable!("a batch returns a count"),
        }
    }
}

/// Drops the test schemas whose tests ended minutes ago: a test run
/// leaves them behind, because a server holds its schema while it runs.
/// One process at a time holds the cleanup lock, so the test binaries
/// that run in parallel clean nothing under each other's feet. A schema
/// whose owner still runs is named in `pg_stat_activity` under its
/// `application_name`, and registered with its start time, so the
/// schema of a test that just began is never dropped.
fn drop_stale_schemas(maker: &PgClient) {
    let got = maker.query_opt("SELECT pg_try_advisory_lock($1)", &[P::i64(CLEAN_LOCK)]);
    let Ok(got) = got else {
        // This connection cannot even ask: another open's cleanup runs, or
        // the database is in trouble. The next open tries again.
        return;
    };
    // The answer is a row that holds the verdict: a `false` row means
    // another cleanup runs, and this one stands down.
    if !got.is_some_and(|row| row.get::<_, bool>(0)) {
        return;
    }
    // The registry names every test schema with its start time, in
    // `public`, spelled in full: the maker's search path is the
    // database's default, and no test schema holds the table.
    if let Ok(rows) = maker.query(
        "SELECT r.schema FROM public.scion_test_schemas AS r
         JOIN pg_catalog.pg_namespace AS n ON n.nspname = r.schema
         WHERE r.started < extract(epoch from now())::bigint - 600
           AND r.schema NOT IN (
               SELECT application_name FROM pg_catalog.pg_stat_activity
               WHERE application_name LIKE 'scion_test_%')",
        &[],
    ) {
        for row in rows {
            let name: String = row.get(0);
            // Only the names this backend makes: no other schema drops,
            // whatever a row of the registry names.
            if !name.starts_with("scion_test_") {
                continue;
            }
            if let Err(why) = maker.batch(&format!(
                "DROP SCHEMA IF EXISTS {name} CASCADE;
                 DELETE FROM public.scion_test_schemas WHERE schema = '{name}'"
            )) {
                tracing::warn!(schema = %name, error = %why, "cannot drop a stale test schema");
            }
        }
    }
    // The lock frees when this connection closes: a failed unlock needs
    // nothing but the drop of the maker.
    if let Err(why) = maker.query_opt("SELECT pg_advisory_unlock($1)", &[P::i64(CLEAN_LOCK)]) {
        tracing::warn!(error = %why, "cannot unlock the test-schema cleanup");
    }
}

/// The advisory lock of the test-schema cleanup.
const CLEAN_LOCK: i64 = 0x5343_494F_4E53_5445;

/// The libpq options of every connection of this backend. The commit of
/// a transaction reaches the drive before it returns, and no statement
/// runs longer than the graph-walk bound. The search path is not set
/// here: it is a statement on the live connection, which the server
/// cannot lose between connections.
fn base_options() -> String {
    // A lock a writer waits for is a busy database: 5 s, like the busy
    // timeout of the SQLite store. A statement itself may walk the graph
    // for minutes, so only the lock is under the short clock.
    format!(
        "-csynchronous_commit=on -cstatement_timeout=600000 -clock_timeout={LOCK_TIMEOUT_FOR_WRITERS}"
    )
}

/// The lock clock every writer runs under: a lock a writer waits for is a
/// busy database, answered Busy so the client retries. The one exception
/// is the collector's key, whose wait has its own bound (the commit's
/// `LOCK_TIMEOUT_FOR_KEY`); everything after it is back on this clock.
/// The value is bare: the base options take it as `-clock_timeout=<v>`,
/// and the SQL `SET LOCAL` in `Pg::commit` quotes it.
const LOCK_TIMEOUT_FOR_WRITERS: &str = "5s";

/// How long the collector's key wait may run, in `Pg::commit` below: the
/// queue drains as fast as the writers commit.
const LOCK_TIMEOUT_FOR_KEY: &str = "60s";

impl Pg {
    /// Opens the database at `url`, and the blob folder under `data`, as
    /// its server: the one `Pg` that may land. `readers` clients serve
    /// reads beside it. With `ca`, a certificate the server must present
    /// is checked against this CA (PEM); a CA is required for a host that
    /// is not on this machine.
    ///
    /// # Errors
    ///
    /// A URL that does not parse, a database that does not connect, a
    /// schema of a newer build, or a migration that fails.
    pub(crate) fn open(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        Self::open_in(url, ca, readers, data, None, false, marker)
    }

    /// [`Pg::open`], as the one server process of the database: a second
    /// `serve` or `derive` on the same URL is refused. Import runs beside a
    /// server, so it opens without the lock.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`], and another process holds the one-server lock.
    pub(crate) fn open_exclusive(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        Self::open_in(url, ca, readers, data, None, true, marker)
    }

    /// [`Pg::open`], in `schema` instead of the database's default one, for
    /// tests: each data directory gets its own corner of one database. The
    /// schema is created if missing, and the migrations run in it.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`].
    pub(crate) fn open_test(
        url: &str,
        schema: &str,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        prepare_test_schema(url, schema)?;
        // The test store is the one server of its schema: it takes the
        // per-schema one-server lock, and its loss fences the process,
        // exactly as a serve does on the database key.
        Self::open_in(url, None, readers, data, Some(schema), true, marker)
    }

    /// [`Pg::open_test`], as a process that does not land: it makes the
    /// schema the same way and takes no one-server lock, and its loss
    /// fences nothing, like a `gc`, `fsck` or `import` beside a live
    /// server.
    pub(crate) fn open_test_shared(
        url: &str,
        schema: &str,
        readers: usize,
        data: &Path,
        marker: Marker,
    ) -> Result<Self> {
        prepare_test_schema(url, schema)?;
        Self::open_in(url, None, readers, data, Some(schema), false, marker)
    }

    /// [`Pg::open`], in the search path of `schema` when it is set.
    #[allow(clippy::unused_self)]
    fn open_in(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
        schema: Option<&str>,
        exclusive: bool,
        marker: Marker,
    ) -> Result<Self> {
        let options = base_options();
        let fence = Arc::new(Fence::new());
        // The one-server lock of the database, or of one test schema: a
        // key per schema, so parallel tests do not block each other.
        let lock_key = if exclusive {
            Some(schema.map_or(SERVER_LOCK, schema_lock_key))
        } else {
            None
        };
        let mut writer = if exclusive {
            connect_exclusive(
                url,
                ca,
                &options,
                schema,
                lock_key.expect("an exclusive open names a lock"),
                Arc::clone(&fence),
            )?
        } else {
            connect_with(url, ca, &options, schema)?
        };
        migrate(&mut writer)?;
        // The durability of a commit is a promise of this backend: check
        // that the server kept its side of it.
        let on: String = writer.query("SHOW synchronous_commit", &[])?[0].get(0);
        if on != "on" {
            return Err(Error::FailedPrecondition(format!(
                "the database runs synchronous_commit={on}; this backend \
                 promises that a returned commit is durable, so it needs \
                 synchronous_commit=on"
            )));
        }
        // The identity of this database, minted at the first open. The
        // marker file of the data directory carries the same UUID, and an
        // open that finds the two different refuses: a database restored
        // on a directory of another store would name blobs of another disk.
        let first = writer.query("SELECT uuid FROM store_uuid WHERE one = 1", &[])?;
        let store_uuid = if let Some(row) = first.first() {
            // The database already names its store, and the comparison
            // runs before anything is written: no marker, or the marker
            // of another store, both refuse here. A database restored on
            // the directory of another store would serve metadata whose
            // blobs are not on this disk.
            let uuid = uuid_of(row.get::<_, Vec<u8>>("uuid"));
            match marker.uuid {
                None => {
                    return Err(Error::FailedPrecondition(format!(
                        "this database names the store {}, but the data \
                         directory carries no marker of it: give the \
                         directory the database was served with, or a \
                         directory it has never served",
                        hex16(&uuid)
                    )));
                }
                Some(held) if held != uuid => {
                    return Err(Error::FailedPrecondition(format!(
                        "this data directory belongs to the store whose \
                         UUID is {}, but the database names the store {}: \
                         one was restored over the other; give the \
                         directory of the database's own store, or a \
                         directory the database has never served",
                        hex16(&held),
                        hex16(&uuid)
                    )));
                }
                Some(_) => {}
            }
            uuid
        } else {
            // A marker that names a store is the identity to adopt: a
            // dump taken before the UUID existed restores next to its
            // own directory, and they agree. A fresh database mints.
            let fresh = marker.uuid.unwrap_or_else(uuid_new);
            writer.query(
                "INSERT INTO store_uuid (one, uuid) VALUES (1, $1) \
                 ON CONFLICT (one) DO NOTHING",
                &[P::bytes(&fresh)],
            )?;
            // A concurrent first open may have minted its own: read the
            // row again, and whatever it holds is the identity.
            uuid_of(
                writer.query("SELECT uuid FROM store_uuid WHERE one = 1", &[])?[0]
                    .get::<_, Vec<u8>>("uuid"),
            )
        };
        let blobs = Blobs::open(data)?;
        blobs.sweep_tmp(false)?;
        let inner = Arc::new(PgInner {
            writer: Mutex::new(writer),
            fence,
            store_uuid,
            readers: Readers::new_with(readers, url, ca, &options, schema)?,
            blobs,
            writers: Mutex::new(0),
            write_woken: Condvar::new(),
            derive: crate::derived::DeriveFlights::new(crate::store::DERIVE_WALKS_CAP),
            #[cfg(any(test, feature = "test-hooks"))]
            hooks: crate::store::ImportHooks::default(),
        });
        // The probe of a server that lost its lock: once a minute the
        // session checks itself, through the job queue like every
        // statement, so the lock and the fence are checked on the way.
        // It rides a weak handle: the probe dies with the backend, and
        // holds nothing open.
        if exclusive {
            let probe = Arc::downgrade(&inner);
            std::thread::Builder::new()
                .name("pg-probe".into())
                .spawn(move || {
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(60));
                        let Some(inner) = probe.upgrade() else {
                            return;
                        };
                        // A dead connection or a fenced store both fail
                        // here and are seen on the next real call; the
                        // probe exists to trip the fence on time.
                        let _ = inner.writer.lock().unwrap().query_opt("SELECT 1", &[]);
                    }
                })
                .map_err(crate::error::Error::Io)?;
        }
        Ok(Self { inner })
    }

    /// The UUID of this database, which the marker file of the data
    /// directory also carries.
    pub(crate) fn store_uuid(&self) -> [u8; 16] {
        self.inner.store_uuid
    }

    /// Fails when the fence tripped: the process lost the database to
    /// another one, and serves nothing from here on.
    fn check_fenced(&self) -> Result<()> {
        if self.inner.fence.tripped() {
            return Err(Error::FailedPrecondition(
                "this server process lost the database to another server \
                 process; it serves nothing from here on"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Whether this process lost the database to another one: every read
    pub(crate) fn fenced(&self) -> bool {
        self.inner.fence.tripped()
    }

    /// The fence flag, for the health service of a serving process.
    pub(crate) fn fence_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.inner.fence.tripped)
    }

    /// The blob folder.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        &self.inner.blobs
    }

    /// Gives the pool the server's metrics registry. Called once by the
    /// server before it serves; import, fsck, and gc pass none, and the
    /// pool then records nothing.
    pub(crate) fn set_metrics(&self, metrics: Arc<Metrics>) {
        self.inner.readers.set_metrics(metrics);
    }

    /// The age of the oldest open read transaction, for the
    /// `scion_pg_tx_age_seconds` gauge. Zero when no read is open.
    #[must_use]
    pub fn tx_age(&self) -> Duration {
        self.inner
            .readers
            .oldest_open()
            .map(|started| started.elapsed())
            .unwrap_or_default()
    }

    /// A read on the write client, so it sees the caller's own commits.
    pub fn read_unflushed<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.check_fenced()?;
        let (mut writer, _waiting) = self.lock_writer();
        let mut pg = PgTx {
            c: &mut writer,
            ended: true,
        };
        f(&mut pg)
    }

    /// A strong read on one client of the pool. The read runs in one
    /// transaction, so all its statements see one snapshot; on this backend
    /// every commit is durable when it returns, so what `f` saw is durable.
    /// The transaction is bounded: a statement past
    /// [`READ_STATEMENT_TIMEOUT`] or an idle past [`READ_IDLE_TIMEOUT`]
    /// fails, so an abandoned read cannot hold a reader forever.
    ///
    /// # Errors
    ///
    /// What `f` returns, a database error from begin, or
    /// [`Error::Unavailable`] when no reader was free in time.
    pub fn read<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.read_bounded(f, READ_STATEMENT_TIMEOUT, READ_IDLE_TIMEOUT, READER_WAIT)
    }

    /// [`Pg::read`], with the bounds in the open: the per-statement
    /// timeout, the idle-in-transaction timeout, and how long a caller
    /// may wait for a reader. The tests use short bounds; the server
    /// uses [`Pg::read`].
    pub(crate) fn read_bounded<T>(
        &self,
        f: impl FnOnce(&mut PgTx<'_>) -> Result<T>,
        statement: Duration,
        idle: Duration,
        wait: Duration,
    ) -> Result<T> {
        self.check_fenced()?;
        let mut reader = self.inner.readers.take(wait)?;
        let out = {
            let mut tx = PgTx::begin_read(reader.client(), statement, idle)?;
            let out = f(&mut tx);
            // A read transaction: nothing to keep. The rollback's own error
            // must not hide the error of `f`; the drop rolls back anyway.
            let _ = tx.rollback();
            out
        };
        drop(reader);
        out
    }

    /// Runs `f` in one write transaction. It commits when `f` returns `Ok`
    /// and rolls back otherwise. A commit is durable when it returns, so
    /// this is also `write`.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from begin or commit.
    pub fn commit<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.check_fenced()?;
        let (mut writer, _waiting) = self.lock_writer();
        // A write transaction runs at READ COMMITTED, the Postgres
        // default: every change of the caller checks the newest state of
        // the row it touches (the CAS predicates).
        let mut tx = PgTx::begin(&mut writer)?;
        // Every writer and the collector's batches of one schema order on
        // one advisory key, held to the end of the transaction: what a
        // SQLite write transaction gets from the data directory's one
        // write connection (store.rs `commit`,
        // `TransactionBehavior::Immediate`).
        //
        // SHORTCUT: one key per schema orders every writer of the schema
        // against one collector, which is right for one server per schema
        // (#178): a test database hosts one store per schema, so tests of
        // different stores do not serialize against each other. Many
        // servers (#47) need a finer design: per-repo keys, or the
        // collector's batches reading a snapshot instead of holding a
        // lock. One key, not one per blob: a transaction that puts
        // thousands of blobs would hold thousands of entries in Postgres's
        // shared lock table and run it out (E53200,
        // `max_locks_per_transaction`). A changeset that names a blob the
        // collector is about to remove
        // commits first, and the collector's re-read pins the blob; a
        // batch of the collector waits for every writer that has begun.
        // The wait ignores the writer's 5 s lock clock: the queue drains
        // as fast as the writers commit, and a writer that gave up would
        // turn healthy contention into Busy for every parallel caller.
        // The 60 s covers only this key: right after the statement, the
        // transaction is back on the writer's clock, so the row locks it
        // meets later still answer Busy at 5 s (SET LOCAL lasts to the
        // end of the transaction).
        tx.c.query(
            &format!("SET LOCAL lock_timeout = '{LOCK_TIMEOUT_FOR_KEY}'"),
            &[],
        )?;
        tx.c.query_opt(
            "SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc'))",
            &[],
        )?;
        tx.c.query(
            &format!("SET LOCAL lock_timeout = '{LOCK_TIMEOUT_FOR_WRITERS}'"),
            &[],
        )?;
        match f(&mut tx) {
            Ok(value) => {
                tx.commit()?;
                Ok(value)
            }
            Err(error) => {
                // The rollback's own error must not hide the error of `f`;
                // the drop rolls back anyway.
                let _ = tx.rollback();
                Err(error)
            }
        }
    }

    /// [`Pg::commit`]: on this backend, durable on return.
    ///
    /// # Errors
    ///
    /// As [`Pg::commit`].
    pub fn write<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.commit(f)
    }

    /// Takes the write client, counted as a writer that waits: the
    /// derivation walk waits while writers wait, so a steady stream of
    /// writers delays a cold read up to its deadline, never a land
    /// ([#142]). The count falls when the client is back, so a walk step
    /// that ends hands the client to a writer, not to its own next step.
    fn lock_writer(&self) -> (MutexGuard<'_, PgClient>, PgWriteWaiter<'_>) {
        *self
            .inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner) += 1;
        probe("pg.lock_writer", "waiting for the writer mutex");
        let taken = (
            self.inner
                .writer
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            PgWriteWaiter(&self.inner.writers, &self.inner.write_woken),
        );
        probe("pg.lock_writer", "writer mutex held");
        taken
    }

    /// The walks in flight of this backend's derivations.
    pub(crate) fn derive_flights(&self) -> &crate::derived::DeriveFlights {
        &self.inner.derive
    }

    /// The import's test hooks and busy-retry count.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn hooks(&self) -> &crate::store::ImportHooks {
        &self.inner.hooks
    }

    /// How many writers wait for the write client right now.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn writers_waiting(&self) -> usize {
        *self
            .inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits while writers wait for the write client, until the deadline.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn wait_for_writers(
        &self,
        deadline: Option<&crate::deadline::Deadline>,
    ) -> Result<()> {
        let mut waiting = self
            .inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        probe("pg.wait_for_writers", format!("enter waiting={}", *waiting));
        while *waiting > 0 {
            let Some(deadline) = deadline else {
                waiting = self
                    .inner
                    .write_woken
                    .wait(waiting)
                    .unwrap_or_else(PoisonError::into_inner);
                probe("pg.wait_for_writers", format!("woken waiting={}", *waiting));
                continue;
            };
            let budget = deadline
                .at()
                .saturating_duration_since(std::time::Instant::now());
            let (guard, timed_out) = self
                .inner
                .write_woken
                .wait_timeout_while(waiting, budget, |waiting| *waiting > 0)
                .unwrap_or_else(PoisonError::into_inner);
            waiting = guard;
            if timed_out.timed_out() {
                return Err(Error::DeadlineExceeded(
                    "the call ran past its deadline".to_owned(),
                ));
            }
        }
        probe("pg.wait_for_writers", "exit");
        Ok(())
    }
}

/// The count of a taken write client, while it is taken: it falls, and
/// wakes the waiters, when the client is back.
struct PgWriteWaiter<'a>(&'a Mutex<usize>, &'a Condvar);

impl Drop for PgWriteWaiter<'_> {
    fn drop(&mut self) {
        let mut waiting = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *waiting -= 1;
        self.1.notify_all();
    }
}

/// One client of the database, inside one transaction. The queries are its
/// [`MetaTx`] methods. The transaction is begun and ended with statements
/// of the client, so a `PgTx` is a plain client handle: it stays covariant
/// in its lifetime, which the backend enum's transaction needs.
pub struct PgTx<'a> {
    c: &'a mut PgClient,
    /// The transaction was ended by `commit` or `rollback`; a drop then
    /// leaves the client alone.
    ended: bool,
}

impl<'a> PgTx<'a> {
    /// Begins one transaction on the client.
    fn begin(c: &'a mut PgClient) -> Result<PgTx<'a>> {
        c.execute("BEGIN", &[])?;
        Ok(Self { c, ended: false })
    }

    /// Begins one read-only transaction at repeatable read: every
    /// statement of the caller's read sees one snapshot, not the newest
    /// state of each statement. The transaction is bounded server side:
    /// one statement may take `statement`, and the transaction may sit
    /// idle between statements at most `idle` (the timeouts are
    /// `SET LOCAL`, so they end with the transaction, and a stream that
    /// stopped reading cannot hold the reader forever).
    //
    // SHORTCUT: every read runs at repeatable read, one snapshot per
    // read, even though reads that touch only immutable rows would be
    // safe at read committed with a shorter snapshot hold. Nobody has
    // measured the gain, and the split needs an audit of every walk that
    // re-reads a ref. Measure first, then split.
    fn begin_read(c: &'a mut PgClient, statement: Duration, idle: Duration) -> Result<PgTx<'a>> {
        c.execute(
            &format!(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY; \
                 SET LOCAL statement_timeout = {}; \
                 SET LOCAL idle_in_transaction_session_timeout = {};",
                statement.as_millis(),
                idle.as_millis()
            ),
            &[],
        )?;
        Ok(Self { c, ended: false })
    }

    /// One transaction over the same client, for a caller that wraps this
    /// one. It ends nothing: `commit` and `rollback` stay with the original.
    pub(crate) fn reborrow(&mut self) -> PgTx<'_> {
        PgTx {
            c: self.c,
            ended: true,
        }
    }

    /// Reads back the land another writer recorded for this request key
    /// first: its land wins, and its input must match this one's.
    fn read_back_land(&mut self, repo: RepoId, caller: &Caller, req: &LandRequest) -> Result<i64> {
        let (land_id, input, target) = self
            .c
            .query_opt(
                "SELECT land_id, input_cs, target_ref FROM lands
                 WHERE repo_id = $1 AND caller = $2 AND request_key = $3",
                &[
                    P::i64(repo.0),
                    P::text(&caller.0),
                    P::text(&req.request_key),
                ],
            )?
            .map(|row| {
                (
                    row.get::<_, i64>(0),
                    row.get::<_, Vec<u8>>(1),
                    row.get::<_, String>(2),
                )
            })
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "land {:?} of {:?} was recorded by another process and \
                     then vanished",
                    req.request_key, caller.0
                ))
            })?;
        if input != req.changeset.as_bytes() || target != req.target_ref {
            return Err(Error::InvalidArgument(format!(
                "request key {:?} was already used for a different land",
                req.request_key
            )));
        }
        Ok(land_id)
    }

    /// Commits the transaction. The client runs again afterwards.
    fn commit(&mut self) -> Result<()> {
        self.ended = true;
        self.c.execute("COMMIT", &[])?;
        Ok(())
    }

    /// Rolls the transaction back. The client runs again afterwards.
    fn rollback(&mut self) -> Result<()> {
        if self.ended {
            return Ok(());
        }
        self.ended = true;
        self.c.execute("ROLLBACK", &[])?;
        Ok(())
    }
}

impl Drop for PgTx<'_> {
    fn drop(&mut self) {
        // A transaction that its caller did not end rolls back, as a
        // dropped SQLite transaction does.
        if !self.ended {
            let _ = self.c.execute("ROLLBACK", &[]);
        }
    }
}

/// Connects one client of `url`. TLS follows the rule of the gRPC client: a
/// host on this machine may go without TLS; a host off it must present a
/// certificate that `ca` signed.
/// The session setup a worker applies to its client: the schema it runs
/// in, and, for the one server process of a database, the advisory lock
/// that refuses a second one.
enum SetupOutcome {
    /// The schema was set, and the lock, if one was asked for, was given.
    Applied,
    /// The database answered and refused the one-server lock: another
    /// session holds it.
    LockRefused,
    /// The database could not be reached, or refused the schema
    /// statements: nothing is known about the lock.
    Unreachable,
}

struct Setup {
    schema: Option<String>,
    /// The advisory lock this session takes, if any: the one-server lock
    /// of a database, or of one test schema. None for a client that
    /// takes no lock (a reader, an import).
    lock_key: Option<i64>,
    outcome: SetupOutcome,
}

impl Setup {
    /// Applies the setup to a fresh client. A failed setup leaves the
    /// statements failed until a reconnect applies it again.
    fn new(schema: Option<&str>, lock_key: Option<i64>, client: &mut Client) -> Self {
        let mut setup = Self {
            schema: schema.map(str::to_owned),
            lock_key,
            outcome: SetupOutcome::Unreachable,
        };
        setup.outcome = setup.apply(client);
        setup
    }

    /// Whether the setup holds: it ran to the end, and a one-server
    /// lock, if one was asked for, is held.
    fn ok(&self) -> bool {
        matches!(self.outcome, SetupOutcome::Applied)
    }
}

impl Setup {
    fn apply(&mut self, client: &mut Client) -> SetupOutcome {
        if let Some(schema) = &self.schema {
            // Every statement that follows resolves its unqualified names
            // inside the schema, and the cleanup of stale test schemas
            // sees the connection in `pg_stat_activity` under its name.
            if client
                .batch_execute(&format!(
                    "SET search_path TO {schema}; SET application_name TO '{schema}'"
                ))
                .is_err()
            {
                return SetupOutcome::Unreachable;
            }
        }
        if let Some(key) = self.lock_key
            && !server_lock_raw(client, key)
        {
            // The database answered: another session holds the lock.
            return SetupOutcome::LockRefused;
        }
        SetupOutcome::Applied
    }
}

/// A fresh store UUID: 16 bytes of system randomness.
fn uuid_new() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("system randomness");
    bytes
}

/// The 16 bytes of a UUID row, checked: a row of another length is a
/// database that is not this backend's.
fn uuid_of(raw: Vec<u8>) -> [u8; 16] {
    let len = raw.len();
    raw.try_into()
        .unwrap_or_else(|_| panic!("a store UUID row holds 16 bytes, got {len}"))
}

/// The statement's parameters as the trait objects `postgres` binds.
fn as_refs(params: &[Param]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

/// Trips the process fence of the exclusive client, when it has one:
/// a lock that once was held is lost, so another process may serve the
/// database now.
fn trip_fence(fence: Option<&Arc<Fence>>) {
    if let Some(fence) = fence {
        fence.trip();
    }
}

/// Runs one statement, and once more on a fresh session when the
/// connection died under it: the first statement after a loss goes
/// through the reconnect and the lock check, and answers what the fresh
/// session holds, not the death of the old one. Only a job outside a
/// transaction retries: a transaction was dropped with the connection,
/// and its caller must see that.
fn run_with_retry(
    client: &mut Client,
    setup: &mut Setup,
    conn: &Conn,
    job: &Job,
    tx: &mut TxState,
    ever_held: bool,
    fence: Option<&Arc<Fence>>,
) -> Result<Reply> {
    let out = run_statement(client, job);
    if out.is_ok() || !client.is_closed() {
        return out;
    }
    let Ok(fresh) = reconnect(client, conn) else {
        // The connection stays dead; the original error stands, and the
        // next job reconnects.
        return out;
    };
    *setup = fresh;
    if !setup.ok() {
        if ever_held && matches!(setup.outcome, SetupOutcome::LockRefused) {
            trip_fence(fence);
        }
        return Err(setup_error(setup, ever_held));
    }
    // The fresh session holds no transaction: one that was in progress
    // was dropped with the old session, and nothing of it runs here.
    if tx.in_progress() {
        *tx = TxState::Lost;
        return Err(Error::Corrupt(TX_LOST.into()));
    }
    run_statement(client, job)
}

/// Runs one statement of a job on the client.
fn run_statement(client: &mut Client, job: &Job) -> Result<Reply> {
    let refs = as_refs(&job.params);
    match job.kind {
        Kind::Batch => client.batch_execute(&job.sql).map(|(): ()| Reply::Count(0)),
        Kind::Count => client.execute(&job.sql, &refs).map(Reply::Count),
        Kind::Rows => client.query(&job.sql, &refs).map(Reply::Rows),
        Kind::Raw => unreachable!("a raw job ran in its own branch"),
    }
    .map_err(map_pg_error)
}

/// The reply of a failed setup: a takeover names the loss; an
/// unreachable database answers what a client retries against.
fn setup_error(setup: &Setup, ever_held: bool) -> Error {
    if matches!(setup.outcome, SetupOutcome::Unreachable) {
        return Error::Busy;
    }
    Error::FailedPrecondition(lost_message(ever_held).into())
}

/// The reply of a statement a reconnect robbed of its transaction.
const TX_LOST: &str = "the transaction was lost with the connection; \
     roll it back and run it again";

/// The answer of a setup that does not hold: a process that once held
/// the lock lost it, and a process that never got one may not land
/// beside the holder.
fn lost_message(setup_held: bool) -> &'static str {
    if setup_held {
        "this server process lost the database to another \
         server process; it serves nothing from here on"
    } else {
        "another server holds this database"
    }
}

/// Reconnects a dead client, applies the setup again, and hands the new
/// setup back: the caller folds it into `setup_held` and the lost
/// transaction state.
fn reconnect(client: &mut Client, conn: &Conn) -> Result<Setup> {
    let fresh = connect_on(&conn.url, conn.ca.as_deref(), &conn.options)?;
    *client = fresh;
    Ok(Setup::new(conn.schema.as_deref(), conn.lock_key, client))
}

/// Whether the worker's client is inside the transaction a `BEGIN`
/// opened. The worker sees the statements, so it knows when a reconnect
/// has silently dropped one.
#[derive(Clone, Copy)]
enum TxState {
    Idle,
    InTx,
    Lost,
}

impl TxState {
    fn in_progress(self) -> bool {
        matches!(self, TxState::InTx | TxState::Lost)
    }

    fn lost(self) -> bool {
        matches!(self, TxState::Lost)
    }

    fn after(self, sql: &str, ok: bool) -> Self {
        // Only a BEGIN that ran opens a transaction: one the connection
        // refused leaves the worker outside it, and the caller's Drop
        // rolls no transaction back that never opened.
        if sql.starts_with("BEGIN") && ok && !self.lost() {
            return TxState::InTx;
        }
        if sql.starts_with("ROLLBACK") || (sql.starts_with("COMMIT") && !self.lost()) {
            return TxState::Idle;
        }
        if self.lost() && !sql.starts_with("COMMIT") {
            return self;
        }
        self
    }
}

/// How a worker reconnects: where to connect, and what setup to apply
/// again. One per [`ClientConfig`].
struct Conn {
    url: String,
    ca: Option<PathBuf>,
    options: String,
    /// The schema a reconnect narrows itself to again.
    schema: Option<String>,
    /// The advisory lock this session re-takes after every reconnect,
    /// if any.
    lock_key: Option<i64>,
}

/// What a worker needs to run, and to connect again when its client dies.
struct ClientConfig {
    client: Client,
    conn: Conn,
    /// The process fence of the exclusive client: tripped when a setup
    /// that once held the lock no longer does.
    fence: Option<Arc<Fence>>,
}

/// The advisory-lock key of one test schema, so parallel tests of
/// different schemas do not block each other on the one lock.
pub(crate) fn schema_lock_key(schema: &str) -> i64 {
    let hash = blake3::hash(schema.as_bytes());
    SERVER_LOCK.wrapping_add(i64::from_be_bytes(
        hash.as_bytes()[..8].try_into().expect("8 of 32 bytes"),
    ))
}

/// 16 bytes as 32 lowercase hex digits.
fn hex16(bytes: &[u8; 16]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(32), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// The advisory lock of the one server process of a database, or of one
/// test schema: a second `serve` or `derive` on the same target is
/// refused. Runs on the worker thread, like every statement.
fn server_lock_raw(client: &mut Client, key: i64) -> bool {
    client
        .query_opt("SELECT pg_try_advisory_lock($1)", &[&key])
        .is_ok_and(|row| row.is_some_and(|row| row.get::<_, bool>(0)))
}

fn connect(url: &str, ca: Option<&Path>) -> Result<PgClient> {
    connect_with(url, ca, "", None)
}

/// Connects one client of `url`, with libpq `options` added. The connect
/// runs on a plain thread: `postgres` drives its own runtime, which must
/// not start inside the server's tokio one.
fn connect_with(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
) -> Result<PgClient> {
    spawn_client(url, ca, options, schema, None, None)
}

/// [`connect_with`], as the one server process of the database: the
/// advisory lock it takes survives as long as the client, and a
/// reconnecting client takes it again. A lost lock trips the fence
/// ([`Fence`]).
fn connect_exclusive(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
    lock_key: i64,
    fence: Arc<Fence>,
) -> Result<PgClient> {
    // Whether the lock was given is decided at the spawn: one probe job
    // brings a refusal here, to the open, so an operator whose second
    // server starts against a taken database sees it now, and not in the
    // errors of the first call a client makes.
    let client = spawn_client(url, ca, options, schema, Some(lock_key), Some(fence))?;
    client.query_opt("SELECT 1", &[])?;
    Ok(client)
}

/// Puts one client on its worker thread. A `lock_key` is re-taken after
/// every reconnect, and its loss trips the fence.
fn spawn_client(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
    lock_key: Option<i64>,
    fence: Option<Arc<Fence>>,
) -> Result<PgClient> {
    let client =
        std::thread::scope(|scope| scope.spawn(|| connect_on(url, ca, options)).join().unwrap())?;
    // The lock is taken on the worker thread, like every statement: the
    // caller may be a tokio one, which must never block on this client.
    Ok(PgClient::spawn(ClientConfig {
        client,
        conn: Conn {
            url: url.to_owned(),
            ca: ca.map(Path::to_path_buf),
            options: options.to_owned(),
            schema: schema.map(str::to_owned),
            lock_key,
        },
        fence,
    }))
}

/// The libpq `options` a connection starts with: the ones the URL carries
/// first, then the backend's own, which win on a clash. The backend checks
/// `synchronous_commit` itself after the connect, so an operator's
/// `search_path` in the URL survives, and its clashes do not matter.
fn merged_options(from_url: Option<&str>, own: &str) -> String {
    match from_url {
        Some(from_url) if !from_url.is_empty() => format!("{from_url} {own}"),
        _ => own.to_owned(),
    }
}

/// [`connect_with`]'s work: parse, then connect.
fn connect_on(url: &str, ca: Option<&Path>, options: &str) -> Result<Client> {
    let mut config: Config = url
        .parse()
        .map_err(|e| Error::InvalidArgument(format!("cannot parse the database URL: {e}")))?;
    let options = merged_options(config.get_options(), options);
    config.options(&options);
    // A dead connection is noticed in seconds, not held open forever.
    config.connect_timeout(std::time::Duration::from_secs(10));
    config.keepalives(true);
    config.keepalives_idle(std::time::Duration::from_secs(30));
    config.tcp_user_timeout(std::time::Duration::from_secs(30));
    // The host is on this machine only when every host of the URL is, and
    // no `hostaddr` names one by address: an address has no name to check
    // a certificate against.
    let local = !config.get_hosts().is_empty()
        && config.get_hostaddrs().is_empty()
        && config.get_hosts().iter().all(|host| match host {
            postgres::config::Host::Tcp(host) => {
                host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            }
            // A Unix socket is as local as it gets.
            postgres::config::Host::Unix(_) => true,
        });
    let wants_tls = config.get_ssl_mode() == postgres::config::SslMode::Require;
    // Off this machine the traffic is encrypted, whatever a man in the
    // middle answers to the start: `prefer` falls back to plaintext when
    // the server's answer says no, so it is refused here.
    if !local && !wants_tls {
        return Err(Error::InvalidArgument(
            "the database host is off this machine, so its traffic must be \
             encrypted: name sslmode=require or stronger in the URL"
                .to_owned(),
        ));
    }
    // A connect that fails is a database the caller may wait for: the
    // same states a statement answers, mapped the same way, so a caller
    // that retries a busy database retries a starting one too.
    let client = match (local, ca) {
        // The connector upgrades every connection, and checks the
        // certificate the server presents against `ca`.
        (_, Some(ca)) => config.connect(make_tls(ca)?).map_err(map_pg_error)?,
        (true, None) => config.connect(NoTls).map_err(map_pg_error)?,
        (false, None) => {
            return Err(Error::InvalidArgument(
                "the database host is off this machine: give --postgres-ca, \
                 so that its certificate can be checked"
                    .to_owned(),
            ));
        }
    };
    Ok(client)
}

/// A database error that a caller may retry maps to [`Error::Busy`]: the
/// importer retries a batch on it, and the gRPC status becomes
/// UNAVAILABLE. The rest keep their database error.
fn map_pg_error(e: postgres::Error) -> Error {
    // A connection the database dropped is as retryable as a busy one.
    if e.is_closed() {
        return Error::Busy;
    }
    match e.code() {
        // Deadlock, serialization failure, an administrative shutdown, a
        // database that starts up, a lock another writer holds.
        Some(code)
            if matches!(code.code(), "40P01" | "40001" | "57P01" | "57P03" | "55P03")
                || code.code().starts_with("08") =>
        {
            tracing::warn!("the database is busy: {e}");
            Error::Busy
        }
        // The lock or the statement waited past its clock, as a SQLite
        // write does past its busy timeout: the caller has been at it
        // too long, and the client retries past its own deadline.
        Some(code) if code.code() == "57014" => {
            tracing::warn!("the database gave the statement up: {e}");
            Error::DeadlineExceeded("the database kept the caller waiting".to_owned())
        }
        // A violation the code handles (a duplicate key, a check, a
        // foreign key) is one answer among many: it logs quietly.
        Some(code) if code.code().starts_with("23") => {
            tracing::debug!("the database refused a statement: {e}");
            Error::Pg(e)
        }
        // A statement the database never saw: the connection could not
        // be made or was cut on the way. No SQLSTATE names it, and a
        // client retries it as it retries a busy one.
        None => {
            tracing::warn!("the database is unreachable: {e}");
            Error::Busy
        }
        // The rest keep their database error, and reach the log in full:
        // a bug of this backend, such as a bad parameter, must not look
        // busy, or the importer retries it and the log says nothing but
        // "busy".
        _ => {
            tracing::warn!("the database refused a statement: {e}");
            Error::Pg(e)
        }
    }
}

/// The TLS connector of a database whose host is off this machine: the
/// certificate it presents must be one `ca` signed. There is no mode that
/// accepts any certificate.
fn make_tls(ca: &Path) -> Result<MakeRustlsConnect> {
    use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject as _};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, crypto::CryptoProvider};

    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(ca)
        .map_err(|e| Error::InvalidArgument(format!("cannot read the database CA: {e}")))?
    {
        let certificate = certificate
            .map_err(|e| Error::InvalidArgument(format!("the database CA is not PEM: {e}")))?;
        roots
            .add(certificate)
            .map_err(|_| Error::InvalidArgument("the database CA is not a root".to_owned()))?;
    }
    let provider: Arc<CryptoProvider> =
        Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::InvalidArgument(format!("no supported TLS version: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

/// Brings the database to this build's schema: runs every migration it has
/// not run yet, in one transaction each, and refuses a schema of a newer
/// build.
fn migrate(client: &mut PgClient) -> Result<()> {
    // One process at a time migrates: a lock of the session, held across
    // the migrations, so a server and an import started together take
    // turns while the migrations are quick, and the version the loser
    // reads is the one the winner left. A lock past its 5 s clock leaves
    // the taker busy: an open that hits one waits and tries again.
    client.query_opt("SELECT pg_advisory_lock($1)", &[P::i64(SCHEMA_LOCK)])?;
    let outcome = migrate_under_lock(client);
    // The lock lives on the client's session: it goes back whatever the
    // migrations did. A client that died mid-migration lost the lock with
    // its connection, and the unlock names a lock the new connection
    // never held, which answers false and costs nothing.
    let _ = client.query_opt("SELECT pg_advisory_unlock($1)", &[P::i64(SCHEMA_LOCK)]);
    outcome
}

/// The migrations themselves, under the session's schema lock.
fn migrate_under_lock(client: &mut PgClient) -> Result<()> {
    // The ledger of the versions, made first, in its own transaction: a
    // migration that fails leaves the ledger and every version before it.
    client.batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version BIGINT PRIMARY KEY,
             name    TEXT NOT NULL
         )",
    )?;
    let version: i64 = client
        .query("SELECT max(version) FROM schema_migrations", &[])?
        .first()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0);
    let version = u32::try_from(version).map_err(|_| {
        Error::FailedPrecondition(format!(
            "database schema version {version} is not a version"
        ))
    })?;
    if version > PG_SCHEMA_VERSION {
        return Err(Error::FailedPrecondition(format!(
            "database schema version {version} is newer than this build \
             ({PG_SCHEMA_VERSION}); run this build's older server, or upgrade it"
        )));
    }
    for (n, sql) in PG_MIGRATIONS.iter().enumerate() {
        let n = u32::try_from(n + 1).expect("a schema version fits in a u32");
        if n <= version {
            continue;
        }
        // One transaction per migration, with its version bump: a
        // migration that fails rolls its own statements back, and the
        // versions before it stand.
        let mut tx = PgTx::begin(client)?;
        tx.c.batch(&format!(
            "{sql}\nINSERT INTO schema_migrations (version, name) \
             VALUES ({}, 'postgres schema')",
            i64::from(n)
        ))?;
        tx.commit()?;
    }
    Ok(())
}

/// Reads a tree's rows and checks them against its ID.
fn load_tree(c: &PgClient, id: TreeId) -> Result<(Tree, Vec<u8>)> {
    let body: Vec<u8> = c
        .query_opt(
            "SELECT body FROM trees WHERE id = $1",
            &[P::bytes(&id_bytes(id.as_bytes()))],
        )?
        .ok_or_else(|| Error::NotFound(format!("tree {id}")))?
        .get(0);
    let (tree, actual) =
        Tree::decode_with_id(&body).map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))?;
    if actual != id {
        return Err(Error::Corrupt(format!("tree {id} does not match its ID")));
    }
    Ok((tree, body))
}

/// The error of a graph walk that reached its bound.
fn too_deep(max_visits: usize) -> Error {
    Error::ResourceExhausted(format!(
        "the history is too deep: the answer needs more than {max_visits} changesets"
    ))
}

fn to_id(bytes: &[u8]) -> Result<ChangesetId> {
    ChangesetId::try_from(bytes).map_err(|e| Error::Corrupt(format!("stored changeset ID: {e}")))
}

/// The bytes of a content address, as a BYTEA parameter.
fn id_bytes(id: &[u8; 32]) -> Vec<u8> {
    id.to_vec()
}

impl MetaTx for PgTx<'_> {
    /// Creates a repository, or returns the existing one with that name.
    fn create_repo(&mut self, name: &str) -> Result<RepoId> {
        self.c.execute(
            "INSERT INTO repos (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[P::text(name)],
        )?;
        self.repo_id(name)
    }

    /// Looks up a repository by name.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no repository has that name.
    fn repo_id(&mut self, name: &str) -> Result<RepoId> {
        let row = self
            .c
            .query_opt(
                "SELECT repo_id FROM repos WHERE name = $1",
                &[P::text(name)],
            )?
            .ok_or_else(|| Error::NotFound(format!("repo {name:?}")))?;
        Ok(RepoId(row.get(0)))
    }

    /// Stores a changeset and adds it to `repo`. Every parent must already
    /// be in `repo`. Safe to repeat.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if a parent is not in `repo`.
    fn insert_changeset(&mut self, repo: RepoId, cs: &Changeset) -> Result<ChangesetId> {
        // This transaction holds the collector's key from its begin, so
        // the collector's check waits for it, and its re-read pins every
        // blob this changeset puts. The files themselves the lens of
        // `store_changeset` has already read inside this transaction: a
        // blob whose file the collector removed fails there, before any
        // write, on both backends.
        let (body, id) = cs.encode_with_id();
        let mut parent_gen = 0;
        for parent in &cs.parents {
            parent_gen = parent_gen.max(self.generation(repo, *parent)?);
        }
        self.c.execute(
            "INSERT INTO changesets (id, gen, body) VALUES ($1, $2, $3)
             ON CONFLICT (id) DO NOTHING",
            &[
                P::bytes(&id_bytes(id.as_bytes())),
                P::i64(parent_gen + 1),
                P::bytes(&body),
            ],
        )?;
        for (n, parent) in (0_i64..).zip(&cs.parents) {
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent) VALUES ($1, $2, $3)
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytes(&id_bytes(id.as_bytes())),
                    P::i64(n),
                    P::bytes(&id_bytes(parent.as_bytes())),
                ],
            )?;
        }
        self.c.execute(
            "INSERT INTO repo_changesets (repo_id, cs) VALUES ($1, $2)
             ON CONFLICT (repo_id, cs) DO NOTHING",
            &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
        )?;
        Ok(id)
    }

    /// Adds `blob` to the blobs that `repo` may name. Returns whether it is
    /// new.
    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        let added = self.c.execute(
            "INSERT INTO repo_blobs (repo_id, blob) VALUES ($1, $2)
             ON CONFLICT (repo_id, blob) DO NOTHING",
            &[P::i64(repo.0), P::bytes(&id_bytes(blob.as_bytes()))],
        )?;
        Ok(added == 1)
    }

    /// Whether `repo` may name `blob`.
    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        Ok(self
            .c
            .query_opt(
                "SELECT 1 FROM repo_blobs WHERE repo_id = $1 AND blob = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(blob.as_bytes()))],
            )?
            .is_some())
    }

    /// Reads a changeset that `repo` may see and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it, [`Error::Corrupt`]
    /// if it does not decode or match.
    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset> {
        let body: Vec<u8> = self
            .c
            .query_opt(
                "SELECT body FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))?
            .get(0);
        let (cs, actual) = Changeset::decode_with_id(&body)
            .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
        if actual != id {
            return Err(Error::Corrupt(format!(
                "changeset {id} does not match its ID"
            )));
        }
        Ok(cs)
    }

    /// Whether `repo` may see changeset `id`.
    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool> {
        Ok(self
            .c
            .query_opt(
                "SELECT 1 FROM repo_changesets WHERE repo_id = $1 AND cs = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .is_some())
    }

    /// The generation of a changeset that `repo` may see.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it.
    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64> {
        self.c
            .query_opt(
                "SELECT gen FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = $2",
                &[P::i64(repo.0), P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .map(|row| row.get::<_, i64>(0))
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    /// The generation of a changeset, in any repo. For derived data.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no changeset has this ID.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64> {
        self.c
            .query_opt(
                "SELECT gen FROM changesets WHERE id = $1",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .map(|row| row.get::<_, i64>(0))
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    /// The first parent of a changeset, or `None` for a root.
    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>> {
        let parent = self
            .c
            .query_opt(
                "SELECT parent FROM changeset_parents WHERE cs = $1 AND n = 0",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .map(|row| row.get::<_, Vec<u8>>(0));
        parent.map(|bytes| to_id(&bytes)).transpose()
    }

    /// Every parent of changeset `id`, in order, with its generation.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a parent's generation is not below `id_gen`.
    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>> {
        let rows = self.c.query(
            "SELECT parent, gen FROM changeset_parents JOIN changesets ON id = parent
             WHERE cs = $1 ORDER BY n",
            &[P::bytes(&id_bytes(id.as_bytes()))],
        )?;
        rows.iter()
            .map(|row| {
                let parent: Vec<u8> = row.get(0);
                let parent_gen: i64 = row.get(1);
                let parent = to_id(&parent)?;
                if parent_gen >= id_gen {
                    return Err(Error::Corrupt(format!(
                        "changeset {id} has generation {id_gen}, its parent {parent} {parent_gen}"
                    )));
                }
                Ok((parent, parent_gen))
            })
            .collect()
    }

    /// Whether `ancestor` is `descendant` or reachable from it through
    /// parents, within a bounded walk.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have either changeset.
    /// [`Error::ResourceExhausted`] if the walk needs more than `max_visits`
    /// changesets.
    fn is_ancestor(
        &mut self,
        repo: RepoId,
        ancestor: ChangesetId,
        descendant: ChangesetId,
        max_visits: usize,
    ) -> Result<bool> {
        let floor = self.generation(repo, ancestor)?;
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![(descendant, self.generation(repo, descendant)?)];
        while let Some((id, id_gen)) = stack.pop() {
            if id == ancestor {
                return Ok(true);
            }
            if id_gen <= floor || !seen.insert(id) {
                continue;
            }
            if seen.len() > max_visits {
                return Err(too_deep(max_visits));
            }
            stack.extend(self.parents(id, id_gen)?);
        }
        Ok(false)
    }

    /// Where a ref points, if it exists.
    fn get_ref(&mut self, repo: RepoId, name: &str) -> Result<Option<RefState>> {
        self.c
            .query_opt(
                "SELECT target, version FROM refs WHERE repo_id = $1 AND name = $2",
                &[P::i64(repo.0), P::text(name)],
            )?
            .map(|row| -> Result<RefState> {
                let target: Vec<u8> = row.get(0);
                Ok(RefState {
                    target: to_id(&target)?,
                    version: row.get(1),
                })
            })
            .transpose()
    }

    /// Moves a ref if its version is still `expected`. `None` creates the
    /// ref and expects it not to exist. Returns whether the ref moved.
    ///
    /// # Errors
    ///
    /// Database errors, or [`Error::NotFound`] if `repo` does not have
    /// `target`.
    fn cas_ref(
        &mut self,
        repo: RepoId,
        name: &str,
        expected: Option<i64>,
        target: ChangesetId,
    ) -> Result<bool> {
        self.generation(repo, target)?;
        let changed = match expected {
            None => self.c.execute(
                "INSERT INTO refs (repo_id, name, target, version) VALUES ($1, $2, $3, 1)
                 ON CONFLICT (repo_id, name) DO NOTHING",
                &[
                    P::i64(repo.0),
                    P::text(name),
                    P::bytes(&id_bytes(target.as_bytes())),
                ],
            )?,
            Some(version) => self.c.execute(
                "UPDATE refs SET target = $3, version = version + 1
                 WHERE repo_id = $1 AND name = $2 AND version = $4",
                &[
                    P::i64(repo.0),
                    P::text(name),
                    P::bytes(&id_bytes(target.as_bytes())),
                    P::i64(version),
                ],
            )?,
        };
        Ok(changed == 1)
    }

    /// The stored bytes of a derived value, if computed.
    fn get_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
    ) -> Result<Option<Vec<u8>>> {
        let version = i64::from(version);
        Ok(self
            .c
            .query_opt(
                "SELECT value FROM derived WHERE kind = $1 AND version = $2 AND cs = $3",
                &[
                    P::text(kind),
                    P::i64(version),
                    P::bytes(&id_bytes(cs.as_bytes())),
                ],
            )?
            .map(|row| row.get(0)))
    }

    /// Records a derived value. A second write must agree with the first:
    /// derivation is deterministic, so a mismatch means corruption.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a different value is already stored.
    fn put_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
        value: &[u8],
    ) -> Result<()> {
        let version64 = i64::from(version);
        let inserted = self.c.execute(
            "INSERT INTO derived (kind, version, cs, value) VALUES ($1, $2, $3, $4)
             ON CONFLICT (kind, version, cs) DO NOTHING",
            &[
                P::text(kind),
                P::i64(version64),
                P::bytes(&id_bytes(cs.as_bytes())),
                P::bytes(value),
            ],
        )?;
        if inserted == 0 && self.get_derived(kind, version, cs)?.as_deref() != Some(value) {
            return Err(Error::Corrupt(format!(
                "derived {kind} v{version} for {cs} differs from the stored value"
            )));
        }
        Ok(())
    }

    /// Deletes up to `limit` derived values of `kind` with a version below
    /// `version`, and returns how many.
    fn delete_derived_below(&mut self, kind: &str, version: u32, limit: usize) -> Result<usize> {
        let version = i64::from(version);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let deleted = self.c.execute(
            "DELETE FROM derived WHERE (kind, version, cs) IN (
                 SELECT kind, version, cs FROM derived
                 WHERE kind = $1 AND version < $2 LIMIT $3)",
            &[P::text(kind), P::i64(version), P::i64(limit)],
        )?;
        Ok(usize::try_from(deleted).unwrap_or(0))
    }

    /// The view that the per-tree memo holds for a (rule set, version,
    /// real tree, context), if derived.
    ///
    /// # Errors
    ///
    /// Database errors, or a stored view that is not a tree ID.
    fn get_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
    ) -> Result<Option<TreeId>> {
        let bytes: Option<Vec<u8>> = self
            .c
            .query_opt(
                "SELECT view FROM view_trees \
                 WHERE repo_id = $1 AND digest = $2 AND version = $3 AND tree = $4 AND ctx = $5",
                &[
                    P::i64(repo.0),
                    P::bytes(digest),
                    P::i64(i64::from(version)),
                    P::bytes(&id_bytes(tree.as_bytes())),
                    P::bytes(&ctx),
                ],
            )?
            .map(|row| row.get(0));
        bytes
            .map(|b| TreeId::try_from(b.as_slice()).map_err(|_| Error::Corrupt("bad view".into())))
            .transpose()
    }

    /// Records one row of the per-tree memo. A second write must agree
    /// with the first: derivation is deterministic, so a mismatch means
    /// corruption.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a different view is already stored.
    fn put_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
        view: TreeId,
    ) -> Result<()> {
        let inserted = self.c.execute(
            "INSERT INTO view_trees (repo_id, digest, version, tree, ctx, view) \
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (repo_id, digest, version, tree, ctx) DO NOTHING",
            &[
                P::i64(repo.0),
                P::bytes(digest),
                P::i64(i64::from(version)),
                P::bytes(&id_bytes(tree.as_bytes())),
                P::bytes(&ctx),
                P::bytes(&id_bytes(view.as_bytes())),
            ],
        )?;
        if inserted == 0 && self.get_view_tree(repo, digest, version, tree, ctx)? != Some(view) {
            return Err(Error::Corrupt(format!(
                "the view of tree {tree} under its context differs from the stored value"
            )));
        }
        Ok(())
    }

    /// Deletes at most `limit` dead rows of the per-tree memo: rows of an
    /// older projection version, then rows of a digest that no grant of
    /// this repo hashes to any more, as far as this server's access file
    /// knows. The LIMIT lives in the subquery over the primary key, as the
    /// derived-row expiry does. A digest that another server still needs
    /// is re-derived, at that server's next read.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn delete_stale_view_trees(
        &mut self,
        repo: RepoId,
        live: &[[u8; 32]],
        version: u32,
        limit: usize,
    ) -> Result<usize> {
        let limit = u64::try_from(limit).map_err(|_| Error::InvalidArgument("limit".into()))?;
        let version = i64::from(version);
        // Rows of an older projection version are dead, whoever used them.
        let mut removed = self.c.execute(
            "DELETE FROM view_trees WHERE (repo_id, digest, version, tree, ctx) IN (
                 SELECT repo_id, digest, version, tree, ctx FROM view_trees
                 WHERE repo_id = $1 AND version < $2 LIMIT $3)",
            &[
                P::i64(repo.0),
                P::i64(version),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        // Rows of a digest that no grant of this repo hashes to any more.
        // `live` is never empty here: the deriving caller's digest is among
        // the digests of its access snapshot (the assert in `view_root`),
        // and a caller that cannot prove that has no business sweeping.
        if removed < limit && !live.is_empty() {
            let live: Vec<Vec<u8>> = live.iter().map(|d| d.to_vec()).collect();
            let left = limit - removed;
            removed += self.c.execute(
                "DELETE FROM view_trees WHERE (repo_id, digest, version, tree, ctx) IN (
                     SELECT repo_id, digest, version, tree, ctx FROM view_trees
                     WHERE repo_id = $1 AND version = $2
                       AND NOT (digest = ANY($3))
                     LIMIT $4)",
                &[
                    P::i64(repo.0),
                    P::i64(version),
                    P::bytea_array(live),
                    P::i64(i64::try_from(left).unwrap_or(i64::MAX)),
                ],
            )?;
        }
        Ok(usize::try_from(removed).unwrap_or(0))
    }

    /// Stores a tree. Safe to repeat.
    fn put_tree(&mut self, tree: &Tree) -> Result<TreeId> {
        let (body, id) = tree.encode_with_id();
        self.c.execute(
            "INSERT INTO trees (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            &[P::bytes(&id_bytes(id.as_bytes())), P::bytes(&body)],
        )?;
        Ok(id)
    }

    /// Reads a tree and checks it against its ID.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent, [`Error::Corrupt`] if it does not
    /// decode or match.
    fn get_tree(&mut self, id: TreeId) -> Result<Tree> {
        load_tree(self.c, id).map(|(tree, _)| tree)
    }

    /// Reads a tree's stored `TreeV1` bytes and checks them against its ID.
    ///
    /// # Errors
    ///
    /// As [`MetaTx::get_tree`].
    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>> {
        load_tree(self.c, id).map(|(_, body)| body)
    }

    /// The length of a tree's stored `TreeV1` bytes, without reading them.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if absent.
    fn tree_size(&mut self, id: TreeId) -> Result<usize> {
        let size: i64 = self
            .c
            .query_opt(
                // octet_length of a bytea is int4; the read wants int8.
                "SELECT octet_length(body)::bigint FROM trees WHERE id = $1",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .ok_or_else(|| Error::NotFound(format!("tree {id}")))?
            .get(0);
        usize::try_from(size).map_err(|_| Error::Corrupt(format!("tree {id}: length {size}")))
    }

    /// Inserts a land, or finds the land for the same request key. A land
    /// to a ref that does not exist is recorded as rejected at once. A new
    /// land also expires old ones.
    fn record_land(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<(RepoId, LandOutcome)> {
        let repo = self.repo_id(&req.repo)?;
        self.generation(repo, req.changeset)?;
        let existing = self
            .c
            .query_opt(
                "SELECT land_id, input_cs, target_ref FROM lands
                 WHERE repo_id = $1 AND caller = $2 AND request_key = $3",
                &[
                    P::i64(repo.0),
                    P::text(&caller.0),
                    P::text(&req.request_key),
                ],
            )?
            .map(|row| {
                (
                    row.get::<_, i64>(0),
                    row.get::<_, Vec<u8>>(1),
                    row.get::<_, String>(2),
                )
            });
        let land_id = if let Some((land_id, input, target)) = existing {
            if input != req.changeset.as_bytes() || target != req.target_ref {
                return Err(Error::InvalidArgument(format!(
                    "request key {:?} was already used for a different land",
                    req.request_key
                )));
            }
            land_id
        } else {
            let (state, detail) = if self.get_ref(repo, &req.target_ref)?.is_some() {
                (LandState::Pending, String::new())
            } else {
                let detail = format!("ref {:?} does not exist", req.target_ref);
                (LandState::Rejected, detail)
            };
            if let Some(limit) = pending_limit {
                // The pending-lands quota counts new pending rows. A retry
                // of an existing key resumes its land and adds no row, so a
                // client can always finish what it started.
                let pending: i64 = self
                    .c
                    .query_opt(
                        "SELECT count(*) FROM lands WHERE caller = $1 AND state = 'pending'",
                        &[P::text(&caller.0)],
                    )?
                    .and_then(|row| row.get(0))
                    .unwrap_or(0);
                if pending >= i64::try_from(limit).unwrap_or(i64::MAX) {
                    return Err(Error::ResourceExhausted(format!(
                        "principal {:?} has its {limit} lands pending \
                         (--max-principal-pending-lands {limit}); wait for one to finish",
                        caller.0
                    )));
                }
            }
            let inserted = self
                .c
                .query_opt(
                    "INSERT INTO lands
                     (repo_id, caller, request_key, target_ref, input_cs, state, detail,
                      finished_at, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7,
                         CASE WHEN $6 = 'pending' THEN NULL ELSE now_epoch() END,
                         now_epoch())
                     ON CONFLICT DO NOTHING
                     RETURNING land_id",
                    &[
                        P::i64(repo.0),
                        P::text(&caller.0),
                        P::text(&req.request_key),
                        P::text(&req.target_ref),
                        P::bytes(&id_bytes(req.changeset.as_bytes())),
                        P::text(state.as_str()),
                        P::text(&detail),
                    ],
                )?
                .map(|row| row.get::<_, i64>(0));
            let land_id = match inserted {
                Some(land_id) => land_id,
                None => self.read_back_land(repo, caller, req)?,
            };
            self.expire_lands()?;
            land_id
        };
        Ok((repo, self.land_outcome(repo, caller, land_id)?))
    }

    /// The pending lands that started more than `stale_secs` ago.
    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<(RepoId, i64, String)>> {
        let rows = self.c.query(
            "SELECT repo_id, land_id, target_ref FROM lands
             WHERE state = 'pending' AND created_at > 0
                 AND created_at < now_epoch() - $1",
            &[P::i64(stale_secs)],
        )?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1), row.get(2)))
            .collect())
    }

    /// Rejects a land that is still pending; 1 if it changed, else 0.
    fn reject_stale_land(&mut self, land_id: i64, detail: String) -> Result<u64> {
        let changed = self.c.execute(
            "UPDATE lands SET state = 'rejected', detail = $1, finished_at = now_epoch()
             WHERE land_id = $2 AND state = 'pending'",
            &[P::text(&detail), P::i64(land_id)],
        )?;
        Ok(changed)
    }

    /// Deletes up to [`EXPIRE_PER_LAND`] lands that finished more than
    /// [`LAND_RETENTION_SECS`] ago, oldest first. The newest land always
    /// stays; its ID is the largest, and the sequence never hands an ID
    /// out twice.
    fn expire_lands(&mut self) -> Result<()> {
        let deleted = self.c.execute(
            "DELETE FROM lands WHERE land_id IN (
                 SELECT land_id FROM lands
                 WHERE finished_at < now_epoch() - $1
                     AND land_id < (SELECT max(land_id) FROM lands)
                 ORDER BY finished_at LIMIT $2)",
            &[P::i64(LAND_RETENTION_SECS), P::i64(EXPIRE_PER_LAND)],
        )?;
        if deleted > 0 {
            tracing::debug!(deleted, "expired finished lands");
        }
        Ok(())
    }

    /// A land that `caller` started in `repo`.
    fn land_outcome(&mut self, repo: RepoId, caller: &Caller, land_id: i64) -> Result<LandOutcome> {
        let row = self
            .c
            .query_opt(
                "SELECT state, result_cs, detail, conflict_paths FROM lands
                 WHERE repo_id = $1 AND land_id = $2 AND caller = $3",
                &[P::i64(repo.0), P::i64(land_id), P::text(&caller.0)],
            )?
            .ok_or_else(|| Error::NotFound(format!("land {land_id}")))?;
        let state: String = row.get(0);
        let result: Option<Vec<u8>> = row.get(1);
        let detail: String = row.get(2);
        let paths: Vec<u8> = row.get(3);
        let result = result
            .map(|bytes| ChangesetId::try_from(bytes.as_slice()))
            .transpose()
            .map_err(|e| Error::Corrupt(format!("land {land_id} result: {e}")))?;
        Ok(LandOutcome {
            land_id,
            state: LandState::parse(&state)?,
            result,
            detail,
            conflict_paths: decode_paths(&paths)
                .ok_or_else(|| Error::Corrupt(format!("land {land_id} conflict paths")))?,
        })
    }

    /// Moves a pending land to its final state and returns the outcome.
    fn set_land_state(
        &mut self,
        repo: RepoId,
        land_id: i64,
        state: LandState,
        result: Option<ChangesetId>,
        detail: String,
        conflict_paths: Vec<RepoPath>,
    ) -> Result<LandOutcome> {
        let updated = self.c.execute(
            "UPDATE lands SET state = $3, result_cs = $4, detail = $5, conflict_paths = $6,
                 finished_at = now_epoch()
             WHERE repo_id = $1 AND land_id = $2 AND state = 'pending'",
            &[
                P::i64(repo.0),
                P::i64(land_id),
                P::text(state.as_str()),
                P::opt_bytes(result.as_ref().map(|id| id_bytes(id.as_bytes()))),
                P::text(&detail),
                P::bytes(&encode_paths(&conflict_paths)),
            ],
        )?;
        if updated != 1 {
            return Err(Error::Corrupt(format!(
                "land {land_id} is no longer pending"
            )));
        }
        Ok(LandOutcome {
            land_id,
            state,
            result,
            detail,
            conflict_paths,
        })
    }

    /// Records that a git commit became the changeset. Safe to repeat.
    fn put_git_import(&mut self, repo: RepoId, commit: ObjectId, cs: ChangesetId) -> Result<()> {
        self.c.execute(
            "INSERT INTO git_import (repo_id, git_algo, git_oid, cs) VALUES ($1, $2, $3, $4)
             ON CONFLICT (repo_id, git_algo, git_oid) DO NOTHING",
            &[
                P::i64(repo.0),
                P::i64(SHA1),
                P::bytes(commit.as_slice()),
                P::bytes(&id_bytes(cs.as_bytes())),
            ],
        )?;
        Ok(())
    }

    /// Every git commit already imported into `repo`.
    fn imported_commits(
        &mut self,
        repo: RepoId,
    ) -> Result<std::collections::HashMap<ObjectId, ChangesetId>> {
        let rows = self.c.query(
            "SELECT git_oid, cs FROM git_import WHERE repo_id = $1 AND git_algo = $2",
            &[P::i64(repo.0), P::i64(SHA1)],
        )?;
        let mut out = std::collections::HashMap::new();
        for row in &rows {
            let oid: Vec<u8> = row.get(0);
            let cs: Vec<u8> = row.get(1);
            let corrupt = || Error::Corrupt("git_import row".to_owned());
            let oid = ObjectId::try_from(oid.as_slice()).map_err(|_| corrupt())?;
            let cs = ChangesetId::try_from(cs.as_slice()).map_err(|_| corrupt())?;
            out.insert(oid, cs);
        }
        Ok(out)
    }

    /// Stores a history node under its content address. Safe to repeat.
    /// Returns whether the row was new.
    fn put_history_node(&mut self, id: NodeId, body: Vec<u8>) -> Result<(NodeId, bool)> {
        let stored = self.c.execute(
            "INSERT INTO history_nodes (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            &[P::bytes(&id.0), P::bytes(&body)],
        )?;
        Ok((id, stored == 1))
    }

    /// Reads a history node and checks it against its ID. Also returns the
    /// length of its stored bytes.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if absent or if it does not match its ID.
    fn get_history_node(&mut self, id: NodeId) -> Result<(Vec<u8>, usize)> {
        let body: Vec<u8> = self
            .c
            .query_opt(
                "SELECT body FROM history_nodes WHERE id = $1",
                &[P::bytes(&id.0)],
            )?
            .ok_or_else(|| Error::Corrupt(format!("history node {id} is missing")))?
            .get(0);
        if NodeId::of(&body) != id {
            return Err(Error::Corrupt(format!(
                "history node {id} does not match its ID"
            )));
        }
        let len = body.len();
        Ok((body, len))
    }
    /// Every repo, in ID order.
    fn repos(&mut self) -> Result<Vec<RepoId>> {
        let rows = self
            .c
            .query("SELECT repo_id FROM repos ORDER BY repo_id", &[])?;
        Ok(rows.iter().map(|row| RepoId(row.get(0))).collect())
    }

    /// The `(repo, changeset ID, body)` of the stored changesets after
    /// `(repo, cs)`, in that order, at most `limit`.
    fn repo_changesets_after(
        &mut self,
        from_repo: RepoId,
        from_cs: &[u8],
        limit: usize,
    ) -> Result<Vec<RepoChangeset>> {
        let rows = self.c.query(
            "SELECT rc.repo_id, rc.cs, ch.body
             FROM repo_changesets rc JOIN changesets ch ON ch.id = rc.cs
             WHERE (rc.repo_id, rc.cs) > ($1, $2)
             ORDER BY rc.repo_id, rc.cs LIMIT $3",
            &[
                P::i64(from_repo.0),
                P::bytes(from_cs),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows
            .iter()
            .map(|row| RepoChangeset(RepoId(row.get(0)), row.get(1), row.get(2)))
            .collect())
    }

    /// The changeset IDs that `repo` holds, all of them.
    fn repo_changeset_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        let rows = self.c.query(
            "SELECT cs FROM repo_changesets WHERE repo_id = $1 ORDER BY cs",
            &[P::i64(repo.0)],
        )?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// The body of changeset `cs`, or `None` when it is not stored.
    fn stored_changeset_body(&mut self, cs: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .c
            .query_opt("SELECT body FROM changesets WHERE id = $1", &[P::bytes(cs)])?
            .map(|row| row.get(0)))
    }

    /// The blobs that `repo` may name, all of them.
    fn repo_blob_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        let rows = self.c.query(
            "SELECT blob FROM repo_blobs WHERE repo_id = $1 ORDER BY blob",
            &[P::i64(repo.0)],
        )?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Removes the row that names `blob` for `repo`.
    fn delete_repo_blob(&mut self, repo: RepoId, blob: &[u8]) -> Result<()> {
        self.c.execute(
            "DELETE FROM repo_blobs WHERE repo_id = $1 AND blob = $2",
            &[P::i64(repo.0), P::bytes(blob)],
        )?;
        Ok(())
    }

    /// Whether any repo names `blob`. Driven from `repos`, as the SQLite
    /// one is: `repo_blobs` has no index on `blob` alone.
    fn any_repo_names(&mut self, blob: &[u8]) -> Result<bool> {
        Ok(self
            .c
            .query_opt(
                "SELECT 1 FROM repos r
                 WHERE EXISTS (SELECT 1 FROM repo_blobs rb
                               WHERE rb.repo_id = r.repo_id AND rb.blob = $1)
                 LIMIT 1",
                &[P::bytes(blob)],
            )?
            .is_some())
    }

    /// The `(changeset ID, body)` of the changesets after `from`, in ID
    /// order, at most `limit`.
    fn changeset_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM changesets WHERE id > $1 ORDER BY id LIMIT $2",
            &[
                P::bytes(from),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// The `(tree ID, body)` of the trees after `from`, in ID order, at
    /// most `limit`.
    fn tree_bodies_after(&mut self, from: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM trees WHERE id > $1 ORDER BY id LIMIT $2",
            &[
                P::bytes(from),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// The `(node ID, body)` of the history nodes after `from`, in ID
    /// order, at most `limit`.
    fn history_node_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM history_nodes WHERE id > $1 ORDER BY id LIMIT $2",
            &[
                P::bytes(from),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// The `repo_blobs` rows after `(repo, blob)`, at most `limit`.
    fn repo_blob_rows_after(
        &mut self,
        from_repo: RepoId,
        from_blob: &[u8],
        limit: usize,
    ) -> Result<Vec<(RepoId, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT repo_id, blob FROM repo_blobs
             WHERE (repo_id, blob) > ($1, $2) ORDER BY repo_id, blob LIMIT $3",
            &[
                P::i64(from_repo.0),
                P::bytes(from_blob),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1)))
            .collect())
    }

    /// The refs whose target no `repo_changesets` row of their repo holds.
    fn dangling_refs(&mut self) -> Result<Vec<(RepoId, String, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT repo_id, name, target FROM refs r
             WHERE NOT EXISTS (
                 SELECT 1 FROM repo_changesets rc
                 WHERE rc.repo_id = r.repo_id AND rc.cs = r.target
             )",
            &[],
        )?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1), row.get(2)))
            .collect())
    }

    /// Every land whose input or result changeset is not stored, as
    /// `(land, changeset, which column)`.
    fn lands_with_missing_changesets(&mut self) -> Result<Vec<(i64, Vec<u8>, &'static str)>> {
        let mut out = Vec::new();
        for (column, when) in [
            ("input_cs", "1 = 1"),
            ("result_cs", "result_cs IS NOT NULL"),
        ] {
            let sql = format!(
                "SELECT land_id, {column} FROM lands l
                 WHERE {when} AND NOT EXISTS (
                     SELECT 1 FROM changesets c WHERE c.id = l.{column}
                 )"
            );
            let rows = self.c.query(&sql, &[])?;
            for row in rows {
                out.push((row.get(0), row.get(1), column));
            }
        }
        Ok(out)
    }

    /// The derived rows after `(kind, version, cs)`, at most `limit`, with
    /// whether their changeset is stored.
    fn derived_rows_after(
        &mut self,
        kind: &str,
        version: u32,
        cs: &[u8],
        limit: usize,
    ) -> Result<Vec<FsckDerivedRow>> {
        let rows = self.c.query(
            "SELECT d.kind, d.version, d.cs, d.value,
                    EXISTS (SELECT 1 FROM changesets c WHERE c.id = d.cs)
             FROM derived d
             WHERE (d.kind, d.version, d.cs) > ($1, $2, $3)
             ORDER BY d.kind, d.version, d.cs LIMIT $4",
            &[
                P::text(kind),
                P::i64(i64::from(version)),
                P::bytes(cs),
                P::i64(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        rows.iter()
            .map(|row| -> Result<FsckDerivedRow> {
                // d.version is BIGINT here and u32 in the row: read the
                // int8 first, like every numeric expression here.
                let version: i64 = row.get(1);
                Ok(FsckDerivedRow(
                    row.get(0),
                    u32::try_from(version)
                        .map_err(|_| Error::Corrupt(format!("derived version {version}")))?,
                    row.get(2),
                    row.get(3),
                    row.get(4),
                ))
            })
            .collect()
    }
}

impl Meta for Pg {
    type Tx<'a>
        = PgTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        // Every commit is durable when it returns, so what `f` sees is
        // durable, and the read need not wait.
        Pg::read(self, f)
    }

    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64) {
        // The count is for a caller that waits before it replies; on this
        // backend there is nothing to wait for.
        (Pg::read(self, f), 0)
    }

    fn wait_durable(&self, _seen: u64) -> Result<()> {
        Ok(())
    }

    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Pg::read_unflushed(self, f)
    }

    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Pg::commit(self, f)
    }

    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        // Durable on return, so `write` is `commit`.
        Pg::write(self, f)
    }

    fn flush(&self) -> Result<()> {
        // synchronous_commit is on: a commit is durable when it returns.
        // The blob files are the backend's own disk state, though: on a
        // drive whose cache fsync does not clear, they reach the drive on
        // their own schedule, like a SQLite store's.
        if crate::store::FSYNC_LEAVES_DRIVE_CACHE {
            self.inner.blobs.full_flush()?;
        }
        Ok(())
    }
}

/// Makes the test schema, and registers it, before any connection
/// narrows its search path to it. One maker at a time: two concurrent
/// `CREATE TABLE IF NOT EXISTS` of the registry race on the type
/// catalog, and the loser dies.
fn prepare_test_schema(url: &str, schema: &str) -> Result<()> {
    let preparer = connect(url, None)?;
    preparer.as_raw({
        let name = schema.to_owned();
        move |client| {
            client
                .batch_execute(&format!("SET application_name TO '{name}'"))
                .map_err(map_pg_error)
        }
    })?;
    drop_stale_schemas(&preparer);
    preparer.as_raw({
        let name = schema.to_owned();
        move |client| {
            client
                .batch_execute(&format!(
                    "SELECT pg_advisory_xact_lock({CLEAN_LOCK});
                         CREATE SCHEMA IF NOT EXISTS {name};
                         CREATE TABLE IF NOT EXISTS public.scion_test_schemas (
                             schema  TEXT PRIMARY KEY,
                             started BIGINT NOT NULL
                         );
                         INSERT INTO public.scion_test_schemas (schema, started)
                         VALUES ('{name}', extract(epoch from now())::bigint)
                         ON CONFLICT (schema) DO UPDATE SET started = excluded.started"
                ))
                .map_err(map_pg_error)
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::merged_options;

    const OWN: &str = "-csynchronous_commit=on -cstatement_timeout=600000 -clock_timeout=5000";

    #[test]
    fn the_urls_options_survive_the_backends_own() {
        assert_eq!(
            merged_options(Some("--search_path=scion_test_1"), OWN),
            format!("--search_path=scion_test_1 {OWN}")
        );
    }

    #[test]
    fn on_a_clash_the_backend_options_win() {
        // The backend's option comes last, and a `-c` of the startup
        // options overrides one before it.
        let merged = merged_options(Some("-cstatement_timeout=1"), OWN);
        assert!(merged.ends_with(OWN), "{merged}");
    }

    #[test]
    fn a_url_without_options_gets_the_backends_only() {
        assert_eq!(merged_options(None, OWN), OWN);
        assert_eq!(merged_options(Some(""), OWN), OWN);
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::store::READERS;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// A Postgres backend of a test database, or [`None`] on SQLite: the
    /// pool is a Postgres shape, so the pool tests skip the data
    /// directory.
    fn pg() -> Option<Pg> {
        if !crate::backend::Backend::test_is_postgres() {
            eprintln!("skip on sqlite: the reader pool is Postgres");
            return None;
        }
        let data = tempfile::tempdir().unwrap();
        match crate::backend::Backend::open_test(data.path()).unwrap() {
            crate::backend::Backend::Postgres(pg) => Some(pg),
            crate::backend::Backend::Sqlite(_) => unreachable!("the env named postgres"),
        }
    }

    /// One statement that runs past its bound is cancelled, and the
    /// reader comes back to the pool.
    #[test]
    fn a_read_statement_past_its_bound_is_cancelled() {
        let Some(pg) = pg() else { return };
        let err = pg
            .read_bounded(
                |tx| {
                    tx.c.query_opt("SELECT pg_sleep(2)", &[])?;
                    Ok(())
                },
                Duration::from_millis(50),
                Duration::from_secs(30),
                READER_WAIT,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Pg(_)), "{err}");
        // The reader returned: the next read runs at once.
        let again = pg.read(|tx| {
            tx.c.query_opt("SELECT 1", &[])
                .map(|row| row.map(|row| row.get::<_, i32>(0)))
        });
        assert_eq!(again.unwrap(), Some(1));
    }

    /// A call whose wait for a reader passes the bound hears
    /// `Unavailable`, instead of blocking to its deadline.
    #[test]
    fn a_call_that_waits_past_the_pool_bound_is_refused() {
        let Some(pg) = pg() else { return };
        let pg = Arc::new(pg);
        let busy = Arc::new(AtomicUsize::new(0));
        let holds: Vec<_> = (0..READERS)
            .map(|_| {
                let pg = Arc::clone(&pg);
                let busy = Arc::clone(&busy);
                std::thread::spawn(move || {
                    pg.read_bounded(
                        |_| {
                            busy.fetch_add(1, AtomicOrdering::SeqCst);
                            std::thread::sleep(Duration::from_millis(600));
                            Ok(())
                        },
                        Duration::from_secs(300),
                        Duration::from_secs(30),
                        Duration::from_secs(30),
                    )
                    .unwrap();
                })
            })
            .collect::<Vec<_>>();
        // Every reader is out of the pool.
        let wait = std::time::Instant::now() + Duration::from_secs(10);
        while busy.load(AtomicOrdering::SeqCst) < READERS && std::time::Instant::now() < wait {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            busy.load(AtomicOrdering::SeqCst),
            READERS,
            "all readers busy"
        );
        let refused = pg
            .read_bounded(
                |tx| {
                    tx.c.query_opt("SELECT 1", &[])?;
                    Ok(())
                },
                Duration::from_secs(300),
                Duration::from_secs(30),
                Duration::from_millis(50),
            )
            .unwrap_err();
        assert!(matches!(refused, Error::Unavailable(_)), "{refused}");
        for hold in holds {
            hold.join().unwrap();
        }
    }

    /// The pool wait lands in the histogram, and the busy gauge returns
    /// to zero when the reads are done.
    #[test]
    fn the_pool_records_its_waits_and_its_busy_readers() {
        let Some(pg) = pg() else { return };
        let metrics = Arc::new(Metrics::default());
        pg.set_metrics(Arc::clone(&metrics));
        let pg = Arc::new(pg);
        let busy = Arc::new(AtomicUsize::new(0));
        let holds: Vec<_> = (0..READERS)
            .map(|_| {
                let pg = Arc::clone(&pg);
                let busy = Arc::clone(&busy);
                std::thread::spawn(move || {
                    pg.read_bounded(
                        |_| {
                            busy.fetch_add(1, AtomicOrdering::SeqCst);
                            std::thread::sleep(Duration::from_millis(500));
                            Ok(())
                        },
                        Duration::from_secs(30),
                        Duration::from_secs(30),
                        Duration::from_millis(400),
                    )
                    .unwrap();
                })
            })
            .collect::<Vec<_>>();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while busy.load(AtomicOrdering::SeqCst) < READERS && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // One more call waits ~400 ms for a reader, then takes one as the
        // holders return theirs.
        let waited = std::time::Instant::now();
        pg.read_bounded(
            |tx| {
                tx.c.query_opt("SELECT 1", &[])?;
                Ok(())
            },
            Duration::from_secs(300),
            Duration::from_secs(30),
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            waited.elapsed() >= Duration::from_millis(100),
            "the wait was real"
        );
        for hold in holds {
            hold.join().unwrap();
        }
        // The gauges read the rendered text, as a scraper would: the
        // busy gauge is back to zero, and the wait is in the histogram.
        let text = metrics.render();
        let gauge = text
            .lines()
            .find(|line| line.starts_with("scion_pg_readers_busy "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(u64::MAX);
        assert_eq!(gauge, 0, "{text}");
        let count = text
            .lines()
            .find(|line| line.starts_with("scion_pg_pool_wait_seconds_count "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        assert!(count >= 1, "the wait was recorded: {text}");
        let sum = text
            .lines()
            .find(|line| line.starts_with("scion_pg_pool_wait_seconds_sum "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        assert!(sum > 0, "sum {sum}");
        assert_eq!(pg.tx_age(), Duration::ZERO);
    }

    /// The tx-age gauge says how old the oldest open read is, and goes
    /// back to zero when it ends.
    #[test]
    fn the_tx_age_gauge_tracks_the_oldest_open_read() {
        let Some(pg) = pg() else { return };
        let pg = Arc::new(pg);
        assert_eq!(pg.tx_age(), Duration::ZERO);
        let hold = {
            let pg = Arc::clone(&pg);
            std::thread::spawn(move || {
                pg.read_bounded(
                    |_| {
                        std::thread::sleep(Duration::from_millis(300));
                        Ok(())
                    },
                    Duration::from_secs(30),
                    Duration::from_secs(30),
                    Duration::from_secs(5),
                )
                .unwrap();
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        assert!(pg.tx_age() > Duration::ZERO, "{:?}", pg.tx_age());
        hold.join().unwrap();
        assert_eq!(pg.tx_age(), Duration::ZERO);
    }
}

//! The Postgres metadata backend of [`Meta`](crate::meta::Meta)
//! ([#47](https://github.com/bnsd55/openscion/issues/47)).
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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use gix::ObjectId;
use postgres::types::ToSql;
use postgres::{Client, Config, NoTls};

/// Shorthand for the owned query parameter.
type P = Param;
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};
use scion_proto::runtime::metrics::Metrics;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::auth::Caller;
use crate::backend::Marker;
use crate::error::{Error, Result, Storage};
use crate::history::NodeId;
use crate::import::SHA1;
use crate::land::{
    EXPIRE_PER_LAND, LAND_RETENTION_SECS, LandOutcome, LandRequest, LandState, decode_paths,
    encode_paths,
};
use crate::meta::{FsckDerivedRow, Meta, MetaTx, RefState, RepoChangeset, RepoId, StaleLand};
use crate::store::Blobs;

#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/pg.rs"]
pub(crate) mod test_hooks;

/// The schema version this build makes and reads.
pub(crate) const PG_SCHEMA_VERSION: u32 = 5;

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
    include_str!("../migrations/postgres/0005_parent_numbering.sql"),
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

pub(crate) struct Pg {
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
    /// The schema versions this open moved, when it migrated. Version 0's
    /// first open creates the schema, and reports none.
    migrated: Option<(u32, u32)>,
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
    hooks: crate::store::test_hooks::ImportHooks,
    /// The operator metrics, set once by the server before it serves: a
    /// commit's wait for the collector's advisory key records how long
    /// it waited. Empty in import, fsck and gc, which export none.
    metrics: std::sync::OnceLock<std::sync::Arc<crate::metrics::Metrics>>,
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
        // The idle lock guards the pool list only: it is dropped before
        // the tail, whose report_busy takes it again. Holding it across
        // the tail deadlocks take against itself on the first read.
        let client = {
            let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if let Some(client) = idle.pop() {
                    break client;
                }
                let left = wait.checked_sub(started.elapsed());
                let Some(left) = left else {
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
            }
        };
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
        drop(open);
        self.report_busy();
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
    /// A list of integers, for the bulk seed's and the batch flush's
    /// `unnest` over `int8[]`.
    I64Array(Vec<i64>),
    /// A list of strings, for the bulk seed's `unnest` over `text[]`.
    #[cfg(any(test, feature = "test-hooks"))]
    TextArray(Vec<&'static str>),
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

    /// A list of integers, as one `int8[]` parameter.
    fn i64_array(v: Vec<i64>) -> Param {
        Param::I64Array(v)
    }

    /// A list of strings, as one `text[]` parameter.
    #[cfg(any(test, feature = "test-hooks"))]
    fn text_array(v: Vec<&'static str>) -> Param {
        Param::TextArray(v)
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
            Self::I64Array(v) => v.to_sql(t, out),
            #[cfg(any(test, feature = "test-hooks"))]
            Self::TextArray(v) => v.to_sql(t, out),
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
            Self::I64Array(v) => v.to_sql(t, out),
            #[cfg(any(test, feature = "test-hooks"))]
            Self::TextArray(v) => v.to_sql(t, out),
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
                let mut setup = Setup::new(conn.schema.as_deref(), conn.exclusive, &mut client);
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
                    // statement until its caller ends it. Its COMMIT
                    // never runs: on the fresh session it would answer
                    // Ok about a transaction that is gone. A BEGIN may
                    // pass: the reconnect ended the old tx, so a BEGIN
                    // starts a fresh one on the fresh session, and a new
                    // BEGIN comes only from a new holder of the client,
                    // which the lost transaction cannot reach.
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
                        setup = Setup::new(conn.schema.as_deref(), conn.exclusive, &mut client);
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
        // The schema the URL rides is the connection's own business: the
        // setup asks the database `current_schema()` and locks that, so
        // parallel servers of distinct schemas do not fence each other,
        // and a plain URL fences the whole default schema.
        Self::open_in(url, ca, readers, data, None, true, marker)
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
        // The one-server lock of the schema the connection lands in: the
        // setup asks the database `current_schema()` and locks that, so a
        // plain URL and one that rides `--search_path=public` take the
        // same lock, and schemas ride separately. A reader takes none.
        let mut writer = if exclusive {
            connect_exclusive(url, ca, &options, schema, Arc::clone(&fence))?
        } else {
            connect_with(url, ca, &options, schema)?
        };
        let migrated = migrate(&mut writer)?;
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
            // own directory, and they agree. But only a schema that
            // holds the store it names: a store that ever served wrote
            // its repo rows, so a schema with no repos row was never
            // served by the directory's store, and adopting would give
            // the marker's identity to an empty schema - a wrong
            // --postgres-url, a typo in its search_path, or one never
            // served - and every caller would read "repo not found"
            // with no error at the start. An empty schema refuses while
            // the marker names a store, and mints when it names none.
            let served = writer.query("SELECT 1 FROM repos LIMIT 1", &[])?;
            if let (Some(held), []) = (marker.uuid, served.as_slice()) {
                return Err(Error::FailedPrecondition(format!(
                    "this data directory belongs to the store whose UUID \
                     is {}, but this database holds no store: check \
                     --postgres-url and its search_path",
                    hex16(&held)
                )));
            }
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
            migrated,
            readers: Readers::new_with(readers, url, ca, &options, schema)?,
            blobs,
            writers: Mutex::new(0),
            write_woken: Condvar::new(),
            derive: crate::derived::DeriveFlights::new(crate::store::DERIVE_WALKS_CAP),
            #[cfg(any(test, feature = "test-hooks"))]
            hooks: crate::store::test_hooks::ImportHooks::default(),
            metrics: std::sync::OnceLock::new(),
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

    /// The schema versions this open moved, when it migrated: the
    /// process that opened records the move in its audit log.
    pub(crate) fn migrated(&self) -> Option<(u32, u32)> {
        self.inner.migrated
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
    pub(crate) fn blobs(&self) -> &Blobs {
        &self.inner.blobs
    }

    /// Gives the pool the server's metrics registry. Called once by the
    /// server before it serves; import, fsck, and gc pass none, and the
    /// pool then records nothing.
    pub(crate) fn set_metrics(&self, metrics: Arc<Metrics>) {
        let _ = self.inner.metrics.set(Arc::clone(&metrics));
        self.inner.blobs.set_metrics(Arc::clone(&metrics));
        self.inner.derive.set_metrics(Arc::clone(&metrics));
        self.inner.readers.set_metrics(metrics);
    }

    /// The age of the oldest open read transaction, for the
    /// `scion_pg_tx_age_seconds` gauge. Zero when no read is open.
    #[must_use]
    pub(crate) fn tx_age(&self) -> Duration {
        self.inner
            .readers
            .oldest_open()
            .map(|started| started.elapsed())
            .unwrap_or_default()
    }

    /// A read on the write client, so it sees the caller's own commits.
    pub(crate) fn read_unflushed<T>(
        &self,
        f: impl FnOnce(&mut PgTx<'_>) -> Result<T>,
    ) -> Result<T> {
        self.check_fenced()?;
        let (mut writer, _waiting) = self.lock_writer();
        let mut pending = Vec::new();
        let mut pg = PgTx {
            c: &mut writer,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            cs_pending: Some(&mut pending),
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
    pub(crate) fn read<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
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
    pub(crate) fn commit<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        self.check_fenced()?;
        let (mut writer, _waiting) = self.lock_writer();
        // A write transaction runs at READ COMMITTED, the Postgres
        // default: every change of the caller checks the newest state of
        // the row it touches (the CAS predicates).
        let mut pending = Vec::new();
        let mut tx = PgTx::begin(&mut writer, &mut pending)?;
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
        let key_started = std::time::Instant::now();
        tx.c.query_opt(
            "SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc'))",
            &[],
        )?;
        if let Some(metrics) = self.inner.metrics.get() {
            metrics.collector_key_wait(
                u64::try_from(key_started.elapsed().as_micros()).unwrap_or(u64::MAX),
            );
        }
        tx.c.query(
            &format!("SET LOCAL lock_timeout = '{LOCK_TIMEOUT_FOR_WRITERS}'"),
            &[],
        )?;
        match f(&mut tx) {
            Ok(value) => {
                if tx
                    .cs_pending
                    .as_deref()
                    .is_some_and(|pending| !pending.is_empty())
                {
                    // A caller that turned the importer's batching on and
                    // ended the transaction without the flush would
                    // commit changesets with no parents and no repo
                    // membership: rows that name changesets no read can
                    // reach. The transaction rolls back and refuses, so
                    // the bug surfaces at the caller that forgot the
                    // flush, not as missing rows later.
                    tracing::error!(
                        pending = tx.cs_pending.as_ref().map_or(0, |pending| pending.len()),
                        "the transaction holds changeset rows it never flushed; \
                         the transaction rolls back"
                    );
                    let _ = tx.rollback();
                    return Err(Error::FailedPrecondition(
                        "the transaction holds changeset rows it never flushed".to_owned(),
                    ));
                }
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
    pub(crate) fn write<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
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
        (
            self.inner
                .writer
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            PgWriteWaiter(&self.inner.writers, &self.inner.write_woken),
        )
    }

    /// The walks in flight of this backend's derivations.
    pub(crate) fn derive_flights(&self) -> &crate::derived::DeriveFlights {
        &self.inner.derive
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
        while *waiting > 0 {
            let Some(deadline) = deadline else {
                waiting = self
                    .inner
                    .write_woken
                    .wait(waiting)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            };
            let budget = deadline.remaining();
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
///
/// `pub` with the unreachable-pub allow, like [`SqliteTx`](crate::db::SqliteTx):
/// [`Meta::Tx`] names it, and the trait re-exports through
/// `test_support::meta`.
#[allow(unreachable_pub)]
pub struct PgTx<'a> {
    c: &'a mut PgClient,
    /// The generations this transaction has already read. A changeset's
    /// generation is fixed at insert and never updated, so one read per
    /// transaction is one round trip saved for every later read of the
    /// same changeset - the derived walks and the land ask for the same
    /// few ids again and again.
    gen_cache: std::collections::HashMap<(i64, Vec<u8>), i64>,
    /// The generations this transaction has inserted, by id: the reads of
    /// the same transaction (the derived writes of the commit it just
    /// stored) must see them before anything else does. The generations
    /// are fixed at insert and never updated.
    inserted_gens: std::collections::HashMap<Vec<u8>, i64>,
    /// Whether the importer's batching is on: `insert_changeset` then
    /// writes the changesets row only, and the parents and the repo
    /// membership wait in `cs_pending` for the flush at the transaction's
    /// end, which writes them one statement per 1,000 rows. The
    /// changesets row cannot wait: the derived rows of the same commit
    /// name it by a foreign key that checks at the statement.
    cs_batching: bool,
    /// The changesets the transaction inserted while batching: their
    /// parents and repo membership rows wait for the flush at the
    /// transaction's end, which writes them one statement per 1,000
    /// rows. A crash loses whole commits either way: the rows land in
    /// the caller's transaction, before its commit. The list is shared
    /// with the transaction's reborrows (the backend's transaction
    /// wrapper runs the caller's closure on one), so the commit path of
    /// the original transaction sees what they inserted and refuses to
    /// commit without the flush. `None` on a read transaction, which
    /// inserts nothing.
    cs_pending: Option<&'a mut Vec<(ChangesetId, Vec<ChangesetId>)>>,
    /// The transaction was ended by `commit` or `rollback`; a drop then
    /// leaves the client alone.
    ended: bool,
}

impl<'a> PgTx<'a> {
    /// Begins one transaction on the client, sharing `pending` with the
    /// caller, which flushes it and refuses to commit while it holds rows.
    fn begin(
        c: &'a mut PgClient,
        pending: &'a mut Vec<(ChangesetId, Vec<ChangesetId>)>,
    ) -> Result<PgTx<'a>> {
        c.execute("BEGIN", &[])?;
        Ok(Self {
            c,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            cs_pending: Some(pending),
            ended: false,
        })
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
        // Three statements, not one string: the client prepares its
        // queries, and Postgres takes no second command in a prepared
        // statement (E42601).
        c.execute(
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            &[],
        )?;
        // The transaction exists from here on: a SET LOCAL that fails
        // drops this `PgTx`, and its Drop rolls the transaction back, so
        // the pooled client never stays inside one whose statements no
        // longer run.
        let tx = Self {
            c,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            cs_pending: None,
            ended: false,
        };
        tx.c.execute(
            &format!("SET LOCAL statement_timeout = {}", statement.as_millis()),
            &[],
        )?;
        tx.c.execute(
            &format!(
                "SET LOCAL idle_in_transaction_session_timeout = {}",
                idle.as_millis()
            ),
            &[],
        )?;
        Ok(tx)
    }

    /// One transaction over the same client, for a caller that wraps this
    /// one. It ends nothing: `commit` and `rollback` stay with the original.
    pub(crate) fn reborrow(&mut self) -> PgTx<'_> {
        PgTx {
            c: self.c,
            gen_cache: std::collections::HashMap::new(),
            inserted_gens: std::collections::HashMap::new(),
            cs_batching: false,
            // The reborrows share the pending list with the original, so
            // its commit path sees what they inserted.
            cs_pending: self.cs_pending.as_deref_mut(),
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
    /// The URL's `search_path` names no existing schema: the database
    /// answered, only the schema is missing. A refusal, not an
    /// unreachable: no retry can fix the URL.
    NoSchema,
    /// The database could not be reached, or refused the schema
    /// statements: nothing is known about the lock.
    Unreachable,
}

struct Setup {
    schema: Option<String>,
    /// Whether this session takes the one-server lock of the schema it
    /// lands in (an exclusive writer; None-equivalent for a reader, an
    /// import).
    exclusive: bool,
    outcome: SetupOutcome,
}

impl Setup {
    /// Applies the setup to a fresh client. A failed setup leaves the
    /// statements failed until a reconnect applies it again.
    fn new(schema: Option<&str>, exclusive: bool, client: &mut Client) -> Self {
        let mut setup = Self {
            schema: schema.map(str::to_owned),
            exclusive,
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
        if self.exclusive {
            // What the connection actually writes into, whatever the URL
            // spelled: a plain URL and one that rides
            // `--search_path=public` land in the same schema and must
            // fence each other, and schemas ride separately.
            let Ok(row) = client.query_one("SELECT current_schema()::text", &[]) else {
                return SetupOutcome::Unreachable;
            };
            // A search_path whose members all miss answers NULL: the
            // connection would create nothing, and no lock names it.
            // The database answered, so this is a URL mistake, not an
            // unreachable database: refuse with the fix.
            let Some(current) = row.get::<_, Option<String>>(0) else {
                return SetupOutcome::NoSchema;
            };
            if !server_lock_raw(client, schema_lock_key(&current)) {
                // The database answered: another session holds the lock.
                return SetupOutcome::LockRefused;
            }
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

/// The statements this process has run against Postgres since the last
/// reset. A test hook: the land round-trip guard reads the delta around
/// one land and fails the run when it grows past the budget. The count
/// lives on the worker thread's side, so the read crosses no lock; the
/// guard's binary runs one measuring test, so a process-wide total is
/// one land's count.
#[cfg(any(test, feature = "test-hooks"))]
static TEST_STATEMENTS: AtomicU64 = AtomicU64::new(0);

/// The SQL of the statements run since the last reset, for a test that
/// wants the kinds and not just the count.
#[cfg(any(test, feature = "test-hooks"))]
static TEST_STATEMENT_LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// The SQL of the statements run since the last reset.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_statement_log() -> Vec<String> {
    TEST_STATEMENT_LOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// The statement count of this process since the last reset.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_statement_count() -> u64 {
    TEST_STATEMENTS.load(Ordering::Relaxed)
}

/// Sets the statement count back to zero.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_reset_statement_count() {
    TEST_STATEMENTS.store(0, Ordering::Relaxed);
    TEST_STATEMENT_LOG
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

/// Runs one statement of a job on the client.
fn run_statement(client: &mut Client, job: &Job) -> Result<Reply> {
    #[cfg(any(test, feature = "test-hooks"))]
    {
        TEST_STATEMENTS.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut log) = TEST_STATEMENT_LOG.try_lock() {
            let sql: String = job.sql.chars().take(90).collect();
            log.push(sql.replace('\n', " "));
        }
    }
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
    if matches!(setup.outcome, SetupOutcome::NoSchema) {
        return Error::FailedPrecondition(
            "the URL's search_path names no existing schema; create it first".into(),
        );
    }
    if matches!(setup.outcome, SetupOutcome::Unreachable) {
        return Error::Storage(Storage::busy(std::io::Error::other(
            "the database was unreachable at setup",
        )));
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
        "another server holds this store"
    }
}

/// Reconnects a dead client, applies the setup again, and hands the new
/// setup back: the caller folds it into `setup_held` and the lost
/// transaction state.
fn reconnect(client: &mut Client, conn: &Conn) -> Result<Setup> {
    let fresh = connect_on(&conn.url, conn.ca.as_deref(), &conn.options)?;
    *client = fresh;
    Ok(Setup::new(conn.schema.as_deref(), conn.exclusive, client))
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
        // rolls no transaction back that never opened. A BEGIN that ran
        // after a reconnect opens a real one too: the lost transaction
        // is gone with the old session, and its caller cannot commit it
        // through a new BEGIN, whose statements belong to the client's
        // next holder.
        if sql.starts_with("BEGIN") && ok {
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
    /// Whether this session re-takes the one-server lock after every
    /// reconnect (an exclusive writer; a reader takes none).
    exclusive: bool,
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

/// Connects one client of `url`, with libpq `options` added. The connect
/// runs on a plain thread: `postgres` drives its own runtime, which must
/// not start inside the server's tokio one.
fn connect_with(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
) -> Result<PgClient> {
    spawn_client(url, ca, options, schema, false, None)
}

/// Undoes the identity a failed restore's open minted in the database:
/// a fresh store names no store, and the next open mints again. The
/// tables the migration made stay (they are empty, and the next open's
/// migration is idempotent); only the name goes.
pub(crate) fn reset_identity(url: &str, ca: Option<&Path>) -> Result<()> {
    connect_with(url, ca, "", None)?.batch("DELETE FROM store_uuid WHERE one = 1")
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
    fence: Arc<Fence>,
) -> Result<PgClient> {
    // Whether the lock was given is decided at the spawn: one probe job
    // brings a refusal here, to the open, so an operator whose second
    // server starts against a taken database sees it now, and not in the
    // errors of the first call a client makes.
    let client = spawn_client(url, ca, options, schema, true, Some(fence))?;
    client.query_opt("SELECT 1", &[])?;
    Ok(client)
}

/// Puts one client on its worker thread. An exclusive client re-takes
/// the one-server lock after every reconnect, and its loss trips the
/// fence.
fn spawn_client(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
    exclusive: bool,
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
            exclusive,
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

/// A database error that a caller may retry maps to a busy [`Storage`]:
/// the importer retries a batch on it, and the gRPC status becomes
/// UNAVAILABLE. The rest keep the engine's error as the source.
fn map_pg_error(e: postgres::Error) -> Error {
    let code = e
        .code()
        .map(postgres::error::SqlState::code)
        .map(str::to_owned);
    pg_error_of(code.as_deref(), e)
}

/// The mapping on one SQLSTATE, split from [`map_pg_error`] so a unit
/// test can hand it a code: the `postgres` crate's error type cannot be
/// built with a chosen one.
fn pg_error_of(code: Option<&str>, e: postgres::Error) -> Error {
    // A connection the database dropped is as retryable as a busy one.
    if e.is_closed() {
        return Error::Storage(Storage::busy(e));
    }
    match code {
        // Deadlock, serialization failure, an administrative shutdown, a
        // database that starts up, a lock another writer holds.
        Some(code)
            if matches!(code, "40P01" | "40001" | "57P01" | "57P03" | "55P03")
                || code.starts_with("08") =>
        {
            tracing::warn!("the database is busy: {e}");
            Error::Storage(Storage::busy(e))
        }
        // The lock or the statement waited past its clock, as a SQLite
        // write does past its busy timeout: the caller has been at it
        // too long, and the client retries past its own deadline.
        Some("57014") => {
            tracing::warn!("the database gave the statement up: {e}");
            Error::DeadlineExceeded("the database kept the caller waiting".to_owned())
        }
        // A violation the code handles (a duplicate key, a check, a
        // foreign key) is one answer among many: it logs quietly.
        Some(code) if code.starts_with("23") => {
            tracing::debug!("the database refused a statement: {e}");
            Error::Storage(Storage::internal(e))
        }
        // Out of space: the same shortage, wherever the metadata lives.
        // 53100 is the database's one disk-full code - errcode_for_file_access
        // maps ENOSPC to it for every file, temp files included (elog.c).
        // The other codes of the same classes stay internal on purpose:
        // 53200 is out of MEMORY (the fixed reply would name a disk and
        // lie), 53000 is the class's catch-all (too vague to promise a
        // disk answer), 53400 is a configured cap - temp_file_limit, or
        // the replication slots - the same refusal every time, so a
        // retry-later reply tells the client to retry what cannot
        // succeed - and 58030 is an io_error - a failing device is the
        // EIO row's internal, not a space shortage.
        Some("53100") => {
            tracing::error!("the server is out of disk space: {e}");
            Error::Storage(Storage::disk_full(e))
        }
        // A statement the database never saw: the connection could not
        // be made or was cut on the way. No SQLSTATE names it, and a
        // client retries it as it retries a busy one.
        None => {
            tracing::warn!("the database is unreachable: {e}");
            Error::Storage(Storage::busy(e))
        }
        // The rest keep the engine's error as the source, and it reaches
        // the log in full: a bug of this backend, such as a bad parameter,
        // must not look busy, or the importer retries it and the log says
        // nothing but "busy".
        _ => {
            tracing::warn!("the database refused a statement: {e}");
            Error::Storage(Storage::internal(e))
        }
    }
}

/// The TLS connector of a database whose host is off this machine: the
/// certificate it presents must be one `ca` signed. There is no mode that
/// accepts any certificate.
fn make_tls(ca: &Path) -> Result<MakeRustlsConnect> {
    use tokio_rustls::rustls::pki_types::{CertificateDer, pem::PemObject as _};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, crypto::CryptoProvider};

    // The CA must stay whole: a swapped one changes whom the database
    // connection trusts, so it is read through one checked descriptor.
    let ca_bytes = scion_proto::runtime::auth::read_shared_file(ca)
        .map_err(|e| Error::InvalidArgument(format!("cannot read the database CA: {e}")))?;
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(&ca_bytes) {
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
/// build. Returns the `(from, to)` versions when it migrated, and `None`
/// when the database was at this build's version already. A database of
/// version 0 was no schema at all: the first open creates one, and a
/// creation moves nothing.
#[allow(clippy::needless_pass_by_ref_mut)]
fn migrate(client: &mut PgClient) -> Result<Option<(u32, u32)>> {
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
fn migrate_under_lock(client: &mut PgClient) -> Result<Option<(u32, u32)>> {
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
    if version == PG_SCHEMA_VERSION {
        return Ok(None);
    }
    tracing::info!(
        from_version = version,
        to_version = PG_SCHEMA_VERSION,
        "migrating the metadata schema"
    );
    for (n, sql) in PG_MIGRATIONS.iter().enumerate() {
        let n = u32::try_from(n + 1).expect("a schema version fits in a u32");
        if n <= version {
            continue;
        }
        // One transaction per migration, with its version bump: a
        // migration that fails rolls its own statements back, and the
        // versions before it stand.
        let step = std::time::Instant::now();
        let mut pending = Vec::new();
        let mut tx = PgTx::begin(client, &mut pending)?;
        tx.c.batch(&format!(
            "{sql}\nINSERT INTO schema_migrations (version, name) \
             VALUES ({}, 'postgres schema')",
            i64::from(n)
        ))?;
        tx.commit()?;
        tracing::info!(
            from_version = n - 1,
            to_version = n,
            elapsed_ms = u64::try_from(step.elapsed().as_millis()).unwrap_or(u64::MAX),
            "the metadata schema migrated one step"
        );
    }
    tracing::info!(
        from_version = version,
        to_version = PG_SCHEMA_VERSION,
        "the metadata migration is done"
    );
    // Version 0 was no schema at all: the first open creates one, and a
    // creation moves nothing.
    let migrated = if version == 0 {
        None
    } else {
        Some((version, PG_SCHEMA_VERSION))
    };
    Ok(migrated)
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
        let parents: Vec<Vec<u8>> = cs
            .parents()
            .iter()
            .map(|parent| id_bytes(parent.as_bytes()))
            .collect();
        // The generations this transaction already holds answer the
        // parents it inserted itself, so a batch's chain reads the
        // database only for the parents it did not insert, usually the
        // first commit's. The read carries only those misses: a parent
        // this transaction inserted has no membership row yet (the flush
        // holds it back), and the read's join with repo_changesets would
        // drop it.
        let mut misses: Vec<Vec<u8>> = Vec::new();
        for parent in cs.parents() {
            let key = id_bytes(parent.as_bytes());
            match self.gen_cache.get(&(repo.0, key.clone())) {
                Some(cached) => parent_gen = parent_gen.max(*cached),
                None => {
                    if !misses.contains(&key) {
                        misses.push(key);
                    }
                }
            }
        }
        if !misses.is_empty() {
            // One round trip for every parent this transaction did not
            // insert, however many a merge has.
            let rows = self.c.query(
                "SELECT id, gen FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = ANY($2)",
                &[P::i64(repo.0), P::bytea_array(misses.clone())],
            )?;
            let gens: std::collections::HashMap<Vec<u8>, i64> = rows
                .into_iter()
                .map(|row| (row.get::<_, Vec<u8>>(0), row.get::<_, i64>(1)))
                .collect();
            for (parent, key) in cs.parents().iter().zip(&parents) {
                // A repeated parent reads the same row twice; `get`, not
                // `remove`, so parents [X, X] store like they did when
                // each parent was its own statement.
                let parent_gen_i = match self.gen_cache.get(&(repo.0, key.clone())) {
                    Some(cached) => *cached,
                    None => *gens
                        .get(key)
                        .ok_or_else(|| Error::NotFound(format!("changeset {parent}")))?,
                };
                parent_gen = parent_gen.max(parent_gen_i);
                self.gen_cache.insert((repo.0, key.clone()), parent_gen_i);
            }
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
        let generation = parent_gen + 1;
        self.inserted_gens
            .insert(id_bytes(id.as_bytes()), generation);
        self.gen_cache
            .insert((repo.0, id_bytes(id.as_bytes())), generation);
        // While the import's batching is on, the parents and the repo
        // membership of this transaction's changesets wait for the
        // flush, which writes them one statement per 1,000 rows. The
        // changesets row itself lands per commit: the derived rows of
        // the same commit name it by an immediate foreign key. When the
        // last per-commit writer of a commit moves into the batch, this
        // row moves with it, and every table writes at the end in the
        // order the foreign keys read: changesets, changeset_parents,
        // repo_changesets, derived, history, git_import.
        if self.cs_batching {
            if let Some(pending) = self.cs_pending.as_deref_mut() {
                pending.push((id, cs.parents().to_vec()));
            }
            return Ok(id);
        }
        // Every parent as one statement, however many a merge has: the
        // row number is the parent's place in the array. SQLite numbers
        // the rows from zero, so the ordinality (one-based) shifts down.
        if !parents.is_empty() {
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent)
                 SELECT $1, ord - 1, parent
                 FROM unnest($2::bytea[]) WITH ORDINALITY AS parents(parent, ord)
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytes(&id_bytes(id.as_bytes())),
                    P::bytea_array(parents.clone()),
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

    /// Turns the import's changeset batching on or off. While it is on,
    /// `insert_changeset` writes the changesets row only, and the parents
    /// and the repo membership of this transaction's changesets wait for
    /// `flush_changeset_rows`.
    fn set_changeset_batching(&mut self, on: bool) {
        self.cs_batching = on;
        if !on && let Some(pending) = self.cs_pending.as_deref_mut() {
            pending.clear();
        }
    }

    /// Writes the parents and the repo membership of the changesets the
    /// transaction inserted while batching, one statement per 1,000 rows
    /// per table, and returns how many statements it wrote. The parents'
    /// row numbers follow the per-commit insert's contract (the parent's
    /// place, counting from one, as `WITH ORDINALITY` numbers it).
    ///
    /// # Errors
    ///
    /// Database errors.
    fn flush_changeset_rows(&mut self, repo: RepoId) -> Result<()> {
        const CHUNK: usize = 1_000;
        let Some(pending) = self.cs_pending.as_deref_mut() else {
            return Ok(());
        };
        if pending.is_empty() {
            return Ok(());
        }
        let mut parent_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();
        let mut member_rows: Vec<(i64, Vec<u8>)> = Vec::new();
        for (id, parents) in pending.drain(..) {
            let key = id_bytes(id.as_bytes());
            for (n, parent) in (1_i64..).zip(&parents) {
                parent_rows.push((key.clone(), n, id_bytes(parent.as_bytes())));
            }
            member_rows.push((repo.0, key));
        }
        for chunk in parent_rows.chunks(CHUNK) {
            let css = chunk
                .iter()
                .map(|(cs, _, _)| cs.clone())
                .collect::<Vec<_>>();
            let ns = chunk.iter().map(|(_, n, _)| *n).collect::<Vec<_>>();
            let parents = chunk
                .iter()
                .map(|(_, _, parent)| parent.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent)
                 SELECT * FROM unnest($1::bytea[], $2::int8[], $3::bytea[])
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytea_array(css),
                    P::i64_array(ns),
                    P::bytea_array(parents),
                ],
            )?;
        }
        for chunk in member_rows.chunks(CHUNK) {
            let repos = chunk.iter().map(|(repo, _)| *repo).collect::<Vec<_>>();
            let css = chunk.iter().map(|(_, cs)| cs.clone()).collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO repo_changesets (repo_id, cs)
                 SELECT * FROM unnest($1::int8[], $2::bytea[])
                 ON CONFLICT (repo_id, cs) DO NOTHING",
                &[P::i64_array(repos), P::bytea_array(css)],
            )?;
        }
        Ok(())
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

    /// Adds every blob as one statement, however many the changeset
    /// puts.
    fn add_repo_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }
        let ids: Vec<Vec<u8>> = blobs.iter().map(|blob| id_bytes(blob.as_bytes())).collect();
        self.c.execute(
            "INSERT INTO repo_blobs (repo_id, blob)
             SELECT $1, x FROM unnest($2::bytea[]) AS x
             ON CONFLICT (repo_id, blob) DO NOTHING",
            &[P::i64(repo.0), P::bytea_array(ids)],
        )?;
        Ok(())
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

    /// The blobs of the list that the repo does not have. One statement
    /// per 1,000 ids, the chunk size of [`add_repo_blobs`](Self::add_repo_blobs):
    /// a changeset may name 100,000 changes, and one query per blob
    /// would cost one round trip each.
    fn repo_missing_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<Vec<BlobId>> {
        const CHUNK: usize = 1_000;
        let mut missing = Vec::new();
        for chunk in blobs.chunks(CHUNK) {
            let ids: Vec<Vec<u8>> = chunk.iter().map(|blob| id_bytes(blob.as_bytes())).collect();
            let rows = self.c.query(
                "SELECT x FROM unnest($2::bytea[]) AS x
                 WHERE NOT EXISTS (
                     SELECT 1 FROM repo_blobs WHERE repo_id = $1 AND blob = x
                 )",
                &[P::i64(repo.0), P::bytea_array(ids)],
            )?;
            for row in rows {
                let bytes: Vec<u8> = row.get(0);
                missing.push(BlobId::from_bytes(
                    bytes
                        .try_into()
                        .expect("the stored blob column holds 32-byte ids"),
                ));
            }
        }
        Ok(missing)
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

    /// One page of the first-parent chain, one statement: the recursive
    /// term follows the indexed first-parent rows (cs, n = 0), so every
    /// hop is one index lookup, and the join carries the generation and
    /// the body. Every fetched changeset is an ancestor of `start`, so
    /// the repo check rides the start row, as `get_changeset` checks it.
    fn first_parent_page(
        &mut self,
        repo: RepoId,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
        let rows = self.c.query(
            "WITH RECURSIVE walk(id, depth) AS (
                     SELECT rc.cs, 0::bigint FROM repo_changesets rc
                     WHERE rc.repo_id = $1 AND rc.cs = $2
                     UNION ALL
                     SELECT cp.parent, w.depth + 1
                     FROM walk w
                     JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                     WHERE w.depth < $3
                 )
                 SELECT c.id, c.gen, c.body FROM walk
                 JOIN changesets c ON c.id = walk.id
                 ORDER BY walk.depth",
            &[
                P::i64(repo.0),
                P::bytes(&id_bytes(start.as_bytes())),
                P::i64(limit),
            ],
        )?;
        if rows.is_empty() {
            return Err(Error::NotFound(format!("changeset {start}")));
        }
        rows.into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get(1), row.get(2)))
            })
            .collect()
    }

    /// The generation of a changeset that `repo` may see.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have it.
    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64> {
        let key = (repo.0, id_bytes(id.as_bytes()));
        if let Some(cached) = self.gen_cache.get(&key) {
            return Ok(*cached);
        }
        let found = self
            .c
            .query_opt(
                "SELECT gen FROM changesets JOIN repo_changesets ON cs = id
                 WHERE repo_id = $1 AND id = $2",
                &[P::i64(repo.0), P::bytes(&key.1)],
            )?
            .map(|row| row.get::<_, i64>(0))
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")));
        if let Ok(found_gen) = &found {
            self.gen_cache.insert(key, *found_gen);
        }
        found
    }

    /// The generation of a changeset, in any repo. For derived data.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if no changeset has this ID.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64> {
        if let Some(cached) = self.inserted_gens.get(&id_bytes(id.as_bytes())) {
            return Ok(*cached);
        }
        self.c
            .query_opt(
                "SELECT gen FROM changesets WHERE id = $1",
                &[P::bytes(&id_bytes(id.as_bytes()))],
            )?
            .map(|row| row.get::<_, i64>(0))
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    /// The first parent of a changeset, or `None` for a root.
    #[cfg(any(test, feature = "test-hooks"))]
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

    /// The parents of up to `DERIVED_BATCH` merges, in the order asked,
    /// in one statement: the ids travel as one `bytea[]`, and the reply
    /// has at most one row per parent. A merge asked twice gets its
    /// parents twice, once per ask. The generation check is the one
    /// [`MetaTx::parents`] makes, with the asked merge's generation.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] if a parent's generation is not below its
    /// merge's.
    fn parents_many(
        &mut self,
        merges: &[(ChangesetId, i64)],
    ) -> Result<Vec<Vec<(ChangesetId, i64)>>> {
        let ids = merges
            .iter()
            .map(|(id, _)| id_bytes(id.as_bytes()))
            .collect::<Vec<_>>();
        let rows = self.c.query(
            "SELECT cs, parent, gen FROM changeset_parents JOIN changesets ON id = parent
             WHERE cs = ANY($1) ORDER BY cs, n",
            &[P::bytea_array(ids)],
        )?;
        let mut out: Vec<Vec<(ChangesetId, i64)>> = vec![Vec::new(); merges.len()];
        // One id can be asked twice: two histories reach the same merge.
        let mut asked: HashMap<&[u8], Vec<usize>> = HashMap::new();
        for (i, (id, _)) in merges.iter().enumerate() {
            asked.entry(id.as_bytes()).or_default().push(i);
        }
        for row in &rows {
            let cs: Vec<u8> = row.get(0);
            let parent: Vec<u8> = row.get(1);
            let parent_gen: i64 = row.get(2);
            let parent = to_id(&parent)?;
            for &i in &asked[cs.as_slice()] {
                let (id, id_gen) = merges[i];
                if parent_gen >= id_gen {
                    return Err(Error::Corrupt(format!(
                        "changeset {id} has generation {id_gen}, its parent {parent} {parent_gen}"
                    )));
                }
                out[i].push((parent, parent_gen));
            }
        }
        Ok(out)
    }

    /// Whether `ancestor` is `descendant` or reachable from it through
    /// parents, within a bounded walk.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not have either changeset.
    /// [`Error::ResourceExhausted`] if the walk needs more than `max_visits`
    /// changesets.
    #[cfg(any(test, feature = "test-hooks"))]
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
                return Err(test_hooks::too_deep(max_visits));
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

    fn restore_ref(&mut self, repo: RepoId, name: &str, target: &[u8], version: i64) -> Result<()> {
        self.c.execute(
            "INSERT INTO refs (repo_id, name, target, version) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (repo_id, name) DO UPDATE SET target = $3, version = $4",
            &[
                P::i64(repo.0),
                P::text(name),
                P::bytes(target),
                P::i64(version),
            ],
        )?;
        Ok(())
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

    fn get_derived_batch(
        &mut self,
        kind: &str,
        version: u32,
        ids: &[ChangesetId],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        // One statement for the whole batch: the ids travel as one bytea
        // array, the reply has at most one row per id. The caller's read
        // transaction holds for the batch, so a land between two ids of the
        // batch cannot mix two states.
        let version = i64::from(version);
        let cs = ids
            .iter()
            .map(|id| id_bytes(id.as_bytes()))
            .collect::<Vec<_>>();
        let rows = self.c.query(
            "SELECT cs, value FROM derived
             WHERE kind = $1 AND version = $2 AND cs = ANY($3)",
            &[P::text(kind), P::i64(version), P::bytea_array(cs)],
        )?;
        let mut found = rows
            .into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get::<_, Option<Vec<u8>>>(1)))
            })
            .collect::<Result<std::collections::HashMap<_, _>>>()?;
        ids.iter()
            .map(|id| Ok(found.remove(id).flatten()))
            .collect()
    }

    fn get_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, Vec<u8>)>> {
        // One statement for the whole chain: the recursive term follows the
        // indexed first-parent rows (cs, n = 0), so every hop is one index
        // lookup, and the join keeps only the rows that carry a stored
        // value. Every fetched changeset is an ancestor of `start`, so no
        // repo filter is needed.
        let version = i64::from(version);
        let rows = self.c.query(
            "WITH RECURSIVE walk(id, depth) AS (
                 SELECT $1::bytea, 0::bigint
                 UNION ALL
                 SELECT cp.parent, w.depth + 1
                 FROM walk w
                 JOIN changeset_parents cp ON cp.cs = w.id AND cp.n = 0
                 WHERE w.depth < $2
             )
             SELECT d.cs, d.value FROM walk
             JOIN derived d ON d.cs = walk.id
             WHERE d.kind = $3 AND d.version = $4",
            &[
                P::bytes(id_bytes(start.as_bytes()).as_slice()),
                P::i64(limit),
                P::text(kind),
                P::i64(version),
            ],
        )?;
        rows.into_iter()
            .map(|row| {
                let cs: Vec<u8> = row.get(0);
                Ok((to_id(&cs)?, row.get(1)))
            })
            .collect()
    }

    /// The test seed, in batches: the changesets, their parents, the repo
    /// membership, and the derived rows, 1,000 rows per statement.
    /// Parents must come before their children in `css`, so the
    /// generations compute in order; the derived rows must find an empty
    /// schema, like the trait doc says.
    #[cfg(any(test, feature = "test-hooks"))]
    fn bulk_seed(
        &mut self,
        repo: RepoId,
        css: &[scion_core::Changeset],
        derived: &[(&'static str, u32, ChangesetId, Vec<u8>)],
    ) -> Result<Vec<ChangesetId>> {
        let mut gens: std::collections::HashMap<Vec<u8>, i64> = std::collections::HashMap::new();
        let mut ids = Vec::with_capacity(css.len());
        let mut cs_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();
        let mut parent_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();
        let mut member_rows: Vec<(i64, Vec<u8>)> = Vec::new();
        for cs in css {
            let (body, id) = cs.encode_with_id();
            let key = id_bytes(id.as_bytes());
            let mut parent_gen = 0;
            for parent in cs.parents() {
                let parent_gen_i =
                    gens.get(&id_bytes(parent.as_bytes()))
                        .copied()
                        .ok_or_else(|| {
                            Error::Corrupt(format!("the seed's {parent} comes after its child"))
                        })?;
                parent_gen = parent_gen.max(parent_gen_i);
            }
            let generation = 1 + parent_gen;
            gens.insert(key.clone(), generation);
            for (n, parent) in cs.parents().iter().enumerate() {
                let n = i64::try_from(n).map_err(|_| {
                    Error::Corrupt(format!(
                        "the seed's {id} holds more parents than i64 counts"
                    ))
                })?;
                parent_rows.push((key.clone(), n, id_bytes(parent.as_bytes())));
            }
            member_rows.push((repo.0, key.clone()));
            cs_rows.push((key, generation, body));
            ids.push(id);
        }
        self.insert_seed_changesets(&cs_rows)?;
        self.insert_seed_parents(&parent_rows)?;
        self.insert_seed_members(&member_rows)?;
        let derived_rows: Vec<(&str, i64, Vec<u8>, Vec<u8>)> = derived
            .iter()
            .map(|(kind, version, cs, value)| {
                (
                    *kind,
                    i64::from(*version),
                    id_bytes(cs.as_bytes()),
                    value.clone(),
                )
            })
            .collect();
        self.insert_seed_derived(&derived_rows)?;
        Ok(ids)
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

    /// Stores pre-encoded trees, whose ids are their content hashes, as
    /// one statement however many the chain built.
    fn put_trees(&mut self, trees: &[(TreeId, Vec<u8>)]) -> Result<()> {
        if trees.is_empty() {
            return Ok(());
        }
        let ids: Vec<Vec<u8>> = trees
            .iter()
            .map(|(id, _)| id_bytes(id.as_bytes()))
            .collect();
        let bodies: Vec<Vec<u8>> = trees.iter().map(|(_, body)| body.clone()).collect();
        self.c.execute(
            "INSERT INTO trees (id, body)
             SELECT * FROM unnest($1::bytea[], $2::bytea[])
             ON CONFLICT (id) DO NOTHING",
            &[P::bytea_array(ids), P::bytea_array(bodies)],
        )?;
        Ok(())
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

    /// The stored bytes of the trees the ids name, in the order asked.
    /// One statement per 1,000 ids: the ids travel as one bytea array.
    fn get_trees(&mut self, ids: &[TreeId]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(crate::meta::DERIVED_BATCH) {
            let keys: Vec<Vec<u8>> = chunk.iter().map(|id| id_bytes(id.as_bytes())).collect();
            let rows = self.c.query(
                "SELECT id, body FROM trees WHERE id = ANY($1)",
                &[P::bytea_array(keys.clone())],
            )?;
            let mut bodies: std::collections::HashMap<Vec<u8>, Vec<u8>> = rows
                .into_iter()
                .map(|row| (row.get::<_, Vec<u8>>(0), row.get::<_, Vec<u8>>(1)))
                .collect();
            for key in keys {
                out.push(bodies.remove(&key));
            }
        }
        Ok(out)
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
                // client can always finish what it started. The count and
                // the insert need no lock of their own: every write
                // transaction of the schema holds the schema's one advisory
                // key (see `commit`) until it commits, so two lands that
                // record at the same time, even from two processes, run
                // one after the other, and the second counts the first
                // one's row. SQLite gets the same from its one writer.
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
    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<StaleLand>> {
        let rows = self.c.query(
            "SELECT l.repo_id, l.land_id, l.target_ref, l.caller, l.request_key, r.name
             FROM lands l JOIN repos r ON r.repo_id = l.repo_id
             WHERE l.state = 'pending' AND l.created_at > 0
                 AND l.created_at < now_epoch() - $1",
            &[P::i64(stale_secs)],
        )?;
        Ok(rows
            .iter()
            .map(|row| StaleLand {
                repo: RepoId(row.get(0)),
                land_id: row.get(1),
                target_ref: row.get(2),
                caller: row.get(3),
                request_key: row.get(4),
                repo_name: row.get(5),
            })
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
            moved: false,
            land_id,
            state: LandState::parse(&state)?,
            result,
            previous: None,
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
            "UPDATE lands SET state = $3, result_cs = $4, detail = $5,
                 conflict_paths = $6, finished_at = now_epoch()
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
            moved: false,
            land_id,
            state,
            result,
            previous: None,
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

    /// Records that the git commits became the changesets. One statement
    /// per 1,000 pairs: the ids travel as one bytea array each, and the
    /// conflict rule is the single row's, so a repeated pair is skipped.
    fn put_git_imports(&mut self, repo: RepoId, pairs: &[(ObjectId, ChangesetId)]) -> Result<()> {
        for chunk in pairs.chunks(crate::meta::DERIVED_BATCH) {
            let oids: Vec<Vec<u8>> = chunk
                .iter()
                .map(|(oid, _)| oid.as_slice().to_vec())
                .collect();
            let css: Vec<Vec<u8>> = chunk
                .iter()
                .map(|(_, cs)| id_bytes(cs.as_bytes()))
                .collect();
            self.c.execute(
                "INSERT INTO git_import (repo_id, git_algo, git_oid, cs)
                 SELECT $1, $2, u.oid, u.cs
                 FROM unnest($3::bytea[], $4::bytea[]) AS u(oid, cs)
                 ON CONFLICT (repo_id, git_algo, git_oid) DO NOTHING",
                &[
                    P::i64(repo.0),
                    P::i64(SHA1),
                    P::bytea_array(oids),
                    P::bytea_array(css),
                ],
            )?;
        }
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
    /// Every repo with its name, in ID order.
    fn repos(&mut self) -> Result<Vec<(RepoId, String)>> {
        let rows = self
            .c
            .query("SELECT repo_id, name FROM repos ORDER BY repo_id", &[])?;
        Ok(rows
            .iter()
            .map(|row| (RepoId(row.get(0)), row.get(1)))
            .collect())
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

    fn repo_changesets_with_bodies(
        &mut self,
        repo: RepoId,
    ) -> Result<Vec<(Vec<u8>, i64, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT c.id, c.gen, c.body FROM repo_changesets r \
             JOIN changesets c ON c.id = r.cs WHERE r.repo_id = $1 ORDER BY c.gen, c.id",
            &[P::i64(repo.0)],
        )?;
        Ok(rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }

    fn list_refs(&mut self, repo: RepoId) -> Result<Vec<(String, Vec<u8>, i64)>> {
        let rows = self.c.query(
            "SELECT name, target, version FROM refs WHERE repo_id = $1 ORDER BY name",
            &[P::i64(repo.0)],
        )?;
        Ok(rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }

    fn pending_lands(&mut self, repo: RepoId) -> Result<Vec<crate::meta::PendingLand>> {
        let rows = self.c.query(
            "SELECT caller, request_key, target_ref, input_cs FROM lands \
             WHERE repo_id = $1 AND state = 'pending' ORDER BY land_id",
            &[P::i64(repo.0)],
        )?;
        Ok(rows
            .iter()
            .map(|row| crate::meta::PendingLand {
                caller: row.get(0),
                request_key: row.get(1),
                target_ref: row.get(2),
                input_cs: row.get(3),
            })
            .collect())
    }

    /// The bodies of the changesets the ids name, as `(id, body)` for
    /// every id that has a stored body. One statement: the ids travel as
    /// one bytea array, and the reply has at most one row per id.
    fn stored_changeset_bodies(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.c.query(
            "SELECT id, body FROM changesets WHERE id = ANY($1)",
            &[P::bytea_array(ids.to_vec())],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
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

    /// Removes the rows that name any of `blobs` for `repo`. One
    /// statement: the blobs travel as one bytea array.
    fn delete_repo_blobs(&mut self, repo: RepoId, blobs: &[Vec<u8>]) -> Result<()> {
        self.c.execute(
            "DELETE FROM repo_blobs WHERE repo_id = $1 AND blob = ANY($2)",
            &[P::i64(repo.0), P::bytea_array(blobs.to_vec())],
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

/// The statement helpers of [`PgTx::bulk_seed`], outside the trait impl:
/// each inserts its rows in batches of 1,000 through one `unnest`.
#[cfg(any(test, feature = "test-hooks"))]
impl PgTx<'_> {
    fn insert_seed_changesets(&mut self, rows: &[(Vec<u8>, i64, Vec<u8>)]) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let ids = chunk
                .iter()
                .map(|(id, _, _)| id.clone())
                .collect::<Vec<_>>();
            let gens = chunk
                .iter()
                .map(|(_, generation, _)| *generation)
                .collect::<Vec<_>>();
            let bodies = chunk
                .iter()
                .map(|(_, _, body)| body.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO changesets (id, gen, body)
                 SELECT * FROM unnest($1::bytea[], $2::int8[], $3::bytea[])
                 ON CONFLICT (id) DO NOTHING",
                &[
                    P::bytea_array(ids),
                    P::i64_array(gens),
                    P::bytea_array(bodies),
                ],
            )?;
        }
        Ok(())
    }

    fn insert_seed_parents(&mut self, rows: &[(Vec<u8>, i64, Vec<u8>)]) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let css = chunk
                .iter()
                .map(|(cs, _, _)| cs.clone())
                .collect::<Vec<_>>();
            let ns = chunk.iter().map(|(_, n, _)| *n).collect::<Vec<_>>();
            let parents = chunk
                .iter()
                .map(|(_, _, parent)| parent.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO changeset_parents (cs, n, parent)
                 SELECT * FROM unnest($1::bytea[], $2::int8[], $3::bytea[])
                 ON CONFLICT (cs, n) DO NOTHING",
                &[
                    P::bytea_array(css),
                    P::i64_array(ns),
                    P::bytea_array(parents),
                ],
            )?;
        }
        Ok(())
    }

    fn insert_seed_members(&mut self, rows: &[(i64, Vec<u8>)]) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let repos = chunk.iter().map(|(repo, _)| *repo).collect::<Vec<_>>();
            let css = chunk.iter().map(|(_, cs)| cs.clone()).collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO repo_changesets (repo_id, cs)
                 SELECT * FROM unnest($1::int8[], $2::bytea[])
                 ON CONFLICT (repo_id, cs) DO NOTHING",
                &[P::i64_array(repos), P::bytea_array(css)],
            )?;
        }
        Ok(())
    }

    fn insert_seed_derived(
        &mut self,
        rows: &[(&'static str, i64, Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        const CHUNK: usize = 1_000;
        for chunk in rows.chunks(CHUNK) {
            let kinds = chunk.iter().map(|(kind, ..)| *kind).collect::<Vec<_>>();
            let versions = chunk
                .iter()
                .map(|(_, version, ..)| *version)
                .collect::<Vec<_>>();
            let css = chunk
                .iter()
                .map(|(_, _, cs, _)| cs.clone())
                .collect::<Vec<_>>();
            let values = chunk
                .iter()
                .map(|(_, _, _, value)| value.clone())
                .collect::<Vec<_>>();
            self.c.execute(
                "INSERT INTO derived (kind, version, cs, value)
                 SELECT * FROM unnest($1::text[], $2::int8[], $3::bytea[], $4::bytea[])
                 ON CONFLICT (kind, version, cs) DO NOTHING",
                &[
                    P::text_array(kinds),
                    P::i64_array(versions),
                    P::bytea_array(css),
                    P::bytea_array(values),
                ],
            )?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::{Client, NoTls, P, merged_options};
    use crate::backend::old_store;
    use crate::pg::Pg;
    use std::sync::Arc;

    const OWN: &str = "-csynchronous_commit=on -cstatement_timeout=600000 -clock_timeout=5000";

    #[test]
    fn the_urls_options_survive_the_backends_own() {
        assert_eq!(
            merged_options(Some("--search_path=scion_test_1"), OWN),
            format!("--search_path=scion_test_1 {OWN}")
        );
    }

    /// A URL whose `search_path` names no existing schema refuses with
    /// the fix named, not 'unreachable': the database answered, and
    /// only the schema is missing.
    #[test]
    fn a_search_path_that_names_no_schema_refuses_with_the_fix() {
        if !crate::Backend::test_is_postgres() {
            return;
        }
        let url = crate::backend::test_hooks::test_url().unwrap();
        let mut client = Client::connect(&url, NoTls).unwrap();
        let setup = super::Setup::new(Some("scion_test_missing"), true, &mut client);
        assert!(!setup.ok(), "a missing schema cannot hold the setup");
        let error = super::setup_error(&setup, false);
        assert!(
            error
                .to_string()
                .contains("the URL's search_path names no existing schema; create it first"),
            "{error}"
        );
    }

    #[test]
    fn a_53100_database_error_is_disk_full_with_the_fixed_reply() {
        use super::pg_error_of;
        use crate::{Error, StorageClass};
        // A source of the right type with no SQLSTATE of its own: a
        // connect to port 1, which every kernel refuses. The mapping
        // runs on the SQLSTATE the test hands it, not on this source.
        let Err(refused) = Client::connect("postgres://127.0.0.1:1/x", NoTls) else {
            panic!("a connect to port 1 is refused");
        };
        assert!(!refused.is_closed());
        assert!(refused.code().is_none());
        let error = pg_error_of(Some("53100"), refused);
        assert!(
            matches!(&error, Error::Storage(s) if s.class() == StorageClass::DiskFull),
            "{error:?}"
        );
        // The reply the client sees is the fixed one: it names the
        // shortage and the retry, not the server path.
        let status = tonic::Status::from(error);
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "the server is out of disk space; retry later"
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

    /// A Postgres store of schema version 1 opens on this build, migrates
    /// forward through every migration it has not run, and serves what it
    /// held ([#318]). The fixture is the schema of
    /// `migrations/postgres/0001` with the rows an old build's write
    /// paths left: one repo, its root changeset, the blobs it names, and
    /// its ref. The plain open runs migrations 2 to the end under the
    /// schema lock, and the shared asserts of `backend::old_store` run
    /// the rest. Postgres only: it needs the test database.
    #[test]
    fn a_store_of_schema_version_1_opens_upgrades_and_serves() {
        if !crate::Backend::test_is_postgres() {
            return;
        }
        let url = crate::backend::test_hooks::test_url().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let schema = crate::backend::test_hooks::test_schema(dir.path());
        // The blob files, made by this build's writer and copied into the
        // old store's object folder: blob files carry no schema.
        let (blob_a, blob_b) = {
            let scratch = tempfile::tempdir().unwrap();
            let store = crate::store::Store::open(scratch.path()).unwrap();
            let a = store.blobs().put(old_store::A).unwrap();
            let b = store.blobs().put(old_store::B).unwrap();
            for id in [a, b] {
                // The file of `id` sits at objects/blob/<2 hex>/<rest>;
                // the copy must land at the same place in the old store's
                // object folder. A prefix strip would miss: an open may
                // spell the same directory through its canonical path.
                let (_, from) = store.blobs().location(id);
                let hex = id.to_string();
                let to = dir
                    .path()
                    .join("objects")
                    .join("blob")
                    .join(&hex[..2])
                    .join(&hex[2..]);
                std::fs::create_dir_all(to.parent().unwrap()).unwrap();
                std::fs::copy(from, &to).unwrap();
            }
            (a, b)
        };
        // The metadata at version 1: the schema of 0001, the ledger at 1,
        // and the rows an old build's write paths left.
        let cs = old_store::changeset(blob_a, blob_b);
        let (body, id) = cs.encode_with_id();
        {
            let mut db = Client::connect(&url, NoTls).unwrap();
            db.batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path TO {schema}"
            ))
            .unwrap();
            db.batch_execute(super::PG_MIGRATIONS[0]).unwrap();
            // The ledger is not a migration file: an old build's open
            // made it under its lock, at the shape of `migrate_under_lock`.
            db.batch_execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                     version BIGINT PRIMARY KEY,
                     name    TEXT NOT NULL
                 )",
            )
            .unwrap();
            db.batch_execute(
                "INSERT INTO schema_migrations (version, name) \
                 VALUES (1, 'postgres schema')",
            )
            .unwrap();
            db.execute("INSERT INTO repos (repo_id, name) VALUES (1, 'main')", &[])
                .unwrap();
            db.execute(
                "INSERT INTO changesets (id, gen, body) VALUES ($1, 1, $2)",
                &[&P::bytes(&id.as_bytes()[..]), &P::bytes(&body)],
            )
            .unwrap();
            db.execute(
                "INSERT INTO repo_changesets (repo_id, cs) VALUES (1, $1)",
                &[&P::bytes(&id.as_bytes()[..])],
            )
            .unwrap();
            db.execute(
                "INSERT INTO repo_blobs (repo_id, blob) VALUES (1, $1), (1, $2)",
                &[
                    &P::bytes(&blob_a.as_bytes()[..]),
                    &P::bytes(&blob_b.as_bytes()[..]),
                ],
            )
            .unwrap();
            db.execute(
                "INSERT INTO refs (repo_id, name, target, version) \
                 VALUES (1, 'main', $1, 1)",
                &[&P::bytes(&id.as_bytes()[..])],
            )
            .unwrap();
        }
        // A plain open of this build runs the migrations it has not run,
        // under the schema's lock, and serves.
        let pg = Pg::open_test(
            &url,
            &schema,
            2,
            dir.path(),
            crate::backend::Marker::read(dir.path()),
        )
        .unwrap();
        let ledger: i64 = {
            let mut db = Client::connect(&url, NoTls).unwrap();
            db.batch_execute(&format!("SET search_path TO {schema}"))
                .unwrap();
            db.query_one("SELECT max(version) FROM schema_migrations", &[])
                .unwrap()
                .get(0)
        };
        assert_eq!(ledger, i64::from(super::PG_SCHEMA_VERSION));
        let backend = crate::Backend::postgres(pg);
        old_store::assert_serves(&backend, &cs, id, blob_a, blob_b);
    }

    /// The shipped Postgres migrations are frozen, like the SQLite ones
    /// (the twin test in store.rs): a database an old build made was
    /// built from these files as they shipped, and an edit would cut it
    /// off ([#318]).
    #[test]
    fn the_shipped_migrations_stay_as_they_shipped() {
        for (name, file, hex) in [
            (
                "0001_init",
                include_bytes!("../migrations/postgres/0001_init.sql").as_slice(),
                "9f95c671379b0d1c8e2ac2af34d95badca7929d379edcf0f8e8a02572f4a0c63",
            ),
            (
                "0002_view_roots",
                include_bytes!("../migrations/postgres/0002_view_roots.sql").as_slice(),
                "483a299895752535dbe3856ed496894687f4840b5edfcc2fdc24b6eece02dff2",
            ),
            (
                "0003_view_trees",
                include_bytes!("../migrations/postgres/0003_view_trees.sql").as_slice(),
                "3935e99d760f8848860acf33aeb33633a288b03aadd856a65cbe474d88989716",
            ),
            (
                "0004_store_uuid",
                include_bytes!("../migrations/postgres/0004_store_uuid.sql").as_slice(),
                "6ac9d420e9499b40e38196c598d4c7c75c88180ed729261844b4e44f368dfab4",
            ),
        ] {
            assert_eq!(
                blake3::hash(file).to_hex().as_str(),
                hex,
                "migration {name} shipped: add a new migration, never edit one",
            );
        }
    }

    /// A write that waits for the collector's key records the wait in the
    /// metrics: the gauge an operator watches for a busy database.
    #[test]
    fn a_commit_records_its_wait_for_the_collectors_key() {
        if !crate::Backend::test_is_postgres() {
            return;
        }
        let url = crate::backend::test_hooks::test_url().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let schema = crate::backend::test_hooks::test_schema(dir.path());
        let metrics = std::sync::Arc::new(crate::metrics::Metrics::default());
        let pg = Pg::open_test(
            &url,
            &schema,
            2,
            dir.path(),
            crate::backend::Marker::read(dir.path()),
        )
        .unwrap();
        pg.set_metrics(Arc::clone(&metrics));
        assert!(
            !metrics.render().contains("scion_pg_collector_key_wait"),
            "nothing waited yet"
        );
        // Another client holds the key, so the commit below really waits.
        let mut holder = Client::connect(&url, NoTls).unwrap();
        holder
            .batch_execute(&format!(
                "SET search_path TO {schema};
                 BEGIN;
                 SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':gc'));"
            ))
            .unwrap();
        let writer = std::thread::spawn(move || {
            pg.commit(|_tx| Ok(())).unwrap();
        });
        // Give the writer time to reach the key, then let it in.
        for _ in 0..2_000 {
            std::thread::yield_now();
        }
        holder.batch_execute("COMMIT").unwrap();
        let mut spins = 0;
        while !writer.is_finished() {
            spins += 1;
            assert!(spins < 1_000_000, "the writer never finished");
            std::thread::yield_now();
        }
        writer.join().unwrap();
        let text = metrics.render();
        assert!(
            text.contains("scion_pg_collector_key_wait_seconds_count 1\n"),
            "{text}"
        );
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
        let backend = crate::backend::Backend::open_test(data.path()).unwrap();
        let pg = backend
            .as_postgres()
            .cloned()
            .expect("the env named postgres");
        Some(pg)
    }

    /// A transaction whose connection dies mid-way leaves the worker's
    /// client usable: the next read on the same reader takes a fresh
    /// session, and a BEGIN on it opens a real transaction. A build that
    /// keeps the lost state on a running BEGIN bricks the reader: every
    /// later read on it fails, and its session holds an open transaction
    /// forever.
    #[test]
    fn a_read_whose_connection_died_mid_transaction_recovers() {
        let Some(pg) = pg() else { return };
        // One reader client, out of the pool, in a transaction.
        let mut reader = pg.inner.readers.take(READER_WAIT).unwrap();
        let client = reader.client();
        client.execute("BEGIN", &[]).unwrap();
        let pid: i32 = client.query("SELECT pg_backend_pid()", &[]).unwrap()[0].get(0);
        // The session dies under the transaction, from another
        // connection. The next statement reconnects, finds the
        // transaction lost, and answers what a caller rolls back.
        pg.commit(|tx| {
            tx.c.execute(
                "SELECT pg_terminate_backend($1::bigint::int)",
                &[P::i64(i64::from(pid))],
            )?;
            Ok(())
        })
        .unwrap();
        let err = client.query_opt("SELECT 1", &[]).unwrap_err();
        assert!(
            matches!(err, Error::Corrupt(ref m) if m.contains("lost")),
            "the lost transaction answered {err}"
        );
        drop(reader);
        // The same reader, back from the pool: a full read on it runs.
        let again = pg.read(|tx| tx.c.query_opt("SELECT 1", &[]).map(|row| row.is_some()));
        assert!(again.unwrap(), "the reader never recovered");
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
        // SQLSTATE 57014 is the statement clock, and map_pg_error gives
        // it the deadline answer, as for a SQLite write past its busy
        // timeout.
        assert!(
            matches!(err, Error::DeadlineExceeded(_)),
            "the cancelled read came back as {err}"
        );
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

    /// A reader that goes back to the pool is taken again. The take's
    /// tail reports the busy gauge, and the report takes the idle lock
    /// that the take itself still held: the first read of a store then
    /// hung forever, with no panic and nothing on the database. A 10 s
    /// ceiling keeps this a fast failure that names the step.
    #[test]
    fn a_reader_taken_and_given_back_is_taken_again() {
        let Some(pg) = pg() else { return };
        let pg = Arc::new(pg);
        let done = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            {
                let pg = Arc::clone(&pg);
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    for round in 1..=2 {
                        pg.read_bounded(
                            |tx| {
                                tx.c.query_opt("SELECT 1", &[])?;
                                Ok(())
                            },
                            Duration::from_secs(30),
                            Duration::from_secs(30),
                            Duration::from_secs(5),
                        )
                        .unwrap();
                        done.store(round, AtomicOrdering::SeqCst);
                    }
                });
            }
            let wait = std::time::Instant::now() + Duration::from_secs(10);
            while done.load(AtomicOrdering::SeqCst) < 2 && std::time::Instant::now() < wait {
                std::thread::sleep(Duration::from_millis(50));
            }
            assert_eq!(
                done.load(AtomicOrdering::SeqCst),
                2,
                "the pool take deadlocked at step {}",
                done.load(AtomicOrdering::SeqCst) + 1
            );
        });
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
            .and_then(|n| n.parse::<f64>().ok())
            .unwrap_or(0.0);
        assert!(sum > 0.0, "sum {sum}");
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

    /// Two lands of one principal that record at the same time, from two
    /// independent backends over one database, cannot both pass the
    /// pending-lands cap. The count and the insert are one step: every
    /// write transaction of the schema holds the schema's advisory key
    /// (`Pg::commit`) until it commits, so the second land counts the
    /// first one's row and hears the limit's answer.
    #[test]
    fn two_lands_that_record_together_respect_the_pending_cap() {
        let data = tempfile::tempdir().unwrap();
        let a = crate::backend::Backend::open_test(data.path()).unwrap();
        let b = crate::backend::Backend::open_test_shared(data.path()).unwrap();
        let cs = a
            .commit(|c| {
                let repo = c.create_repo("r")?;
                let cs = c.insert_changeset(
                    repo,
                    &Changeset::new(
                        vec![],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            0,
                            0,
                        )
                        .expect("identity fits the format"),
                        None,
                        b"root".to_vec(),
                        std::collections::BTreeMap::new(),
                    )
                    .expect("changeset fits the format"),
                )?;
                assert!(c.cas_ref(repo, "main", None, cs)?);
                Ok(cs)
            })
            .unwrap();
        let req = move |key: &str| LandRequest {
            repo: "r".into(),
            changeset: cs,
            target_ref: "main".into(),
            request_key: key.into(),
            deadline: crate::deadline::Deadline::new(Instant::now() + Duration::from_secs(60)),
        };

        // Both backends release together, one caller, a cap of one, two
        // new keys: exactly one land records, and the other is over the
        // cap. Every wait has a ceiling that names its stall.
        let go = Arc::new(std::sync::Barrier::new(2));
        let done = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut threads = Vec::new();
        for (store, key) in [(a, "k1"), (b, "k2")] {
            let (go, done, results) = (Arc::clone(&go), Arc::clone(&done), Arc::clone(&results));
            threads.push(std::thread::spawn(move || {
                go.wait();
                let out = store.commit(|c| c.record_land(&Caller("t".into()), &req(key), Some(1)));
                results
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(out);
                done.fetch_add(1, AtomicOrdering::SeqCst);
            }));
        }
        let ceiling = Instant::now() + Duration::from_secs(30);
        while done.load(AtomicOrdering::SeqCst) < 2 {
            assert!(
                Instant::now() < ceiling,
                "the two record_land calls never finished within 30 s"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let results = std::mem::take(&mut *results.lock().unwrap_or_else(PoisonError::into_inner));
        let recorded = results
            .iter()
            .filter(|out| matches!(out, Ok((_, row)) if row.state == LandState::Pending))
            .count();
        let refused = results
            .iter()
            .filter(|out| matches!(out, Err(Error::ResourceExhausted(_))))
            .count();
        assert_eq!((recorded, refused), (1, 1), "{results:?}");
    }

    /// The out-of-space SQLSTATEs of the Postgres docs' class 53 and 58:
    /// which map to the fixed `ResourceExhausted` shortage and which
    /// stay internal, and that the decision is the same for every code.
    #[test]
    fn the_disk_full_sqlstates_answer_the_fixed_shortage() {
        use super::pg_error_of;
        use crate::{Error, StorageClass};
        // A source of the right type with no SQLSTATE of its own: a
        // connect to port 1, which every kernel refuses. The mapping
        // runs on the SQLSTATE the test hands it, not on this source.
        let source = || match Client::connect("postgres://127.0.0.1:1/x", NoTls) {
            Ok(_) => panic!("a connect to port 1 is refused"),
            Err(refused) => refused,
        };
        let full = |code: &str| {
            let error = pg_error_of(Some(code), source());
            (
                matches!(&error, Error::Storage(s) if s.class() == StorageClass::DiskFull),
                tonic::Status::from(error),
            )
        };
        // 53100: the database's one disk-full code, for every file kind
        // (elog.c maps ENOSPC to it). The client sees the fixed
        // retry-later reply.
        {
            let (full, status) = full("53100");
            assert!(full, "53100 must map to the disk-full storage class");
            assert_eq!(status.code(), tonic::Code::ResourceExhausted, "53100");
            assert_eq!(
                status.message(),
                "the server is out of disk space; retry later",
                "53100"
            );
        }
        // The codes of the same classes that are NOT a disk shortage
        // stay internal, on purpose: 53200 is out of memory, 53000 is
        // the class's catch-all, 53400 is a configured cap (a
        // temp_file_limit or the replication slots - the same refusal
        // every time, so a retry-later reply would tell the client to
        // retry what cannot succeed), and 58030 is an io_error (a
        // failing device is the EIO row's internal, not a space
        // shortage). Pinning them keeps a later "helpful" widening from
        // answering a memory error with a disk reply.
        for code in ["53200", "53000", "53400", "58030"] {
            let (full, status) = full(code);
            assert!(!full, "{code} must not map to the disk-full class");
            assert_eq!(status.code(), tonic::Code::Internal, "{code}");
        }
    }

    /// The importer's batching holds a transaction's parents and repo
    /// membership for the flush: a transaction that ends without the
    /// flush refuses, rolls back, and writes nothing ([#545] review).
    #[test]
    fn a_batch_that_never_flushes_refuses_to_commit() {
        use crate::test_support::meta::{Meta as _, MetaTx as _};

        if !crate::Backend::test_is_postgres() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Backend::open_test(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo("main")).unwrap();
        let cs = Changeset::new(
            Vec::new(),
            scion_core::IdentityTime::new(
                b"Ada".to_vec(),
                b"ada@example.com".to_vec(),
                1_700_000_000,
                0,
            )
            .unwrap(),
            None,
            b"t\n".to_vec(),
            std::collections::BTreeMap::new(),
        )
        .unwrap();
        let (_body, id) = cs.encode_with_id();
        let error = store
            .write(|c| {
                c.set_changeset_batching(true);
                c.insert_changeset(repo, &cs)
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("never flushed"),
            "the refusal names the flush: {error}"
        );
        // The transaction rolled back: the changeset row is not in the
        // database.
        assert!(
            !store.read(|c| c.has_changeset(repo, id)).unwrap(),
            "the changeset row of a transaction that never flushed survived"
        );
        // A normal write after the refusal writes as before.
        let again = store.write(|c| c.insert_changeset(repo, &cs)).unwrap();
        assert_eq!(again, id);
    }
}

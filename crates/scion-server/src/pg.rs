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
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use gix::ObjectId;
use postgres::types::ToSql;
use postgres::{Client, Config, NoTls};

/// Shorthand for the owned query parameter.
type P = Param;
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::auth::Caller;
use crate::error::{Error, Result};
use crate::history::NodeId;
use crate::import::SHA1;
use crate::land::{
    EXPIRE_PER_LAND, LAND_RETENTION_SECS, LandOutcome, LandRequest, LandState, decode_paths,
    encode_paths,
};
use crate::meta::{Meta, MetaTx, RefState, RepoId};
use crate::store::Blobs;

/// The schema version this build makes and reads.
pub(crate) const PG_SCHEMA_VERSION: u32 = 1;

/// The advisory lock of the one server process of a database.
const SERVER_LOCK: i64 = 0x5343_494F_4E53_5256;
/// The advisory lock of one migration run of a database.
const SCHEMA_LOCK: i64 = 0x5343_494F_4E53_4348;

/// Schema migrations in order. Migration `n`, from 1, moves the database
/// from version `n - 1` to version `n`. A released migration never changes.
const PG_MIGRATIONS: [&str; PG_SCHEMA_VERSION as usize] =
    [include_str!("../migrations/postgres/0001_init.sql")];

/// Handle to one Postgres database. Cheap to clone.
#[derive(Clone)]
pub struct Pg {
    inner: Arc<PgInner>,
}

struct PgInner {
    /// The write client, for every commit and [`Pg::read_unflushed`].
    writer: Mutex<PgClient>,
    /// Clients for [`Pg::read`], at most [`READERS`](crate::store::READERS)
    /// at once, like the readers of a SQLite store.
    readers: Readers,
    /// The blob folder: blob files stay on local disk in every backend.
    blobs: Blobs,
}

/// Clients for [`Pg::read`], so at most [`READERS`](crate::store::READERS)
/// reads run at once. A caller takes one out and returns it when it is
/// done.
struct Readers {
    idle: Mutex<Vec<PgClient>>,
    returned: Condvar,
}

impl Readers {
    /// [`Readers::new`], with libpq `options` on every client.
    fn new_with(size: usize, url: &str, ca: Option<&Path>, options: &str) -> Result<Self> {
        // The open is sequential, so every client sees the same schema.
        let idle = (0..size)
            .map(|_| {
                let client = connect_with(url, ca, options, None)?;
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
        })
    }

    /// Takes a reader, or waits for one.
    fn take(&self) -> ReaderGuard<'_> {
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        while idle.is_empty() {
            idle = self
                .returned
                .wait(idle)
                .unwrap_or_else(PoisonError::into_inner);
        }
        let client = idle.pop().expect("the pool is not empty");
        ReaderGuard {
            readers: self,
            client: Some(client),
        }
    }
}

/// A reader that is out of the pool. Returns it when it drops.
struct ReaderGuard<'a> {
    readers: &'a Readers,
    client: Option<PgClient>,
}

impl ReaderGuard<'_> {
    fn client(&mut self) -> &mut PgClient {
        self.client.as_mut().expect("a taken reader has a client")
    }
}

impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.readers
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(client);
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
                    url,
                    ca,
                    options,
                    schema,
                    exclusive,
                } = cfg;
                // The schema of the connection, named again after every
                // reconnect; the one-server lock, taken again after every
                // reconnect. A lock the database does not give back fails
                // every statement that follows: another server process may
                // hold it, and this one must not land beside it.
                let mut setup = Setup::new(schema.as_deref(), exclusive, &mut client);
                // BEGIN opens one; COMMIT and ROLLBACK end it. A
                // transaction a reconnect lost fails every statement until
                // its rollback: on the new connection, autocommit would
                // answer COMMIT with Ok.
                let mut tx = TxState::Idle;
                for job in rx {
                    if tx.lost() && !job.sql.starts_with("ROLLBACK") {
                        let _ = job.reply.send(Err(Error::Corrupt(
                            "the transaction was lost with the connection; \
                             roll it back and run it again"
                                .into(),
                        )));
                        continue;
                    }
                    // A client the server closed, as after a restart of the
                    // database, connects again: the caller sees one error at
                    // most, not one until the process restarts. A reconnect
                    // inside a transaction loses the transaction.
                    let lost_tx = tx.in_progress();
                    if client.is_closed() {
                        match connect_on(&url, ca.as_deref(), &options) {
                            Ok(fresh) => {
                                client = fresh;
                                setup = Setup::new(schema.as_deref(), exclusive, &mut client);
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
                    }
                    if !setup.ok() {
                        let _ = job.reply.send(Err(Error::FailedPrecondition(
                            "this server process lost the database to another \
                             server process; it serves nothing from here on"
                                .into(),
                        )));
                        continue;
                    }
                    if job.kind == Kind::Raw {
                        let mut job = job;
                        let f = job.raw.take().expect("a raw job carries its closure");
                        let _ = job.reply.send(f(&mut client).map(|(): ()| Reply::Count(0)));
                        continue;
                    }
                    let refs: Vec<&(dyn ToSql + Sync)> = job
                        .params
                        .iter()
                        .map(|p| p as &(dyn ToSql + Sync))
                        .collect();
                    let out = match job.kind {
                        Kind::Batch => client.batch_execute(&job.sql).map(|(): ()| Reply::Count(0)),
                        Kind::Count => client.execute(&job.sql, &refs).map(Reply::Count),
                        Kind::Rows => client.query(&job.sql, &refs).map(Reply::Rows),
                        Kind::Raw => unreachable!("a raw job ran above"),
                    }
                    .map_err(map_pg_error);
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

/// Drops the test schemas whose connections are all gone: a test run
/// leaves them behind, because a server holds its schema while it runs.
/// One process at a time holds the cleanup lock, so the test binaries
/// that run in parallel clean nothing under each other's feet. A schema
/// whose owner still runs is named in `pg_stat_activity`: its
/// `application_name` is the schema.
fn drop_stale_schemas(maker: &PgClient) {
    let Ok(got) = maker.query_opt("SELECT pg_try_advisory_lock($1)", &[P::i64(CLEAN_LOCK)]) else {
        return;
    };
    if got.is_none() {
        return;
    }
    if let Ok(rows) = maker.query(
        "SELECT nspname FROM pg_catalog.pg_namespace
         WHERE nspname LIKE 'scion_test_%'
           AND nspname NOT IN (
               SELECT application_name FROM pg_catalog.pg_stat_activity
               WHERE application_name LIKE 'scion_test_%')",
        &[],
    ) {
        for row in rows {
            let name: String = row.get(0);
            let _ = maker.batch(&format!("DROP SCHEMA IF EXISTS {name} CASCADE"));
        }
    }
    let _ = maker.query_opt("SELECT pg_advisory_unlock($1)", &[P::i64(CLEAN_LOCK)]);
}

/// The advisory lock of the test-schema cleanup.
const CLEAN_LOCK: i64 = 0x5343_494F_4E53_5445;

/// The libpq options of every connection of this backend. The commit of
/// a transaction reaches the drive before it returns, and no statement
/// runs longer than the graph-walk bound. The search path is not set
/// here: it is a statement on the live connection, which the server
/// cannot lose between connections.
fn base_options() -> String {
    String::from("-csynchronous_commit=on -cstatement_timeout=600000")
}

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
    pub fn open(url: &str, ca: Option<&Path>, readers: usize, data: &Path) -> Result<Self> {
        Self::open_in(url, ca, readers, data, None, false)
    }

    /// [`Pg::open`], as the one server process of the database: a second
    /// `serve` or `derive` on the same URL is refused. Import runs beside a
    /// server, so it opens without the lock.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`], and another process holds the one-server lock.
    pub fn open_exclusive(
        url: &str,
        ca: Option<&Path>,
        readers: usize,
        data: &Path,
    ) -> Result<Self> {
        Self::open_in(url, ca, readers, data, None, true)
    }

    /// [`Pg::open`], in `schema` instead of the database's default one, for
    /// tests: each data directory gets its own corner of one database. The
    /// schema is created if missing, and the migrations run in it.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`].
    pub fn open_test(url: &str, schema: &str, readers: usize, data: &Path) -> Result<Self> {
        {
            // One throwaway client makes the schema, before any connection
            // narrows its search path to it. It names itself first, so no
            // parallel cleanup mistakes a live schema for a stale one.
            let maker = connect(url, None)?;
            maker.as_raw({
                let name = schema.to_owned();
                move |client| {
                    client
                        .batch_execute(&format!("SET application_name TO '{name}'"))
                        .map_err(map_pg_error)
                }
            })?;
            drop_stale_schemas(&maker);
            maker.as_raw({
                let name = schema.to_owned();
                move |client| {
                    client
                        .batch_execute(&format!(
                            "CREATE SCHEMA IF NOT EXISTS {name};
                                 INSERT INTO scion_test_schemas (schema, started)
                                 VALUES ('{name}', extract(epoch from now())::bigint)
                                 ON CONFLICT (schema) DO UPDATE SET started = excluded.started"
                        ))
                        .map_err(map_pg_error)
                }
            })?;
        }
        Self::open_in(url, None, readers, data, Some(schema), false)
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
    ) -> Result<Self> {
        let options = base_options();
        let mut writer = if exclusive {
            connect_exclusive(url, ca, &options, schema)?
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
        let blobs = Blobs::open(data)?;
        blobs.sweep_tmp(false)?;
        Ok(Self {
            inner: Arc::new(PgInner {
                writer: Mutex::new(writer),
                readers: Readers::new_with(readers, url, ca, &options)?,
                blobs,
            }),
        })
    }

    /// The blob folder.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        &self.inner.blobs
    }

    /// A read on the write client, so it sees the caller's own commits.
    pub fn read_unflushed<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        let mut writer = self.lock_writer();
        let mut pg = PgTx {
            c: &mut writer,
            ended: true,
        };
        f(&mut pg)
    }

    /// A strong read on one client of the pool. The read runs in one
    /// transaction, so all its statements see one snapshot; on this backend
    /// every commit is durable when it returns, so what `f` saw is durable.
    pub fn read<T>(&self, f: impl FnOnce(&mut PgTx<'_>) -> Result<T>) -> Result<T> {
        let mut reader = self.inner.readers.take();
        let mut tx = PgTx::begin_read(reader.client())?;
        let out = f(&mut tx);
        // A read transaction: nothing to keep. The rollback's own error
        // must not hide the error of `f`; the drop rolls back anyway.
        let _ = tx.rollback();
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
        let mut writer = self.lock_writer();
        // A write transaction runs at READ COMMITTED, the Postgres
        // default: every change of the caller checks the newest state of
        // the row it touches (the CAS predicates).
        let mut tx = PgTx::begin(&mut writer)?;
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

    fn lock_writer(&self) -> MutexGuard<'_, PgClient> {
        self.inner
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
    /// state of each statement.
    fn begin_read(c: &'a mut PgClient) -> Result<PgTx<'a>> {
        c.execute(
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
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
struct Setup {
    schema: Option<String>,
    exclusive: bool,
    locked: bool,
}

impl Setup {
    fn new(schema: Option<&str>, exclusive: bool, client: &mut Client) -> Self {
        let mut setup = Self {
            schema: schema.map(str::to_owned),
            exclusive,
            locked: !exclusive,
        };
        if setup.apply(client) {
            setup
        } else {
            // A failed setup leaves the statements failed until a
            // reconnect applies it again.
            Self {
                schema: setup.schema,
                exclusive: setup.exclusive,
                locked: false,
            }
        }
    }

    /// Whether the setup holds: a schema was named and set, and a
    /// one-server lock, if one was asked for, is held.
    fn ok(&self) -> bool {
        !self.exclusive || self.locked
    }

    fn apply(&mut self, client: &mut Client) -> bool {
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
                return false;
            }
        }
        if self.exclusive && !server_lock_raw(client) {
            return false;
        }
        self.locked = self.exclusive;
        true
    }
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

    fn after(self, sql: &str, _ok: bool) -> Self {
        if sql.starts_with("BEGIN") && !self.lost() {
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

/// What a worker needs to run, and to connect again when its client dies.
struct ClientConfig {
    client: Client,
    url: String,
    ca: Option<PathBuf>,
    options: String,
    /// The schema a reconnect narrows itself to again.
    schema: Option<String>,
    /// Re-take the one-server lock after every reconnect.
    exclusive: bool,
}

/// The advisory lock of the one server process of a database: a second
/// `serve` or `derive` on the same URL is refused. Runs on the worker
/// thread, like every statement.
fn server_lock_raw(client: &mut Client) -> bool {
    client
        .query_opt("SELECT pg_try_advisory_lock($1)", &[&SERVER_LOCK])
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
    let client =
        std::thread::scope(|scope| scope.spawn(|| connect_on(url, ca, options)).join().unwrap())?;
    Ok(PgClient::spawn(ClientConfig {
        client,
        url: url.to_owned(),
        ca: ca.map(Path::to_path_buf),
        options: options.to_owned(),
        schema: schema.map(str::to_owned),
        exclusive: false,
    }))
}

/// [`connect_with`], as the one server process of the database: the
/// advisory lock it takes survives as long as the client, and a
/// reconnecting client takes it again.
fn connect_exclusive(
    url: &str,
    ca: Option<&Path>,
    options: &str,
    schema: Option<&str>,
) -> Result<PgClient> {
    let client =
        std::thread::scope(|scope| scope.spawn(|| connect_on(url, ca, options)).join().unwrap())?;
    // The lock is taken on the worker thread, like every statement: the
    // caller may be a tokio one, which must never block on this client.
    Ok(PgClient::spawn(ClientConfig {
        client,
        url: url.to_owned(),
        ca: ca.map(Path::to_path_buf),
        options: options.to_owned(),
        schema: schema.map(str::to_owned),
        exclusive: true,
    }))
}

/// [`connect_with`]'s work: parse, then connect.
fn connect_on(url: &str, ca: Option<&Path>, options: &str) -> Result<Client> {
    let mut config: Config = url
        .parse()
        .map_err(|e| Error::InvalidArgument(format!("cannot parse the database URL: {e}")))?;
    if !options.is_empty() {
        config.options(options);
    }
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
    let client = match (local, ca) {
        // The connector upgrades every connection, and checks the
        // certificate the server presents against `ca`.
        (_, Some(ca)) => config.connect(make_tls(ca)?)?,
        (true, None) => config.connect(NoTls)?,
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
    match e.code() {
        // Deadlock, serialization failure, an administrative shutdown, a
        // database that starts up, any lost connection.
        Some(code)
            if matches!(code.code(), "40P01" | "40001" | "57P01" | "57P03")
                || code.code().starts_with("08") =>
        {
            Error::Busy
        }
        // A connection the database dropped answers with no SQLSTATE at
        // all: as retryable as one it named.
        None => Error::Busy,
        Some(_) => Error::Pg(e),
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
    // One transaction holds the schema lock, so a server and an import
    // started together migrate one after the other, and the version the
    // loser reads is the one the winner left.
    let mut tx = PgTx::begin(client)?;
    tx.c.execute("SELECT pg_advisory_xact_lock($1)", &[P::i64(SCHEMA_LOCK)])?;
    tx.c.batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version BIGINT PRIMARY KEY,
             name    TEXT NOT NULL
         )",
    )?;
    let version: i64 =
        tx.c.query("SELECT max(version) FROM schema_migrations", &[])?
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
        tx.c.batch(&format!(
            "{sql}\nINSERT INTO schema_migrations (version, name) \
             VALUES ({}, 'postgres schema')",
            i64::from(n)
        ))?;
    }
    tx.commit()
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
                "SELECT octet_length(body) FROM trees WHERE id = $1",
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

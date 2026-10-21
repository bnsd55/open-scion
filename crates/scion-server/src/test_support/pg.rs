//! `pg.rs`'s test hooks, as its own child module: `pg.rs` declares
//! this file with `#[path]`, so everything here keeps the privacy of
//! its module unchanged, and the file sits under `src/test_support/`
//! for the coverage exclusion.

#![forbid(unsafe_code)]

use super::{
    Client, Error, Job, Kind, Marker, P, Path, Pg, PgClient, Reply, Result, connect_with,
    map_pg_error,
};

impl PgClient {
    /// Runs one closure with the client on the worker thread. The sync
    /// client must never be touched from a tokio thread, which this
    /// keeps true for the few setup statements a caller needs. Only the
    /// test schema's preparation calls it.
    #[cfg(any(test, feature = "test-hooks"))]
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
}

/// Drops the test schemas whose tests ended minutes ago: a test run
/// leaves them behind, because a server holds its schema while it runs.
/// One process at a time holds the cleanup lock, so the test binaries
/// that run in parallel clean nothing under each other's feet. A schema
/// whose owner still runs is named in `pg_stat_activity` under its
/// `application_name`, and registered with its start time, so the
/// schema of a test that just began is never dropped.
#[cfg(any(test, feature = "test-hooks"))]
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
#[cfg(any(test, feature = "test-hooks"))]
const CLEAN_LOCK: i64 = 0x5343_494F_4E53_5445;

/// A plain connection. Only the test schema's preparation calls it.
#[cfg(any(test, feature = "test-hooks"))]
fn connect(url: &str, ca: Option<&Path>) -> Result<PgClient> {
    connect_with(url, ca, "", None)
}

/// The error of a graph walk that reached its bound.
#[cfg(any(test, feature = "test-hooks"))]
pub(super) fn too_deep(max_visits: usize) -> Error {
    Error::ResourceExhausted(format!(
        "the history is too deep: the answer needs more than {max_visits} changesets"
    ))
}

/// Makes the test schema, and registers it, before any connection
/// narrows its search path to it. One maker at a time: two concurrent
/// `CREATE TABLE IF NOT EXISTS` of the registry race on the type
/// catalog, and the loser dies.
#[cfg(any(test, feature = "test-hooks"))]
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

impl Pg {
    /// [`Pg::open`], in `schema` instead of the database's default one, for
    /// tests: each data directory gets its own corner of one database. The
    /// schema is created if missing, and the migrations run in it.
    ///
    /// # Errors
    ///
    /// As [`Pg::open`].
    #[cfg(any(test, feature = "test-hooks"))]
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
    #[cfg(any(test, feature = "test-hooks"))]
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
    /// The import's test hooks and busy-retry count.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub(crate) fn hooks(&self) -> &crate::store::test_hooks::ImportHooks {
        &self.inner.hooks
    }
}

//! `backend.rs`'s test hooks, as its own child module: `backend.rs`
//! declares this file with `#[path]`, so everything here keeps the
//! privacy of its module unchanged, and the file sits under
//! `src/test_support/` for the coverage exclusion.

#![forbid(unsafe_code)]

use std::path::Path;

use super::{
    Backend, BackendTx, Inner, Marker, PG_READERS, Store, marker_agrees, refuse_damaged_marker,
    refuse_postgres_marker, refuse_sqlite_dir,
};
use crate::error::Result;
use crate::pg::Pg;

impl Backend {
    /// A backend on the data directory's own store, for the crate's
    /// tests: they prepare a store (a walk cap, a page cap) and then
    /// wrap it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn sqlite(store: Store) -> Self {
        Self {
            inner: Inner::Sqlite(store),
        }
    }
    /// The data directory's own store, when this backend is one.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn as_sqlite(&self) -> Option<&Store> {
        match &self.inner {
            Inner::Sqlite(store) => Some(store),
            Inner::Postgres(_) => None,
        }
    }
    /// The advisory-lock key of the test schema's one-server lock, for
    /// the test that takes the lock from its own connection, as a
    /// second server process would.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_server_lock(data: &Path) -> Result<i64> {
        Ok(crate::pg::schema_lock_key(&test_schema(data)))
    }
    /// [`Backend::open_test`], as a process that does not land: on the
    /// data directory this is a shared [`Store`](crate::store::Store).
    ///
    /// # Errors
    ///
    /// What the store or the database returns, and a missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test_shared(data: &Path) -> Result<Self> {
        if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres") {
            refuse_damaged_marker(data)?;
            let marker = Marker::read(data);
            refuse_sqlite_dir(data)?;
            let pg =
                Pg::open_test_shared(&test_url()?, &test_schema(data), PG_READERS, data, marker)?;
            marker_agrees(data, pg.store_uuid())?;
            return Ok(Self {
                inner: Inner::Postgres(pg),
            });
        }
        refuse_postgres_marker(data)?;
        Self::open_shared(data, None, None)
    }
    /// Opens the backend that `SCION_TEST_BACKEND` names, for the
    /// integration tests: `postgres` runs them against a Postgres database,
    /// whose URL comes from `SCION_TEST_POSTGRES_URL`; any other value, or
    /// none, opens the data directory. The URL is never logged.
    ///
    /// # Errors
    ///
    /// What the store or the database returns, and a missing test URL.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn open_test(data: &Path) -> Result<Self> {
        if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres") {
            refuse_damaged_marker(data)?;
            let marker = Marker::read(data);
            refuse_sqlite_dir(data)?;
            let pg = Pg::open_test(&test_url()?, &test_schema(data), PG_READERS, data, marker)?;
            marker_agrees(data, pg.store_uuid())?;
            return Ok(Self {
                inner: Inner::Postgres(pg),
            });
        }
        refuse_postgres_marker(data)?;
        Self::open(data, None, None)
    }
    /// Whether `SCION_TEST_BACKEND` names Postgres. A test that asserts the
    /// rows of the data directory's database skips itself on Postgres: its
    /// assertions are about the SQLite schema, which the schema drift test
    /// covers for Postgres.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_is_postgres() -> bool {
        std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres")
    }

    /// The Postgres statements this process has run since the last
    /// reset. The land round-trip guard measures the delta around one
    /// land; on SQLite the count is always zero, which is why the guard
    /// skips there.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_statement_count() -> u64 {
        crate::pg::test_statement_count()
    }

    /// Sets the statement count back to zero.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn test_reset_statement_count() {
        crate::pg::test_reset_statement_count();
    }

    /// Whether the server's first stale-land sweep has finished: a test
    /// that counts a call's statements waits for it, so the sweep's own
    /// statements cannot fall inside its window.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_first_sweep_done() -> bool {
        crate::service::TEST_FIRST_SWEEP_DONE.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The SQL of the statements run since the last reset.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_statement_log() -> Vec<String> {
        crate::pg::test_statement_log()
    }
    /// The schema that [`Backend::open_test`] gave this data directory on
    /// Postgres: a test that counts rows through its own Postgres client
    /// names it the same way. A result, so the callers unwrap one shape.
    /// The schema a test backend of this directory uses, when the tests
    /// run on Postgres. Hidden: a test holds a lock in it from a
    /// connection of its own.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn test_schema(data: &Path) -> Option<String> {
        if Self::test_is_postgres() {
            Some(test_schema(data))
        } else {
            None
        }
    }
    /// Write transactions run so far. SQLite only.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn txes(&self) -> u64 {
        match &self.inner {
            Inner::Sqlite(store) => store.txes(),
            Inner::Postgres(_) => 0,
        }
    }
    /// Commits made so far. SQLite only: it counts the transactions of the
    /// write connection, which is what the tests of the batching watch.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn commits(&self) -> u64 {
        match &self.inner {
            Inner::Sqlite(store) => store.commits(),
            Inner::Postgres(_) => 0,
        }
    }
    /// The commit count a completed flush last covered, so every commit
    /// below it is durable. SQLite only: a Postgres commit is durable when
    /// it returns (`synchronous_commit = on`).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn flushed_upto(&self) -> u64 {
        match &self.inner {
            Inner::Sqlite(store) => store.flushed_upto(),
            Inner::Postgres(_) => u64::MAX,
        }
    }
    /// Sets how many of the next commits of the data directory fail as a
    /// busy database. A Postgres database has no busy error, so this only
    /// reaches SQLite.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_next_commits(&self, n: u32) {
        if let Inner::Sqlite(store) = &self.inner {
            store.fail_next_commits(n);
        }
    }
    /// Sets how long the write connection of the data directory waits for
    /// a busy database.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_busy_timeout(&self, timeout: std::time::Duration) {
        if let Inner::Sqlite(store) = &self.inner {
            store.set_busy_timeout(timeout);
        }
    }
    /// The import's test hooks and busy-retry count, the same state on
    /// both backends.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    fn hooks(&self) -> &crate::store::test_hooks::ImportHooks {
        match &self.inner {
            Inner::Sqlite(store) => store.hooks(),
            Inner::Postgres(pg) => pg.hooks(),
        }
    }
    /// Sets the step the tests run after each batch of the import commits.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_after_batch(&self, step: Option<crate::store::test_hooks::AfterBatch>) {
        self.hooks().set_after_batch(step);
    }
    /// How many busy retries of the import's batches ran so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn busy_retries(&self) -> u64 {
        self.hooks().busy_retries()
    }
    /// Runs the test step of a batch boundary, with the count of batches
    /// done so far.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn run_after_batch(&self, batches: usize) {
        self.hooks().run_after_batch(batches);
    }
    /// Counts one busy retry of the import's batch.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn note_busy_retry(&self) {
        self.hooks().note_busy_retry();
    }
}

impl BackendTx<'_> {
    /// The connection of the data directory, for a test that asserts its
    /// rows. [`None`] on Postgres, whose rows a test reads through the
    /// [`MetaTx`] queries.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn sqlite(&self) -> Option<&rusqlite::Connection> {
        match self {
            // SqliteTx derefs to the connection.
            Self::Sqlite(tx) => Some(tx),
            Self::Postgres(_) => None,
        }
    }
}

/// The URL of the test database. Its callers sit behind the test-hooks
/// feature, and so does this.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_url() -> Result<String> {
    std::env::var("SCION_TEST_POSTGRES_URL").map_err(|_| {
        crate::error::Error::InvalidArgument(
            "SCION_TEST_POSTGRES_URL must name the test database".into(),
        )
    })
}

/// A database schema name that belongs to `data`, so the tests of one
/// directory share it and no others see it. Its callers sit behind the
/// test-hooks feature, and so does this.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn test_schema(data: &Path) -> String {
    let path = data.canonicalize().unwrap_or_else(|_| data.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
    format!("scion_test_{:016x}", std::hash::Hasher::finish(&hasher))
}

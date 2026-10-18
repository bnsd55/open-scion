//! The metadata backend of a server: one of the supported databases,
//! behind [`Meta`](crate::meta::Meta).
//!
//! [`Backend::Sqlite`] is the data directory of [`Store`](crate::store::Store).
//! [`Backend::Postgres`] is a shared database ([`Pg`](crate::pg::Pg)). A
//! caller holds one and calls the [`Meta`] methods; the transaction type
//! differs per backend, so the queries of a call go through
//! [`BackendTx`], which forwards them to the backend's own transaction.

use gix::ObjectId;
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};

use crate::auth::Caller;
use crate::db::SqliteTx;
use crate::error::Result;
use crate::history::NodeId;
use crate::land::{LandOutcome, LandRequest, LandState};
use crate::meta::{FsckDerivedRow, Meta, MetaTx, RefState, RepoChangeset, RepoId};
use crate::pg::{Pg, PgTx};
use std::path::Path;

use crate::store::{Blobs, Store};

/// The metadata storage of a server: the data directory, or a shared
/// Postgres database.
#[derive(Clone)]
pub enum Backend {
    /// One data directory on this machine.
    Sqlite(Store),
    /// A Postgres database that a fleet of servers can share.
    Postgres(Pg),
}

/// The transaction of a [`Backend`]: the queries of one call, forwarded to
/// the backend's own transaction.
pub enum BackendTx<'a> {
    /// A transaction of the data directory.
    Sqlite(&'a mut SqliteTx<'a>),
    /// A transaction of the Postgres database.
    Postgres(PgTx<'a>),
}

impl BackendTx<'_> {
    /// The connection of the data directory, for a test that asserts its
    /// rows. [`None`] on Postgres, whose rows a test reads through the
    /// [`MetaTx`] queries.
    #[doc(hidden)]
    #[must_use]
    pub fn sqlite(&self) -> Option<&rusqlite::Connection> {
        match self {
            // SqliteTx derefs to the connection.
            Self::Sqlite(tx) => Some(tx),
            Self::Postgres(_) => None,
        }
    }
}

/// Clients that a Postgres backend pools for its reads: as many as a
/// data directory holds, so the graph-walk bound of the server keeps its
/// meaning on both backends.
const PG_READERS: usize = crate::store::READERS;

const _: () = assert!(PG_READERS == crate::store::READERS);

/// The file that names the backend of a data directory. Absent, the
/// directory is a SQLite one: directories of older builds have no file.
const MARKER: &str = "backend";

/// Marks `data` as Postgres-backed, and refuses to open Postgres on a
/// directory whose metadata a SQLite store already holds.
fn postgres_marker(data: &Path) -> Result<()> {
    let db = data.join("scion.db");
    if db.metadata().is_ok_and(|m| m.len() > 0) {
        return Err(crate::error::Error::InvalidArgument(
            "this data directory already keeps its metadata in its own \
             database: drop --postgres-url to use it"
                .into(),
        ));
    }
    // The open may name a directory that does not exist yet: the marker
    // is its first file.
    std::fs::create_dir_all(data).map_err(crate::error::Error::Io)?;
    std::fs::write(data.join(MARKER), "postgres\n").map_err(crate::error::Error::Io)?;
    Ok(())
}

/// Refuses to open the data directory as SQLite when its marker names
/// Postgres: its rows live in the database, and a second, empty store
/// here would look like a server with no data at all.
fn refuse_postgres_marker(data: &Path) -> Result<()> {
    if std::fs::read_to_string(data.join(MARKER)).is_ok_and(|s| s.trim() == "postgres") {
        return Err(crate::error::Error::InvalidArgument(
            "this data directory keeps its metadata in Postgres: \
             give --postgres-url"
                .into(),
        ));
    }
    Ok(())
}

/// The URL of the test database.
pub(crate) fn test_url() -> Result<String> {
    std::env::var("SCION_TEST_POSTGRES_URL").map_err(|_| {
        crate::error::Error::InvalidArgument(
            "SCION_TEST_POSTGRES_URL must name the test database".into(),
        )
    })
}

/// A database schema name that belongs to `data`, so the tests of one
/// directory share it and no others see it.
pub(crate) fn test_schema(data: &Path) -> String {
    let path = data.canonicalize().unwrap_or_else(|_| data.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
    format!("scion_test_{:016x}", std::hash::Hasher::finish(&hasher))
}

impl Backend {
    /// The derive walks of this backend: which are in flight, and the cap
    /// on how many may be.
    pub(crate) fn derive_flights(&self) -> &crate::derived::DeriveFlights {
        match self {
            Self::Sqlite(store) => store.derive_flights(),
            Self::Postgres(pg) => pg.derive_flights(),
        }
    }

    /// How many writers wait for the write connection.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn writers_waiting(&self) -> usize {
        match self {
            Self::Sqlite(store) => store.writers_waiting(),
            Self::Postgres(pg) => pg.writers_waiting(),
        }
    }

    /// Waits while writers wait for the write connection, until the
    /// deadline: a derivation step yields to a writer that waited.
    ///
    /// # Errors
    ///
    /// [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn wait_for_writers(
        &self,
        deadline: Option<&crate::deadline::Deadline>,
    ) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.wait_for_writers(deadline),
            Self::Postgres(pg) => pg.wait_for_writers(deadline),
        }
    }

    /// Opens the backend of a server: Postgres at `postgres_url` when it is
    /// set, the data directory otherwise. `ca` verifies the certificate of
    /// the Postgres server; it is required for a host that is not local.
    ///
    /// # Errors
    ///
    /// What the store or the database returns.
    pub fn open(data: &Path, postgres_url: Option<&str>, ca: Option<&Path>) -> Result<Self> {
        if let Some(url) = postgres_url {
            postgres_marker(data)?;
            Ok(Self::Postgres(Pg::open_exclusive(
                url, ca, PG_READERS, data,
            )?))
        } else {
            refuse_postgres_marker(data)?;
            Ok(Self::Sqlite(Store::open(data)?))
        }
    }

    /// Opens the backend of a process that does not land, such as an
    /// import. On Postgres this is [`Backend::open`]: the database sorts its
    /// writers out itself.
    ///
    /// # Errors
    ///
    /// What the store or the database returns.
    pub fn open_shared(data: &Path, postgres_url: Option<&str>, ca: Option<&Path>) -> Result<Self> {
        if let Some(url) = postgres_url {
            postgres_marker(data)?;
            Ok(Self::Postgres(Pg::open(url, ca, PG_READERS, data)?))
        } else {
            refuse_postgres_marker(data)?;
            Ok(Self::Sqlite(Store::open_shared(data)?))
        }
    }

    /// [`Backend::open_test`], as a process that does not land: on the
    /// data directory this is a shared [`Store`](crate::store::Store).
    ///
    /// # Errors
    ///
    /// What the store or the database returns, and a missing test URL.
    #[doc(hidden)]
    pub fn open_test_shared(data: &Path) -> Result<Self> {
        if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres") {
            postgres_marker(data)?;
            let pg = Pg::open_test(&test_url()?, &test_schema(data), PG_READERS, data)?;
            return Ok(Self::Postgres(pg));
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
    pub fn open_test(data: &Path) -> Result<Self> {
        if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres") {
            postgres_marker(data)?;
            let pg = Pg::open_test(&test_url()?, &test_schema(data), PG_READERS, data)?;
            return Ok(Self::Postgres(pg));
        }
        refuse_postgres_marker(data)?;
        Self::open(data, None, None)
    }

    /// Whether `SCION_TEST_BACKEND` names Postgres. A test that asserts the
    /// rows of the data directory's database skips itself on Postgres: its
    /// assertions are about the SQLite schema, which the schema drift test
    /// covers for Postgres.
    #[doc(hidden)]
    #[must_use]
    pub fn test_is_postgres() -> bool {
        std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres")
    }

    /// The schema a test backend of this directory uses, when the tests
    /// run on Postgres. Hidden: a test holds a lock in it from a
    /// connection of its own.
    #[doc(hidden)]
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
    #[must_use]
    pub fn txes(&self) -> u64 {
        match self {
            Self::Sqlite(store) => store.txes(),
            Self::Postgres(_) => 0,
        }
    }

    /// The blob folder. Blob files stay on local disk in every backend.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        match self {
            Self::Sqlite(store) => store.blobs(),
            Self::Postgres(pg) => pg.blobs(),
        }
    }

    /// Whether the backend is a data directory that other processes share.
    /// A shared one keeps its write transactions short; a Postgres database
    /// sorts its writers out itself.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        match self {
            Self::Sqlite(store) => store.is_shared(),
            Self::Postgres(_) => false,
        }
    }

    /// Commits made so far. SQLite only: it counts the transactions of the
    /// write connection, which is what the tests of the batching watch.
    #[doc(hidden)]
    #[must_use]
    pub fn commits(&self) -> u64 {
        match self {
            Self::Sqlite(store) => store.commits(),
            Self::Postgres(_) => 0,
        }
    }
}

impl Backend {
    /// Sets how many of the next commits of the data directory fail as a
    /// busy database. A Postgres database has no busy error, so this only
    /// reaches SQLite.
    #[doc(hidden)]
    pub fn fail_next_commits(&self, n: u32) {
        if let Self::Sqlite(store) = self {
            store.fail_next_commits(n);
        }
    }

    /// Sets how long the write connection of the data directory waits for
    /// a busy database.
    #[doc(hidden)]
    pub fn set_busy_timeout(&self, timeout: std::time::Duration) {
        if let Self::Sqlite(store) = self {
            store.set_busy_timeout(timeout);
        }
    }

    /// Sets the step the tests run after each batch of the import commits.
    /// A Postgres database has no shared data directory, so this only
    /// reaches SQLite.
    #[doc(hidden)]
    pub fn set_after_batch(&self, step: Option<crate::store::AfterBatch>) {
        if let Self::Sqlite(store) = self {
            store.set_after_batch(step);
        }
    }

    /// How many busy retries of the import's batches ran so far.
    #[doc(hidden)]
    #[must_use]
    pub fn busy_retries(&self) -> u64 {
        match self {
            Self::Sqlite(store) => store.busy_retries(),
            Self::Postgres(_) => 0,
        }
    }

    /// Runs the test step of a batch boundary, with the count of batches
    /// done so far.
    #[doc(hidden)]
    pub fn run_after_batch(&self, batches: usize) {
        if let Self::Sqlite(store) = self {
            store.run_after_batch(batches);
        }
    }

    /// Counts one busy retry of the import's batch.
    #[doc(hidden)]
    pub fn note_busy_retry(&self) {
        if let Self::Sqlite(store) = self {
            store.note_busy_retry();
        }
    }
}

impl Meta for Backend {
    type Tx<'a>
        = BackendTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match self {
            Self::Sqlite(store) => store.read(|tx| f(&mut BackendTx::Sqlite(tx))),
            Self::Postgres(pg) => pg.read(|tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }

    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64) {
        match self {
            Self::Sqlite(store) => {
                let (result, seen) = store.read_counted(|tx| f(&mut BackendTx::Sqlite(tx)));
                (result, seen)
            }
            Self::Postgres(pg) => {
                let (result, seen) =
                    pg.read_counted(|tx| f(&mut BackendTx::Postgres(tx.reborrow())));
                (result, seen)
            }
        }
    }

    fn wait_durable(&self, seen: u64) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.wait_durable(seen),
            Self::Postgres(pg) => pg.wait_durable(seen),
        }
    }

    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match self {
            Self::Sqlite(store) => store.read_unflushed(|tx| f(&mut BackendTx::Sqlite(tx))),
            Self::Postgres(pg) => {
                pg.read_unflushed(|tx| f(&mut BackendTx::Postgres(tx.reborrow())))
            }
        }
    }

    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match self {
            Self::Sqlite(store) => store.commit(move |tx| f(&mut BackendTx::Sqlite(tx))),
            Self::Postgres(pg) => pg.commit(move |tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }

    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match self {
            Self::Sqlite(store) => store.write(move |tx| f(&mut BackendTx::Sqlite(tx))),
            Self::Postgres(pg) => pg.write(move |tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }

    fn flush(&self) -> Result<()> {
        match self {
            Self::Sqlite(store) => store.flush(),
            Self::Postgres(pg) => pg.flush(),
        }
    }
}

impl MetaTx for BackendTx<'_> {
    fn create_repo(&mut self, name: &str) -> Result<RepoId> {
        match self {
            Self::Sqlite(tx) => tx.create_repo(name),
            Self::Postgres(pg) => pg.create_repo(name),
        }
    }

    fn repo_id(&mut self, name: &str) -> Result<RepoId> {
        match self {
            Self::Sqlite(tx) => tx.repo_id(name),
            Self::Postgres(pg) => pg.repo_id(name),
        }
    }

    fn insert_changeset(&mut self, repo: RepoId, cs: &Changeset) -> Result<ChangesetId> {
        match self {
            Self::Sqlite(tx) => tx.insert_changeset(repo, cs),
            Self::Postgres(pg) => pg.insert_changeset(repo, cs),
        }
    }

    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset> {
        match self {
            Self::Sqlite(tx) => tx.get_changeset(repo, id),
            Self::Postgres(pg) => pg.get_changeset(repo, id),
        }
    }

    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.has_changeset(repo, id),
            Self::Postgres(pg) => pg.has_changeset(repo, id),
        }
    }

    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64> {
        match self {
            Self::Sqlite(tx) => tx.generation(repo, id),
            Self::Postgres(pg) => pg.generation(repo, id),
        }
    }

    fn generation_of(&mut self, id: ChangesetId) -> Result<i64> {
        match self {
            Self::Sqlite(tx) => tx.generation_of(id),
            Self::Postgres(pg) => pg.generation_of(id),
        }
    }

    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>> {
        match self {
            Self::Sqlite(tx) => tx.first_parent(id),
            Self::Postgres(pg) => pg.first_parent(id),
        }
    }

    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>> {
        match self {
            Self::Sqlite(tx) => tx.parents(id, id_gen),
            Self::Postgres(pg) => pg.parents(id, id_gen),
        }
    }

    fn get_ref(&mut self, repo: RepoId, name: &str) -> Result<Option<RefState>> {
        match self {
            Self::Sqlite(tx) => tx.get_ref(repo, name),
            Self::Postgres(pg) => pg.get_ref(repo, name),
        }
    }

    fn delete_derived_below(&mut self, kind: &str, version: u32, limit: usize) -> Result<usize> {
        match self {
            Self::Sqlite(tx) => tx.delete_derived_below(kind, version, limit),
            Self::Postgres(pg) => pg.delete_derived_below(kind, version, limit),
        }
    }

    fn get_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
    ) -> Result<Option<TreeId>> {
        match self {
            Self::Sqlite(tx) => tx.get_view_tree(repo, digest, version, tree, ctx),
            Self::Postgres(pg) => pg.get_view_tree(repo, digest, version, tree, ctx),
        }
    }

    fn put_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
        view: TreeId,
    ) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.put_view_tree(repo, digest, version, tree, ctx, view),
            Self::Postgres(pg) => pg.put_view_tree(repo, digest, version, tree, ctx, view),
        }
    }

    fn delete_stale_view_trees(
        &mut self,
        repo: RepoId,
        live: &[[u8; 32]],
        version: u32,
        limit: usize,
    ) -> Result<usize> {
        match self {
            Self::Sqlite(tx) => tx.delete_stale_view_trees(repo, live, version, limit),
            Self::Postgres(pg) => pg.delete_stale_view_trees(repo, live, version, limit),
        }
    }

    fn put_tree(&mut self, tree: &Tree) -> Result<TreeId> {
        match self {
            Self::Sqlite(tx) => tx.put_tree(tree),
            Self::Postgres(pg) => pg.put_tree(tree),
        }
    }

    fn get_tree(&mut self, id: TreeId) -> Result<Tree> {
        match self {
            Self::Sqlite(tx) => tx.get_tree(id),
            Self::Postgres(pg) => pg.get_tree(id),
        }
    }

    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>> {
        match self {
            Self::Sqlite(tx) => tx.get_tree_body(id),
            Self::Postgres(pg) => pg.get_tree_body(id),
        }
    }

    fn tree_size(&mut self, id: TreeId) -> Result<usize> {
        match self {
            Self::Sqlite(tx) => tx.tree_size(id),
            Self::Postgres(pg) => pg.tree_size(id),
        }
    }

    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.add_repo_blob(repo, blob),
            Self::Postgres(pg) => pg.add_repo_blob(repo, blob),
        }
    }

    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<(RepoId, i64, String)>> {
        match self {
            Self::Sqlite(tx) => tx.stale_lands(stale_secs),
            Self::Postgres(pg) => pg.stale_lands(stale_secs),
        }
    }

    fn reject_stale_land(&mut self, land_id: i64, detail: String) -> Result<u64> {
        match self {
            Self::Sqlite(tx) => tx.reject_stale_land(land_id, detail),
            Self::Postgres(pg) => pg.reject_stale_land(land_id, detail),
        }
    }

    fn expire_lands(&mut self) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.expire_lands(),
            Self::Postgres(pg) => pg.expire_lands(),
        }
    }

    fn put_history_node(&mut self, id: NodeId, body: Vec<u8>) -> Result<(NodeId, bool)> {
        match self {
            Self::Sqlite(tx) => tx.put_history_node(id, body),
            Self::Postgres(pg) => pg.put_history_node(id, body),
        }
    }

    fn get_history_node(&mut self, id: NodeId) -> Result<(Vec<u8>, usize)> {
        match self {
            Self::Sqlite(tx) => tx.get_history_node(id),
            Self::Postgres(pg) => pg.get_history_node(id),
        }
    }

    fn repos(&mut self) -> Result<Vec<RepoId>> {
        match self {
            Self::Sqlite(tx) => tx.repos(),
            Self::Postgres(pg) => pg.repos(),
        }
    }

    fn repo_changesets_after(
        &mut self,
        from_repo: RepoId,
        from_cs: &[u8],
        limit: usize,
    ) -> Result<Vec<RepoChangeset>> {
        match self {
            Self::Sqlite(tx) => tx.repo_changesets_after(from_repo, from_cs, limit),
            Self::Postgres(pg) => pg.repo_changesets_after(from_repo, from_cs, limit),
        }
    }

    fn repo_changeset_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        match self {
            Self::Sqlite(tx) => tx.repo_changeset_ids(repo),
            Self::Postgres(pg) => pg.repo_changeset_ids(repo),
        }
    }

    fn stored_changeset_body(&mut self, cs: &[u8]) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Sqlite(tx) => tx.stored_changeset_body(cs),
            Self::Postgres(pg) => pg.stored_changeset_body(cs),
        }
    }

    fn repo_blob_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        match self {
            Self::Sqlite(tx) => tx.repo_blob_ids(repo),
            Self::Postgres(pg) => pg.repo_blob_ids(repo),
        }
    }

    fn delete_repo_blob(&mut self, repo: RepoId, blob: &[u8]) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.delete_repo_blob(repo, blob),
            Self::Postgres(pg) => pg.delete_repo_blob(repo, blob),
        }
    }

    fn any_repo_names(&mut self, blob: &[u8]) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.any_repo_names(blob),
            Self::Postgres(pg) => pg.any_repo_names(blob),
        }
    }

    fn changeset_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.changeset_bodies_after(from, limit),
            Self::Postgres(pg) => pg.changeset_bodies_after(from, limit),
        }
    }

    fn tree_bodies_after(&mut self, from: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.tree_bodies_after(from, limit),
            Self::Postgres(pg) => pg.tree_bodies_after(from, limit),
        }
    }

    fn history_node_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.history_node_bodies_after(from, limit),
            Self::Postgres(pg) => pg.history_node_bodies_after(from, limit),
        }
    }

    fn repo_blob_rows_after(
        &mut self,
        from_repo: RepoId,
        from_blob: &[u8],
        limit: usize,
    ) -> Result<Vec<(RepoId, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.repo_blob_rows_after(from_repo, from_blob, limit),
            Self::Postgres(pg) => pg.repo_blob_rows_after(from_repo, from_blob, limit),
        }
    }

    fn dangling_refs(&mut self) -> Result<Vec<(RepoId, String, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.dangling_refs(),
            Self::Postgres(pg) => pg.dangling_refs(),
        }
    }

    fn lands_with_missing_changesets(&mut self) -> Result<Vec<(i64, Vec<u8>, &'static str)>> {
        match self {
            Self::Sqlite(tx) => tx.lands_with_missing_changesets(),
            Self::Postgres(pg) => pg.lands_with_missing_changesets(),
        }
    }

    fn derived_rows_after(
        &mut self,
        kind: &str,
        version: u32,
        cs: &[u8],
        limit: usize,
    ) -> Result<Vec<FsckDerivedRow>> {
        match self {
            Self::Sqlite(tx) => tx.derived_rows_after(kind, version, cs, limit),
            Self::Postgres(pg) => pg.derived_rows_after(kind, version, cs, limit),
        }
    }

    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.repo_has_blob(repo, blob),
            Self::Postgres(pg) => pg.repo_has_blob(repo, blob),
        }
    }

    fn is_ancestor(
        &mut self,
        repo: RepoId,
        ancestor: ChangesetId,
        descendant: ChangesetId,
        max_visits: usize,
    ) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.is_ancestor(repo, ancestor, descendant, max_visits),
            Self::Postgres(pg) => pg.is_ancestor(repo, ancestor, descendant, max_visits),
        }
    }

    fn cas_ref(
        &mut self,
        repo: RepoId,
        name: &str,
        expected: Option<i64>,
        target: ChangesetId,
    ) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.cas_ref(repo, name, expected, target),
            Self::Postgres(pg) => pg.cas_ref(repo, name, expected, target),
        }
    }

    fn get_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
    ) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Sqlite(tx) => tx.get_derived(kind, version, cs),
            Self::Postgres(pg) => pg.get_derived(kind, version, cs),
        }
    }

    fn put_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
        value: &[u8],
    ) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.put_derived(kind, version, cs, value),
            Self::Postgres(pg) => pg.put_derived(kind, version, cs, value),
        }
    }

    fn record_land(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<(RepoId, LandOutcome)> {
        match self {
            Self::Sqlite(tx) => tx.record_land(caller, req, pending_limit),
            Self::Postgres(pg) => pg.record_land(caller, req, pending_limit),
        }
    }

    fn land_outcome(&mut self, repo: RepoId, caller: &Caller, land_id: i64) -> Result<LandOutcome> {
        match self {
            Self::Sqlite(tx) => tx.land_outcome(repo, caller, land_id),
            Self::Postgres(pg) => pg.land_outcome(repo, caller, land_id),
        }
    }

    fn set_land_state(
        &mut self,
        repo: RepoId,
        land_id: i64,
        state: LandState,
        result: Option<ChangesetId>,
        detail: String,
        conflict_paths: Vec<RepoPath>,
    ) -> Result<LandOutcome> {
        match self {
            Self::Sqlite(tx) => {
                tx.set_land_state(repo, land_id, state, result, detail, conflict_paths)
            }
            Self::Postgres(pg) => {
                pg.set_land_state(repo, land_id, state, result, detail, conflict_paths)
            }
        }
    }

    fn put_git_import(&mut self, repo: RepoId, oid: ObjectId, cs: ChangesetId) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.put_git_import(repo, oid, cs),
            Self::Postgres(pg) => pg.put_git_import(repo, oid, cs),
        }
    }

    fn imported_commits(
        &mut self,
        repo: RepoId,
    ) -> Result<std::collections::HashMap<ObjectId, ChangesetId>> {
        match self {
            Self::Sqlite(tx) => tx.imported_commits(repo),
            Self::Postgres(pg) => pg.imported_commits(repo),
        }
    }
}

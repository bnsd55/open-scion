//! The metadata backend of a server: one of the supported databases,
//! behind the crate's internal metadata trait.
//!
//! A backend is opaque: the caller opens it (a data directory, or a
//! shared Postgres database from a URL) and reads or writes through it,
//! without naming which engine it holds. The transaction type differs
//! per backend, so the queries of a call go through `BackendTx`, which
//! forwards them to the backend's own transaction.

use gix::ObjectId;
use scion_core::{BlobId, Changeset, ChangesetId, RepoPath, Tree, TreeId};
use scion_proto::runtime::metrics::Metrics;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::Caller;
use crate::db::SqliteTx;
use crate::error::Result;
use crate::history::NodeId;
use crate::land::{LandOutcome, LandRequest, LandState};
use crate::meta::{FsckDerivedRow, Meta, MetaTx, RefState, RepoChangeset, RepoId, StaleLand};
use crate::pg::{Pg, PgTx};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use crate::store::{Blobs, Store};

#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/backend.rs"]
pub(crate) mod test_hooks;

/// The metadata storage of a server: the data directory, or a shared
/// Postgres database. The backend is opaque — which engine it holds is
/// private, so a caller matches classes of behavior through the
/// methods, never the engine: a new engine (or a major bump of one) is
/// then a private change of this crate, not a break of its API.
#[derive(Clone)]
pub struct Backend {
    inner: Inner,
}

/// The one engine this build holds, behind the opaque [`Backend`].

#[derive(Clone)]
enum Inner {
    /// One data directory on this machine.
    Sqlite(Store),
    /// A Postgres database that a fleet of servers can share.
    Postgres(Pg),
}

impl Backend {
    /// A backend on a prepared Postgres handle, for the crate's tests.
    #[cfg(test)]
    pub(crate) fn postgres(pg: Pg) -> Self {
        Self {
            inner: Inner::Postgres(pg),
        }
    }

    /// The Postgres handle, when this backend is one. The crate's own
    /// code reads it to tell the one-writer engines apart (a Postgres
    /// database always has one writer; a data directory may be shared).
    /// The schema versions this open moved, when the open migrated: the
    /// process that opened records the move in its audit log. A fresh
    /// open creates the schema and reports none.
    #[must_use]
    pub fn migrated(&self) -> Option<(u32, u32)> {
        match &self.inner {
            Inner::Sqlite(store) => store.migrated(),
            Inner::Postgres(pg) => pg.migrated(),
        }
    }

    pub(crate) fn as_postgres(&self) -> Option<&Pg> {
        match &self.inner {
            Inner::Postgres(pg) => Some(pg),
            Inner::Sqlite(_) => None,
        }
    }
}

/// The transaction of a [`Backend`]: the queries of one call, forwarded to
/// the backend's own transaction.
// Nameable through the pub `Meta` impl for `Backend` (its associated
// `Tx`), and the test seam serves the trait; unreachable_pub does not
// see trait-impl reachability.
#[allow(unreachable_pub)]
pub enum BackendTx<'a> {
    /// A transaction of the data directory.
    Sqlite(&'a mut SqliteTx<'a>),
    /// A transaction of the Postgres database.
    Postgres(PgTx<'a>),
}

impl BackendTx<'_> {}

/// Clients that a Postgres backend pools for its reads: as many as a
/// data directory holds, so the graph-walk bound of the server keeps its
/// meaning on both backends.
const PG_READERS: usize = crate::store::READERS;

const _: () = assert!(PG_READERS == crate::store::READERS);

/// The file that names the backend of a data directory. Absent, the
/// directory is a SQLite one: directories of older builds have no file.
const MARKER: &str = "backend";

/// The store UUID the data directory's marker held when an open began.
#[derive(Clone, Copy, Default)]
pub(crate) struct Marker {
    pub(crate) uuid: Option<[u8; 16]>,
}

impl Marker {
    /// Reads the marker of `data`, if any.
    pub(crate) fn read(data: &Path) -> Self {
        Self {
            uuid: marker_uuid(data),
        }
    }
}

/// Refuses to open Postgres on a directory whose metadata a SQLite
/// store already holds: its rows live in that database, and a second,
/// empty store here would look like a server with no data at all.
fn refuse_sqlite_dir(data: &Path) -> Result<()> {
    let db = data.join("scion.db");
    if db.metadata().is_ok_and(|m| m.len() > 0) {
        return Err(crate::error::Error::InvalidArgument(
            "this data directory already keeps its metadata in its own \
             database: drop --postgres-url to use it"
                .into(),
        ));
    }
    Ok(())
}

/// The marker's first line, if the file exists: `postgres` for this
/// build, and the second line, when present, is the store UUID. Read as
/// bytes, so a damaged marker is refused, not mistaken for absent.
fn marker_first_line(data: &Path) -> Result<Option<String>> {
    let bytes = match std::fs::read(data.join(MARKER)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(crate::error::Error::Io(e)),
    };
    let line = bytes.split(|&b| b == b'\n').next().unwrap_or_default();
    // For the comparison only: the file this build wrote is ASCII, and a
    // byte that is not UTF-8 becomes a replacement character, which the
    // comparison refuses.
    let text = String::from_utf8_lossy(line);
    Ok(Some(text.trim().to_owned()))
}

/// Refuses a marker whose first line is neither `postgres` nor absent.
/// This build writes either nothing (no marker, the directories of older
/// builds) or `postgres`; an empty or garbled marker is damage, and a
/// marker read as absent opens a Postgres directory as a fresh, empty
/// SQLite store, which looks like a server with no data at all.
fn refuse_damaged_marker(data: &Path) -> Result<()> {
    match marker_first_line(data)? {
        Some(first) if first != "postgres" => {
            let path = data.join(MARKER);
            Err(crate::error::Error::FailedPrecondition(format!(
                "the backend marker {} is damaged: its first line is \
                 {:?}, but this build wrote either no marker or `postgres`. \
                 Check whether the directory was restored over, \
                 hand-edited, or written by another build; give \
                 --postgres-url if its metadata lives in Postgres",
                path.display(),
                first
            )))
        }
        _ => Ok(()),
    }
}

/// Refuses to open the data directory as SQLite when its marker names
/// Postgres: its rows live in the database, and a second, empty store
/// here would look like a server with no data at all.
fn refuse_postgres_marker(data: &Path) -> Result<()> {
    if marker_first_line(data)?.as_deref() == Some("postgres") {
        return Err(crate::error::Error::InvalidArgument(
            "this data directory keeps its metadata in Postgres: \
             give --postgres-url"
                .into(),
        ));
    }
    Ok(())
}

/// The store UUID a marker carries, if it names one: 32 hex digits on
/// its second line.
/// The store UUID the data directory's marker holds, for the archive's
/// manifest: it names where the metadata came from.
pub(crate) fn store_uuid_of(data: &Path) -> Option<[u8; 16]> {
    marker_uuid(data)
}

fn marker_uuid(data: &Path) -> Option<[u8; 16]> {
    let text = std::fs::read_to_string(data.join(MARKER)).ok()?;
    let line = text.lines().nth(1)?.trim();
    if line.len() != 32 || !line.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&line[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

/// Puts the database's UUID in the marker, and refuses when the marker
/// names another store: a database restored on the directory of another
/// store would serve metadata whose blobs are not on this disk. A
/// refusal writes nothing: the UUID was compared in the database open,
/// before any blob of this directory was named.
fn marker_agrees(data: &Path, uuid: [u8; 16]) -> Result<()> {
    let marker = data.join(MARKER);
    match marker_uuid(data) {
        Some(held) if held != uuid => {
            return Err(crate::error::Error::FailedPrecondition(format!(
                "this data directory belongs to the store whose UUID is \
                 {}, but the database names the store {}: one was \
                 restored over the other; give the directory of the \
                 database's own store, or a directory the database has \
                 never served",
                hex16(&held),
                hex16(&uuid)
            )));
        }
        Some(_) => {}
        None => {
            // A fresh pair, a marker that predates the UUID, or a first
            // open cut before it wrote the marker: the database's
            // identity is the one this directory takes, and
            // `write_marker` writes it durable at once.
            write_marker(&marker, uuid)?;
        }
    }
    Ok(())
}

/// Writes the marker with `uuid` on its second line, durable: a marker
/// half-written would name no store and refuse the next open.
fn write_marker(marker: &Path, uuid: [u8; 16]) -> Result<()> {
    let tmp = tempfile::NamedTempFile::new_in(marker.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(crate::error::Error::Io)?;
    (|| -> std::io::Result<()> {
        use std::io::Write as _;
        tmp.as_file()
            .write_all(format!("postgres\n{}\n", hex16(&uuid)).as_bytes())?;
        tmp.as_file().sync_all()?;
        tmp.persist(marker).map_err(|e| e.error)?;
        if let Some(dir) = marker.parent() {
            let _ = crate::store::fsync_dir(dir);
        }
        Ok(())
    })()
    .map_err(crate::error::Error::Io)
}

/// 16 bytes as 32 lowercase hex digits.
fn hex16(bytes: &[u8; 16]) -> String {
    let mut hex = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

impl Backend {
    /// Where a ref points.
    ///
    /// # Errors
    ///
    /// [`crate::error::Error::NotFound`] if the repo or ref does not
    /// exist.
    pub fn resolve_ref(&self, repo: &str, name: &str) -> Result<ChangesetId> {
        use crate::meta::Meta as _;
        self.read(|c| {
            let repo = c.repo_id(repo)?;
            c.get_ref(repo, name)?
                .map(|r| r.target)
                .ok_or_else(|| crate::error::Error::NotFound(format!("ref {name:?}")))
        })
    }

    /// The derive walks of this backend: which are in flight, and the cap
    /// on how many may be.
    pub(crate) fn derive_flights(&self) -> &crate::derived::DeriveFlights {
        match &self.inner {
            Inner::Sqlite(store) => store.derive_flights(),
            Inner::Postgres(pg) => pg.derive_flights(),
        }
    }

    /// How many writers wait for the write connection.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn writers_waiting(&self) -> usize {
        match &self.inner {
            Inner::Sqlite(store) => store.writers_waiting(),
            Inner::Postgres(pg) => pg.writers_waiting(),
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
        match &self.inner {
            Inner::Sqlite(store) => store.wait_for_writers(deadline),
            Inner::Postgres(pg) => pg.wait_for_writers(deadline),
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
            refuse_damaged_marker(data)?;
            let marker = Marker::read(data);
            refuse_sqlite_dir(data)?;
            let pg = Pg::open_exclusive(url, ca, PG_READERS, data, marker)?;
            marker_agrees(data, pg.store_uuid())?;
            Ok(Self {
                inner: Inner::Postgres(pg),
            })
        } else {
            refuse_damaged_marker(data)?;
            refuse_postgres_marker(data)?;
            Ok(Self {
                inner: Inner::Sqlite(Store::open(data)?),
            })
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
            refuse_damaged_marker(data)?;
            let marker = Marker::read(data);
            refuse_sqlite_dir(data)?;
            let pg = Pg::open(url, ca, PG_READERS, data, marker)?;
            marker_agrees(data, pg.store_uuid())?;
            Ok(Self {
                inner: Inner::Postgres(pg),
            })
        } else {
            refuse_damaged_marker(data)?;
            refuse_postgres_marker(data)?;
            Ok(Self {
                inner: Inner::Sqlite(Store::open_shared(data)?),
            })
        }
    }

    /// Whether this backend lost its database to another process: every
    /// read and write fails from then on. SQLite has no lock to lose, so
    /// it is never fenced.
    #[must_use]
    pub fn fenced(&self) -> bool {
        match &self.inner {
            Inner::Sqlite(_) => false,
            Inner::Postgres(pg) => pg.fenced(),
        }
    }

    /// The flag of [`Backend::fenced`], for the health service of a
    /// serving process: it flips to `NOT_SERVING` when the fence trips.
    /// None on SQLite, which has no lock to lose. The server sets it on
    /// the health service before it serves; it is wiring, not a test
    /// seam, so it is public and documented instead of doc(hidden).
    #[must_use]
    pub fn fence_flag(&self) -> Option<Arc<AtomicBool>> {
        match &self.inner {
            Inner::Sqlite(_) => None,
            Inner::Postgres(pg) => Some(pg.fence_flag()),
        }
    }

    /// The blob folder. Blob files stay on local disk in every backend.
    #[must_use]
    pub fn blobs(&self) -> &Blobs {
        match &self.inner {
            Inner::Sqlite(store) => store.blobs(),
            Inner::Postgres(pg) => pg.blobs(),
        }
    }

    /// Whether the backend is a data directory that other processes share.
    /// A shared one keeps its write transactions short; a Postgres database
    /// sorts its writers out itself.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        match &self.inner {
            Inner::Sqlite(store) => store.is_shared(),
            Inner::Postgres(_) => false,
        }
    }

    /// Gives the backend the server's metrics registry, once before the
    /// server serves: the Postgres pool's reader metrics (a no-op on the
    /// data-directory backend, whose readers are threads of one process),
    /// and on the data directory the blob full flushes, the derive-walk
    /// gauges and the collector-key wait. Without it, import, fsck and
    /// gc record nothing.
    pub fn set_metrics(&self, metrics: Arc<Metrics>) {
        match &self.inner {
            Inner::Sqlite(store) => store.set_metrics(metrics),
            Inner::Postgres(pg) => pg.set_metrics(metrics),
        }
    }

    /// The age of the oldest open Postgres read transaction, for the
    /// tx-age gauge. Zero on the data directory backend, whose reads are
    /// immediate and whose readers are pages, not connections.
    #[must_use]
    pub fn pg_tx_age(&self) -> Duration {
        match &self.inner {
            Inner::Sqlite(_) => Duration::ZERO,
            Inner::Postgres(pg) => pg.tx_age(),
        }
    }

    /// Runs `f` so every statement sees one snapshot, as an export's
    /// sections claim. On Postgres, [`Backend::read`] already runs in one
    /// REPEATABLE READ transaction. On SQLite, [`Backend::read`] runs each
    /// statement autocommit, so the snapshot needs an explicit read
    /// transaction on the reader connection.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from the transaction's
    /// begin.
    pub(crate) fn read_snapshot<T>(
        &self,
        f: impl FnOnce(&mut BackendTx<'_>) -> Result<T>,
    ) -> Result<T> {
        match &self.inner {
            Inner::Sqlite(store) => store.read_snapshot(|tx| f(&mut BackendTx::Sqlite(tx))),
            Inner::Postgres(pg) => pg.read(|tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }
}

impl Backend {}

impl Meta for Backend {
    type Tx<'a>
        = BackendTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match &self.inner {
            Inner::Sqlite(store) => store.read(|tx| f(&mut BackendTx::Sqlite(tx))),
            Inner::Postgres(pg) => pg.read(|tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }

    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64) {
        match &self.inner {
            Inner::Sqlite(store) => {
                let (result, seen) = store.read_counted(|tx| f(&mut BackendTx::Sqlite(tx)));
                (result, seen)
            }
            Inner::Postgres(pg) => {
                let (result, seen) =
                    pg.read_counted(|tx| f(&mut BackendTx::Postgres(tx.reborrow())));
                (result, seen)
            }
        }
    }

    fn wait_durable(&self, seen: u64) -> Result<()> {
        match &self.inner {
            Inner::Sqlite(store) => store.wait_durable(seen),
            Inner::Postgres(pg) => pg.wait_durable(seen),
        }
    }

    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match &self.inner {
            Inner::Sqlite(store) => store.read_unflushed(|tx| f(&mut BackendTx::Sqlite(tx))),
            Inner::Postgres(pg) => {
                pg.read_unflushed(|tx| f(&mut BackendTx::Postgres(tx.reborrow())))
            }
        }
    }

    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match &self.inner {
            Inner::Sqlite(store) => store.commit(move |tx| f(&mut BackendTx::Sqlite(tx))),
            Inner::Postgres(pg) => pg.commit(move |tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }

    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        match &self.inner {
            Inner::Sqlite(store) => store.write(move |tx| f(&mut BackendTx::Sqlite(tx))),
            Inner::Postgres(pg) => pg.write(move |tx| f(&mut BackendTx::Postgres(tx.reborrow()))),
        }
    }

    fn flush(&self) -> Result<()> {
        match &self.inner {
            Inner::Sqlite(store) => store.flush(),
            Inner::Postgres(pg) => pg.flush(),
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

    fn set_changeset_batching(&mut self, on: bool) {
        match self {
            Self::Sqlite(tx) => tx.set_changeset_batching(on),
            Self::Postgres(pg) => pg.set_changeset_batching(on),
        }
    }

    fn preload_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.preload_derived_chain(kind, version, start, limit),
            Self::Postgres(pg) => pg.preload_derived_chain(kind, version, start, limit),
        }
    }

    fn changeset_batching(&self) -> bool {
        match self {
            Self::Sqlite(tx) => tx.changeset_batching(),
            Self::Postgres(pg) => pg.changeset_batching(),
        }
    }

    fn flush_changeset_rows(&mut self, repo: RepoId) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.flush_changeset_rows(repo),
            Self::Postgres(pg) => pg.flush_changeset_rows(repo),
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

    fn first_parent_page(
        &mut self,
        repo: RepoId,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.first_parent_page(repo, start, limit),
            Self::Postgres(pg) => pg.first_parent_page(repo, start, limit),
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

    #[cfg(any(test, feature = "test-hooks"))]
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

    fn parents_many(
        &mut self,
        merges: &[(ChangesetId, i64)],
    ) -> Result<Vec<Vec<(ChangesetId, i64)>>> {
        match self {
            Self::Sqlite(tx) => tx.parents_many(merges),
            Self::Postgres(pg) => pg.parents_many(merges),
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

    fn put_trees(&mut self, trees: &[(TreeId, Vec<u8>)]) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.put_trees(trees),
            Self::Postgres(pg) => pg.put_trees(trees),
        }
    }

    fn get_tree(&mut self, id: TreeId) -> Result<Tree> {
        match self {
            Self::Sqlite(tx) => tx.get_tree(id),
            Self::Postgres(pg) => pg.get_tree(id),
        }
    }

    fn get_trees(&mut self, ids: &[TreeId]) -> Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Sqlite(tx) => tx.get_trees(ids),
            Self::Postgres(pg) => pg.get_trees(ids),
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

    fn add_repo_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.add_repo_blobs(repo, blobs),
            Self::Postgres(pg) => pg.add_repo_blobs(repo, blobs),
        }
    }

    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<StaleLand>> {
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

    fn repos(&mut self) -> Result<Vec<(RepoId, String)>> {
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

    fn stored_changeset_bodies(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.stored_changeset_bodies(ids),
            Self::Postgres(pg) => pg.stored_changeset_bodies(ids),
        }
    }

    fn stored_changeset_body_lengths(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, i64)>> {
        match self {
            Self::Sqlite(tx) => tx.stored_changeset_body_lengths(ids),
            Self::Postgres(pg) => pg.stored_changeset_body_lengths(ids),
        }
    }

    fn repo_blob_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>> {
        match self {
            Self::Sqlite(tx) => tx.repo_blob_ids(repo),
            Self::Postgres(pg) => pg.repo_blob_ids(repo),
        }
    }

    fn repo_changesets_with_bodies(
        &mut self,
        repo: RepoId,
    ) -> Result<Vec<(Vec<u8>, i64, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.repo_changesets_with_bodies(repo),
            Self::Postgres(pg) => pg.repo_changesets_with_bodies(repo),
        }
    }

    fn list_refs(&mut self, repo: RepoId) -> Result<Vec<(String, Vec<u8>, i64)>> {
        match self {
            Self::Sqlite(tx) => tx.list_refs(repo),
            Self::Postgres(pg) => pg.list_refs(repo),
        }
    }

    fn pending_lands(&mut self, repo: RepoId) -> Result<Vec<crate::meta::PendingLand>> {
        match self {
            Self::Sqlite(tx) => tx.pending_lands(repo),
            Self::Postgres(pg) => pg.pending_lands(repo),
        }
    }

    fn delete_repo_blob(&mut self, repo: RepoId, blob: &[u8]) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.delete_repo_blob(repo, blob),
            Self::Postgres(pg) => pg.delete_repo_blob(repo, blob),
        }
    }

    fn delete_repo_blobs(&mut self, repo: RepoId, blobs: &[Vec<u8>]) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.delete_repo_blobs(repo, blobs),
            Self::Postgres(pg) => pg.delete_repo_blobs(repo, blobs),
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

    fn clear_restore_mark(&mut self) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.clear_restore_mark(),
            Self::Postgres(pg) => pg.clear_restore_mark(),
        }
    }

    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        match self {
            Self::Sqlite(tx) => tx.repo_has_blob(repo, blob),
            Self::Postgres(pg) => pg.repo_has_blob(repo, blob),
        }
    }

    fn repo_missing_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<Vec<BlobId>> {
        match self {
            Self::Sqlite(tx) => tx.repo_missing_blobs(repo, blobs),
            Self::Postgres(pg) => pg.repo_missing_blobs(repo, blobs),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
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

    fn restore_ref(&mut self, repo: RepoId, name: &str, target: &[u8], version: i64) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.restore_ref(repo, name, target, version),
            Self::Postgres(pg) => pg.restore_ref(repo, name, target, version),
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

    fn get_derived_batch(
        &mut self,
        kind: &str,
        version: u32,
        ids: &[ChangesetId],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Sqlite(tx) => tx.get_derived_batch(kind, version, ids),
            Self::Postgres(pg) => pg.get_derived_batch(kind, version, ids),
        }
    }

    fn get_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, Vec<u8>)>> {
        match self {
            Self::Sqlite(tx) => tx.get_derived_chain(kind, version, start, limit),
            Self::Postgres(pg) => pg.get_derived_chain(kind, version, start, limit),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn bulk_seed(
        &mut self,
        repo: RepoId,
        css: &[scion_core::Changeset],
        derived: &[(&'static str, u32, ChangesetId, Vec<u8>)],
    ) -> Result<Vec<ChangesetId>> {
        match self {
            Self::Sqlite(tx) => tx.bulk_seed(repo, css, derived),
            Self::Postgres(pg) => pg.bulk_seed(repo, css, derived),
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

    fn put_git_imports(&mut self, repo: RepoId, pairs: &[(ObjectId, ChangesetId)]) -> Result<()> {
        match self {
            Self::Sqlite(tx) => tx.put_git_imports(repo, pairs),
            Self::Postgres(pg) => pg.put_git_imports(repo, pairs),
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

/// The fixtures and the checks of the old-store tests (#318): a data
/// store of the previous schema version must open on this build, migrate
/// forward, and serve what it held. The SQLite test builds its store at
/// version 1, the Postgres test at its version 1; the checks here run on
/// both.
#[cfg(test)]
pub(crate) mod old_store {
    use crate::deadline::Deadline;
    use crate::meta::{Meta as _, MetaTx as _};
    use scion_core::{BlobId, Changeset, FileState, RepoPath};

    /// The bytes of the two files the old store holds.
    pub(crate) const A: &[u8] = b"the old store's README\n";
    pub(crate) const B: &[u8] = b"\x00\x01\xfeold bytes\n";

    /// The root changeset the old store holds: it puts both files. The
    /// files name their content blobs, so the blob ids come first — each
    /// fixture writes the blobs with this build's writer, then names
    /// them here.
    pub(crate) fn changeset(blob_a: BlobId, blob_b: BlobId) -> Changeset {
        Changeset::new(
            vec![],
            scion_core::IdentityTime::new(
                b"Ada".to_vec(),
                b"ada@example.com".to_vec(),
                1_700_000_000,
                0,
            )
            .expect("identity fits the format"),
            None,
            b"root\n".to_vec(),
            [
                (
                    RepoPath::new("README").unwrap(),
                    Some(FileState::Regular {
                        content: blob_a,
                        executable: false,
                    }),
                ),
                (
                    RepoPath::new("data.bin").unwrap(),
                    Some(FileState::Regular {
                        content: blob_b,
                        executable: false,
                    }),
                ),
            ]
            .into(),
        )
        .expect("changeset fits the format")
    }

    /// What a plain open of an old store must serve after it migrated:
    /// the ref and the changeset read back, the blobs read back byte for
    /// byte, a file reads through the rebuilt tree (the SQLite upgrade
    /// drops trees stored in the old format and rebuilds them from the
    /// changesets), and fsck is clean.
    pub(crate) fn assert_serves(
        backend: &super::Backend,
        cs: &Changeset,
        id: scion_core::ChangesetId,
        blob_a: BlobId,
        blob_b: BlobId,
    ) {
        let repo = backend.read(|c| c.repo_id("main")).unwrap();
        let state = backend.read(|c| c.get_ref(repo, "main")).unwrap().unwrap();
        assert_eq!(state.target, id, "the ref survived the migration");
        assert_eq!(state.version, 1);
        let read = backend.read(|c| c.get_changeset(repo, id)).unwrap();
        assert_eq!(read.encode(), cs.encode(), "the changeset survived");
        assert_eq!(backend.blobs().get(blob_a).unwrap(), A);
        assert_eq!(backend.blobs().get(blob_b).unwrap(), B);
        let deadline =
            Deadline::new(std::time::Instant::now() + std::time::Duration::from_secs(30));
        let bytes = crate::read::read_file(
            backend,
            "main",
            id,
            &RepoPath::new("README").unwrap(),
            &deadline,
        )
        .unwrap();
        assert_eq!(bytes, A, "a file reads through the rebuilt tree");
        let report = crate::fsck::run(backend, 100, &mut |_| {}).unwrap();
        assert!(
            report.problems.is_empty() && report.old.is_empty(),
            "fsck found damage: {:?} {:?}",
            report.problems,
            report.old
        );
    }
}

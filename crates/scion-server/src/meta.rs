//! The metadata storage that a server needs, as one trait, with SQLite
//! behind it ([#47](https://github.com/bnsd55/open-scion/issues/47)).
//!
//! [`Store`] is the SQLite implementation: one data directory, one write
//! connection, readers around it, and the group flush that makes a reply
//! wait for durability. Its queries are [`MetaTx`] methods, split by table
//! owner: the shared tables in `db.rs`, the tables of lands in `land.rs`,
//! and so on. A backend that many servers share implements the same traits
//! with its own transaction type, and the callers do not change.
//!
//! The seven methods of [`Meta`] keep today's semantics, because the
//! durability rules of protocol section 4 must hold in every backend:
//!
//! - `read` is a strong read: it returns only after what it saw is durable.
//! - `read_unflushed` reads on the connection that writes, so it sees the
//!   caller's own writes before they are durable.
//! - `commit` runs one write transaction and returns before it is durable;
//!   the caller calls `flush` before it replies with anything the commit
//!   wrote.
//! - `write` is `commit` plus `flush`.
//! - `flush` returns once every earlier commit is durable.
//! - `read_counted` and `wait_durable` are the counted form of `read`, for
//!   a caller that reads first and waits before it replies.
//!
//! On a backend whose commit is durable on return, `write` is `commit`,
//! `flush` does nothing, and a read need not wait.

use scion_core::RepoPath;
use scion_core::{BlobId, Changeset, ChangesetId, Tree, TreeId};

use gix::ObjectId;

use crate::auth::Caller;
use crate::db::SqliteTx;
use crate::error::Result;
use crate::history::NodeId;
use crate::land::{LandOutcome, LandRequest, LandState};
use crate::store::Store;

/// The metadata storage of a server. One implementation per backend; a
/// caller holds a concrete one and calls these methods.
pub trait Meta: Send + Sync + 'static {
    /// One set of queries, alive for one read or one transaction.
    type Tx<'a>: MetaTx
    where
        Self: 'a;

    /// A strong read: `f` sees every commit that finished before it
    /// started, and the call returns only after what it saw is durable.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a flush error.
    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// Like [`Meta::read`], but returns before what `f` saw is durable,
    /// with a count that covers every commit `f` could see. The caller
    /// passes that count to [`Meta::wait_durable`] before it replies.
    ///
    /// # Errors
    ///
    /// What `f` returns.
    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64);

    /// Returns once every commit that a read with count `seen` could see is
    /// durable.
    ///
    /// # Errors
    ///
    /// A flush error.
    fn wait_durable(&self, seen: u64) -> Result<()>;

    /// A read on the connection that writes, so it sees the caller's own
    /// commits before they are durable. The caller flushes before it
    /// replies with anything those reads saw.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from taking the connection.
    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// Runs `f` in one write transaction. It commits when `f` returns `Ok`
    /// and rolls back otherwise, and returns before the commit is durable.
    /// A backend with retryable errors may run `f` again; it must not
    /// change anything outside the transaction.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from begin or commit.
    fn commit<T>(&self, f: impl FnMut(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// [`Meta::commit`], and then [`Meta::flush`]: durable on return.
    ///
    /// # Errors
    ///
    /// As [`Meta::commit`] and [`Meta::flush`].
    fn write<T>(&self, f: impl FnMut(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// Returns once every commit made before this call is durable. No
    /// reply shows a commit that is not durable.
    ///
    /// # Errors
    ///
    /// A flush error.
    fn flush(&self) -> Result<()>;
}

/// The queries on the metadata tables. Every query fails with a database
/// error, or with the `NotFound` or `Corrupt` error that its doc says; the
/// docs do not repeat that per method. A query may be called in a read or
/// in a write transaction; the ones that write are called in transactions.
#[allow(clippy::missing_errors_doc)]
pub trait MetaTx {
    /// Creates a repository, or returns the existing one with that name.
    fn create_repo(&mut self, name: &str) -> Result<RepoId>;

    /// Looks up a repository by name.
    fn repo_id(&mut self, name: &str) -> Result<RepoId>;

    /// Stores a changeset and adds it to `repo`. Safe to repeat.
    fn insert_changeset(&mut self, repo: RepoId, cs: &Changeset) -> Result<ChangesetId>;

    /// Reads a changeset of `repo`, and checks it against its ID.
    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset>;

    /// Whether `repo` may see changeset `id`.
    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool>;

    /// The generation of a changeset that `repo` may see.
    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64>;

    /// The generation of a changeset, in any repo. For derived data.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64>;

    /// The first parent of a changeset, if it has one.
    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>>;

    /// Every parent of `id`, in order, with its generation.
    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>>;

    /// Whether `ancestor` is `descendant` or reachable from it, within a
    /// bounded walk.
    fn is_ancestor(
        &mut self,
        repo: RepoId,
        ancestor: ChangesetId,
        descendant: ChangesetId,
        max_visits: usize,
    ) -> Result<bool>;

    /// Where a ref points, and the version to compare-and-swap against.
    fn get_ref(&mut self, repo: RepoId, name: &str) -> Result<Option<RefState>>;

    /// Moves a ref if its version is still `expected`; `None` creates it.
    fn cas_ref(
        &mut self,
        repo: RepoId,
        name: &str,
        expected: Option<i64>,
        target: ChangesetId,
    ) -> Result<bool>;

    /// The stored bytes of a derived value, if computed.
    fn get_derived(&mut self, kind: &str, version: u32, cs: ChangesetId)
    -> Result<Option<Vec<u8>>>;

    /// Records a derived value. A second write must agree with the first.
    fn put_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
        value: &[u8],
    ) -> Result<()>;

    /// Deletes up to `limit` derived values of `kind` below `version`.
    fn delete_derived_below(&mut self, kind: &str, version: u32, limit: usize) -> Result<usize>;

    /// The view that a rule-set digest of `version` derives for the real
    /// tree `tree` under the path-context digest `ctx`, if derived already.
    /// The changeset's root view is the row of its real root tree under the
    /// rule set's full context.
    fn get_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
    ) -> Result<Option<TreeId>>;

    /// Records a derived view tree. A second write must agree with the
    /// first: derivation is deterministic, so a mismatch means corruption.
    fn put_view_tree(
        &mut self,
        repo: RepoId,
        digest: &[u8; 32],
        version: u32,
        tree: TreeId,
        ctx: [u8; 32],
        view: TreeId,
    ) -> Result<()>;

    /// Deletes up to `limit` view rows of `repo` that no live rule-set
    /// digest can serve any more: rows of older projection versions, and
    /// rows of digests outside `live`. Returns how many went.
    fn delete_stale_view_trees(
        &mut self,
        repo: RepoId,
        live: &[[u8; 32]],
        version: u32,
        limit: usize,
    ) -> Result<usize>;

    /// Stores a tree. Safe to repeat.
    fn put_tree(&mut self, tree: &Tree) -> Result<TreeId>;

    /// Reads a tree, and checks it against its ID.
    fn get_tree(&mut self, id: TreeId) -> Result<Tree>;

    /// Reads a tree's stored `TreeV1` bytes, checked against its ID.
    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>>;

    /// The length of a tree's stored bytes, without reading them.
    fn tree_size(&mut self, id: TreeId) -> Result<usize>;

    /// Adds a blob to the blobs that `repo` may name. `Ok(true)` if new.
    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool>;

    /// Inserts a land, or finds the land for the same request key. Returns
    /// the repo and the land's outcome, pending or final. `pending_limit`
    /// bounds the lands one principal has pending; `None` turns it off.
    fn record_land(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<(RepoId, LandOutcome)>;

    /// Lists the pending lands that started more than `stale_secs` ago, as
    /// (repo, land ID, target ref).
    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<(RepoId, i64, String)>>;

    /// Rejects a land that is still pending, and returns whether it changed
    /// (1) or was no longer pending (0). The caller holds the ref's lock
    /// slot, so a retry that is finishing the land wins.
    fn reject_stale_land(&mut self, land_id: i64, detail: String) -> Result<u64>;

    /// Deletes up to a bound of lands that finished before the retention
    /// window, oldest first, keeping the newest.
    fn expire_lands(&mut self) -> Result<()>;

    /// Reads a land that `caller` started in `repo`.
    fn land_outcome(&mut self, repo: RepoId, caller: &Caller, land_id: i64) -> Result<LandOutcome>;

    /// Moves a pending land to its final state, and returns the outcome.
    fn set_land_state(
        &mut self,
        repo: RepoId,
        land_id: i64,
        state: LandState,
        result: Option<ChangesetId>,
        detail: String,
        conflict_paths: Vec<RepoPath>,
    ) -> Result<LandOutcome>;

    /// Records that a git commit became the changeset. Safe to repeat.
    fn put_git_import(&mut self, repo: RepoId, oid: ObjectId, cs: ChangesetId) -> Result<()>;

    /// Every git commit already imported into `repo`.
    fn imported_commits(
        &mut self,
        repo: RepoId,
    ) -> Result<std::collections::HashMap<ObjectId, ChangesetId>>;

    /// Stores a node of a path history under its content address. Safe to
    /// repeat. Returns whether the row was new, so a caller can charge only
    /// bytes that actually landed.
    fn put_history_node(&mut self, id: NodeId, body: Vec<u8>) -> Result<(NodeId, bool)>;

    /// Reads a history node's stored bytes, and returns them with the
    /// length of their stored form.
    fn get_history_node(&mut self, id: NodeId) -> Result<(Vec<u8>, usize)>;

    /// Whether `repo` may name `blob`.
    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool>;
}

/// The row ID of a repository, private to the backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RepoId(pub i64);

/// Where a ref points, and the version to compare-and-swap against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefState {
    /// The changeset the ref points to.
    pub target: ChangesetId,
    /// Bumped on every move.
    pub version: i64,
}

impl Meta for Store {
    type Tx<'a>
        = SqliteTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::read(self, |c| f(&mut SqliteTx(c)))
    }

    fn read_counted<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> (Result<T>, u64) {
        Store::read_counted(self, |c| f(&mut SqliteTx(c)))
    }

    fn wait_durable(&self, seen: u64) -> Result<()> {
        Store::wait_durable(self, seen)
    }

    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::read_unflushed(self, |c| f(&mut SqliteTx(c)))
    }

    fn commit<T>(&self, mut f: impl FnMut(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::commit(self, |c| f(&mut SqliteTx(c)))
    }

    fn write<T>(&self, mut f: impl FnMut(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::write(self, |c| f(&mut SqliteTx(c)))
    }

    fn flush(&self) -> Result<()> {
        Store::flush(self)
    }
}

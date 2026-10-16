//! The metadata storage that a server needs, as one trait, with SQLite
//! behind it ([#47](https://github.com/bnsd55/open-scion/issues/47)).
//!
//! [`Store`] is the SQLite implementation: one data directory, one write
//! connection, readers around it, and the group flush that makes a reply
//! wait for durability. Its queries live in `db.rs`, and [`SqliteTx`] hands
//! them out as [`MetaTx`] methods. A backend that many servers share
//! implements the same two traits with its own transaction type, and the
//! callers do not change.
//!
//! The five methods of [`Meta`] keep today's semantics, because the
//! durability rules of protocol section 4 must hold in every backend. On a
//! backend whose commits are durable on return, `write` is `commit` plus
//! nothing, `flush` does nothing, and `read` never waits.

use rusqlite::Connection;
use scion_core::{BlobId, Changeset, ChangesetId, Tree, TreeId};

use crate::db::{self, RefState, RepoId};
use crate::error::Result;
use crate::store::Store;

/// The metadata storage of a server. One implementation per backend;
/// callers hold a `Store` today and any `M: Meta` later.
pub trait Meta: Send + Sync + 'static {
    /// One set of queries, alive for one read or one transaction.
    type Tx<'a>: MetaTx
    where
        Self: 'a;

    /// A strong read: `f` sees every commit that finished before it
    /// started, and the call returns only after what `f` saw is durable.
    ///
    /// # Errors
    ///
    /// What `f` returns, or an error from [`Meta::flush`].
    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// A read on the connection that writes, so it sees this flow's own
    /// commits before they are durable. The caller flushes before it
    /// replies with anything those reads saw.
    ///
    /// # Errors
    ///
    /// What `f` returns.
    fn read_unflushed<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// Runs `f` in one write transaction. It commits when `f` returns `Ok`,
    /// and rolls back otherwise. Returns before the commit is durable: the
    /// caller calls [`Meta::flush`] before it replies with anything the
    /// commit wrote. A backend with retryable errors may run `f` again
    /// after a lost race; `f` must not change anything outside the
    /// transaction.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a backend error from begin or commit.
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
    /// An error from the backend. The commits stay; a retry sees them.
    fn flush(&self) -> Result<()>;
}

/// The queries on the shared tables: repos, changesets, the blobs of each
/// repo, refs, trees, and derived data. The tables of one module (lands,
/// git import) keep their queries on the same trait, next to these.
///
/// Every query fails with a database error, or with the `NotFound` or
/// `Corrupt` error that its db.rs twin says; the docs do not repeat that
/// per method.
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

    /// The generation of a changeset, in any repo. For derived data: the
    /// caller reaches `id` from a changeset whose repo it checked.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64>;

    /// The first parent of a changeset, if it has one.
    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>>;

    /// Every parent of `id`, in order, with its generation.
    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>>;

    /// Whether `ancestor` is `descendant` or reachable from it.
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

    /// Whether `repo` may name `blob`.
    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool>;
}

impl Meta for Store {
    type Tx<'a>
        = SqliteTx<'a>
    where
        Self: 'a;

    fn read<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::read(self, |c| f(&mut SqliteTx(c)))
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

/// The transaction handle of the SQLite [`Store`]: a reader's connection,
/// or the write connection inside one transaction.
pub struct SqliteTx<'a>(&'a Connection);

impl MetaTx for SqliteTx<'_> {
    fn create_repo(&mut self, name: &str) -> Result<RepoId> {
        db::create_repo(self.0, name)
    }

    fn repo_id(&mut self, name: &str) -> Result<RepoId> {
        db::repo_id(self.0, name)
    }

    fn insert_changeset(&mut self, repo: RepoId, cs: &Changeset) -> Result<ChangesetId> {
        db::insert_changeset(self.0, repo, cs)
    }

    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset> {
        db::get_changeset(self.0, repo, id)
    }

    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool> {
        db::has_changeset(self.0, repo, id)
    }

    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64> {
        db::generation(self.0, repo, id)
    }

    fn generation_of(&mut self, id: ChangesetId) -> Result<i64> {
        db::generation_of(self.0, id)
    }

    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>> {
        db::first_parent(self.0, id)
    }

    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>> {
        db::parents(self.0, id, id_gen)
    }

    fn is_ancestor(
        &mut self,
        repo: RepoId,
        ancestor: ChangesetId,
        descendant: ChangesetId,
        max_visits: usize,
    ) -> Result<bool> {
        db::is_ancestor(self.0, repo, ancestor, descendant, max_visits)
    }

    fn get_ref(&mut self, repo: RepoId, name: &str) -> Result<Option<RefState>> {
        db::get_ref(self.0, repo, name)
    }

    fn cas_ref(
        &mut self,
        repo: RepoId,
        name: &str,
        expected: Option<i64>,
        target: ChangesetId,
    ) -> Result<bool> {
        db::cas_ref(self.0, repo, name, expected, target)
    }

    fn get_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
    ) -> Result<Option<Vec<u8>>> {
        db::get_derived(self.0, kind, version, cs)
    }

    fn put_derived(
        &mut self,
        kind: &str,
        version: u32,
        cs: ChangesetId,
        value: &[u8],
    ) -> Result<()> {
        db::put_derived(self.0, kind, version, cs, value)
    }

    fn delete_derived_below(&mut self, kind: &str, version: u32, limit: usize) -> Result<usize> {
        db::delete_derived_below(self.0, kind, version, limit)
    }

    fn put_tree(&mut self, tree: &Tree) -> Result<TreeId> {
        db::put_tree(self.0, tree)
    }

    fn get_tree(&mut self, id: TreeId) -> Result<Tree> {
        db::get_tree(self.0, id)
    }

    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>> {
        db::get_tree_body(self.0, id)
    }

    fn tree_size(&mut self, id: TreeId) -> Result<usize> {
        db::tree_size(self.0, id)
    }

    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        db::add_repo_blob(self.0, repo, blob)
    }

    fn repo_has_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool> {
        db::repo_has_blob(self.0, repo, blob)
    }
}

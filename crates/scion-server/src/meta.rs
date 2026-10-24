//! The metadata storage that a server needs, as one trait, with SQLite
//! behind it ([#47](https://github.com/bnsd55/openscion/issues/47)).
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

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs), and unreachable_pub does not see
// through a re-export, so this marker would fire on all of them
// under any feature set.
#![allow(unreachable_pub)]
use scion_core::RepoPath;
use scion_core::{BlobId, Changeset, ChangesetId, Entry, Tree, TreeId};

use gix::ObjectId;

use crate::auth::Caller;
use crate::db::SqliteTx;
use crate::error::{Error, Result};
use crate::history::NodeId;
use crate::land::{LandOutcome, LandRequest, LandState};
use crate::store::Store;

/// One pending land the stale-land sweep found, with everything the
/// answer to its intent line names: the repo (by ID for the ref's lock,
/// by name for the line), the land ID, the target ref, the principal
/// and the request key.
pub struct StaleLand {
    /// The repo the land belongs to, as the ref's lock names it.
    pub repo: RepoId,
    /// The land row's ID.
    pub land_id: i64,
    /// The ref the land would move.
    pub target_ref: String,
    /// The principal that asked for the land.
    pub caller: String,
    /// The request key a retry would resume the land with.
    pub request_key: String,
    /// The repo's name, as an audit line carries it.
    pub repo_name: String,
}

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
    /// Every backend runs `f` exactly once; a retryable error comes back
    /// from the call instead of rerunning `f`, and `f` changes nothing
    /// outside the transaction.
    ///
    /// # Errors
    ///
    /// What `f` returns, or a database error from begin or commit.
    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// [`Meta::commit`], and then [`Meta::flush`]: durable on return.
    ///
    /// # Errors
    ///
    /// As [`Meta::commit`] and [`Meta::flush`].
    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T>;

    /// Returns once every commit made before this call is durable. No
    /// reply shows a commit that is not durable.
    ///
    /// # Errors
    ///
    /// A flush error.
    fn flush(&self) -> Result<()>;
}

/// Most ids one `get_derived_batch` call fetches. The statement stays
/// small (1,000 x 32-byte ids as one array parameter, the reply at most
/// 1,000 rows), and a walk reads at most 1,000 nodes per step, so one
/// batch covers one step.
pub(crate) const DERIVED_BATCH: usize = 1_000;

/// [`DERIVED_BATCH`] as the chain limit the statements take.
#[allow(clippy::cast_possible_wrap)] // 1,000, far from any wrap
pub(crate) const DERIVED_BATCH_I64: i64 = DERIVED_BATCH as i64;

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

    /// Turns the importer's changeset batching on or off. While it is on,
    /// `insert_changeset` writes the changesets row only: the parents and
    /// the repo membership of the transaction's changesets wait for
    /// [`MetaTx::flush_changeset_rows`], which writes them in batches at
    /// the transaction's end. The derived values of the transaction's
    /// changesets wait the same way: `put_derived` holds the row, and
    /// `get_derived` answers from the values the transaction inserted
    /// before it reads the database, so the batch's derives chain through
    /// it. A transaction that turns this on must flush before it ends,
    /// and its changesets must come parents first, so the batch's
    /// generations chain in memory.
    fn set_changeset_batching(&mut self, on: bool);

    /// Holds the derived values on the first-parent chain that starts at
    /// `start`, so the batch's reads of them cost nothing: the importer
    /// preloads the first commits' parents' chains, and a jump outside
    /// what the transaction inserted then costs one read for the whole
    /// chain, not one per commit.
    fn preload_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<()>;

    /// Whether the import's changeset batching is on: the caller writes
    /// the transaction's own changesets, so their derived checks that
    /// read what was just written wait for the flush.
    fn changeset_batching(&self) -> bool;

    /// Writes the parents and the repo membership that
    /// [`MetaTx::set_changeset_batching`] held back, one statement per
    /// 1,000 rows per table where the backend can.
    ///
    /// # Errors
    ///
    /// Database errors.
    fn flush_changeset_rows(&mut self, repo: RepoId) -> Result<()>;

    /// Reads a changeset of `repo`, and checks it against its ID.
    fn get_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<Changeset>;

    /// `start` and up to `limit - 1` of its first-parent ancestors, as
    /// (id, generation, body), newest first, in walk order: the page one
    /// statement fetches where the backend can. Every row is an ancestor
    /// of `start`, so the repo check rides the start row alone, as
    /// [`MetaTx::get_changeset`] checks it. The page is short at the root
    /// of the history.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if `repo` does not hold `start`.
    /// [`Error::Corrupt`] if a row's id is not a changeset id.
    fn first_parent_page(
        &mut self,
        repo: RepoId,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, i64, Vec<u8>)>>;

    /// Whether `repo` may see changeset `id`.
    fn has_changeset(&mut self, repo: RepoId, id: ChangesetId) -> Result<bool>;

    /// The generation of a changeset that `repo` may see.
    fn generation(&mut self, repo: RepoId, id: ChangesetId) -> Result<i64>;

    /// The generation of a changeset, in any repo. For derived data.
    fn generation_of(&mut self, id: ChangesetId) -> Result<i64>;

    /// The first parent of a changeset, if it has one.
    #[cfg(any(test, feature = "test-hooks"))]
    fn first_parent(&mut self, id: ChangesetId) -> Result<Option<ChangesetId>>;

    /// Every parent of `id`, in order, with its generation.
    fn parents(&mut self, id: ChangesetId, id_gen: i64) -> Result<Vec<(ChangesetId, i64)>>;

    /// The parents of up to [`DERIVED_BATCH`] merges, in the order asked:
    /// for each merge, its parents in order with their generations. One
    /// statement where the backend can. A merge asked twice in the batch
    /// gets its parents once per ask. A merge with no parent rows gives an
    /// empty list, as [`MetaTx::parents`] would.
    ///
    /// [`DERIVED_BATCH`]: crate::meta::DERIVED_BATCH
    fn parents_many(
        &mut self,
        merges: &[(ChangesetId, i64)],
    ) -> Result<Vec<Vec<(ChangesetId, i64)>>>;

    /// Whether `ancestor` is `descendant` or reachable from it, within a
    /// bounded walk.
    #[cfg(any(test, feature = "test-hooks"))]
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

    /// The stored bytes of up to [`DERIVED_BATCH`] derived values, in the
    /// order asked. One read transaction, one statement: a land between two
    /// ids of the batch cannot mix two states.
    ///
    /// [`DERIVED_BATCH`]: crate::meta::DERIVED_BATCH
    fn get_derived_batch(
        &mut self,
        kind: &str,
        version: u32,
        ids: &[ChangesetId],
    ) -> Result<Vec<Option<Vec<u8>>>>;

    /// The stored bytes of the derived values on the first-parent chain
    /// that starts at `start`: `start` itself and up to `limit - 1` of its
    /// ancestors, following first parents. One statement: the chain follows
    /// the indexed first-parent rows, and every fetched changeset is an
    /// ancestor of `start`, so no repo filter is needed. A changeset
    /// without a stored value has no row, as [`MetaTx::get_derived`]
    /// would. `limit` bounds the hops.
    fn get_derived_chain(
        &mut self,
        kind: &str,
        version: u32,
        start: ChangesetId,
        limit: i64,
    ) -> Result<Vec<(ChangesetId, Vec<u8>)>>;

    /// Writes a test seed: `css` oldest first, every parent before its
    /// children, file-less (no tree entries, no blobs), and the derived
    /// values to store beside them. One statement per batch, not per
    /// changeset, where the backend can. The default writes them one by
    /// one through the ordinary path; backends where a round trip costs
    /// real time override it. The rows must find an empty schema: the
    /// bulk write does not replay the ordinary write's conflict checks.
    ///
    /// Returns the ids, in `css` order.
    #[cfg(any(test, feature = "test-hooks"))]
    fn bulk_seed(
        &mut self,
        repo: RepoId,
        css: &[scion_core::Changeset],
        derived: &[(&'static str, u32, ChangesetId, Vec<u8>)],
    ) -> Result<Vec<ChangesetId>> {
        let mut ids = Vec::with_capacity(css.len());
        for cs in css {
            ids.push(self.insert_changeset(repo, cs)?);
        }
        for (kind, version, cs, value) in derived {
            self.put_derived(kind, *version, *cs, value)?;
        }
        Ok(ids)
    }

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

    /// Stores pre-encoded trees, whose ids are their content hashes. Safe
    /// to repeat. The batch is one round trip per table on Postgres,
    /// however deep the tree chain was; a store that has no batch form
    /// stores the rows one by one. No id may differ from its body's
    /// hash: the callers take each id from `encode_with_id`.
    fn put_trees(&mut self, trees: &[(TreeId, Vec<u8>)]) -> Result<()> {
        for (id, body) in trees {
            let (tree, body_id) = Tree::decode_with_id(body)
                .map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))?;
            if body_id != *id {
                return Err(Error::Corrupt(format!(
                    "tree {id} does not match its body's id {body_id}"
                )));
            }
            self.put_tree(&tree)?;
        }
        Ok(())
    }

    /// Reads a tree, and checks it against its ID.
    fn get_tree(&mut self, id: TreeId) -> Result<Tree>;

    /// The stored bytes of the trees the ids name, in the order asked:
    /// `None` for an id that is not stored. Unchecked bytes: the caller
    /// decodes each and checks it against its id, as `get_tree` does.
    fn get_trees(&mut self, ids: &[TreeId]) -> Result<Vec<Option<Vec<u8>>>>;

    /// Reads a tree's stored `TreeV1` bytes, checked against its ID.
    fn get_tree_body(&mut self, id: TreeId) -> Result<Vec<u8>>;

    /// Finds one entry of a tree by name, from the tree's stored bytes,
    /// without decoding the whole node. The bytes are checked against
    /// their ID before the scan, and the scan refuses what the decoder
    /// refuses, so a body that is not canonical fails here as it would
    /// in [`MetaTx::get_tree`].
    fn lookup_tree_entry(&mut self, id: TreeId, name: &[u8]) -> Result<Option<Entry>> {
        let body = self.get_tree_body(id)?;
        Tree::find_entry(&body, name).map_err(|e| Error::Corrupt(format!("tree {id}: {e}")))
    }

    /// The length of a tree's stored bytes, without reading them.
    fn tree_size(&mut self, id: TreeId) -> Result<usize>;

    /// Adds a blob to the blobs that `repo` may name. `Ok(true)` if new.
    fn add_repo_blob(&mut self, repo: RepoId, blob: BlobId) -> Result<bool>;

    /// Adds every blob to the blobs that `repo` may name. The batch is
    /// one round trip on Postgres, however many blobs the changeset
    /// puts; a store that has no batch form adds them one by one. Each
    /// blob that is already there stays as it is.
    fn add_repo_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<()> {
        for blob in blobs {
            self.add_repo_blob(repo, *blob)?;
        }
        Ok(())
    }

    /// Inserts a land, or finds the land for the same request key. Returns
    /// the repo and the land's outcome, pending or final. `pending_limit`
    /// bounds the lands one principal has pending; `None` turns it off.
    fn record_land(
        &mut self,
        caller: &Caller,
        req: &LandRequest,
        pending_limit: Option<usize>,
    ) -> Result<(RepoId, LandOutcome)>;

    /// Lists the pending lands that started more than `stale_secs` ago, with
    /// what an answer to their intent names: the repo and its name, the land
    /// ID, the target ref, the principal and the request key.
    fn stale_lands(&mut self, stale_secs: i64) -> Result<Vec<StaleLand>>;

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

    /// Records that the git commits became the changesets, all of the
    /// pairs, in one statement where the backend can. Safe to repeat.
    fn put_git_imports(&mut self, repo: RepoId, pairs: &[(ObjectId, ChangesetId)]) -> Result<()>;

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

    /// The blobs of the list that `repo` may not name: what the member
    /// check of `CreateChangeset` refuses. One read for the whole list, so
    /// a changeset's cost does not grow with its change count.
    fn repo_missing_blobs(&mut self, repo: RepoId, blobs: &[BlobId]) -> Result<Vec<BlobId>>;

    /// Every repo with its name, in ID order. The walker of `gc`.
    fn repos(&mut self) -> Result<Vec<(RepoId, String)>>;

    /// The `(repo, changeset ID, body)` of the stored changesets after
    /// `(repo, cs)`, in that order, at most `limit`. The batched walk of
    /// `gc`'s live set.
    fn repo_changesets_after(
        &mut self,
        from_repo: RepoId,
        from_cs: &[u8],
        limit: usize,
    ) -> Result<Vec<RepoChangeset>>;

    /// The changeset IDs that `repo` holds, all of them.
    fn repo_changeset_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>>;

    /// The bodies of the changesets the ids name, as `(id, body)` for
    /// every id that has a stored body. Unchecked: `fsck` reads damaged
    /// bodies. The ids travel as one statement on Postgres: a batch is
    /// the size the caller chooses, and a caller that reads per id pays
    /// per statement.
    fn stored_changeset_bodies(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// The length of the stored body of every id that has one, as
    /// `(id, length)`: the gc's prune sizes its body chunks from the
    /// lengths before it fetches any body, so a chunk's memory is the
    /// byte budget plus one body, not the whole batch.
    fn stored_changeset_body_lengths(&mut self, ids: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, i64)>>;

    /// The blobs that `repo` may name, all of them.
    fn repo_blob_ids(&mut self, repo: RepoId) -> Result<Vec<Vec<u8>>>;

    /// The `(changeset ID, generation, body)` of the changesets `repo`
    /// holds, in generation order. The archive's export reads them in this
    /// order, and a restore replays them in it, so parents come first.
    fn repo_changesets_with_bodies(&mut self, repo: RepoId) -> Result<Vec<ChangesetRow>>;

    /// The refs of `repo`, as `(name, target, version)`, in name order.
    /// The archive's export reads them.
    fn list_refs(&mut self, repo: RepoId) -> Result<Vec<(String, Vec<u8>, i64)>>;

    /// The lands of `repo` that are still `pending`, as the caller, the
    /// request key, the target ref, and the input changeset, in land ID
    /// order. The archive's export reads them.
    fn pending_lands(&mut self, repo: RepoId) -> Result<Vec<PendingLand>>;

    /// Puts ref `name` of `repo` at `target` with the exact `version`,
    /// whatever is there now. The archive's restore uses it to carry the
    /// version the export read; live traffic moves refs with
    /// [`MetaTx::cas_ref`] instead, which bumps the version.
    ///
    /// # Errors
    ///
    /// A database error.
    fn restore_ref(&mut self, repo: RepoId, name: &str, target: &[u8], version: i64) -> Result<()>;

    /// Removes the row that names `blob` for `repo`.
    fn delete_repo_blob(&mut self, repo: RepoId, blob: &[u8]) -> Result<()>;

    /// Removes the rows that name any of `blobs` for `repo`, all of
    /// them, in one statement where the backend can.
    fn delete_repo_blobs(&mut self, repo: RepoId, blobs: &[Vec<u8>]) -> Result<()>;

    /// Whether any repo names `blob`.
    fn any_repo_names(&mut self, blob: &[u8]) -> Result<bool>;

    /// The `(changeset ID, body)` of the changesets after `from`, in ID
    /// order, at most `limit`. `fsck`'s walk over every stored changeset.
    fn changeset_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// The `(tree ID, body)` of the trees after `from`, in ID order, at
    /// most `limit`.
    fn tree_bodies_after(&mut self, from: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// The `(node ID, body)` of the history nodes after `from`, in ID
    /// order, at most `limit`.
    fn history_node_bodies_after(
        &mut self,
        from: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// The `repo_blobs` rows after `(repo, blob)`, at most `limit`.
    fn repo_blob_rows_after(
        &mut self,
        from_repo: RepoId,
        from_blob: &[u8],
        limit: usize,
    ) -> Result<Vec<(RepoId, Vec<u8>)>>;

    /// The refs whose target no `repo_changesets` row of their repo holds.
    fn dangling_refs(&mut self) -> Result<Vec<(RepoId, String, Vec<u8>)>>;

    /// Every land whose input or result changeset is not stored, as
    /// `(land, changeset, which column)`.
    fn lands_with_missing_changesets(&mut self) -> Result<Vec<(i64, Vec<u8>, &'static str)>>;

    /// The derived rows after `(kind, version, cs)`, at most `limit`, with
    /// whether their changeset is stored.
    fn derived_rows_after(
        &mut self,
        kind: &str,
        version: u32,
        cs: &[u8],
        limit: usize,
    ) -> Result<Vec<FsckDerivedRow>>;

    /// Removes the restore's staging mark, in the caller's transaction.
    /// Publication is atomic with this: a store that answers reads
    /// never carries the mark a later restore would drop it by.
    /// # Errors
    ///
    /// A database error.
    fn clear_restore_mark(&mut self) -> Result<()>;
}

/// The row ID of a repository, private to the backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepoId(pub i64);

/// One stored changeset with its repo, as the collector's walk reads it:
/// `(repo, changeset ID, body)`.
pub struct RepoChangeset(pub RepoId, pub Vec<u8>, pub Vec<u8>);

/// One row of [`MetaTx::repo_changesets_with_bodies`]: the changeset's
/// ID, its derived generation, and its encoded body.
pub type ChangesetRow = (Vec<u8>, i64, Vec<u8>);

/// A land that is still pending, as the archive's export reads it: the
/// fields a restore needs to record it again, so a retry with the same
/// request key resumes it.
pub struct PendingLand {
    /// The principal that sent the land.
    pub caller: String,
    /// The client's request key, the idempotency of the retry.
    pub request_key: String,
    /// The ref the land moves.
    pub target_ref: String,
    /// The changeset the land puts on the ref.
    pub input_cs: Vec<u8>,
}

/// A derived row of `fsck`: kind, version, changeset, value, and whether
/// the changeset is stored.
pub struct FsckDerivedRow(pub String, pub u32, pub Vec<u8>, pub Vec<u8>, pub bool);

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

    fn commit<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::commit(self, |c| f(&mut SqliteTx(c)))
    }

    fn write<T>(&self, f: impl FnOnce(&mut Self::Tx<'_>) -> Result<T>) -> Result<T> {
        Store::write(self, |c| f(&mut SqliteTx(c)))
    }

    fn flush(&self) -> Result<()> {
        Store::flush(self)
    }
}

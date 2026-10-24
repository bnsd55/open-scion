//! One-way import from a local git repository.
//!
//! Each git commit becomes one changeset. Its parents are the changesets of
//! the git parents, in git order. Its `changes` are the git tree diff against
//! the first parent. The import records each root tree as it goes, and checks
//! a sample of them against git before the batch that holds them commits.
//!
//! Git headers that Scion does not model (`encoding`, `gpgsig`, `mergetag`)
//! are dropped, so a changeset ID is not a function of the git object name.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
#[cfg(test)]
use std::sync::{Mutex, PoisonError};
// Both `Cell` users sit behind a cfg: the slow-record model of a test,
// and the check counter the test-hooks feature turns on.
#[cfg(any(test, feature = "test-hooks"))]
use std::cell::Cell;
use std::time::{Duration, Instant};

use gix::ObjectId;
use gix::actor::SignatureRef;
use hashlink::LruCache;
use scion_core::{
    BlobId, Changeset, ChangesetId, Entry, FileState, GitOid, IdentityTime, PathElem, RepoPath,
    TreeId,
};

use crate::backend::Backend;
use crate::deadline::{CALL_LIMIT, Deadline};
use crate::derived::Kind;
use crate::error::{Error, Git, Result};
use crate::graph;
use crate::meta::Meta as _;
use crate::meta::{MetaTx, RepoId};
use crate::store::{BlobBatch, BlobStatus, MAX_OBJECT_BYTES};
use crate::tree;
use crate::write::MAX_CHANGES;

/// `git_import.git_algo` for SHA-1 object names.
pub(crate) const SHA1: i64 = 1;
/// Commits per blob batch: their blobs are prepared and made durable
/// together, outside any write transaction.
const BATCH: usize = 1000;
/// A write transaction of an import ends once it has run this long, so a
/// server that shares the data directory waits at most this, well under its
/// `busy_timeout` of 5 s (issue #100). One commit's record and check can run
// SHORTCUT: one commit of a batch can still run longer than the budget, and
// a server write then waits for it. Ceiling: the changes and trees of the
// biggest commit. Upgrade: a separate write connection for the import, or
// per-statement yields.
const TX_BUDGET: Duration = Duration::from_secs(1);
/// How long an import stays off the write lock between two transactions, so
/// a server write that waited for one can take the lock before the next
/// transaction begins.
const TX_YIELD: Duration = Duration::from_millis(100);

/// How many times one batch waits out a busy write lock and runs again.
/// The batch's writes are idempotent, so a retry is safe; past the bound
/// the import fails with the busy error. A client that shares the data
/// directory reads this to bound its own wait for an import.
// The test seam re-exports this (test_support.import in lib.rs), and
// unreachable_pub does not see through a re-export.
#[allow(unreachable_pub)]
pub const BUSY_TRIES: u32 = 10;

/// The most swaps one import makes for one ref. Like a land's swaps (see
/// `MAX_ATTEMPTS` in `land`), a swap loses only to another move of the
/// ref; a ref that moves this often in a row would keep the import
/// walking its history, so past the bound the import stops and says so.
const REF_TRIES: u32 = 3;

/// A test's move of the ref between `point_ref`'s read of it and its
/// compare-and-swap, so the swap loses as if another server had moved the
/// ref. Tests set it and drop it back to `None`.
#[cfg(test)]
type RefMover = Box<dyn Fn(&Backend, RepoId, &str) -> Result<()> + Send>;
#[cfg(test)]
static REF_MOVER: Mutex<Option<RefMover>> = Mutex::new(None);

// How long the write transaction sleeps per commit of a batch, for the
// tests of a slow batch. It models a batch whose records are slow.
#[cfg(test)]
thread_local! {
    static SLOW_RECORD: Cell<Duration> = const { Cell::new(Duration::ZERO) };
}

thread_local! {
    /// Tree checks that [`Checker::check`] ran on this thread. Tests of a
    /// rolled-back batch read it: with the checker reset, the checks of
    /// the rolled-back transaction run again in the retry.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub static CHECKS: Cell<usize> = const { Cell::new(0) };
    /// A test's lower bound on one commit or tree object's bytes, and on
    /// one tree's entries: the refusal is proved with small objects, and
    /// the product caps stay the documented ones. `None` is the product.
    /// Thread-local by design: the cap applies only on the thread that
    /// set it, so an import on another thread runs at the product's caps.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-hooks"))]
    pub static STRUCTURE_CAPS: Cell<Option<(u64, u64)>> = const { Cell::new(None) };
}
/// Cache for decoded git objects. A tree diff re-reads the trees that the
/// previous commit's diff just read.
const OBJECT_CACHE_BYTES: usize = 64 << 20;
/// Most entries in each blob cache of an import. An entry takes at most about
/// 100 bytes, so a full cache takes at most about 26 MB. Most histories have
/// fewer distinct blobs, so they never evict.
const BLOB_CACHE_ENTRIES: usize = 1 << 18;

/// What to import.
#[derive(Clone, Debug)]
pub struct ImportOptions {
    /// Git revision to import, for example `HEAD` or `main`.
    pub rev: String,
    /// Scion ref to point at the imported tip.
    pub target_ref: String,
    /// How many non-root, non-merge commits to check against git, besides
    /// every root and merge. Only commits that this run imports are checked.
    pub verify_sample: usize,
}

/// What an import did.
#[derive(Clone, Debug)]
pub struct ImportReport {
    /// Commits reachable from the revision.
    pub commits: usize,
    /// Commits newly stored (the rest were imported before).
    pub imported: usize,
    /// Commits whose Scion tree was checked against the git tree.
    pub verified: usize,
    /// Refs the import moved onto the tip: 1 when `target_ref` took the
    /// tip, 0 when it already held or contained it.
    pub refs_moved: usize,
    /// The changeset for the imported revision.
    pub tip: ChangesetId,
}

/// Imports every commit reachable from `opts.rev` and points
/// `opts.target_ref` at the tip. Safe to repeat.
///
/// Commits are written in batches, parents first. A batch commits only after
/// its sampled commits match git, so an interrupted or failed import keeps
/// the batches before it and a rerun continues from there.
///
/// # Errors
///
/// [`Error::Git`] if git cannot be read, or a path or file is not valid in
/// Scion. [`Error::Corrupt`] if a verified commit's tree differs from git's,
/// or names a blob the import wrote that is not stored intact. The import
/// verifies the blobs it writes; a blob it skipped, because a committed row
/// of the repo already names it and its file is present, is trusted, and
/// damage inside an already stored file is fsck's job.
/// [`Error::FailedPrecondition`] if `opts.target_ref` points at a changeset
/// that the tip does not descend from; the commits stay imported and the ref
/// is left alone.
pub fn import_git(
    store: &Backend,
    repo: &str,
    git_dir: &Path,
    opts: &ImportOptions,
) -> Result<ImportReport> {
    import_with_cache(store, repo, git_dir, opts, BLOB_CACHE_ENTRIES, MAX_CHANGES)
}

/// Opens the git repository of an import and resolves the commit `rev`
/// names: the tip the import works toward. Only SHA-1 repositories are
/// supported.
fn open_tip(git_dir: &Path, rev: &str) -> Result<(gix::Repository, gix::hash::ObjectId)> {
    let mut git = gix::open(git_dir)
        .map_err(|e| Error::Git(Git::cause(format!("cannot open {}", git_dir.display()), e)))?;
    if git.object_hash() != gix::hash::Kind::Sha1 {
        return Err(Error::Git(Git::message(format!(
            "{}: only SHA-1 repositories are supported",
            git_dir.display()
        ))));
    }
    git.object_cache_size_if_unset(OBJECT_CACHE_BYTES);
    let tip = git
        .rev_parse_single(rev)
        .and_then(|id| id.object())
        .and_then(gix::Object::peel_to_commit)
        .map_err(|e| Error::Git(Git::cause(format!("cannot resolve {rev:?} to a commit"), e)))?
        .id;
    Ok((git, tip))
}

/// [`import_git`] with at most `cache_entries` entries in each blob cache,
/// and a commit capped at `max_changes` changed paths (the write path's
/// [`MAX_CHANGES`]; a test overrides it with a smaller one).
fn import_with_cache(
    store: &Backend,
    repo: &str,
    git_dir: &Path,
    opts: &ImportOptions,
    cache_entries: usize,
    max_changes: usize,
) -> Result<ImportReport> {
    let started = Instant::now();
    let (git, tip) = open_tip(git_dir, &opts.rev)?;

    // A shared import re-runs often: look the repo up before opening a
    // write transaction for it, so the common path does not commit at all
    // just to find a repo that is already there.
    let repo_id = match store.read(|c| c.repo_id(repo)) {
        Ok(repo_id) => repo_id,
        Err(Error::NotFound(_)) => store.write(|c| c.create_repo(repo))?,
        Err(error) => return Err(error),
    };
    let mut known = store.read(|c| c.imported_commits(repo_id))?;
    let plan = plan(&git, tip, &known, opts.verify_sample)?;
    tracing::info!(
        reachable = plan.reachable,
        new = plan.todo.len(),
        to_verify = plan.verify.len(),
        "git import planned"
    );

    let mut importer = Importer {
        git: &git,
        store,
        repo_id,
        blobs: LruCache::new(cache_entries),
        put: Vec::new(),
        max_changes,
    };
    let mut checker = Checker {
        git: &git,
        store,
        equal: HashSet::new(),
        blobs: LruCache::new(cache_entries),
        intact: LruCache::new(cache_entries),
    };
    let mut done = 0;
    for (batch_no, batch) in plan.todo.chunks(BATCH).enumerate() {
        // Blobs are made durable first, so no stored metadata points at a
        // blob that a crash could lose.
        let mut blobs = store.blobs().batch();
        let mut prepared = Vec::with_capacity(batch.len());
        let mut in_batch = HashSet::with_capacity(batch.len());
        for &commit in batch {
            let (cs, git_tree) = importer.changeset(commit, &known, &mut blobs)?;
            let id = cs.id();
            known.insert(commit, id);
            in_batch.insert(id);
            prepared.push((commit, cs, git_tree));
        }
        blobs.sync()?;
        // A put writes or reads back its blob, so the checker reads back only
        // blobs from earlier runs, or blobs that its cache dropped.
        checker
            .intact
            .extend(importer.put.drain(..).map(|blob| (blob, ())));
        // A server that shares the data directory waits for the transaction
        // below. So a first parent from before this batch gets its root tree
        // first, in steps: after a version bump it has none, and the
        // transaction would derive its whole history.
        for (_, cs, _) in &prepared {
            if let Some(&parent) = cs.parents().first().filter(|p| !in_batch.contains(*p)) {
                tree::fill_root_tree(store, repo_id, parent)?;
            }
        }
        store_batch(
            store,
            repo_id,
            &prepared,
            &plan,
            &mut checker,
            &mut done,
            started,
        )?;
        // A batch's commit has ended and the write lock is free: this is
        // the counted boundary a test holds the lock at.
        #[cfg(any(test, feature = "test-hooks"))]
        store.run_after_batch(batch_no);
        // A release build reads the batch index nowhere.
        #[cfg(not(any(test, feature = "test-hooks")))]
        let _ = batch_no;
    }

    // The tip was imported before, or planned and imported above.
    let tip = known
        .get(&tip)
        .copied()
        .ok_or_else(|| Error::Corrupt(format!("commit {tip} was not imported")))?;
    let pointed = point_ref(store, repo_id, &opts.target_ref, tip);
    // The ref may contain the tip, or have diverged from git, through a land
    // that the server has not flushed yet. A full flush covers it, as it
    // covers every commit that another process made before it.
    store.flush()?;
    let refs_moved = usize::from(pointed?);
    // Every planned check ran, or the import failed above.
    let report = ImportReport {
        commits: plan.reachable,
        imported: plan.todo.len(),
        verified: plan.verify.len(),
        refs_moved,
        tip,
    };
    tracing::info!(
        commits = report.commits,
        imported = report.imported,
        verified = report.verified,
        refs_moved,
        secs = started.elapsed().as_secs(),
        %tip,
        "git import done"
    );
    Ok(report)
}

/// Stores a batch's prepared commits: each in a write transaction, with its
/// check inside it. A chunk commits whole: a crash loses whole commits,
/// never half a one, and a commit is stored only with its check.
///
/// A `Store` that shares the data directory keeps each transaction short: it
/// ends after [`TX_BUDGET`] plus one commit, and the import stays off the
/// write lock [`TX_YIELD`] between two transactions, so a server that writes
/// waits for that, and not for the whole batch. A `Store` that is alone has
/// nobody to wait for it, and runs the whole batch in one transaction.
#[allow(clippy::too_many_arguments)]
fn store_batch(
    store: &Backend,
    repo_id: RepoId,
    prepared: &[(ObjectId, Changeset, ObjectId)],
    plan: &Plan,
    checker: &mut Checker<'_>,
    done: &mut usize,
    started: Instant,
) -> Result<()> {
    #[cfg(test)]
    let slow_record = SLOW_RECORD.with(Cell::get);
    // A Postgres database has one writer: a long batch holds it, like a
    // shared data directory's write lock does. The budget applies to both.
    let shared = store.is_shared() || store.as_postgres().is_some();
    let mut rest = prepared;
    while !rest.is_empty() {
        let mut taken = 0;
        // The write lock of a shared data directory stays busy when the
        // servers that share it write without a pause, and `busy_timeout`
        // then expires with the transaction rolled back and nothing
        // written. A batch is safe to run again whole: every write in it is
        // content-addressed or `INSERT OR IGNORE`, so it retries a bounded
        // number of times, and waits between tries so it takes its turn.
        let mut tries = 0;
        loop {
            let started_tx = Instant::now();
            // The blob lengths of the batch, read before the write
            // transaction, so the write connection does no file I/O.
            let lens = crate::tree::lens_of_puts(
                store,
                rest.iter()
                    .flat_map(|(_, cs, _)| crate::db::puts(cs))
                    .map(|(_, blob)| blob),
            )?;
            let result = store.write(|c| {
                // The provenance of the commits this transaction took, for
                // the one batch write at its end: a crash then loses whole
                // commits, never a changeset without its provenance row,
                // and the busy retry reruns the closure whole, as today.
                let mut provenance = Vec::new();
                // The changesets row lands per commit, because the
                // derived rows of the same commit name it by an immediate
                // foreign key; the parents and the repo membership of the
                // transaction's changesets wait for the flush below, one
                // statement per 1,000 rows. The commits come parents
                // first, so their generations chain through the
                // generation cache, and the database reads only the
                // parents this transaction did not insert, usually the
                // first commit's. When the last per-commit writer of a
                // commit moves into the batch, the changesets row moves
                // with it, and every table writes at the transaction's
                // end in the order the foreign keys read: changesets,
                // changeset_parents, repo_changesets, derived, history,
                // git_import.
                c.set_changeset_batching(true);
                // The batch's first commits derive from parents this
                // transaction did not insert. Holding their first-parent
                // chains of derived values (one chain read each) makes
                // every read outside the batch - the Myers' jump of
                // commit_graph, the parents' generations - cost one read
                // for the whole chain, never one per commit; a jump past
                // the preloaded chain falls back to a single read.
                if let Some((_, first, _)) = rest.first() {
                    for parent in first.parents() {
                        c.preload_derived_chain(
                            <crate::graph::CommitGraph as Kind>::NAME,
                            <crate::graph::CommitGraph as Kind>::VERSION,
                            *parent,
                            crate::meta::DERIVED_BATCH_I64,
                        )?;
                    }
                }
                for (commit, cs, git_tree) in rest {
                    #[cfg(test)]
                    if !slow_record.is_zero() {
                        std::thread::sleep(slow_record);
                    }
                    let (id, root) =
                        record(c, repo_id, *commit, cs, &mut crate::tree::lens_from(&lens))?;
                    if plan.verify.contains(commit) {
                        checker.check(c, *commit, *git_tree, root)?;
                    }
                    provenance.push((*commit, id));
                    taken += 1;
                    if shared && started_tx.elapsed() >= TX_BUDGET {
                        break;
                    }
                    #[cfg(any(test, feature = "test-hooks"))]
                    if PER_COMMIT_TXS.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
                c.flush_changeset_rows(repo_id)?;
                c.put_git_imports(repo_id, &provenance)?;
                c.set_changeset_batching(false);
                Ok(taken)
            });
            match result {
                Ok(_) => break,
                Err(e) if e.is_busy() && tries < BUSY_TRIES => {
                    tries += 1;
                    taken = 0;
                    checker.reset_for_retry();
                    #[cfg(any(test, feature = "test-hooks"))]
                    store.note_busy_retry();
                    tracing::warn!(tries, "the database stayed busy; the batch retries");
                    std::thread::sleep(TX_YIELD);
                }
                Err(e) => return Err(e),
            }
        }
        rest = &rest[taken..];
        *done += taken;
        tracing::info!(
            done = *done,
            total = plan.todo.len(),
            commits_per_sec = per_sec(*done, started),
            "importing git commits"
        );
        if shared && !rest.is_empty() {
            // A server write that waited for this transaction takes the lock
            // here, before the next one begins.
            std::thread::sleep(TX_YIELD);
        }
    }
    Ok(())
}

/// Whether the import takes one commit per write transaction, for the
/// tests that compare the batch against the per-commit rows.
#[cfg(any(test, feature = "test-hooks"))]
static PER_COMMIT_TXS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Sets whether the import takes one commit per write transaction.
#[cfg(any(test, feature = "test-hooks"))]
pub fn set_import_per_commit_txs(per_commit: bool) {
    PER_COMMIT_TXS.store(per_commit, std::sync::atomic::Ordering::Relaxed);
}

/// What to import, in order.
struct Plan {
    /// How many commits are reachable from the tip.
    reachable: usize,
    /// Commits not imported yet, parents before children.
    todo: Vec<ObjectId>,
    /// Commits in `todo` to check against git.
    verify: HashSet<ObjectId>,
}

/// Walks every commit reachable from `tip` and orders the new ones parents
/// first. Picks every new root and merge, plus `sample` evenly spaced other
/// new commits, for verification.
fn plan(
    git: &gix::Repository,
    tip: ObjectId,
    known: &HashMap<ObjectId, ChangesetId>,
    sample: usize,
) -> Result<Plan> {
    /// Depth-first walk with an explicit stack, so deep history cannot
    /// overflow the call stack.
    enum Step {
        /// Push the commit's parents, then its `Leave`.
        Enter(ObjectId),
        /// Every parent is ordered; order this commit. Roots and merges are
        /// always checked.
        Leave {
            commit: ObjectId,
            always_check: bool,
        },
    }

    let mut seen = HashSet::new();
    let mut todo = Vec::new();
    let mut verify = HashSet::new();
    let mut ordinary = Vec::new();
    let mut stack = vec![Step::Enter(tip)];
    while let Some(step) = stack.pop() {
        match step {
            Step::Enter(commit) => {
                if !seen.insert(commit) {
                    continue;
                }
                let parents: Vec<ObjectId> = find_commit(git, commit)?
                    .parent_ids()
                    .map(gix::Id::detach)
                    .collect();
                // A batch commits only after its parents, so an imported
                // commit's ancestors are imported too. They are still walked,
                // to count them.
                if !known.contains_key(&commit) {
                    stack.push(Step::Leave {
                        commit,
                        always_check: parents.len() != 1,
                    });
                }
                // Reversed, so the first parent's history is ordered first.
                let unseen = parents.into_iter().rev().filter(|p| !seen.contains(p));
                stack.extend(unseen.map(Step::Enter));
            }
            Step::Leave {
                commit,
                always_check,
            } => {
                if always_check {
                    verify.insert(commit);
                } else {
                    ordinary.push(commit);
                }
                todo.push(commit);
            }
        }
    }
    // Evenly spaced, ending at the newest.
    let picks = sample.min(ordinary.len());
    verify.extend((1..=picks).map(|i| ordinary[i * ordinary.len() / picks - 1]));
    Ok(Plan {
        reachable: seen.len(),
        todo,
        verify,
    })
}

/// Stores the changeset `cs` whose changes build the tree `root`, and
/// gives the changeset's id with the tree: the caller writes the git
/// provenance in one batch for the whole transaction.
fn record(
    c: &mut dyn MetaTx,
    repo: RepoId,
    commit: ObjectId,
    cs: &Changeset,
    lens: crate::tree::BlobLens<'_>,
) -> Result<(ChangesetId, TreeId)> {
    let (id, root) = tree::store_changeset(c, repo, cs, lens).map_err(|e| match e {
        Error::Apply(e) => Error::Corrupt(format!(
            "commit {commit}: changes do not apply to the first parent: {e}"
        )),
        other => other,
    })?;
    Ok((id, root))
}

/// Points `name` at `tip`, creating it if missing. Leaves it alone if it is
/// already there, so a repeated import changes nothing.
///
/// Moves an existing ref only forward, to a descendant of where it points.
/// Anything else would drop the changesets on the ref that git does not
/// have, such as lands made after an earlier import.
///
/// The move is a compare-and-swap on the ref's version, as a land's is. A
/// server can land on the ref while the import checks it, so a lost swap
/// checks the ref again.
/// Returns whether the call moved `name` onto `tip` (`false` when the ref
/// already held or contained the tip).
fn point_ref(store: &Backend, repo: RepoId, name: &str, tip: ChangesetId) -> Result<bool> {
    // The check runs in reads, outside any write transaction, so a server
    // that shares the data directory does not wait for it. Each check
    // reads O(log n) nodes of the derived commit graph, and stops at the
    // deadline of a job with no call of its own.
    let deadline = Deadline::new(Instant::now() + CALL_LIMIT);
    let contains = |head, cs| graph::is_ancestor(store, repo, cs, head, &deadline);
    for attempt in 0..REF_TRIES {
        let current = store.read(|c| c.get_ref(repo, name))?;
        if let Some(current) = current {
            // Already there, or the ref already contains the tip (changes
            // were landed on top of an earlier import): nothing to do.
            if current.target == tip || contains(current.target, tip)? {
                return Ok(false);
            }
            if !contains(tip, current.target)? {
                return Err(Error::FailedPrecondition(format!(
                    "ref {name:?} has diverged from git: it points at {}, which is not \
                     an ancestor of the imported tip {tip}; import into another ref",
                    current.target
                )));
            }
        }
        let expected = current.map(|r| r.version);
        #[cfg(test)]
        if let Some(move_ref) = REF_MOVER
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
        {
            move_ref(store, repo, name)?;
        }
        if store.write(|c| c.cas_ref(repo, name, expected, tip))? {
            return Ok(true);
        }
        tracing::info!(
            name,
            attempt,
            "the ref moved during the import; checking it again"
        );
    }
    Err(Error::ResourceExhausted(format!(
        "ref {name:?} kept moving while the import pointed it at {tip}; \
         run the import again once the lands on it have settled"
    )))
}

/// Turns git commits into changesets.
struct Importer<'a> {
    git: &'a gix::Repository,
    store: &'a Backend,
    repo_id: RepoId,
    /// Blobs recently stored in this run, so most are read from git once. A
    /// miss costs a git read and a row probe: a blob that a committed row
    /// of this repo names skips the put (its file is durable, see the skip
    /// in [`Importer::blob`]), and one without a row goes to
    /// `BlobBatch::put`, which keeps a stored blob only after checking it.
    blobs: LruCache<ObjectId, BlobId>,
    /// Most changed paths one commit may bring. Past it the import fails:
    /// Scion stores a commit as one changeset, and the write path accepts
    /// at most [`MAX_CHANGES`] changes in one.
    max_changes: usize,
    /// Blobs this run proved stored and durable: put here, or named by a
    /// committed row. The checker takes them so it reads none back.
    put: Vec<BlobId>,
}

impl Importer<'_> {
    /// Builds the changeset for `commit` and writes its new blobs to `batch`.
    /// Returns it with the commit's git tree. Every parent must be in `known`.
    fn changeset(
        &mut self,
        commit: ObjectId,
        known: &HashMap<ObjectId, ChangesetId>,
        batch: &mut BlobBatch<'_>,
    ) -> Result<(Changeset, ObjectId)> {
        let git = self.git;
        let object = find_commit(git, commit)?;
        let decoded = object
            .decode()
            .map_err(|e| Error::Git(Git::cause(format!("cannot decode commit {commit}"), e)))?;
        let parents = decoded
            .parents()
            .map(|parent| {
                known.get(&parent).copied().ok_or_else(|| {
                    Error::Corrupt(format!(
                        "commit {commit}: parent {parent} is not imported, but a descendant is"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let base = decoded
            .parents()
            .next()
            .map(|parent| {
                find_commit(git, parent)?
                    .tree_id()
                    .map(gix::Id::detach)
                    .map_err(|e| Error::Git(Git::cause(format!("commit {parent}: no tree"), e)))
            })
            .transpose()?;
        let git_tree = decoded.tree();
        let changeset = Changeset::new(
            parents,
            identity(commit, "author", decoded.author())?,
            Some(identity(commit, "committer", decoded.committer())?),
            decoded.message.to_vec(),
            self.diff(commit, base, git_tree, batch)?,
        )
        .expect("changeset fits the format");
        Ok((changeset, git_tree))
    }

    /// The files that differ between git trees `from` (`None` is empty) and
    /// `to`. Subtrees with the same ID on both sides are skipped.
    fn diff(
        &mut self,
        commit: ObjectId,
        from: Option<ObjectId>,
        to: ObjectId,
        batch: &mut BlobBatch<'_>,
    ) -> Result<BTreeMap<RepoPath, Option<FileState>>> {
        let mut changes = BTreeMap::new();
        // (directory path, tree before, tree after). `None` is a directory
        // that does not exist on that side.
        let mut stack = vec![(Vec::new(), from, Some(to))];
        while let Some((dir, from, to)) = stack.pop() {
            if from == to {
                continue;
            }
            let from = read_tree(self.git, commit, from)?;
            let to = read_tree(self.git, commit, to)?;
            for (name, before, after) in pair_up(&from, &to) {
                if before == after {
                    continue;
                }
                let path = child_path(commit, &dir, name)?;
                let (dir_before, dir_after) = (subtree(before), subtree(after));
                if dir_before.is_some() || dir_after.is_some() {
                    stack.push((path.as_bytes().to_vec(), dir_before, dir_after));
                }
                // Puts and deletes are file-level. `tree::apply` runs every
                // delete first, so a file can become a directory and back.
                match (before, after) {
                    (_, Some(GitEntry::File(kind, id))) => {
                        let state =
                            file_state(*kind, *id, |id| self.blob(commit, &path, id, batch))?;
                        self.capped(&mut changes, commit, path, Some(state))?;
                    }
                    (Some(GitEntry::File(..)), _) => {
                        self.capped(&mut changes, commit, path, None)?;
                    }
                    _ => {}
                }
            }
        }
        Ok(changes)
    }

    /// Records one change, and fails the commit once its changed paths pass
    /// the limit. The check runs as the map grows, so a commit with millions
    /// of changes never builds the whole map, and the error names the commit
    /// and the count.
    fn capped(
        &self,
        changes: &mut BTreeMap<RepoPath, Option<FileState>>,
        commit: ObjectId,
        path: RepoPath,
        state: Option<FileState>,
    ) -> Result<()> {
        changes.insert(path, state);
        if changes.len() > self.max_changes {
            return Err(Error::Git(Git::message(format!(
                "commit {commit}: {} changed paths exceed the import's limit of {}, \
                 and Scion stores one commit as one changeset",
                changes.len(),
                self.max_changes
            ))));
        }
        Ok(())
    }

    /// Writes the git blob for `path` in `commit` to `batch` and returns its
    /// Scion ID.
    fn blob(
        &mut self,
        commit: ObjectId,
        path: &RepoPath,
        oid: ObjectId,
        batch: &mut BlobBatch<'_>,
    ) -> Result<BlobId> {
        cached(&mut self.blobs, oid, || {
            // The cap runs on the header, before the body loads: an
            // oversized blob costs one ODB lookup, not its own size in
            // memory. The same class and the same fix as the trees and
            // commits (`capped_object`).
            let (kind, _) = capped_object(self.git, oid, "blob")?;
            if kind != gix::objs::Kind::Blob {
                return Err(Error::Git(Git::message(format!(
                    "commit {commit}: \"{}\" names a {kind:?}, not a blob",
                    path.as_bytes().escape_ascii()
                ))));
            }
            let blob = read_blob(self.git, oid)?;
            let id = BlobId::of(&blob.data);
            // A committed row of this repo names this blob only after the
            // file's and its directory's fsyncs succeeded (the batch syncs
            // before the batch's rows commit; PutBlob commits its row
            // after put_upload). The stored copy is durable, so the put,
            // its read-back check, and the keep fsyncs are not needed. A
            // blob without a row may be the file of a failed write whose
            // fsync never succeeded: it goes through put, and the keep
            // path fsyncs it before it answers.
            // SHORTCUT: the skip sees only this repo's rows, one probe per
            // distinct blob, and checks the file is there: a row whose
            // file someone deleted under the server still refuses the
            // import through put's read-back (fsck lists it too), and the
            // record path still reads the blob's size from its file, so
            // damage that breaks the zstd frame header fails the import.
            // Deeper damage inside an already stored file is fsck's job.
            // A blob
            // that another repo names still goes through put. Upgrade:
            // an index on repo_blobs (blob), then any_repo_names, when a
            // cross-repo import is measured slow.
            if self.store.read(|c| c.repo_has_blob(self.repo_id, id))?
                && self.store.blobs().file_exists(id)
            {
                self.put.push(id);
                return Ok(id);
            }
            let put = batch.put(&blob.data)?;
            debug_assert_eq!(put, id);
            self.put.push(put);
            Ok(id)
        })
    }
}

/// Checks Scion trees against git trees.
struct Checker<'a> {
    git: &'a gix::Repository,
    store: &'a Backend,
    /// Tree pairs already found equal, so shared subtrees are checked once.
    equal: HashSet<(ObjectId, TreeId)>,
    /// Blob IDs hashed from git content, apart from the importer's cache.
    blobs: LruCache<ObjectId, BlobId>,
    /// Stored blobs recently known to be intact: put in this run, or read
    /// back by an earlier check. A blob the importer skipped, because a
    /// committed row of the repo names it and its file is present, counts
    /// as intact too: the import verifies the blobs it writes, and damage
    /// inside an already stored file is fsck's job. A miss costs a
    /// read-back of the stored blob.
    intact: LruCache<BlobId, ()>,
}

impl Checker<'_> {
    /// Drops the state of checks that a rolled-back transaction recorded,
    /// so a batch that runs again verifies from scratch. The git-to-blob
    /// map stays: it hashes content, so it never lies.
    fn reset_for_retry(&mut self) {
        self.equal.clear();
        self.intact.clear();
    }

    /// Fails unless the Scion tree `scion` has exactly the names, kinds,
    /// flags and content of the git tree `git_tree`.
    fn check(
        &mut self,
        c: &mut dyn MetaTx,
        commit: ObjectId,
        git_tree: ObjectId,
        scion: TreeId,
    ) -> Result<()> {
        let mut stack = vec![(Vec::new(), git_tree, scion)];
        while let Some((dir, git_id, scion_id)) = stack.pop() {
            if !self.equal.insert((git_id, scion_id)) {
                continue;
            }
            // Counted per walk, not per call: a pair the cache already
            // cleared did no work.
            #[cfg(any(test, feature = "test-hooks"))]
            CHECKS.with(|checks| checks.set(checks.get() + 1));
            let want = read_tree(self.git, commit, Some(git_id))?;
            let have: BTreeMap<Vec<u8>, Entry> = c
                .get_tree(scion_id)?
                .entries()
                .clone()
                .into_iter()
                .map(|(name, entry)| (name.as_bytes().to_vec(), entry))
                .collect();
            for (name, want, have) in pair_up(&want, &have) {
                let path = join(&dir, name);
                let differs = |what: String| {
                    Error::Corrupt(format!(
                        "commit {commit}: imported tree differs from git at \"{}\": {what}",
                        path.escape_ascii()
                    ))
                };
                match (want, have) {
                    (Some(GitEntry::Dir(git_id)), Some(Entry::Dir(scion_id))) => {
                        stack.push((path, *git_id, *scion_id));
                    }
                    (Some(GitEntry::File(kind, oid)), Some(Entry::File(file))) => {
                        let expected = file_state(*kind, *oid, |oid| self.blob(oid))?;
                        if expected != file.state() {
                            return Err(differs(format!("git {expected:?}, Scion {file:?}")));
                        }
                        if let Some(blob) = file.state().blob()
                            && self.intact.get(&blob).is_none()
                        {
                            let problem = match self.store.blobs().verify(blob)? {
                                BlobStatus::Valid => {
                                    self.intact.insert(blob, ());
                                    continue;
                                }
                                BlobStatus::Missing => "is not stored",
                                BlobStatus::Corrupt => "is corrupt",
                            };
                            return Err(differs(format!("blob {blob} {problem}")));
                        }
                    }
                    (Some(_), Some(_)) => {
                        return Err(differs(
                            "a file on one side, a directory on the other".into(),
                        ));
                    }
                    (Some(_), None) => return Err(differs("missing in Scion".into())),
                    (None, _) => return Err(differs("not in git".into())),
                }
            }
        }
        Ok(())
    }

    /// The Scion ID of a git blob, hashed from its git content.
    fn blob(&mut self, oid: ObjectId) -> Result<BlobId> {
        cached(&mut self.blobs, oid, || {
            Ok(BlobId::of(&read_blob(self.git, oid)?.data))
        })
    }
}

/// Looks up `oid` in `cache`, filling it from `compute` on a miss.
fn cached(
    cache: &mut LruCache<ObjectId, BlobId>,
    oid: ObjectId,
    compute: impl FnOnce() -> Result<BlobId>,
) -> Result<BlobId> {
    if let Some(id) = cache.get(&oid) {
        return Ok(*id);
    }
    let id = compute()?;
    cache.insert(oid, id);
    Ok(id)
}

/// A git tree entry, classified the way Scion stores it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GitEntry {
    Dir(ObjectId),
    File(FileKind, ObjectId),
}

/// What a non-directory git entry becomes. Equal kinds with equal object IDs
/// are the same Scion state, so `100644` and `100664` do not differ.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Regular { executable: bool },
    Symlink,
    Gitlink,
}

fn subtree(entry: Option<&GitEntry>) -> Option<ObjectId> {
    match entry {
        Some(GitEntry::Dir(id)) => Some(*id),
        _ => None,
    }
}

/// The Scion state of a git file entry. `blob` gives the Scion ID of a git
/// blob.
fn file_state(
    kind: FileKind,
    oid: ObjectId,
    mut blob: impl FnMut(ObjectId) -> Result<BlobId>,
) -> Result<FileState> {
    Ok(match kind {
        FileKind::Regular { executable } => FileState::Regular {
            content: blob(oid)?,
            executable,
        },
        FileKind::Symlink => FileState::Symlink { target: blob(oid)? },
        FileKind::Gitlink => FileState::Gitlink {
            commit: match oid {
                ObjectId::Sha1(bytes) => GitOid::Sha1(bytes),
                other => {
                    return Err(Error::Git(Git::message(format!(
                        "unsupported object ID {other}"
                    ))));
                }
            },
        },
    })
}

/// A git tree's entries by raw name, in Scion's byte order. `None` is the
/// empty tree.
fn read_tree(
    git: &gix::Repository,
    commit: ObjectId,
    id: Option<ObjectId>,
) -> Result<BTreeMap<Vec<u8>, GitEntry>> {
    let Some(id) = id else {
        return Ok(BTreeMap::new());
    };
    let bad =
        |what: String| Error::Git(Git::message(format!("commit {commit}: tree {id}: {what}")));
    let cause = |what: String, e: gix::Error| {
        Error::Git(Git::cause(format!("commit {commit}: tree {id}: {what}"), e))
    };
    // The cap runs on the header, before the body loads: an oversized
    // tree costs one ODB lookup, not its own size in memory.
    let (kind, size) = capped_object(git, id, "tree")?;
    if kind != gix::objs::Kind::Tree {
        return Err(bad(format!("{id} is a {kind:?}, not a tree")));
    }
    let tree = git
        .find_tree(id)
        .map_err(|e| cause("cannot read".to_owned(), e))?;
    // The entry bound is the map `read_tree` is about to build: the
    // header names the bytes, and no entry is smaller than
    // `MIN_TREE_ENTRY`, so this is the most entries the decode can
    // produce.
    let most_entries = size / MIN_TREE_ENTRY;
    let cap = tree_entries_cap();
    if most_entries > cap {
        return Err(bad(format!(
            "{id} is {size} bytes, about {most_entries} entries, \
             past the import's limit of {cap} entries per tree"
        )));
    }
    let decoded = tree
        .decode()
        .map_err(|e| cause("cannot decode".to_owned(), e))?;
    let mut entries = BTreeMap::new();
    for e in &decoded.entries {
        let oid = e.oid.to_owned();
        let entry = match e.mode.value() {
            0o040_000 => GitEntry::Dir(oid),
            0o100_644 | 0o100_664 => GitEntry::File(FileKind::Regular { executable: false }, oid),
            0o100_755 => GitEntry::File(FileKind::Regular { executable: true }, oid),
            0o120_000 => GitEntry::File(FileKind::Symlink, oid),
            0o160_000 => GitEntry::File(FileKind::Gitlink, oid),
            mode => {
                return Err(bad(format!(
                    "\"{}\" has unsupported mode {mode:o}",
                    e.filename.escape_ascii()
                )));
            }
        };
        if entries.insert(e.filename.to_vec(), entry).is_some() {
            return Err(bad(format!(
                "duplicate entry \"{}\"",
                e.filename.escape_ascii()
            )));
        }
    }
    Ok(entries)
}

/// Pairs the entries of two directories by name, in byte order.
fn pair_up<'m, A, B>(
    a: &'m BTreeMap<Vec<u8>, A>,
    b: &'m BTreeMap<Vec<u8>, B>,
) -> impl Iterator<Item = (&'m [u8], Option<&'m A>, Option<&'m B>)> {
    let names: BTreeSet<&Vec<u8>> = a.keys().chain(b.keys()).collect();
    names
        .into_iter()
        .map(|name| (name.as_slice(), a.get(name), b.get(name)))
}

fn join(dir: &[u8], name: &[u8]) -> Vec<u8> {
    if dir.is_empty() {
        name.to_vec()
    } else {
        [dir, b"/", name].concat()
    }
}

/// `dir/name` as a Scion path.
fn child_path(commit: ObjectId, dir: &[u8], name: &[u8]) -> Result<RepoPath> {
    let path = join(dir, name);
    PathElem::new(name)
        .and_then(|_| RepoPath::new(path.as_slice()))
        .map_err(|e| {
            Error::Git(Git::cause(
                format!(
                    "commit {commit}: path \"{}\" is not valid in Scion",
                    path.escape_ascii()
                ),
                e,
            ))
        })
}

fn find_commit(git: &gix::Repository, commit: ObjectId) -> Result<gix::Commit<'_>> {
    // The cap runs on the header, before any decode: the walk reads every
    // commit, so an oversized one is refused once, cheaply, and the
    // refusal names the size.
    capped_object(git, commit, "commit")?;
    git.find_commit(commit)
        .map_err(|e| Error::Git(Git::cause(format!("cannot read commit {commit}"), e)))
}

/// The most bytes one commit or tree object may take, checked on the
/// object's header before the import decodes it whole. Files are capped
/// by `MAX_OBJECT_BYTES`; a commit holds its message and a tree one
/// entry per path, and neither needs more than the largest file the
/// store keeps.
const MAX_STRUCTURE_BYTES: u64 = MAX_OBJECT_BYTES as u64;

/// The most entries one tree may hold, bounded before `read_tree` builds
/// its map. An entry takes at least 28 bytes (a mode, a one-byte name, a
/// NUL and a 20-byte object name), so a tree at the byte cap could still
/// claim millions of entries; past this bound the import refuses rather
/// than build a map that size.
const MAX_TREE_ENTRIES: usize = 1_000_000;

// The cap the docs name: a structure object is bounded by the file cap,
// and a test lowers it only through the seam, never in the product.
const _: () = assert!(MAX_STRUCTURE_BYTES == 100 << 20);
const _: () = assert!(MAX_TREE_ENTRIES == 1_000_000);

/// The byte cap this thread's import enforces, or the product's.
fn structure_bytes_cap() -> u64 {
    #[cfg(any(test, feature = "test-hooks"))]
    if let Some((bytes, _)) = STRUCTURE_CAPS.with(Cell::get) {
        return bytes;
    }
    MAX_STRUCTURE_BYTES
}

/// The most entries this thread's import builds a tree map for.
fn tree_entries_cap() -> u64 {
    #[cfg(any(test, feature = "test-hooks"))]
    if let Some((_, entries)) = STRUCTURE_CAPS.with(Cell::get) {
        return entries;
    }
    MAX_TREE_ENTRIES as u64
}

/// The smallest bytes one tree entry can take: a mode (5), a space, a
/// one-byte name, a NUL and a 20-byte object name.
const MIN_TREE_ENTRY: u64 = 5 + 1 + 1 + 1 + 20;

/// Refuses `id` unless its header says it is `kind` and it fits
/// [`MAX_STRUCTURE_BYTES`]. The header is read without the body, so an
/// oversized object costs one ODB lookup, not its own size in memory.
fn capped_object(
    git: &gix::Repository,
    id: ObjectId,
    what: &str,
) -> Result<(gix::objs::Kind, u64)> {
    let header = git
        .try_find_header(id)
        .map_err(|e| Error::Git(Git::cause(format!("cannot read {what} {id}"), e)))?
        .ok_or_else(|| {
            Error::Git(Git::message(format!(
                "{what} {id} is not in the repository"
            )))
        })?;
    let (kind, size) = (header.kind(), header.size());
    let cap = structure_bytes_cap();
    if size > cap {
        return Err(Error::Git(Git::message(format!(
            "{what} {id} is {size} bytes, past the import's limit of {cap}"
        ))));
    }
    Ok((kind, size))
}

fn read_blob(git: &gix::Repository, oid: ObjectId) -> Result<gix::Blob<'_>> {
    git.find_blob(oid)
        .map_err(|e| Error::Git(Git::cause(format!("cannot read blob {oid}"), e)))
}

fn identity(
    commit: ObjectId,
    role: &str,
    signature: gix::Result<SignatureRef<'_>>,
) -> Result<IdentityTime> {
    let bad = |e: gix::Error| Error::Git(Git::cause(format!("commit {commit}: bad {role}"), e));
    let signature = signature.map_err(bad)?;
    let time = signature.time().map_err(bad)?;
    // The date must read back as it was written. The parser behind
    // `SignatureRef::time` treats a timezone it cannot read as zero
    // (gix-date's `parse_header` defaults the offset), and the import
    // must not store history that differs from its source without
    // saying so. The one git spelling that cannot read back as itself
    // is `-0000`, git's written form of an unknown offset: it parses
    // to zero, and zero serializes as `+0000`.
    let raw = signature.time;
    if zone(raw) != Some((time.seconds, time.offset)) {
        return Err(Error::Git(Git::message(format!(
            "commit {commit}: {role} date {raw:?} is not a date git can write"
        ))));
    }
    IdentityTime::new(
        signature.name.to_vec(),
        signature.email.to_vec(),
        time.seconds,
        time.offset / 60,
    )
    .map_err(|e| {
        Error::Git(Git::message(format!(
            "commit {commit}: {role} identity does not fit the format: {e}"
        )))
    })
}

/// The raw git date `<seconds> <sign><hhmm>`, as the pair gix parses it
/// to, or `None` when the bytes are not that grammar. A zone with a
/// seconds field (`+053030`) or a zone too long to read as two two-digit
/// fields is refused here, where gix would coerce its offset to zero.
fn zone(raw: &str) -> Option<(i64, i32)> {
    let (seconds, offset) = raw.split_once(' ')?;
    let seconds = seconds.parse().ok()?;
    let sign = match offset.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let digits = offset.get(1..)?;
    let (hours, minutes) = digits.split_at(2);
    if minutes.len() != 2
        || !hours
            .bytes()
            .chain(minutes.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    // A negative zero (`-0000`, and `-000000` of the same shape) is
    // the one zone that cannot read back: git writes an unknown offset
    // this way, and zero parses to `+0000`. The arithmetic folds the
    // sign into the value (-1 * 0 = 0), so the pair comparison below
    // would accept it; the sign is checked here instead.
    let total = hours.parse::<i32>().ok()? * 3600 + minutes.parse::<i32>().ok()? * 60;
    if sign == -1 && total == 0 {
        return None;
    }
    Some((seconds, sign * total))
}

fn per_sec(count: usize, since: Instant) -> u128 {
    count as u128 * 1000 / since.elapsed().as_millis().max(1)
}

#[cfg(test)]
mod tests {
    use crate::backend::Backend;
    use crate::store::Store;
    use std::fs;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::derived::{self, Kind};
    use crate::tree::RootTree;

    /// Runs git in `dir` with a fixed identity and no user or system config.
    fn git(dir: &Path, args: &[&str]) {
        // The child shares a flock until it execs (see `spawn_guard` in
        // store.rs): the read side keeps this spawn out of a lock-release
        // window of another test.
        let out = crate::store::spawn_guard::across_spawn(|| {
            Command::new("git")
                .args(args)
                .current_dir(dir)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_AUTHOR_NAME", "Ada")
                .env("GIT_AUTHOR_EMAIL", "ada@example.com")
                .env("GIT_COMMITTER_NAME", "Ada")
                .env("GIT_COMMITTER_EMAIL", "ada@example.com")
                .output()
        })
        .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn a_tiny_blob_cache_imports_the_same_changesets() {
        crate::store::spawn_guard::prime_gix_lookup();
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        // Eight distinct blobs, then the same blobs at new paths. With a cache
        // of two entries, most of them are met again after they were evicted.
        for i in 0..8 {
            fs::write(dir.join(format!("f{i}")), format!("{i}\n")).unwrap();
        }
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "add"]);
        fs::create_dir(dir.join("d")).unwrap();
        for i in 0..8 {
            fs::rename(dir.join(format!("f{i}")), dir.join(format!("d/f{i}"))).unwrap();
        }
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "move"]);

        let opts = ImportOptions {
            rev: "main".into(),
            target_ref: "main".into(),
            verify_sample: usize::MAX,
        };
        let import = |cache_entries| {
            let data = tempfile::tempdir().unwrap();
            let store = Backend::sqlite(Store::open(data.path()).unwrap());
            import_with_cache(&store, "main", dir, &opts, cache_entries, MAX_CHANGES).unwrap()
        };
        let tiny = import(2);
        let unbounded = import(usize::MAX);
        assert_eq!((tiny.commits, tiny.verified), (2, 2));
        // Each changeset names its parents by ID, so equal tips mean that
        // every changeset is the same.
        assert_eq!(tiny.tip, unbounded.tip);
    }

    #[test]
    fn a_reimport_trusts_a_stored_blob_it_skips_and_fsck_reports_the_damage() {
        crate::store::spawn_guard::prime_gix_lookup();
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        fs::write(dir.join("f"), "payload\n").unwrap();
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "add"]);

        let data = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(data.path()).unwrap());
        let opts = |rev: &str| ImportOptions {
            rev: rev.into(),
            target_ref: "main".into(),
            verify_sample: usize::MAX,
        };
        import_git(&store, "main", dir, &opts("main")).unwrap();

        // The blob's file is damaged in place: the row and the file are
        // there, the frame header still parses (the import reads the
        // blob's size from it), but the bytes are not what they were.
        let blob = BlobId::of(b"payload\n");
        let (_, path) = store.blobs().location(blob);
        let mut bytes = fs::read(&path).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        // The next commit touches the same path with the same content, so
        // the import's diff names the blob and takes the skip: the row and
        // the file are there. The import trusts it and lands.
        git(dir, &["update-index", "--chmod=+x", "f"]);
        git(dir, &["commit", "-q", "-m", "chmod"]);
        let report = import_git(&store, "main", dir, &opts("main")).unwrap();
        assert_eq!((report.imported, report.verified), (1, 1));

        // The damage is fsck's to find, and it does.
        let report = crate::fsck::run(&store, 1, &mut |_| {}).unwrap();
        assert!(!report.is_clean());
        assert!(
            report
                .problems
                .iter()
                .any(|p| p.to_string().contains(&blob.to_string())),
            "fsck missed the damaged blob: {:?}",
            report.problems
        );
    }

    #[test]
    fn a_commit_with_too_many_changes_is_named_and_refused() {
        crate::store::spawn_guard::prime_gix_lookup();
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        fs::write(dir.join("f"), "one\n").unwrap();
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "one"]);

        let data = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(data.path()).unwrap());
        let opts = ImportOptions {
            rev: "main".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        };
        import_with_cache(&store, "main", dir, &opts, BLOB_CACHE_ENTRIES, MAX_CHANGES).unwrap();

        // Three changed paths against a cap of two: the error names the
        // commit and the count, and the store holds nothing of the commit.
        for i in 0..3 {
            fs::write(dir.join(format!("n{i}")), format!("{i}\n")).unwrap();
        }
        fs::remove_file(dir.join("f")).unwrap();
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", "three"]);
        let error =
            import_with_cache(&store, "main", dir, &opts, BLOB_CACHE_ENTRIES, 2).unwrap_err();
        assert!(
            error.to_string().contains("3 changed paths exceed"),
            "the error does not name the count: {error}"
        );
        // The store is clean: the ref never moved, no changeset landed.
        let report = crate::fsck::run(&store, 1, &mut |_| {}).unwrap();
        assert!(report.is_clean(), "{:?}", report.problems);
    }

    #[test]
    fn a_land_while_the_import_checks_the_ref_is_never_overwritten() {
        crate::store::spawn_guard::prime_gix_lookup();
        let data = tempfile::tempdir().unwrap();
        let server = Backend::sqlite(Store::open(data.path()).unwrap());
        let importer = Backend::sqlite(Store::open_shared(data.path()).unwrap());
        // main is at root. Git has root ─ tip, and a land puts X on root.
        let changeset = |parents, message: &str| {
            Changeset::new(
                parents,
                IdentityTime::new(b"Ada".to_vec(), b"ada@example.com".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                message.into(),
                BTreeMap::new(),
            )
            .expect("changeset fits the format")
        };
        let (repo, tip, x) = server
            .write(|c| {
                let repo = c.create_repo("r")?;
                let root = c.insert_changeset(repo, &changeset(vec![], "root"))?;
                let tip = c.insert_changeset(repo, &changeset(vec![root], "tip"))?;
                let x = c.insert_changeset(repo, &changeset(vec![root], "x"))?;
                assert!(c.cas_ref(repo, "main", None, root)?);
                Ok((repo, tip, x))
            })
            .unwrap();

        // The land's transaction has moved main but not committed, so the
        // import still reads root, then waits to write. Channels, not a
        // barrier: a step whose release never comes fails in seconds with
        // its name, and cannot trap its partner.
        let (in_tx, in_rx) = std::sync::mpsc::channel::<()>();
        let (moved_tx, moved_rx) = std::sync::mpsc::channel::<()>();
        let land = {
            let (server, in_tx, moved_rx) = (server.clone(), in_tx, moved_rx);
            thread::spawn(move || {
                server.write(|c| {
                    let moved = c.cas_ref(repo, "main", Some(1), x)?;
                    in_tx.send(()).unwrap();
                    moved_rx
                        .recv_timeout(Duration::from_secs(30))
                        .unwrap_or_else(|_| panic!("the import never looked again"));
                    thread::sleep(Duration::from_millis(300));
                    Ok(moved)
                })
            })
        };
        in_rx
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|_| panic!("the land never moved the ref"));
        // The import starts its check while the land's transaction still
        // holds the write lock: it reads `root`, then waits to write, like
        // it would against a live land.
        moved_tx.send(()).unwrap();
        let moved = point_ref(&importer, repo, "main", tip);
        assert!(land.join().unwrap().unwrap());
        // The swap lost, and the check again sees that main has diverged.
        assert!(
            matches!(moved, Err(Error::FailedPrecondition(_))),
            "{moved:?}"
        );
        let main = server.read(|c| c.get_ref(repo, "main")).unwrap();
        assert_eq!(main.unwrap().target, x);
    }

    /// Takes a test's ref mover back when the test ends, an assert's panic
    /// included: a mover left set would move other tests' refs.
    struct MoverGuard;

    impl Drop for MoverGuard {
        fn drop(&mut self) {
            *REF_MOVER.lock().unwrap_or_else(PoisonError::into_inner) = None;
        }
    }

    /// A ref that moves before every swap stops the import after `REF_TRIES`
    /// swaps, with a busy answer, instead of walking the history again
    /// without end. Each move lands main forward on the chain below the
    /// tip, so the import never hears "diverged".
    #[test]
    fn an_import_that_keeps_losing_the_ref_swap_gives_up() {
        let data = tempfile::tempdir().unwrap();
        let server = Backend::sqlite(Store::open(data.path()).unwrap());
        let importer = Backend::sqlite(Store::open_shared(data.path()).unwrap());
        // main is at root. The chain root <- a1 <- a2 <- a3 <- tip hangs
        // under it, so every mover target is still an ancestor of the
        // imported tip and the import never hears "diverged".
        let changeset = |parents, message: &str| {
            Changeset::new(
                parents,
                IdentityTime::new(b"Ada".to_vec(), b"ada@example.com".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                message.into(),
                BTreeMap::new(),
            )
            .expect("changeset fits the format")
        };
        let (repo, tip, links): (RepoId, ChangesetId, Arc<[ChangesetId]>) = server
            .write(|c| {
                let repo = c.create_repo("r")?;
                let mut parent = c.insert_changeset(repo, &changeset(vec![], "root"))?;
                let root = parent;
                let mut links = Vec::new();
                for message in ["a1", "a2", "a3", "tip"] {
                    parent = c.insert_changeset(repo, &changeset(vec![parent], message))?;
                    links.push(parent);
                }
                assert!(c.cas_ref(repo, "main", None, root)?);
                Ok((repo, parent, Arc::from(links.into_boxed_slice())))
            })
            .unwrap();

        // The mover points main at the next link of the chain before every
        // swap, so the swap's version is always stale. Dropping the guard
        // takes the mover back, also when an assert fails.
        let _guard = MoverGuard;
        let swaps = Arc::new(AtomicUsize::new(0));
        // Only this test's importer handle moves: other tests' point_ref
        // calls run in the same process, and the mover must stay inert for
        // them.
        let mine = std::ptr::from_ref(&importer) as usize;
        *REF_MOVER.lock().unwrap_or_else(PoisonError::into_inner) = Some(Box::new({
            let (links, swaps) = (Arc::clone(&links), Arc::clone(&swaps));
            move |store: &Backend, repo: RepoId, name: &str| {
                if std::ptr::from_ref(store) as usize != mine {
                    return Ok(());
                }
                let at = swaps
                    .fetch_add(1, AtomicOrdering::SeqCst)
                    .min(links.len() - 2);
                store.write(|c| {
                    let version = c.get_ref(repo, name)?.map(|r| r.version);
                    c.cas_ref(repo, name, version, links[at])
                })?;
                Ok(())
            }
        }));

        let pointed = point_ref(&importer, repo, "main", tip);
        // The swap lost REF_TRIES times, and the import gave up instead of
        // planning again without end.
        assert!(
            matches!(pointed, Err(Error::ResourceExhausted(_))),
            "{pointed:?}"
        );
        assert_eq!(swaps.load(AtomicOrdering::SeqCst), REF_TRIES as usize);
        // The ref moved forward once per swap, to a3, and never to the tip.
        let main = importer.read(|c| c.get_ref(repo, "main")).unwrap().unwrap();
        assert_eq!(main.target, links[2]);
        assert_ne!(main.target, tip);
    }

    #[test]
    fn an_import_derives_missing_parent_root_trees_in_steps_first() {
        crate::store::spawn_guard::prime_gix_lookup();
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        for i in 0..5 {
            fs::write(dir.join("f"), format!("{i}\n")).unwrap();
            git(dir, &["add", "-A"]);
            git(dir, &["commit", "-q", "-m", &i.to_string()]);
        }
        let data = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(data.path()).unwrap());
        let import = |rev: &str| {
            let opts = ImportOptions {
                rev: rev.into(),
                target_ref: "main".into(),
                verify_sample: 0,
            };
            import_git(&store, "main", dir, &opts).unwrap()
        };
        import("main~1");
        // As after a version bump of the root tree: no stored root trees.
        let sql = "DELETE FROM derived WHERE kind = ?1";
        let removed = store.write(|c| {
            c.sqlite()
                .unwrap()
                .execute(sql, [RootTree::NAME])
                .map_err(crate::error::from_sqlite)
        });
        assert_eq!(removed.unwrap(), 4);

        let commits = store.commits();
        assert_eq!(import("main").imported, 1);
        // The repo is already there: its lookup is a read, so only the root
        // trees of the four parents in steps, the batch and the ref commit.
        // The batch derives nothing while it holds the lock.
        assert_eq!(store.commits() - commits, 2 + derived::steps(4));
    }

    #[test]
    fn a_server_write_waits_less_than_busy_timeout_during_a_slow_batch() {
        crate::store::spawn_guard::prime_gix_lookup();
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        // More commits than one budget-sized transaction takes, so the batch
        // splits.
        for i in 0..160 {
            git(
                dir,
                &["commit", "-q", "--allow-empty", "-m", &i.to_string()],
            );
        }
        let data = tempfile::tempdir().unwrap();
        let server = Backend::sqlite(Store::open(data.path()).unwrap());
        let importer = Backend::sqlite(Store::open_shared(data.path()).unwrap());
        let opts = ImportOptions {
            rev: "main".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        };
        // The sleep runs on the import thread, inside each transaction, so
        // one transaction of the whole batch holds the write lock for
        // 160 x 40 ms, past busy_timeout, and the server's writes fail
        // without the budget. With it, a transaction ends at 1 s plus one
        // commit.
        let import = {
            let dir = dir.to_path_buf();
            let importer = importer.clone();
            thread::spawn(move || {
                SLOW_RECORD.with(|c| c.set(Duration::from_millis(40)));
                import_git(&importer, "r", &dir, &opts)
            })
        };
        let mut writes = 0;
        while !import.is_finished() {
            let name = format!("w{writes}");
            // Every write succeeds: a wait past the 5 s busy_timeout fails
            // here, which one transaction of the whole batch would cause.
            server.write(|c| c.create_repo(&name)).unwrap();
            writes += 1;
            thread::sleep(Duration::from_millis(10));
        }
        let report = import.join().unwrap().unwrap();
        assert!(writes > 0, "no server write ran during the import");
        // The budget split the batch into transactions, and the import did
        // its work.
        assert!(importer.txes() > 2, "the batch was one transaction");
        assert_eq!(report.imported, 160);
    }

    #[test]
    fn an_import_that_is_alone_runs_its_batch_in_one_transaction() {
        crate::store::spawn_guard::prime_gix_lookup();
        let repo = tempfile::tempdir().unwrap();
        let dir = repo.path();
        git(dir, &["init", "-q", "-b", "main"]);
        // More slow records than one budget-sized transaction takes, so a
        // shared import would split the batch.
        for i in 0..30 {
            git(
                dir,
                &["commit", "-q", "--allow-empty", "-m", &i.to_string()],
            );
        }
        let data = tempfile::tempdir().unwrap();
        let importer = Backend::sqlite(Store::open(data.path()).unwrap());
        let opts = ImportOptions {
            rev: "main".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        };
        // 30 x 40 ms of records is over the 1 s budget: an import that is
        // alone ignores the budget and runs the batch in one transaction.
        SLOW_RECORD.with(|c| c.set(Duration::from_millis(40)));
        let report = import_git(&importer, "r", dir, &opts).unwrap();
        // One transaction each for the repo, the batch, and the ref: the
        // batch did not split at the budget.
        assert_eq!(importer.txes(), 3);
        assert_eq!(report.imported, 30);
    }
}

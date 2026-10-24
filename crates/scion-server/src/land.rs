//! Lands: rebase a changeset onto a ref and move the ref.
//!
//! ```text
//! land(req)
//!   1. record the land as pending, or find it by (repo, caller, request key);
//!      a land to a ref that does not exist is rejected here. A new land
//!      also deletes a few lands that finished more than 7 days ago
//!   2. take the ref's lock: one land at a time per ref
//!   3. if the row is no longer pending, return it (a retry, or a duplicate)
//!   4. plan: one parent, base on the ref's first-parent history,
//!      no path changed on the ref after the base. Its walks stop at the
//!      request's deadline or once its call has gone, and the land stays
//!      pending. A base that the path index does not reach is checked in the
//!      head's path history; if the head has none yet, release the ref's
//!      lock, derive it, and go to step 2
//!   5. one transaction: apply to the head tree, store the rebased changeset,
//!      compare-and-swap the ref, mark the land landed. If an import moved
//!      the ref since step 4, roll back and go to step 4.
//!   6. release the ref's lock, then wait for a full flush that covers the
//!      commits (`Store::flush`), shared with the lands queued behind it
//! ```
//!
//! A crash at any point leaves either a pending row (a retry finishes it) or a
//! landed one (a retry returns it). The ref and the row move in the same
//! transaction, so they never disagree.

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs), and unreachable_pub does not see
// through a re-export, so this marker would fire on all of them
// under any feature set.
#![allow(unreachable_pub)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};

use hashlink::LruCache;
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};

use crate::auth::Caller;
use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::derived;
use crate::error::{Error, Result};
use crate::graph::{self, CommitGraph};
use crate::history::{self, PathHistory};
use crate::meta::Meta as _;
use crate::meta::StaleLand;
use crate::meta::{MetaTx, RefState, RepoId};
use crate::tree;

/// Longest accepted ref name or request key, in bytes.
const MAX_NAME_BYTES: usize = 256;

/// Plans per land call when other writers keep moving the ref. After the
/// last one the call fails with [`Error::Aborted`] and the land stays pending.
///
/// A git import, in this process or in another one that shares the data
/// directory, moves a ref by compare-and-swap without the land lock, which
/// is in memory. It moves the ref once, at its end. So a land loses a swap
/// only to an import that ends while the land plans, and three in a row are
/// unlikely.
const MAX_ATTEMPTS: u32 = 3;

/// How long a finished land is kept, in seconds: 7 days. Until then a retry
/// with the same request key returns the stored outcome. A client retries
/// within minutes, and a week also covers a client that is down over a long
/// weekend. Pending lands are kept until they finish.
pub(crate) const LAND_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

/// Most expired lands that one new land deletes. A land adds one row, so
/// expiry keeps up, and no land does much work after a long pause. Tests use
/// a small batch, so that they reach the limit.
pub(crate) const EXPIRE_PER_LAND: i64 = if cfg!(test) { 2 } else { 100 };

/// How far, in generations, the ref moves in one prune cycle of a
/// [`PathIndex`]. Tests use a short cycle, so that they prune.
const PRUNE_EVERY: i64 = if cfg!(test) { 4 } else { 10_000 };

/// A base more than this many generations below the tip is checked without
/// growing the [`PathIndex`], so it never holds the floor down. One prune
/// cycle, so bases keep at most about three cycles of history in the index.
/// A client lands soon after it reads the head, so an older base is rare.
/// A land on such a base reads the head's path history ([`check_history`]).
/// Tests use a depth longer than the history in their pruned index.
const MAX_DEPTH: i64 = if cfg!(test) { 16 } else { PRUNE_EVERY };

/// Most bytes that the [`PathIndex`]es of all refs hold together, counted by
/// [`entry_bytes`]: about 1M entries of short paths. An index that would go
/// over first drops the indexes of other refs that no land uses, least
/// recently used first ([`Lands::reserve`]). If that is not enough, an
/// advance or a land moves its floor up to the tip, and a reach back to a
/// base stops, and [`check_history`] checks the land. Tests use a small
/// budget, so that they reach it.
const MAX_INDEX_BYTES: usize = if cfg!(test) { 64 << 10 } else { 128 << 20 };

/// Where a land stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LandState {
    /// Recorded, not finished.
    Pending,
    /// The ref points at the rebased changeset.
    Landed,
    /// A changed path also changed on the ref after the base.
    Conflict,
    /// The land cannot succeed as asked. The outcome is final for its
    /// request key.
    Rejected,
}

impl LandState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Landed => "landed",
            Self::Conflict => "conflict",
            Self::Rejected => "rejected",
        }
    }

    pub(crate) fn parse(s: &str) -> Result<Self> {
        match s {
            "pending" => Ok(Self::Pending),
            "landed" => Ok(Self::Landed),
            "conflict" => Ok(Self::Conflict),
            "rejected" => Ok(Self::Rejected),
            other => Err(Error::Corrupt(format!("unknown land state {other:?}"))),
        }
    }
}

/// A request to land a changeset.
#[derive(Clone, Debug)]
pub struct LandRequest {
    /// Repository name.
    pub repo: String,
    /// The changeset to land. Its first parent is its base.
    pub changeset: ChangesetId,
    /// The ref to move, for example `main`.
    pub target_ref: String,
    /// Caller-chosen key. A retry with the same key returns the same land.
    pub request_key: String,
    /// When the server gives up on the request. The plan stops at it with
    /// [`Error::DeadlineExceeded`], or with [`Error::Cancelled`] once the
    /// call that asked has gone, and the land stays pending.
    pub deadline: Deadline,
}

/// The result of a land.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandOutcome {
    /// Stable ID of this land.
    pub land_id: i64,
    /// Where it stands.
    pub state: LandState,
    /// The rebased changeset, when landed.
    pub result: Option<ChangesetId>,
    /// The ref's target the land's move replaced, when landed: the head
    /// the land rebased onto, set where the move happens, under the
    /// ref's lock, and not stored. A replay has none: the move's head
    /// lives in the land line of the call that made the move.
    pub previous: Option<ChangesetId>,
    /// Whether this call's own settle moved the ref. A replay of an
    /// already-landed request reports the first move's outcome, and
    /// moved nothing itself.
    pub moved: bool,
    /// Why it conflicted or was rejected.
    pub detail: String,
    /// Paths that conflict.
    pub conflict_paths: Vec<RepoPath>,
}

/// Per-ref state, guarded by the ref's lock.
type RefSlot = Arc<Mutex<Option<PathIndex>>>;

/// The test step the settle runs after it recorded the land in the
/// index: it sees the index, and runs under the slot's lock.
#[cfg(test)]
type AfterRecord = Box<dyn FnOnce(&mut Option<PathIndex>) + Send>;

/// Serializes lands per ref and keeps each ref's `PathIndex`.
pub struct Lands {
    /// The most pending lands one principal may hold at once. `None` turns
    /// the check off.
    pending_limit: Option<usize>,
    /// The slot of each ref that holds a path index or that a land uses,
    /// the least recently landed on first. [`Lands::evict`] removes the
    /// others.
    refs: Mutex<LruCache<(RepoId, String), RefSlot>>,
    /// What all path indexes hold, by [`entry_bytes`]: the sum of their
    /// `bytes`. At most [`MAX_INDEX_BYTES`].
    used: Arc<AtomicUsize>,
    /// Indexes that [`Lands::evict`] dropped. Their bytes are back in
    /// `used`. A land frees them after its write connection and ref lock.
    evicted: Mutex<Vec<PathIndex>>,
    /// The metrics that count lands by outcome, when the process exports
    /// any.
    metrics: Option<Arc<crate::metrics::Metrics>>,
    /// A step the tests run between a plan and its settle, once: a test
    /// that moves the ref at exactly that moment makes the land replan
    /// for certain, where a timed race depends on the machine.
    #[cfg(test)]
    before_settle: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    /// A step the tests run inside the slot's critical section, right
    /// after the land recorded its outcome in the index, once: a test
    /// poisons the slot there, where the panic unwinds through its lock.
    #[cfg(test)]
    after_record: Mutex<Option<AfterRecord>>,
}

impl Default for Lands {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl Lands {
    /// Limits each principal to `pending_limit` lands pending at once
    /// (`None` turns the check off), and counts the outcomes in `metrics`
    /// when the process exports any.
    #[must_use]
    pub fn new(
        pending_limit: Option<usize>,
        metrics: Option<Arc<crate::metrics::Metrics>>,
    ) -> Self {
        Self {
            pending_limit,
            refs: Mutex::new(LruCache::new_unbounded()),
            used: Arc::default(),
            evicted: Mutex::default(),
            metrics,
            #[cfg(test)]
            before_settle: Mutex::new(None),
            #[cfg(test)]
            after_record: Mutex::new(None),
        }
    }

    /// Sets the step the next plan runs before its settle, once.
    #[cfg(test)]
    fn set_before_settle(&self, step: Box<dyn FnOnce() + Send>) {
        *self
            .before_settle
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(step);
    }

    /// Sets the step the next settle runs after it recorded the land in
    /// the ref's index, once. The step runs while the slot's lock is
    /// still held, so a panic there poisons it.
    #[cfg(test)]
    fn set_after_record(&self, step: AfterRecord) {
        *self
            .after_record
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(step);
    }

    /// Rejects every land that has been pending for more than `stale_secs`,
    /// with a detail that tells the client to retry with a new key.
    ///
    /// A retry with the same key can still finish a pending land: the land
    /// path takes the lock of the ref and runs the plan again. So each check
    /// here holds the same per-ref lock first, and re-reads the row while it
    /// holds it: a retry that is finishing the land wins, and the sweep sees
    /// the row is no longer pending. A row this stale that no call holds is
    /// beyond any deadline (600 s), so no live call can finish it.
    ///
    /// # Errors
    ///
    /// Database errors.
    /// Rejects every pending land that started more than `stale_secs` ago,
    /// and reports how many. The detail of a rejected land says why.
    ///
    /// A pending land can always be finished by a retry with its key: the
    /// land path takes the lock of the ref and runs the plan again. So this
    /// takes the same lock first, and only rejects a row that is still
    /// pending under it. A row rejected here has no live call: the 600 s
    /// call limit ended long before, and a retry that had the lock would
    /// have finished the land.
    ///
    /// The sweep writes the land's result line itself: the job that would
    /// have written it is long gone, and every intent line gets one result
    /// line, so the audit never holds an intent that nothing answers.
    pub fn sweep_stale(
        &self,
        store: &Backend,
        stale_secs: i64,
        audit: &crate::audit::Audit,
    ) -> Result<u64> {
        let stale = store.read(|c| c.stale_lands(stale_secs))?;
        let mut rejected = 0;
        for StaleLand {
            repo,
            land_id,
            target_ref,
            caller,
            request_key,
            repo_name,
        } in stale
        {
            // The same per-ref lock that the land path holds, so a retry
            // that is finishing this land wins the race: the sweep then
            // finds the row no longer pending, and leaves it alone.
            let slot = self.slot(repo, &target_ref);
            let index = Self::lock_slot(repo, &target_ref, &slot);
            let detail = format!(
                "stale: pending for more than {stale_secs} s, so the server stopped \
                     before it finished; retry with a new request key"
            );
            let changed = store.write(|c| c.reject_stale_land(land_id, detail))?;
            drop(index);
            // The index is unaffected: the sweep moves no ref.
            rejected += changed;
            if changed == 1 {
                tracing::info!(land_id, "rejected a stale pending land");
                // The intent's request id is the call's and is not
                // stored, so this line carries a fresh id of its own:
                // the auditor joins it to the intent on the request key
                // and the land id (both in the line), not on request_id.
                if let Err(why) = audit.record_blocking(&crate::audit::Event::new(
                    "land",
                    "failed",
                    crate::service::json_fields(
                        &scion_proto::runtime::request_id::generate(),
                        &caller,
                        &serde_json::json!({
                            "rpc": "Land",
                            "repo": repo_name,
                            "ref": target_ref,
                            "land_id": land_id,
                            "request_key": request_key,
                            "why": "stale",
                        }),
                    ),
                )) {
                    tracing::error!(
                        reason = %why,
                        land_id,
                        "the sweep's audit line was not made durable"
                    );
                }
            }
        }
        Ok(rejected)
    }

    /// Lands a changeset. A conflict or rejection is an outcome, not an error.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the repo or changeset does not exist.
    /// [`Error::InvalidArgument`] for a malformed request, or a request key
    /// reused for a different land. [`Error::Aborted`] if other writers kept
    /// moving the ref, [`Error::DeadlineExceeded`] if the plan ran past
    /// the request's deadline, or [`Error::Cancelled`] if the call went.
    /// The land then stays pending, and a retry with the same request key
    /// resumes it. Store errors otherwise.
    pub fn land(&self, store: &Backend, caller: &Caller, req: &LandRequest) -> Result<LandOutcome> {
        let outcome = self.land_committed(store, caller, req);
        // An index can hold up to the whole budget, and freeing it takes a
        // while, so it happens here, outside every lock.
        let evicted = mem::take(&mut *self.evicted.lock().unwrap_or_else(PoisonError::into_inner));
        drop(evicted);
        if let Ok(outcome) = &outcome
            && let Some(metrics) = &self.metrics
        {
            metrics.land(match outcome.state {
                LandState::Landed => "landed",
                LandState::Conflict => "conflict",
                LandState::Rejected => "rejected",
                // A pending land waits for a retry with its key.
                LandState::Pending => "retry",
            });
        }
        // Lands on one ref commit one at a time. Waiting for the flush after
        // the ref lock is released lets them share it. Every reply waits for
        // the flush, the error replies included: a reply that says the land
        // stays pending rests on the row the land recorded, and that row
        // must be durable before the reply goes out.
        store.flush()?;
        outcome
    }

    /// [`Lands::land`], up to commits that are not durable yet. Its reads do
    /// not wait for a flush either: `land` flushes before it replies.
    fn land_committed(
        &self,
        store: &Backend,
        caller: &Caller,
        req: &LandRequest,
    ) -> Result<LandOutcome> {
        check_name("target_ref", &req.target_ref)?;
        check_name("request_key", &req.request_key)?;
        let (repo, recorded) = store.commit(|c| c.record_land(caller, req, self.pending_limit))?;
        if recorded.state != LandState::Pending {
            return Ok(recorded);
        }

        // SHORTCUT: an in-process lock serializes lands per ref, so this works
        // with one server process. Upgrade: a single lander per ref that
        // rebases a batch and moves the ref once for all of them.
        let slot = self.slot(repo, &req.target_ref);
        let land_id = recorded.land_id;
        // The head whose history this call derived.
        let mut derived = None;
        loop {
            let mut index = Self::lock_slot(repo, &req.target_ref, &slot);
            // The outcome check rides the plan's read transaction: a call
            // with the same key may have finished the land while this one
            // waited for the lock, and the plan must not run for a land
            // that is already settled.
            let result = finish(self, store, repo, caller, land_id, req, &mut index);
            // The index may be ahead of the database; rebuild it next time. A
            // lost race wrote nothing, so the index is still good. So is an
            // index that a deadline stopped, or that stopped because the
            // call went: an advance adds nothing until it reaches the tip,
            // and a reach adds one whole changeset at a time, so a retry
            // goes on from where the reach stopped.
            if result.as_ref().is_err_and(|e| {
                !matches!(
                    e,
                    Error::Aborted(_) | Error::DeadlineExceeded(_) | Error::Cancelled(_)
                )
            }) {
                *index = None;
            }
            let head = match result.map_err(stays_pending)? {
                Finished::Outcome(outcome) => return Ok(outcome),
                Finished::Derive(head) => head,
            };
            // A long history takes many steps to derive. Other lands on the
            // ref go on meanwhile.
            drop(index);
            if derived.replace(head) == Some(head) {
                tracing::error!(%head, "the head has no path history after it was derived");
                return Err(Error::Corrupt(format!(
                    "the path history of {head} is missing after it was derived"
                )));
            }
            derive_history(store, repo, head, &req.deadline).map_err(stays_pending)?;
        }
    }

    /// Locks the slot of a ref. A poisoned lock means a panic held the
    /// index mid-update: the index may be half-written, and the conflict
    /// check would trust it. The index is a cache and the database is the
    /// truth, so the poisoned one is dropped - the next land rebuilds it
    /// - and the poison is cleared for the locks that follow.
    fn lock_slot<'a>(
        repo: RepoId,
        name: &str,
        slot: &'a RefSlot,
    ) -> MutexGuard<'a, Option<PathIndex>> {
        match slot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = None;
                slot.clear_poison();
                tracing::warn!(
                    repo = ?repo,
                    target_ref = %name,
                    "the path index of a ref was dropped after a panic; it rebuilds"
                );
                guard
            }
        }
    }

    /// The slot of a ref, which becomes the most recently used one.
    fn slot(&self, repo: RepoId, name: &str) -> RefSlot {
        let mut refs = self.lock_refs();
        let key = (repo, name.to_owned());
        if let Some(slot) = refs.get(&key) {
            return Arc::clone(slot);
        }
        let slot = RefSlot::default();
        refs.insert(key, Arc::clone(&slot));
        slot
    }

    fn lock_refs(&self) -> MutexGuard<'_, LruCache<(RepoId, String), RefSlot>> {
        self.refs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes `bytes` from the budget that all path indexes share. If too few
    /// are free, first drops the indexes of other refs ([`Lands::evict`]).
    /// The caller holds the lock of the index that grows, so that one stays.
    /// Returns false if it could not take the bytes.
    fn reserve(&self, bytes: usize) -> bool {
        // A concurrent land can take the room that an eviction freed, so
        // this evicts twice at most.
        for _ in 0..2 {
            if self.take(bytes) {
                return true;
            }
            if !self.evict(bytes) {
                return false;
            }
        }
        self.take(bytes)
    }

    /// Drops the indexes of other refs, least recently used first, until
    /// `bytes` are free, but only if that frees enough. It skips each index
    /// whose ref's lock is held, because a land uses it. Returns false if it
    /// could not free enough. Also removes the slots that hold no index and
    /// that no land uses, so the next eviction does not look at them.
    fn evict(&self, bytes: usize) -> bool {
        let slots: Vec<((RepoId, String), RefSlot)> = {
            let mut refs = self.lock_refs();
            // No land can clone a slot while `refs` is locked, so a slot that
            // only `refs` holds is not in use.
            refs.retain(|_, slot| {
                Arc::strong_count(slot) > 1
                    || !matches!(slot.try_lock(), Ok(index) if index.is_none())
            });
            refs.iter()
                .map(|(key, slot)| (key.clone(), Arc::clone(slot)))
                .collect()
        };
        let free = MAX_INDEX_BYTES - self.used.load(Ordering::Acquire);
        let short = bytes.saturating_sub(free);
        let mut idle = Vec::new();
        let mut freed = 0;
        for ((repo, name), slot) in &slots {
            if freed >= short {
                break;
            }
            let index = match slot.try_lock() {
                Ok(index) => index,
                Err(TryLockError::Poisoned(e)) => {
                    // A panic held this index mid-update: it may be
                    // half-written. Dropped like any evicted index, its
                    // bytes back in the budget; the next land rebuilds it
                    // from the database.
                    let guard = e.into_inner();
                    slot.clear_poison();
                    tracing::warn!(
                        repo = ?repo,
                        target_ref = %name,
                        "the path index of a ref was dropped after a panic; it rebuilds"
                    );
                    guard
                }
                Err(TryLockError::WouldBlock) => continue,
            };
            if let Some(held) = index.as_ref() {
                freed += held.bytes;
                idle.push(index);
            }
        }
        if freed < short {
            return false;
        }
        if !idle.is_empty() {
            tracing::info!(
                dropped = idle.len(),
                freed,
                "the path indexes are full; dropped the least recently used ones"
            );
        }
        let mut evicted = self.evicted.lock().unwrap_or_else(PoisonError::into_inner);
        for mut index in idle {
            let mut dropped = index.take().expect("an index was picked");
            // Its bytes go back now; the land frees its memory later.
            self.used.fetch_sub(dropped.bytes, Ordering::AcqRel);
            dropped.bytes = 0;
            evicted.push(dropped);
        }
        true
    }

    /// Takes `bytes` from the budget if they are free.
    fn take(&self, bytes: usize) -> bool {
        let mut used = self.used.load(Ordering::Acquire);
        while bytes <= MAX_INDEX_BYTES - used {
            let sum = used + bytes;
            match self
                .used
                .compare_exchange_weak(used, sum, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(now) => used = now,
            }
        }
        false
    }
}

/// The current state of a land that `caller` started.
///
/// # Errors
///
/// [`Error::NotFound`] if the repo has no such land, or another caller
/// started it.
pub fn get_land(store: &Backend, caller: &Caller, repo: &str, land_id: i64) -> Result<LandOutcome> {
    store.read(|c| {
        let repo = c.repo_id(repo)?;
        c.land_outcome(repo, caller, land_id)
    })
}

fn check_name(what: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_NAME_BYTES {
        return Err(Error::InvalidArgument(format!(
            "{what} must be 1 to {MAX_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

/// A verdict the head decided, and the ref version the plan read it
/// from: `settle` records it only while the ref still answers that
/// version, and an import that moved it in the window replans.
struct Verdict {
    state: LandState,
    detail: String,
    paths: Vec<RepoPath>,
    /// The ref version the plan read; `None` for a ref the plan read
    /// and did not find.
    expected: Option<i64>,
}

/// What step 4 decided.
enum Plan {
    /// A verdict that does not depend on the ref: the changeset's own
    /// shape refused it before any head was read.
    Reject(String),
    /// A verdict the head decided.
    Verdict(Verdict),
    Land {
        rebased: Changeset,
        head: RefState,
    },
    /// The check needs the history of this head, which is not derived yet
    /// ([`check_history`]).
    Derive(ChangesetId),
}

/// How [`finish`] ended.
#[derive(Debug)]
enum Finished {
    Outcome(LandOutcome),
    /// Derive the history of this head, then plan again.
    Derive(ChangesetId),
}

/// What the plan's read transaction found: the land is already settled
/// by a twin call, or the plan says what it must do now.
enum Planned {
    Settled(LandOutcome),
    Do(Plan),
}

/// Says that a land that stopped at its deadline, or whose call went,
/// stays pending.
fn stays_pending(e: Error) -> Error {
    match e {
        Error::DeadlineExceeded(why) => Error::DeadlineExceeded(format!(
            "{why}; the land stays pending, retry with the same request key"
        )),
        Error::Cancelled(why) => Error::Cancelled(format!(
            "{why}; the land stays pending, retry with the same request key"
        )),
        e => e,
    }
}

/// Derives what [`check_history`] reads for `head`, in steps
/// ([`derived::fill_by`]): its node in the commit graph and its path
/// history. The caller holds no lock, so other requests run between the
/// steps, and the derivation stops between steps at `deadline`. A database
/// from before path history needs this once: each later changeset gets its
/// own when it is written.
fn derive_history(
    store: &Backend,
    repo: RepoId,
    head: ChangesetId,
    deadline: &Deadline,
) -> Result<()> {
    tracing::info!(%head, "deriving the history of the head for a land on an old base");
    derived::fill_by::<CommitGraph>(store, repo, head, deadline)?;
    derived::fill_by::<PathHistory>(store, repo, head, deadline)?;
    Ok(())
}

/// Runs steps 4 and 5 for a pending land. The caller holds the ref's lock.
///
/// Imports do not take that lock, and may run in another process, so the ref
/// can move between the plan and the transaction. The land is then planned
/// again from the new head, and that plan advances the index to it: each
/// plan reads the head from the database.
fn finish(
    lands: &Lands,
    store: &Backend,
    repo: RepoId,
    caller: &Caller,
    land_id: i64,
    req: &LandRequest,
    index: &mut Option<PathIndex>,
) -> Result<Finished> {
    let mut attempt = 1;
    loop {
        // The outcome check rides the plan's read transaction: one read
        // answers both "already settled by a twin call" and "what the
        // land must do now". On SQLite that read is one snapshot; on
        // Postgres `read_unflushed` runs its statements in autocommit,
        // so the outcome row and the plan's reads can see different
        // states. A twin call that settles between them is still
        // stopped at `set_land_state`'s pending guard, which answers
        // "no longer pending" instead of moving the ref twice - the
        // guard is the backstop there, not the shared read.
        let planned = store.read_unflushed(|c| {
            let row = c.land_outcome(repo, caller, land_id)?;
            if row.state != LandState::Pending {
                return Ok(Planned::Settled(row));
            }
            plan(lands, c, repo, req, index).map(Planned::Do)
        })?;
        #[cfg(test)]
        let step = lands
            .before_settle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        #[cfg(test)]
        // The hook runs with the seam's lock released: a hook that arms
        // the next one (a land that must lose the CAS at every attempt)
        // takes the same lock to set it.
        if let Some(step) = step {
            step();
        }
        let plan = match planned {
            Planned::Settled(row) => return Ok(Finished::Outcome(row)),
            Planned::Do(plan) => plan,
        };
        match settle(lands, store, repo, land_id, &req.target_ref, plan, index) {
            Err(Error::Aborted(detail)) if attempt < MAX_ATTEMPTS => {
                tracing::info!(land_id, attempt, %detail, "planning the land again");
                if let Some(metrics) = &lands.metrics {
                    metrics.land_replan();
                }
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

/// Step 4: decides the land against the ref's current head.
fn plan(
    lands: &Lands,
    c: &mut dyn MetaTx,
    repo: RepoId,
    req: &LandRequest,
    index: &mut Option<PathIndex>,
) -> Result<Plan> {
    let target = &req.target_ref;
    let cs = c.get_changeset(repo, req.changeset)?;
    let [base] = cs.parents()[..] else {
        return Ok(Plan::Reject(
            "a land needs a changeset with exactly one parent".into(),
        ));
    };
    let Some(head) = c.get_ref(repo, target)? else {
        return Ok(Plan::Verdict(Verdict {
            state: LandState::Rejected,
            detail: format!("ref {target:?} does not exist"),
            paths: vec![],
            expected: None,
        }));
    };
    let index = match index {
        Some(index) => index,
        None => index.insert(PathIndex::new(c, repo, head.target, &lands.used)?),
    };
    index.advance_to(lands, c, repo, head.target, &req.deadline)?;
    let conflicts = match index.check(lands, c, repo, base, cs.changes(), &req.deadline)? {
        Checked::Paths(conflicts) => conflicts,
        Checked::NotOnHistory => {
            return Ok(Plan::Verdict(Verdict {
                state: LandState::Rejected,
                detail: format!("base {base} is not on the first-parent history of {target:?}"),
                paths: vec![],
                expected: Some(head.version),
            }));
        }
        Checked::Derive => return Ok(Plan::Derive(head.target)),
    };
    if !conflicts.is_empty() {
        let detail = format!(
            "{} path(s) changed on {target:?} after the base",
            conflicts.len()
        );
        return Ok(Plan::Verdict(Verdict {
            state: LandState::Conflict,
            detail,
            paths: conflicts,
            expected: Some(head.version),
        }));
    }
    let rebased = cs
        .with_parents(vec![head.target])
        .expect("one parent fits the format");
    Ok(Plan::Land { rebased, head })
}

/// Step 5: records what the plan decided. If the ref moved since the
/// plan, writes nothing and returns [`Error::Aborted`]: a verdict the
/// head decided is final only against the head it was planned on, so it
/// carries the ref version the plan read, and the recording transaction
/// re-reads the ref and writes only under that version - the same
/// guard the land's own move has in its CAS. An import that moved the
/// ref in the window aborts the verdict, and `finish` replans against
/// the head the import made.
fn settle(
    lands: &Lands,
    store: &Backend,
    repo: RepoId,
    land_id: i64,
    target: &str,
    plan: Plan,
    index: &mut Option<PathIndex>,
) -> Result<Finished> {
    let (rebased, head) = match plan {
        Plan::Derive(head) => return Ok(Finished::Derive(head)),
        Plan::Reject(detail) => {
            return store
                .commit(|c| {
                    c.set_land_state(repo, land_id, LandState::Rejected, None, detail, vec![])
                })
                .map(Finished::Outcome);
        }
        Plan::Verdict(verdict) => {
            return store
                .commit(|c| record_verdict(c, repo, target, land_id, verdict))
                .map(Finished::Outcome);
        }
        Plan::Land { rebased, head } => (rebased, head),
    };

    // See `tree::store_changeset`. The land's flush covers these steps.
    tree::fill_root_tree(store, repo, head.target)?;
    // The blob lengths the tree build needs, read before the write
    // transaction, so the write connection does no file I/O.
    let lens = tree::lens_of_puts(store, crate::db::puts(&rebased).map(|(_, blob)| blob))?;
    let landed = store.commit(|c| {
        let (id, _) = tree::store_changeset(c, repo, &rebased, &mut tree::lens_from(&lens))?;
        if !c.cas_ref(repo, target, Some(head.version), id)? {
            // The error rolls back the transaction, rebased changeset included.
            return Err(Error::Aborted(format!(
                "ref {target:?} moved during the land; retry with the same request key"
            )));
        }
        let generation = c.generation(repo, id)?;
        let mut outcome = c.set_land_state(
            repo,
            land_id,
            LandState::Landed,
            Some(id),
            String::new(),
            vec![],
        )?;
        // The move's CAS pins the version the plan read, so
        // `head.target` is the target this move replaced: the audit's
        // old value, set here where the move happens, under the ref's
        // lock, and not stored.
        outcome.moved = true;
        outcome.previous = Some(head.target);
        Ok((outcome, generation))
    });
    match landed {
        Ok((outcome, generation)) => {
            if let (Some(index), Some(id)) = (index.as_mut(), outcome.result) {
                index.record(lands, id, generation, rebased.changes());
            }
            // The hook runs with the slot's lock still held: a test that
            // panics here poisons the ref's mutex with a half-written
            // index.
            #[cfg(test)]
            if let Some(step) = lands
                .after_record
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                step(index);
            }
            tracing::info!(land_id, id = ?outcome.result, target = %target, "landed");
            Ok(Finished::Outcome(outcome))
        }
        // The path check passed, but the changes still clash with the head's
        // tree (for example a delete of a file the head never had). The
        // transaction rolled back; record the conflict.
        // The path check passed, but the changes still clash with the head's
        // tree (for example a delete of a file the head never had). The
        // transaction rolled back; record the conflict - against the
        // head the apply ran on, with the same version guard as any
        // verdict: the failed transaction and this one are two, and an
        // import can move the ref between them.
        Err(Error::Apply(e)) => {
            let detail = format!("does not apply to {target:?}: {e}");
            store
                .commit(|c| {
                    record_verdict(
                        c,
                        repo,
                        target,
                        land_id,
                        Verdict {
                            state: LandState::Conflict,
                            detail,
                            paths: vec![],
                            expected: Some(head.version),
                        },
                    )
                })
                .map(Finished::Outcome)
        }
        Err(e) => Err(e),
    }
}

/// Records a verdict the head decided, only while the ref still answers
/// the version the plan read it from. The re-read shares the write
/// transaction with the verdict: the write orders this transaction
/// against the import's move on every backend (the one write connection,
/// or the collector's key), so a ref the plan read is either still
/// here, or the import committed before this transaction began and the
/// read below sees it. A ref that moved aborts the verdict, and
/// `finish` replans against the head the import made.
fn record_verdict(
    c: &mut dyn MetaTx,
    repo: RepoId,
    target: &str,
    land_id: i64,
    verdict: Verdict,
) -> Result<LandOutcome> {
    let Verdict {
        state,
        detail,
        paths,
        expected,
    } = verdict;
    let at = c.get_ref(repo, target)?.map(|r| r.version);
    if at != expected {
        return Err(Error::Aborted(format!(
            "ref {target:?} moved during the land; retry with the same request key"
        )));
    }
    c.set_land_state(repo, land_id, state, None, detail, paths)
}

/// For one ref's first-parent history: the newest generation at which each
/// path, and each directory, last changed.
///
/// Generations strictly increase along first parents, so a change happened
/// after a base exactly when its generation is greater than the base's.
/// A conflict check then costs one lookup per path part, not a history walk.
///
/// The index covers the changesets after `floor` up to `tip`. It grows
/// forward as the ref moves and backward when a land has an older base, up
/// to [`MAX_DEPTH`] below the tip. Each [`PRUNE_EVERY`] generations,
/// [`PathIndex::prune`] moves `floor` up to the oldest base that the cycle
/// used, so memory follows the bases in use. [`MAX_INDEX_BYTES`] bounds all
/// indexes together.
struct PathIndex {
    tip: ChangesetId,
    floor: ChangesetId,
    /// Changesets on the indexed history, by generation. A first-parent
    /// history has at most one changeset per generation.
    chain: BTreeMap<i64, ChangesetId>,
    /// File path → newest generation that put or deleted it.
    files: HashMap<Vec<u8>, i64>,
    /// Directory path → newest generation that changed anything below it.
    dirs: HashMap<Vec<u8>, i64>,
    /// Generation of the tip when this prune cycle started.
    cycle_start: i64,
    /// Oldest generation this cycle needs: `cycle_start`, or an older base.
    oldest_base: i64,
    /// What `chain`, `files` and `dirs` hold, by [`entry_bytes`].
    bytes: usize,
    /// [`Lands::used`], which counts `bytes` too.
    used: Arc<AtomicUsize>,
}

impl Drop for PathIndex {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl PathIndex {
    /// An empty index at `head`, counted in `used`. Its first prune cycle
    /// starts there.
    fn new(
        c: &mut dyn MetaTx,
        repo: RepoId,
        head: ChangesetId,
        used: &Arc<AtomicUsize>,
    ) -> Result<Self> {
        Ok(Self::at(head, c.generation(repo, head)?, used))
    }

    /// An empty index at `head`, of generation `head_gen`.
    fn at(head: ChangesetId, head_gen: i64, used: &Arc<AtomicUsize>) -> Self {
        Self {
            tip: head,
            floor: head,
            chain: BTreeMap::new(),
            files: HashMap::new(),
            dirs: HashMap::new(),
            cycle_start: head_gen,
            oldest_base: head_gen,
            bytes: 0,
            used: Arc::clone(used),
        }
    }

    /// The page one walk reads at a time: one statement per page, where
    /// a walk that reads one changeset at a time pays a round trip per
    /// changeset. A page may hold rows past the point the walk stops at;
    /// that costs rows, not statements.
    const WALK_PAGE: i64 = 1_000;

    /// Indexes the changesets from the current tip forward to `head`. Starts
    /// over at `head` if `head` does not descend from the tip (the ref moved
    /// to other history), if `head` is more than [`MAX_DEPTH`] changesets
    /// ahead, or if those changesets would not fit in [`MAX_INDEX_BYTES`].
    /// An index at the head is always correct, and a longer walk would hold
    /// the write connection for little gain.
    fn advance_to(
        &mut self,
        lands: &Lands,
        c: &mut dyn MetaTx,
        repo: RepoId,
        head: ChangesetId,
        deadline: &Deadline,
    ) -> Result<()> {
        if head == self.tip {
            return Ok(());
        }
        let tip_gen = c.generation(repo, self.tip)?;
        let mut walked: Vec<(ChangesetId, i64, BTreeMap<RepoPath, Option<FileState>>)> = Vec::new();
        // The most that the walked changesets can add, to stop the walk early.
        let mut most = 0;
        let mut steps = 0;
        let mut cur = Some(head);
        'walk: while let Some(start) = cur {
            // One statement per page of first parents: the rows carry the
            // generation and the body that the two per-changeset reads
            // fetched. The stop rules below stay per changeset.
            for (id, generation, body) in c.first_parent_page(repo, start, Self::WALK_PAGE)? {
                if id == self.tip {
                    let cost = self.cost(walked.iter().map(|(_, g, changes)| (*g, changes)));
                    let grew = self.grow(lands, cost, |index| {
                        for (id, generation, changes) in walked {
                            index.add(id, generation, &changes);
                        }
                    });
                    if grew {
                        self.tip = head;
                        return Ok(());
                    }
                    tracing::info!("the path indexes are full; it starts over at the head");
                    break 'walk;
                }
                deadline.check()?;
                steps += 1;
                if steps > MAX_DEPTH {
                    tracing::info!(
                        steps,
                        "the ref moved far; the path index starts over at the head"
                    );
                    break 'walk;
                }
                if generation <= tip_gen {
                    break 'walk;
                }
                let cs = decode_page_changeset(id, &body)?;
                most += added_bytes(cs.changes());
                if self.bytes + most > MAX_INDEX_BYTES {
                    tracing::info!(
                        generation,
                        "the path indexes are full; it starts over at the head"
                    );
                    break 'walk;
                }
                walked.push((id, generation, cs.changes().clone()));
                cur = cs.parents().first().copied();
            }
        }
        *self = Self::new(c, repo, head, &self.used)?;
        Ok(())
    }

    /// The paths in `changes` that changed on the history after `base`, if
    /// `base` is on the history. Then prunes.
    ///
    /// A base below the floor is reached: the changesets from the floor back
    /// to it are added to the index, while they fit in [`MAX_INDEX_BYTES`].
    /// If they do not all fit, or the base is more than [`MAX_DEPTH`] below
    /// the tip, [`check_history`] checks the land in the tip's path history
    /// instead. Such a deep base does not count as used for pruning.
    fn check(
        &mut self,
        lands: &Lands,
        c: &mut dyn MetaTx,
        repo: RepoId,
        base: ChangesetId,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
        deadline: &Deadline,
    ) -> Result<Checked> {
        let base_gen = c.generation(repo, base)?;
        let tip_gen = match self.chain.last_key_value() {
            Some((&generation, _)) => generation,
            None => c.generation(repo, self.tip)?,
        };
        let deep = tip_gen - base_gen > MAX_DEPTH;
        let mut reached = base == self.floor || self.chain.get(&base_gen) == Some(&base);
        // A deep base does not grow the index.
        if !reached && !deep {
            match self.reach(lands, c, repo, base, base_gen, deadline)? {
                Walk::Reached => reached = true,
                Walk::Missed => return Ok(Checked::NotOnHistory),
                Walk::Stopped => {}
            }
        }
        let conflicts = if reached {
            // The index holds every changeset after the base.
            changes
                .keys()
                .filter(|path| self.touches(path.as_bytes(), base_gen))
                .cloned()
                .collect()
        } else {
            match check_history(c, self.tip, base, base_gen, changes, deadline)? {
                Checked::Paths(conflicts) => conflicts,
                other => return Ok(other),
            }
        };
        if !deep {
            self.oldest_base = self.oldest_base.min(base_gen);
        }
        self.prune();
        Ok(Checked::Paths(conflicts))
    }

    /// Walks first parents from the floor back to `base`, adding each
    /// changeset and moving the floor down past it. Stops at the first
    /// changeset that does not fit in [`MAX_INDEX_BYTES`], which is then the
    /// floor. The caller has checked that `base` is not the floor and not in
    /// the index. Fails with [`Error::DeadlineExceeded`] once `deadline` has
    /// passed, or [`Error::Cancelled`] once the call has gone.
    fn reach(
        &mut self,
        lands: &Lands,
        c: &mut dyn MetaTx,
        repo: RepoId,
        base: ChangesetId,
        base_gen: i64,
        deadline: &Deadline,
    ) -> Result<Walk> {
        loop {
            // One statement per page of first parents; the stop rules
            // below stay per changeset. A page may hold rows past the
            // point the walk stops at, which costs rows, not statements.
            for (id, generation, body) in c.first_parent_page(repo, self.floor, Self::WALK_PAGE)? {
                deadline.check()?;
                if generation <= base_gen {
                    return Ok(Walk::Missed);
                }
                let cs = decode_page_changeset(id, &body)?;
                let cost = self.cost([(generation, cs.changes())]);
                if !self.grow(lands, cost, |index| index.add(id, generation, cs.changes())) {
                    return Ok(Walk::Stopped);
                }
                let Some(&parent) = cs.parents().first() else {
                    return Ok(Walk::Missed);
                };
                self.floor = parent;
                if parent == base {
                    return Ok(Walk::Reached);
                }
            }
        }
    }

    /// Whether a changeset after `base_gen` touched `path`: the same path, a
    /// directory above it, or anything below it.
    fn touches(&self, path: &[u8], base_gen: i64) -> bool {
        let after =
            |map: &HashMap<Vec<u8>, i64>, key: &[u8]| map.get(key).is_some_and(|&g| g > base_gen);
        after(&self.files, path)
            || after(&self.dirs, path)
            || ancestors(path).any(|dir| after(&self.files, dir))
    }

    /// Once the tip is [`PRUNE_EVERY`] generations past the cycle start,
    /// moves `floor` up to the oldest base the cycle used, drops what is now
    /// below it, and starts a new cycle.
    ///
    /// A later base below the new floor is still checked in full:
    /// [`Self::check`] reads the dropped changesets back from the database.
    fn prune(&mut self) {
        let Some((&tip_gen, _)) = self.chain.last_key_value() else {
            return;
        };
        if tip_gen - self.cycle_start < PRUNE_EVERY {
            return;
        }
        if let Some((&floor_gen, &floor)) = self.chain.range(..=self.oldest_base).next_back() {
            self.chain = self.chain.split_off(&(floor_gen + 1));
            self.floor = floor;
            for map in [&mut self.files, &mut self.dirs] {
                map.retain(|_, &mut g| g > floor_gen);
                // `retain` keeps the capacity, so give it back.
                map.shrink_to_fit();
            }
            let kept = self.measure();
            self.used.fetch_sub(self.bytes - kept, Ordering::AcqRel);
            self.bytes = kept;
            tracing::debug!(floor_gen, kept = self.chain.len(), "pruned the path index");
        }
        self.cycle_start = tip_gen;
        self.oldest_base = tip_gen;
    }

    /// Records a changeset that just became the tip. If it does not fit in
    /// [`MAX_INDEX_BYTES`], the index starts over at it.
    fn record(
        &mut self,
        lands: &Lands,
        id: ChangesetId,
        generation: i64,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
    ) {
        let cost = self.cost([(generation, changes)]);
        if !self.grow(lands, cost, |index| index.add(id, generation, changes)) {
            tracing::info!(
                generation,
                "the path indexes are full; it starts over at the tip"
            );
            *self = Self::at(id, generation, &self.used);
            return;
        }
        self.tip = id;
    }

    /// Runs `add`, which adds `bytes`, as [`PathIndex::cost`] counts them,
    /// if the budget of all indexes has room for them ([`Lands::reserve`]).
    /// Returns false, and runs nothing, if it has not.
    fn grow(&mut self, lands: &Lands, bytes: usize, add: impl FnOnce(&mut Self)) -> bool {
        if !lands.reserve(bytes) {
            return false;
        }
        let before = self.bytes;
        add(self);
        debug_assert_eq!(self.bytes - before, bytes, "the cost of an addition");
        true
    }

    /// What adding changesets with these generations and changes adds to the
    /// index, by [`entry_bytes`]: only the keys that it does not hold, each
    /// once. The changesets are on the index's history, so each generation
    /// is a new chain entry unless the chain holds it.
    fn cost<'c>(
        &self,
        changesets: impl IntoIterator<Item = (i64, &'c BTreeMap<RepoPath, Option<FileState>>)>,
    ) -> usize {
        let mut files = HashSet::new();
        let mut dirs = HashSet::new();
        let mut bytes = 0;
        for (generation, changes) in changesets {
            if !self.chain.contains_key(&generation) {
                bytes += entry_bytes(&[]);
            }
            for path in changes.keys() {
                let path = path.as_bytes();
                if !self.files.contains_key(path) && files.insert(path) {
                    bytes += entry_bytes(path);
                }
                for dir in ancestors(path) {
                    if !self.dirs.contains_key(dir) && dirs.insert(dir) {
                        bytes += entry_bytes(dir);
                    }
                }
            }
        }
        bytes
    }

    fn add(
        &mut self,
        id: ChangesetId,
        generation: i64,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
    ) {
        if self.chain.insert(generation, id).is_none() {
            self.bytes += entry_bytes(&[]);
        }
        for path in changes.keys() {
            let path = path.as_bytes();
            self.bytes += bump(&mut self.files, path, generation);
            for dir in ancestors(path) {
                self.bytes += bump(&mut self.dirs, dir, generation);
            }
        }
    }

    /// What the index holds, by [`entry_bytes`]. Counted from scratch.
    fn measure(&self) -> usize {
        let keys = |map: &HashMap<Vec<u8>, i64>| map.keys().map(|k| entry_bytes(k)).sum::<usize>();
        self.chain.len() * entry_bytes(&[]) + keys(&self.files) + keys(&self.dirs)
    }
}

/// What one index entry costs, roughly: its key, plus the map slot, the
/// key's length and its allocation. A chain entry has no key.
fn entry_bytes(key: &[u8]) -> usize {
    key.len() + 64
}

/// The most that [`PathIndex::add`] adds for a changeset with `changes`.
/// The changeset a page row carries: the same decode and ID check
/// [`MetaTx::get_changeset`] runs, so a page row and a per-changeset read
/// agree.
fn decode_page_changeset(id: ChangesetId, body: &[u8]) -> Result<Changeset> {
    let (cs, actual) = Changeset::decode_with_id(body)
        .map_err(|e| Error::Corrupt(format!("changeset {id}: {e}")))?;
    if actual != id {
        return Err(Error::Corrupt(format!(
            "changeset {id} does not match its ID"
        )));
    }
    Ok(cs)
}

fn added_bytes(changes: &BTreeMap<RepoPath, Option<FileState>>) -> usize {
    let paths = changes.keys().map(|path| {
        let path = path.as_bytes();
        entry_bytes(path) + ancestors(path).map(entry_bytes).sum::<usize>()
    });
    entry_bytes(&[]) + paths.sum::<usize>()
}

/// What [`PathIndex::check`] found.
#[derive(Debug, PartialEq, Eq)]
enum Checked {
    /// The base is on the history, and these paths of the land changed
    /// after it.
    Paths(Vec<RepoPath>),
    /// The base is not on the first-parent history.
    NotOnHistory,
    /// The tip has no node in the commit graph or no path history yet:
    /// derive them, then plan again.
    Derive,
}

/// How a [`PathIndex::reach`] ended.
enum Walk {
    /// It reached the base: the base is on the history.
    Reached,
    /// The base is not on the history.
    Missed,
    /// The index ran out of room before the walk reached the base.
    Stopped,
}

/// Checks a land in the path history of `tip`, for a base that the
/// [`PathIndex`] does not reach: whether `base` is on the first-parent
/// history of `tip`, in the commit graph, and which paths in `changes`
/// changed after it, by the rule of [`PathIndex::touches`]. Reads O(log n)
/// nodes of the commit graph and at most one history node per part of each
/// path, however old the base is. [`Checked::Derive`] if `tip` has no node
/// or no path history yet.
fn check_history(
    c: &mut dyn MetaTx,
    tip: ChangesetId,
    base: ChangesetId,
    base_gen: i64,
    changes: &BTreeMap<RepoPath, Option<FileState>>,
    deadline: &Deadline,
) -> Result<Checked> {
    let Some(root) = derived::load::<PathHistory>(c, tip)? else {
        return Ok(Checked::Derive);
    };
    Ok(
        match graph::on_first_parents(c, tip, base, base_gen, deadline)? {
            None => Checked::Derive,
            Some(false) => Checked::NotOnHistory,
            Some(true) => {
                let conflicts =
                    history::touched_after(c, root, changes.keys(), base_gen, deadline)?;
                tracing::debug!(
                    base_gen,
                    conflicts = conflicts.len(),
                    "checked a base in the path history"
                );
                Checked::Paths(conflicts)
            }
        },
    )
}

/// Raises `key` to `generation`. Returns the bytes added: those of a new
/// entry, else 0.
fn bump(map: &mut HashMap<Vec<u8>, i64>, key: &[u8], generation: i64) -> usize {
    if let Some(slot) = map.get_mut(key) {
        *slot = (*slot).max(generation);
        return 0;
    }
    map.insert(key.to_vec(), generation);
    entry_bytes(key)
}

/// The proper directory prefixes of a path: `a/b/c` gives `a`, `a/b`.
fn ancestors(path: &[u8]) -> impl Iterator<Item = &[u8]> {
    path.iter()
        .enumerate()
        .filter(|&(_, &b)| b == b'/')
        .map(move |(i, _)| &path[..i])
}

/// Paths joined by NUL, which a `RepoPath` cannot contain.
pub(crate) fn encode_paths(paths: &[RepoPath]) -> Vec<u8> {
    paths
        .iter()
        .map(RepoPath::as_bytes)
        .collect::<Vec<_>>()
        .join(&0)
}

pub(crate) fn decode_paths(bytes: &[u8]) -> Option<Vec<RepoPath>> {
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    bytes
        .split(|&b| b == 0)
        .map(|p| RepoPath::new(p).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::backend::Backend;
    use crate::db::{self, SqliteTx};
    use crate::store::Store;
    use rusqlite::{Connection, params};
    use std::time::Instant;

    use super::*;
    use crate::db::testing;
    use crate::store::FSYNC_LEAVES_DRIVE_CACHE;
    use scion_core::{BlobId, Entry, FileEntry, IdentityTime};

    type Change = (RepoPath, Option<FileState>);
    type Edits<'a> = &'a [Change];

    struct Fixture {
        _dir: tempfile::TempDir,
        store: Backend,
        repo: RepoId,
        root: ChangesetId,
        lands: Lands,
    }

    /// Stores every blob file the tests have named so far: the land's tree
    /// builds read their lengths from the files. Repeat calls put nothing
    /// new: a stored copy is kept.
    fn store_blobs(store: &Backend) {
        CONTENTS.with(|c| {
            for bytes in c.borrow().values() {
                store.blobs().put(bytes).unwrap();
            }
        });
    }

    fn author() -> IdentityTime {
        IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0).expect("identity fits the format")
    }

    thread_local! {
        /// What each blob of the test changesets holds, so the fixture can
        /// store the files the real lens reads. Tests use one thread each.
        static CONTENTS: std::cell::RefCell<std::collections::HashMap<BlobId, Vec<u8>>> =
            std::cell::RefCell::new(std::collections::HashMap::new());
    }

    fn put(path: &str, content: &str) -> Change {
        let bytes = content.as_bytes().to_vec();
        let content = BlobId::of(&bytes);
        CONTENTS.with(|c| c.borrow_mut().insert(content, bytes));
        let state = FileState::Regular {
            content,
            executable: false,
        };
        (RepoPath::new(path).unwrap(), Some(state))
    }

    fn del(path: &str) -> Change {
        (RepoPath::new(path).unwrap(), None)
    }

    /// A repo whose `main` points at a root with `a/x`, `b/y` and `c`.
    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        fixture_on(store, dir)
    }

    /// The same repo, on the test backend the environment names: the
    /// verdict race runs on SQLite and on the test database alike.
    fn fixture_shared() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        fixture_on(store, dir)
    }

    fn fixture_on(store: Backend, dir: tempfile::TempDir) -> Fixture {
        let (repo, root) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                let root = Changeset::new(
                    vec![],
                    author(),
                    None,
                    b"root".to_vec(),
                    BTreeMap::from([put("a/x", "1"), put("b/y", "2"), put("c", "3")]),
                )
                .expect("changeset fits the format");
                let id = c.insert_changeset(repo, &root)?;
                assert!(c.cas_ref(repo, "main", None, id)?);
                Ok((repo, id))
            })
            .unwrap();
        store_blobs(&store);
        Fixture {
            _dir: dir,
            store,
            repo,
            root,
            lands: Lands::default(),
        }
    }

    /// The stored fields of an outcome: a replay returns the first
    /// call's outcome. `moved` and `previous` are set in memory by the
    /// call that moved the ref, and a replay has none.
    fn stored(out: &LandOutcome) -> (i64, LandState, Option<ChangesetId>, String) {
        (out.land_id, out.state, out.result, out.detail.clone())
    }

    fn caller() -> Caller {
        Caller("t".into())
    }

    fn req(cs: ChangesetId, key: &str) -> LandRequest {
        LandRequest {
            repo: "r".into(),
            changeset: cs,
            target_ref: "main".into(),
            request_key: key.into(),
            deadline: Deadline::new(Instant::now() + std::time::Duration::from_secs(600)),
        }
    }

    fn changeset(parents: Vec<ChangesetId>, changes: Edits<'_>) -> Changeset {
        Changeset::new(
            parents,
            author(),
            None,
            format!("{changes:?}").into_bytes(),
            changes.iter().cloned().collect(),
        )
        .expect("changeset fits the format")
    }

    impl Fixture {
        fn commit(&self, base: ChangesetId, changes: Edits<'_>) -> ChangesetId {
            self.insert(&changeset(vec![base], changes))
        }

        fn insert(&self, cs: &Changeset) -> ChangesetId {
            store_blobs(&self.store);
            self.store
                .write(|c| c.insert_changeset(self.repo, cs))
                .unwrap()
        }

        /// Moves `main` to `to` the way an import does: outside lands.
        fn move_main(&self, to: ChangesetId) {
            let head = self.main();
            let moved = self
                .store
                .write(|c| c.cas_ref(self.repo, "main", Some(head.version), to))
                .unwrap();
            assert!(moved);
        }

        fn land(&self, cs: ChangesetId, key: &str) -> LandOutcome {
            self.try_land(cs, key).unwrap()
        }

        fn try_land(&self, cs: ChangesetId, key: &str) -> Result<LandOutcome> {
            self.lands.land(&self.store, &caller(), &req(cs, key))
        }

        fn landed_count(&self) -> i64 {
            self.store
                .read(|c| {
                    let sql = "SELECT count(*) FROM lands WHERE state = 'landed'";
                    c.sqlite()
                        .unwrap()
                        .query_row(sql, [], |row| row.get(0))
                        .map_err(crate::error::from_sqlite)
                })
                .unwrap()
        }

        fn get(&self, land_id: i64) -> Result<LandOutcome> {
            get_land(&self.store, &caller(), "r", land_id)
        }

        /// Whether the land expired: `GetLand` no longer finds it.
        fn gone(&self, out: &LandOutcome) -> bool {
            matches!(self.get(out.land_id), Err(Error::NotFound(_)))
        }

        /// Moves the time a land finished `secs` into the past.
        fn age(&self, land_id: i64, secs: i64) {
            let sql = "UPDATE lands SET finished_at = finished_at - ?2 WHERE land_id = ?1";
            let aged = self
                .store
                .write(|c| {
                    c.sqlite()
                        .unwrap()
                        .execute(sql, params![land_id, secs])
                        .map_err(crate::error::from_sqlite)
                })
                .unwrap();
            assert_eq!(aged, 1);
        }

        fn main(&self) -> RefState {
            self.store
                .read(|c| c.get_ref(self.repo, "main"))
                .unwrap()
                .unwrap()
        }

        fn file(&self, cs: ChangesetId, path: &str) -> Option<Entry> {
            let deadline = crate::deadline::Deadline::new(
                std::time::Instant::now() + std::time::Duration::from_secs(60),
            );
            let root = tree::root_tree(&self.store, self.repo, cs, &deadline).unwrap();
            self.store
                .read(|c| tree::lookup(c, root, &RepoPath::new(path).unwrap()))
                .unwrap()
        }

        fn generation(&self, cs: ChangesetId) -> i64 {
            self.store.read(|c| c.generation(self.repo, cs)).unwrap()
        }

        /// Lands `n` changesets in a row, each on the head. The i-th one,
        /// from 1, puts `{prefix}{i}`. Returns them in order.
        fn fill(&self, prefix: &str, n: usize) -> Vec<ChangesetId> {
            (1..=n)
                .map(|i| {
                    let cs = self.commit(self.main().target, &[put(&format!("{prefix}{i}"), "x")]);
                    let out = self.land(cs, &cs.to_string());
                    assert_eq!(out.state, LandState::Landed, "{}", out.detail);
                    cs
                })
                .collect()
        }

        /// Runs `read` on the path index of `main`.
        fn index<T>(&self, read: impl FnOnce(&PathIndex) -> T) -> T {
            let slot = self.slot("main");
            let index = slot.lock().unwrap();
            read(index.as_ref().expect("main has a path index"))
        }

        /// The slot of ref `name`. Unlike a land, this does not count as a
        /// use of the ref.
        fn slot(&self, name: &str) -> RefSlot {
            let refs = self.lands.lock_refs();
            Arc::clone(refs.peek(&(self.repo, name.to_owned())).unwrap())
        }

        /// Checks the byte counts of all path indexes against the shared
        /// budget. Returns whether each ref, by name, has an index.
        fn held(&self) -> BTreeMap<String, bool> {
            let slots: Vec<(String, RefSlot)> = (self.lands.lock_refs().iter())
                .map(|((_, name), slot)| (name.clone(), Arc::clone(slot)))
                .collect();
            let mut sum = 0;
            let mut held = BTreeMap::new();
            for (name, slot) in slots {
                let index = slot.lock().unwrap();
                if let Some(index) = index.as_ref() {
                    assert_eq!(index.bytes, index.measure(), "{name}");
                    sum += index.bytes;
                }
                held.insert(name, index.is_some());
            }
            assert_eq!(self.lands.used.load(Ordering::Acquire), sum);
            assert!(sum <= MAX_INDEX_BYTES, "{sum} bytes");
            held
        }

        /// Lands `cs` on ref `name`.
        fn land_on(&self, name: &str, cs: ChangesetId, key: &str) -> LandOutcome {
            let req = LandRequest {
                target_ref: name.into(),
                ..req(cs, key)
            };
            self.lands.land(&self.store, &caller(), &req).unwrap()
        }
    }

    #[test]
    fn lands_on_the_head_without_rebase() {
        let f = fixture();
        let cs = f.commit(f.root, &[put("a/x", "new")]);
        let out = f.land(cs, "k");
        assert_eq!((out.state, out.result), (LandState::Landed, Some(cs)));
        assert_eq!(f.main().target, cs);
        assert_eq!(f.main().version, 2);
    }

    #[test]
    fn a_land_waits_for_one_full_flush_for_all_its_commits() {
        let f = fixture();
        let cs = f.commit(f.root, &[put("a/x", "new")]);
        let before = f.store.blobs().full_flushes();
        assert_eq!(f.land(cs, "k").state, LandState::Landed);
        // None where `fsync` already flushes the drive cache.
        assert_eq!(
            f.store.blobs().full_flushes() - before,
            u64::from(FSYNC_LEAVES_DRIVE_CACHE)
        );
    }

    /// The path check passes, but the apply still fails: a delete of a
    /// path the head never had has nothing to conflict with on the path
    /// side, and the apply refuses it against the head's tree. The land
    /// must end as a recorded Conflict with the version the apply ran
    /// on - not an error, and not landed.
    #[test]
    fn a_delete_the_head_never_had_ends_in_a_recorded_conflict() {
        let f = fixture_shared();
        let cs_a = f.commit(f.root, &[put("a/x", "A")]);
        assert_eq!(f.land(cs_a, "ka").state, LandState::Landed);
        let cs = f.commit(f.main().target, &[del("never/had.txt")]);
        let out = f.land(cs, "kdel");
        assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
        assert!(
            out.detail.starts_with("does not apply to"),
            "the detail is the apply refusal: {}",
            out.detail
        );
    }

    /// A land aimed at a ref that does not exist is a Rejected verdict,
    /// recorded and named - never an error and never a move.
    #[test]
    fn a_land_on_a_missing_ref_is_a_recorded_rejection() {
        let f = fixture_shared();
        let cs = f.commit(f.root, &[put("a/x", "A")]);
        let out = f
            .lands
            .land(
                &f.store,
                &caller(),
                &LandRequest {
                    repo: "r".into(),
                    changeset: cs,
                    target_ref: "nope".into(),
                    request_key: "knone".into(),
                    deadline: Deadline::new(Instant::now() + std::time::Duration::from_secs(600)),
                },
            )
            .unwrap();
        assert_eq!(out.state, LandState::Rejected, "{}", out.detail);
        assert!(
            out.detail.contains("nope"),
            "the detail names the missing ref: {}",
            out.detail
        );
        // Nothing moved.
        assert_eq!(f.main().target, f.root);
    }

    /// A panic that dies under a ref's lock poisons it with a
    /// half-written index. The index is a cache and the database is the
    /// truth, so the poisoned slot must be dropped and rebuilt: a land
    /// the half-index would wrongly pass still answers conflict. With
    /// the old recover-and-keep code this test fails: the empty
    /// half-index sits at the tip, the conflict check reads no path, and
    /// the land that must conflict lands.
    #[test]
    fn a_poisoned_slot_drops_its_index_and_the_next_land_sees_the_database() {
        let f = fixture_shared();
        // The index records a/x changing at generation 2.
        let cs_a = f.commit(f.root, &[put("a/x", "A")]);
        assert_eq!(f.land(cs_a, "ka").state, LandState::Landed);
        // The next land lands normally, but its settle's hook tears the
        // just-recorded path out of the index - the half-written state a
        // panic mid-add leaves - and panics under the slot's lock.
        f.lands.set_after_record(Box::new(|index| {
            if let Some(index) = index.as_mut() {
                index.files.remove(b"a/x".to_vec().as_slice());
            }
            panic!("the slot's index dies half-written");
        }));
        let cs_b = f.commit(cs_a, &[put("b/y", "B")]);
        let panicked =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.land(cs_b, "kb")));
        assert!(panicked.is_err(), "the armed panic unwound");
        // A land whose base predates the a/x change conflicts. The torn
        // index no longer knows a/x and would land it.
        let cs_c = f.commit(f.root, &[put("a/x", "C")]);
        let out = f.land(cs_c, "kc");
        assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
    }

    /// An error reply says the land stays pending and a retry with the
    /// same request key resumes it. The row that promise rests on must be
    /// durable before the reply goes out, so a crash that follows the
    /// reply cannot lose it. The call's guard drops before the plan
    /// starts, so the land ends in that error reply right after it records
    /// its row, before any settle work runs.
    #[test]
    fn an_error_reply_waits_for_the_pending_row_to_be_durable() {
        let f = fixture();
        // The index starts at B, so the import below moves main past it,
        // and the plan's next step checks the deadline before it derives
        // anything.
        f.move_main(f.commit(f.root, &[put("b/y", "B")]));
        f.fill("f", 1);
        f.move_main(f.commit(f.main().target, &[put("c", "import")]));
        let cs = f.commit(f.main().target, &[put("a/x", "mine")]);
        // The call went before the plan started: the guard drops, the
        // deadline itself is far in the future.
        let deadline = Deadline::new(Instant::now() + std::time::Duration::from_secs(600));
        let guard = deadline.guard();
        let gone = LandRequest {
            deadline,
            ..req(cs, "k")
        };
        drop(guard);
        let commits_before = f.store.commits();
        let err = f.lands.land(&f.store, &caller(), &gone).unwrap_err();
        // The reply says the land stays pending, and a retry with the
        // same request key resumes it.
        assert!(
            matches!(err, Error::Cancelled(ref d)
                if d.contains("the land stays pending, retry with the same request key")),
            "{err}"
        );
        // The row's commit is durable when the reply is built: a flush
        // covered every commit made before the reply, the pending row's
        // included. `flushed_upto` is the store's own durability mark, the
        // one a flush updates on every platform. The check runs before any
        // read of this test: a read of an unflushed commit flushes on its
        // own, which would hide the bug.
        assert!(
            f.store.flushed_upto() > commits_before,
            "the error reply went out before the pending row was durable: \
             flushed to {}, commits reach {}",
            f.store.flushed_upto(),
            commits_before + 1,
        );
        let state: String = f
            .store
            .read(|c| {
                c.sqlite()
                    .unwrap()
                    .query_row(
                        "SELECT state FROM lands WHERE request_key = 'k'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(crate::error::from_sqlite)
            })
            .unwrap();
        assert_eq!(state, "pending");
    }

    #[test]
    fn a_head_without_a_root_tree_is_derived_in_steps_before_the_land() {
        let f = fixture();
        // The fixture stores changesets without root trees, as after a
        // version bump of the kind.
        let head = (0..4).fold(f.root, |head, i| {
            f.commit(head, &[put("c", &i.to_string())])
        });
        f.move_main(head);
        let cs = f.commit(head, &[put("d", "new")]);
        let commits = f.store.commits();
        assert_eq!(f.land(cs, "k").state, LandState::Landed);
        // Record the land, five root trees in steps, then the land itself.
        let steps = crate::derived::steps(5);
        assert_eq!(f.store.commits() - commits, 1 + steps + 1);
    }

    #[test]
    fn rebases_disjoint_changes_from_the_same_base() {
        let f = fixture();
        let one = f.commit(f.root, &[put("a/x", "one")]);
        let two = f.commit(f.root, &[put("b/new", "two"), del("c")]);
        assert_eq!(f.land(one, "1").state, LandState::Landed);
        let out = f.land(two, "2");
        assert_eq!(out.state, LandState::Landed);
        let tip = out.result.unwrap();
        assert_ne!(tip, two, "rebased onto the new head");
        assert_eq!(f.main().target, tip);
        assert!(f.file(tip, "a/x").is_some() && f.file(tip, "b/new").is_some());
        assert!(f.file(tip, "c").is_none());
        let rebased = f.store.read(|c| c.get_changeset(f.repo, tip)).unwrap();
        assert_eq!(rebased.parents(), vec![one]);
    }

    #[test]
    fn overlapping_paths_conflict_and_leave_the_ref_alone() {
        let cases: [(Edits<'_>, Edits<'_>, &str); 4] = [
            (&[put("a/x", "1")], &[put("a/x", "2")], "a/x"),
            (&[del("c")], &[put("c/d", "2")], "c/d"),
            (&[put("b/z/w", "1")], &[put("b/z", "2")], "b/z"),
            (&[put("b/y", "1")], &[del("b/y")], "b/y"),
        ];
        for (first, second, conflict) in cases {
            let f = fixture();
            assert_eq!(
                f.land(f.commit(f.root, first), "1").state,
                LandState::Landed
            );
            let before = f.main();
            let out = f.land(f.commit(f.root, second), "2");
            assert_eq!(out.state, LandState::Conflict, "{first:?} then {second:?}");
            assert_eq!(out.conflict_paths, vec![RepoPath::new(conflict).unwrap()]);
            assert_eq!(f.main(), before);
        }
    }

    #[test]
    fn retry_with_the_same_key_returns_the_same_land() {
        let f = fixture();
        let cs = f.commit(f.root, &[put("a/x", "new")]);
        let first = f.land(cs, "k");
        let replay = f.land(cs, "k");
        assert_eq!(stored(&replay), stored(&first));
        assert!(!replay.moved, "the replay moved nothing itself");
        assert_eq!(f.main().version, 2, "the ref moved once");
        let other = f.commit(f.root, &[put("b/y", "other")]);
        assert!(matches!(
            f.try_land(other, "k"),
            Err(Error::InvalidArgument(_))
        ));
    }

    /// Where the first attempt at a land stopped.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Crash {
        /// The pending row committed, then the server died.
        AfterRecord,
        /// The land was planned, then the server died before the transaction.
        AfterPlan,
        /// An import moved the ref between the plan and the transaction.
        RefMoved,
    }

    /// A ref that moves between the land's plan and its settle makes the
    /// land plan again from the new head, and the replan counts once. The
    /// hook fires at exactly that moment, so the race does not depend on
    /// the machine.
    #[test]
    fn a_land_whose_ref_moves_between_plan_and_settle_replans() {
        let f = fixture();
        let metrics = Arc::new(crate::metrics::Metrics::default());
        let lands = Lands::new(None, Some(Arc::clone(&metrics)));
        // Main moves past the base, so the land must rebase.
        f.move_main(f.commit(f.root, &[put("b/y", "first")]));
        let cs = f.commit(f.root, &[put("a/x", "new")]);
        // The import commits its changeset but holds its ref move for the
        // moment between the land's plan and its settle.
        let import = f.commit(f.main().target, &[put("c", "import")]);
        let hook_store = f.store.clone();
        let hook_repo = f.repo;
        lands.set_before_settle(Box::new(move || {
            let head = hook_store
                .read(|c| {
                    let head = c.get_ref(hook_repo, "main")?.unwrap();
                    Ok(head)
                })
                .unwrap();
            let moved = hook_store
                .write(|c| c.cas_ref(hook_repo, "main", Some(head.version), import))
                .unwrap();
            assert!(moved, "the import's move lost a race it cannot lose");
        }));

        let out = lands.land(&f.store, &caller(), &req(cs, "hook")).unwrap();
        assert_eq!(out.state, LandState::Landed, "{}", out.detail);
        let landed = out.result.expect("landed");
        // The landed changeset sits on top of the import: the land
        // replanned onto the head the import made.
        let parents = f
            .store
            .read(|c| Ok(c.get_changeset(f.repo, landed)?.parents().to_vec()))
            .unwrap();
        assert_eq!(parents, vec![import], "the land rebased onto the import");
        assert_eq!(metrics.land_replans(), 1, "the replan counted once");
        // A retry with the same key returns the same outcome.
        let again = lands.land(&f.store, &caller(), &req(cs, "hook")).unwrap();
        assert_eq!(stored(&again), stored(&out));
    }

    /// The collector of a stale verdict: the plan reads a Conflict off
    /// the head, an import moves the ref before the settle, and the
    /// settle must not record the verdict the old head answered for. The
    /// settle re-reads the ref version inside its own transaction and
    /// replans, so the reply names every path the head the import made
    /// changed after the base - and a retry with the same key reads the
    /// same verdict back.
    #[test]
    fn a_conflict_recorded_while_an_import_moved_the_ref_is_not_final() {
        let f = fixture_shared();
        let metrics = Arc::new(crate::metrics::Metrics::default());
        let lands = Lands::new(None, Some(Arc::clone(&metrics)));
        // Main moves past the base and takes `b/y`, so the plan reads a
        // Conflict on `b/y`.
        f.move_main(f.commit(f.root, &[put("b/y", "import")]));
        let cs = f.commit(f.root, &[put("b/y", "mine"), put("c", "mine")]);
        // The import commits its changeset but holds its ref move for
        // the moment between the land's plan and its settle.
        let import = f.commit(f.main().target, &[put("c", "import too")]);
        let hook_store = f.store.clone();
        let hook_repo = f.repo;
        lands.set_before_settle(Box::new(move || {
            let head = hook_store
                .read(|c| Ok(c.get_ref(hook_repo, "main")?.unwrap()))
                .unwrap();
            let moved = hook_store
                .write(|c| c.cas_ref(hook_repo, "main", Some(head.version), import))
                .unwrap();
            assert!(moved, "the import's move lost a race it cannot lose");
        }));

        let out = lands.land(&f.store, &caller(), &req(cs, "hook")).unwrap();
        assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
        // The verdict is the plan's answer to the head the import made:
        // it names `c`, which only that head changed. The stale verdict
        // planned on the earlier head names `b/y` alone.
        assert!(
            out.conflict_paths.iter().any(|p| p.as_bytes() == b"c"),
            "the verdict names the head the import made: {:?}",
            out.conflict_paths
        );
        assert_eq!(
            metrics.land_replans(),
            1,
            "the stale verdict replanned against the head the import made"
        );
        // A retry with the same key returns the same verdict.
        let again = lands.land(&f.store, &caller(), &req(cs, "hook")).unwrap();
        assert_eq!(stored(&again), stored(&out));
    }

    #[test]
    fn a_crash_at_any_step_lands_exactly_once_on_retry() {
        for crash in [Crash::AfterRecord, Crash::AfterPlan, Crash::RefMoved] {
            let f = fixture();
            // Main moves past the base, so the land must rebase.
            f.move_main(f.commit(f.root, &[put("b/y", "first")]));
            let cs = f.commit(f.root, &[put("a/x", "new")]);
            let req = req(cs, "k");

            // The first attempt runs the steps of `Lands::land` by hand.
            let (repo, recorded) = f
                .store
                .write(|c| c.record_land(&caller(), &req, None))
                .unwrap();
            assert_eq!(recorded.state, LandState::Pending);
            if crash != Crash::AfterRecord {
                let mut index = None;
                let plan = f.store.read(|c| plan(&f.lands, c, repo, &req, &mut index));
                let plan = plan.unwrap();
                let Plan::Land { rebased, .. } = &plan else {
                    panic!("{crash:?}: the land should plan to land");
                };
                let rebased = rebased.id();
                if crash == Crash::RefMoved {
                    f.move_main(f.commit(f.main().target, &[put("c", "import")]));
                    let land_id = recorded.land_id;
                    let err = settle(&f.lands, &f.store, repo, land_id, "main", plan, &mut index)
                        .unwrap_err();
                    assert!(matches!(err, Error::Aborted(_)), "{crash:?}: {err}");
                    let kept = f.store.read(|c| c.has_changeset(repo, rebased));
                    assert!(!kept.unwrap(), "the transaction rolled back");
                }
            }
            let row = get_land(&f.store, &caller(), "r", recorded.land_id).unwrap();
            assert_eq!(row.state, LandState::Pending, "{crash:?}");

            // A new process retries with the same key.
            let before = f.main();
            let fresh = Lands::default();
            let out = fresh.land(&f.store, &caller(), &req).unwrap();
            assert_eq!(out.land_id, recorded.land_id, "{crash:?}: a stable land id");
            assert_eq!(out.state, LandState::Landed, "{crash:?}: {}", out.detail);
            let after = f.main();
            assert_eq!(after.version, before.version + 1, "{crash:?}: moved once");
            assert_eq!(Some(after.target), out.result);
            let landed = f.store.read(|c| c.get_changeset(repo, after.target));
            assert_eq!(landed.unwrap().parents(), [before.target], "{crash:?}");
            let replay = fresh.land(&f.store, &caller(), &req).unwrap();
            assert_eq!(stored(&replay), stored(&out));
            assert!(!replay.moved, "{crash:?}: the replay moved nothing");
            assert_eq!(f.landed_count(), 1, "{crash:?}");
        }
    }

    #[test]
    fn get_land_shows_only_the_callers_own_lands() {
        let f = fixture();
        let out = f.land(f.commit(f.root, &[put("a/x", "new")]), "k");
        assert_eq!(
            stored(&get_land(&f.store, &caller(), "r", out.land_id).unwrap()),
            stored(&out)
        );
        let other = Caller("someone else".into());
        assert!(matches!(
            get_land(&f.store, &other, "r", out.land_id),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn rejects_merges_foreign_bases_and_missing_refs() {
        let f = fixture();
        let side = f.commit(f.root, &[put("s", "1")]);
        let merge = f.insert(&changeset(vec![f.root, side], &[]));
        assert_eq!(f.land(merge, "m").state, LandState::Rejected);

        // `side` itself lands; a child of an older sibling of it does not have a
        // base on main.
        let sibling = f.commit(f.root, &[put("t", "1")]);
        let on_sibling = f.commit(sibling, &[put("u", "1")]);
        assert_eq!(f.land(side, "s").state, LandState::Landed);
        let out = f.land(on_sibling, "u");
        assert_eq!(out.state, LandState::Rejected, "{}", out.detail);

        // A missing ref is rejected without taking a lock slot, so unique bad
        // names cannot grow `Lands`.
        let missing = LandRequest {
            target_ref: "nope".into(),
            ..req(side, "n")
        };
        let out = f.lands.land(&f.store, &caller(), &missing).unwrap();
        assert_eq!(out.state, LandState::Rejected);
        assert_eq!(f.lands.land(&f.store, &caller(), &missing).unwrap(), out);
        let slots = f.lands.refs.lock().unwrap();
        let keys: Vec<_> = slots.iter().map(|(key, _)| key).collect();
        assert_eq!(keys, [&(f.repo, "main".into())]);
    }

    #[test]
    fn index_follows_a_ref_moved_outside_lands() {
        let f = fixture();
        assert_eq!(
            f.land(f.commit(f.root, &[put("a/x", "1")]), "1").state,
            LandState::Landed
        );
        // Something else (an import) moves main forward and touches c.
        f.move_main(f.commit(f.main().target, &[put("c", "external")]));
        let out = f.land(f.commit(f.root, &[put("c", "mine")]), "2");
        assert_eq!(out.state, LandState::Conflict);
        let out = f.land(f.commit(f.root, &[put("b/y", "mine")]), "3");
        assert_eq!(out.state, LandState::Landed);
    }

    fn cycle() -> usize {
        usize::try_from(PRUNE_EVERY).unwrap()
    }

    /// `main` goes root ─ A ─ F1 ─ … ─ Fn, each landed on the head. A puts
    /// `a/x`, and each Fi puts `fi`. Returns the fixture and the history,
    /// root first.
    fn history(n: usize) -> (Fixture, Vec<ChangesetId>) {
        let f = fixture();
        let a = f.commit(f.root, &[put("a/x", "A")]);
        assert_eq!(f.land(a, "a").state, LandState::Landed);
        let mut history = vec![f.root, a];
        history.extend(f.fill("f", n));
        (f, history)
    }

    /// A [`history`] of three prune cycles of F's. By the end, the index has
    /// pruned A and the first F's.
    fn pruned_history() -> (Fixture, Vec<ChangesetId>) {
        let (f, history) = history(3 * cycle());
        let a = history[1];
        let floor = f.index(|index| index.floor);
        assert!(
            f.generation(floor) > f.generation(a),
            "A is below the floor"
        );
        assert!(f.index(|index| !index.files.contains_key(b"a/x".as_slice())));
        (f, history)
    }

    #[test]
    fn a_base_below_the_pruned_floor_still_sees_every_change() {
        use LandState::{Conflict, Landed};
        // (base, the path the land puts, the outcome); bases index the history.
        let cases = [
            (0, "a/x", Conflict), // A changed it
            (0, "a", Conflict),   // A changed a path below it
            (0, "b/y", Landed),   // nothing changed it after root
            (1, "a/x", Landed),   // A's own change is not after A
            (2, "f2", Conflict),  // F2 is below the floor too
            (2, "f1", Landed),
        ];
        for (base, path, state) in cases {
            let (f, history) = pruned_history();
            let cs = f.commit(history[base], &[put(path, "mine")]);
            let out = f.land(cs, "k");
            assert_eq!(out.state, state, "base {base}, {path}: {}", out.detail);
            if state == Conflict {
                assert_eq!(out.conflict_paths, [RepoPath::new(path).unwrap()]);
            }
        }
    }

    #[test]
    fn a_base_beyond_the_depth_is_checked_without_moving_the_floor() {
        use LandState::{Conflict, Landed, Rejected};
        let depth = usize::try_from(MAX_DEPTH).unwrap();
        let (f, history) = history(depth + cycle());
        let root = history[0];
        assert!(f.generation(f.main().target) - f.generation(root) > MAX_DEPTH);
        let last = format!("f{}", history.len() - 2);
        let path = |p: &str| RepoPath::new(p).unwrap();
        // A's a/x and F2's f2 are below the floor, so only the path history
        // sees them. The last F's change is still in the index.
        assert!(f.index(|index| {
            !index.files.contains_key(b"a/x".as_slice())
                && !index.files.contains_key(b"f2".as_slice())
                && index.files.contains_key(last.as_bytes())
        }));

        // (base, the path the land puts, the outcome); one rule each.
        let side = f.commit(root, &[put("s", "side")]);
        let cases = [
            (root, "a/x", Conflict),         // A changed it
            (root, "a", Conflict),           // A changed a path below it
            (root, "f2/z", Conflict),        // F2 changed a file at a parent path
            (root, last.as_str(), Conflict), // the index holds this change
            (root, "a/y", Landed),           // A changed only a sibling
            (side, "t", Rejected),           // not on the history
        ];
        let floor = f.index(|index| index.floor);
        for (base, put_path, state) in cases {
            let cs = f.commit(base, &[put(put_path, "mine")]);
            let out = f.land(cs, &format!("old {put_path}"));
            assert_eq!(out.state, state, "{put_path}: {}", out.detail);
            if state == Conflict {
                assert_eq!(out.conflict_paths, [path(put_path)]);
            }
            assert_eq!(f.index(|index| index.floor), floor, "{put_path}");
        }

        // A client lands on root in every cycle. The old base does not count
        // as used, so each cycle still moves the floor up.
        let mine = [
            put("a/x", "mine"),
            put("b/y", "mine"),
            put("f2/z", "mine"),
            put(&last, "mine"),
        ];
        let mut want = vec![path("a/x"), path("f2/z"), path(&last)];
        want.sort();
        for round in 0..3 {
            let floor = f.index(|index| index.floor);
            let out = f.land(f.commit(root, &mine), &format!("old{round}"));
            assert_eq!(out.conflict_paths, want, "round {round}");
            assert_eq!(f.index(|index| index.floor), floor, "round {round}");
            f.fill(&format!("g{round}/"), cycle());
            let moved = f.index(|index| index.floor);
            assert!(f.generation(moved) > f.generation(floor), "round {round}");
        }
    }

    #[test]
    fn an_index_starts_over_when_the_ref_moved_far() {
        use LandState::{Conflict, Landed};
        let f = fixture();
        f.fill("f", 1);
        let before = f.main().target;
        // An import moves main by more than MAX_DEPTH small changesets.
        let mut head = before;
        for n in 0..=MAX_DEPTH {
            head = f.commit(head, &[put(&format!("i{n}"), "x")]);
        }
        f.move_main(head);

        // The plan does not walk back to the old tip: the index starts over
        // at the head.
        let out = f.land(f.commit(head, &[put("x", "x")]), "on head");
        assert_eq!(out.state, Landed, "{}", out.detail);
        assert_eq!(f.index(|index| index.floor), head);
        // A base before the import still sees the imported changes.
        let out = f.land(f.commit(before, &[put("i3", "mine")]), "old");
        assert_eq!(out.state, Conflict, "{}", out.detail);
        assert_eq!(out.conflict_paths, [RepoPath::new("i3").unwrap()]);
    }

    #[test]
    fn a_land_past_its_deadline_stays_pending_and_a_retry_finishes_it() {
        use LandState::{Conflict, Landed};
        let f = fixture();
        // The index starts at B, so root is below its floor.
        f.move_main(f.commit(f.root, &[put("b/y", "B")]));
        f.fill("f", 1);
        // An import moves main, so the next plan advances the index.
        f.move_main(f.commit(f.main().target, &[put("c", "import")]));
        let on_head = f.commit(f.main().target, &[put("a/x", "mine")]);
        // The plan of this one walks back from the floor.
        let on_root = f.commit(f.root, &[put("c", "mine")]);
        for (cs, key, state) in [(on_head, "head", Landed), (on_root, "root", Conflict)] {
            let before = f.main();
            let late = LandRequest {
                deadline: Deadline::new(Instant::now()),
                ..req(cs, key)
            };
            let err = f.lands.land(&f.store, &caller(), &late).unwrap_err();
            assert!(matches!(err, Error::DeadlineExceeded(_)), "{key}: {err}");
            assert_eq!(f.main(), before, "{key}");
            let sql = "SELECT state FROM lands WHERE request_key = ?1";
            let row: String = f
                .store
                .read(|c| {
                    c.sqlite()
                        .unwrap()
                        .query_row(sql, [key], |row| row.get(0))
                        .map_err(crate::error::from_sqlite)
                })
                .unwrap();
            assert_eq!(row, "pending", "{key}");
            // The index stays, so a retry goes on from where the plan stopped.
            f.index(|_| ());
            let out = f.land(cs, key);
            assert_eq!(out.state, state, "{key}: {}", out.detail);
        }
    }

    /// A land whose call has gone: the deadline has not passed, but the
    /// call's guard dropped, so the plan's next check answers
    /// [`Error::Cancelled`]. The land stays pending, keeps its path index,
    /// and a retry with the same key finishes it: the same promise a
    /// deadline-stopped land keeps.
    #[test]
    fn a_land_whose_call_went_stays_pending_and_a_retry_finishes_it() {
        use LandState::{Conflict, Landed};
        let f = fixture();
        // The index starts at B, so root is below its floor.
        f.move_main(f.commit(f.root, &[put("b/y", "B")]));
        f.fill("f", 1);
        // An import moves main, so the next plan advances the index.
        f.move_main(f.commit(f.main().target, &[put("c", "import")]));
        let on_head = f.commit(f.main().target, &[put("a/x", "mine")]);
        // The plan of this one walks back from the floor.
        let on_root = f.commit(f.root, &[put("c", "mine")]);
        for (cs, key, state) in [
            (on_head, "gone head", Landed),
            (on_root, "gone root", Conflict),
        ] {
            let before = f.main();
            // The call went before the plan started: the guard drops, the
            // deadline itself is far in the future.
            let deadline = Deadline::new(Instant::now() + std::time::Duration::from_secs(600));
            let guard = deadline.guard();
            let gone = LandRequest {
                deadline,
                ..req(cs, key)
            };
            drop(guard);
            let err = f.lands.land(&f.store, &caller(), &gone).unwrap_err();
            assert!(
                matches!(&err, Error::Cancelled(why) if why.contains("its call has gone")),
                "{key}: {err}"
            );
            assert_eq!(f.main(), before, "{key}");
            let sql = "SELECT state FROM lands WHERE request_key = ?1";
            let row: String = f
                .store
                .read(|c| {
                    c.sqlite()
                        .unwrap()
                        .query_row(sql, [key], |row| row.get(0))
                        .map_err(crate::error::from_sqlite)
                })
                .unwrap();
            assert_eq!(row, "pending", "{key}");
            // The index stays, so a retry goes on from where the plan stopped.
            f.index(|_| ());
            let out = f.land(cs, key);
            assert_eq!(out.state, state, "{key}: {}", out.detail);
        }
    }

    /// An import moves `main` to root ─ C1 ─ … ─ Cn, where Ci puts `ci`, and
    /// C1 also puts `d/e/f`. None of them has derived data. Then a land on
    /// the head builds the path index there. Returns the fixture and the
    /// history, root first.
    fn imported(n: usize) -> (Fixture, Vec<ChangesetId>) {
        let f = fixture();
        let mut history = vec![f.root];
        f.store
            .write(|c| {
                for i in 1..=n {
                    let mut changes = vec![put(&format!("c{i}"), "x")];
                    if i == 1 {
                        changes.push(put("d/e/f", "x"));
                    }
                    let cs = changeset(vec![history[i - 1]], &changes);
                    history.push(c.insert_changeset(f.repo, &cs)?);
                }
                Ok(())
            })
            .unwrap();
        f.move_main(history[n]);
        let out = f.land(f.commit(history[n], &[put("warm", "x")]), "warm");
        assert_eq!(out.state, LandState::Landed);
        assert_eq!(f.index(|index| index.floor), history[n]);
        (f, history)
    }

    fn depth() -> usize {
        usize::try_from(MAX_DEPTH).unwrap()
    }

    #[test]
    fn a_deep_base_is_checked_in_the_path_history_of_the_head() {
        use LandState::{Conflict, Landed, Rejected};
        let (f, history) = imported(2 * depth());
        let head = f.main().target;
        let stored = |cs| {
            let value = f.store.read(|c| derived::load::<PathHistory>(c, cs));
            value.unwrap().is_some()
        };
        assert!(!stored(head), "nothing is derived yet");

        // A land past its deadline derives nothing, and stays pending.
        let late = LandRequest {
            deadline: Deadline::new(Instant::now()),
            ..req(f.commit(f.root, &[put("c1", "late")]), "late")
        };
        let err = f.lands.land(&f.store, &caller(), &late).unwrap_err();
        assert!(matches!(err, Error::DeadlineExceeded(_)), "{err}");
        assert!(!stored(head));

        // (base, the path the land puts, the outcome); one rule each. The
        // first land derives the history of the head.
        let side = f.commit(f.root, &[put("s", "side")]);
        let cases = [
            (f.root, "c1", Conflict),     // C1 changed it
            (f.root, "d/e", Conflict),    // C1 changed a path below it
            (f.root, "c2/z", Conflict),   // C2 changed a file at a parent path
            (history[1], "c2", Conflict), // C2 is after C1
            (side, "t", Rejected),        // not on the history
            (history[1], "c1", Landed),   // C1's own change is not after C1
            (f.root, "d/e/g", Landed),    // C1 changed only a sibling
            (f.root, "b/y", Landed),      // as far back as the history goes
        ];
        let floor = f.index(|index| index.floor);
        for (n, (base, put_path, state)) in cases.into_iter().enumerate() {
            let cs = f.commit(base, &[put(put_path, "mine")]);
            let out = f.land(cs, &format!("old {n}"));
            assert_eq!(out.state, state, "{put_path}: {}", out.detail);
            if state == Conflict {
                assert_eq!(out.conflict_paths, [RepoPath::new(put_path).unwrap()]);
            }
            assert_eq!(f.index(|index| index.floor), floor, "{put_path}");
            // Each landed changeset gets its path history when written.
            assert!(stored(f.main().target), "{put_path}");
        }
        assert!(stored(head));
    }

    #[test]
    fn a_deep_land_reads_its_paths_not_the_history() {
        // The same land on two histories, one ten times longer, reads the
        // same rows: the check reads the paths of the land, not the
        // history. The chains put the same few paths, so their levels stay
        // small and the same in both runs.
        let reads = [2, 20].map(|cycles| {
            let f = fixture();
            let mut history = vec![f.root];
            f.store
                .write(|c| {
                    // `c1` and `d/e/f` before the base, `c2` after it,
                    // `warm` after that, many times.
                    for i in 1..=cycles * depth() {
                        let changes = match i {
                            1 => vec![put("c1", "x"), put("d/e/f", "x")],
                            2 => vec![put("c2", "x")],
                            _ => vec![put("warm", "x")],
                        };
                        let cs = changeset(vec![history[i - 1]], &changes);
                        history.push(c.insert_changeset(f.repo, &cs)?);
                    }
                    Ok(())
                })
                .unwrap();
            f.move_main(history[cycles * depth()]);
            let out = f.land(
                f.commit(history[cycles * depth()], &[put("warm", "x")]),
                "warm",
            );
            assert_eq!(out.state, LandState::Landed);
            let out = f.land(f.commit(f.root, &[put("c1", "mine")]), "derives");
            assert_eq!(out.state, LandState::Conflict);
            let changes = [put("c2", "mine"), put("d/e/g", "mine"), put("c1/z", "mine")];
            let cs = f.commit(history[1], &changes);
            db::CHANGESET_READS.set(0);
            history::NODE_READS.set(0);
            let out = f.land(cs, "measured");
            let reads = (db::CHANGESET_READS.get(), history::NODE_READS.get());
            assert_eq!(out.state, LandState::Conflict);
            assert_eq!(out.conflict_paths, [RepoPath::new("c2").unwrap()]);
            reads
        });
        assert_eq!(reads[0], reads[1]);
        // The land's own changeset. The root node, which all three paths
        // share; `d` and `d/e` for `d/e/g`; `c1` and `c2` are files, so
        // nothing below them.
        assert_eq!(reads[0], (1, 3));
    }

    /// The rule of the walk that checked old bases before path history
    /// (`touched_below`): the paths of `land` that `changesets` touched.
    fn touched_by<'c>(
        changesets: impl IntoIterator<Item = &'c Changeset>,
        land: &BTreeMap<RepoPath, Option<FileState>>,
    ) -> Vec<RepoPath> {
        let paths: HashSet<&[u8]> = land.keys().map(RepoPath::as_bytes).collect();
        let parents: HashSet<&[u8]> = paths.iter().flat_map(|p| ancestors(p)).collect();
        let mut files: HashSet<&[u8]> = HashSet::new();
        let mut dirs: HashSet<&[u8]> = HashSet::new();
        for cs in changesets {
            for path in cs.changes().keys() {
                let path = path.as_bytes();
                if let Some(&key) = paths.get(path).or_else(|| parents.get(path)) {
                    files.insert(key);
                }
                dirs.extend(ancestors(path).filter_map(|dir| paths.get(dir)));
            }
        }
        let touched = land.keys().filter(|path| {
            let path = path.as_bytes();
            files.contains(path)
                || dirs.contains(path)
                || ancestors(path).any(|d| files.contains(d))
        });
        touched.cloned().collect()
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]

        /// On any history, with merges, deletes, adds again, and paths that
        /// are files at one time and directories at another: for each tip,
        /// each base and each land, the check in the path history gives the
        /// answer of an index over the changesets after the base, and of the
        /// old walk back to the base. So does `PathIndex::check`, which
        /// reaches a base within `MAX_DEPTH` and checks a deeper one in the
        /// path history.
        #[test]
        fn a_check_in_the_path_history_matches_the_index_and_a_walk(
            (parents, edits) in testing::histories(40),
            room in proptest::option::of(0..4000_usize),
        ) {
            let dir = tempfile::tempdir().unwrap();
            let store = Backend::sqlite(Store::open(dir.path()).unwrap());
            let (repo, ids) = store
                .write(|c| {
                    let repo = c.create_repo("r")?;
                    Ok((repo, testing::insert_history(c.sqlite().unwrap(), repo, &parents, &edits)?))
                })
                .unwrap();
            let lands = Lands::default();
            // With little room, the budget stops `reach`, and the path history
            // checks the rest.
            let tight = Lands::default();
            if let Some(room) = room {
                tight.used.store(MAX_INDEX_BYTES - room, Ordering::Release);
            }
            let deadline = Deadline::new(Instant::now() + std::time::Duration::from_secs(600));
            // One land changes every path. Each path is checked on its own.
            let land: BTreeMap<RepoPath, Option<FileState>> = (testing::PATHS.into_iter())
                .chain(["c", "c/a", "a/b/c/d", "z"])
                .map(|p| (RepoPath::new(p).unwrap(), None))
                .collect();
            // In one transaction: a commit per step would take most of the time.
            store
                .write(|c| {
                    for &tip in &ids {
                        derived::derive::<CommitGraph>(c, repo, tip, &mut crate::tree::lens_none())?;
                        derived::derive::<PathHistory>(c, repo, tip, &mut crate::tree::lens_none())?;
                    }
                    Ok(())
                })
                .unwrap();
            for &tip in &ids {
                store.read(|c| {
                    let tip_gen = c.generation(repo, tip)?;
                    // The first-parent history of the tip, newest first.
                    let mut chain: Vec<(ChangesetId, i64, Changeset)> = Vec::new();
                    let mut next = Some(tip);
                    while let Some(id) = next {
                        let cs = c.get_changeset(repo, id)?;
                        next = cs.parents().first().copied();
                        chain.push((id, c.generation(repo, id)?, cs));
                    }
                    // One index for all bases, as a ref's index serves its lands.
                    let mut index = PathIndex::new(c, repo, tip, &tight.used)?;
                    // Bases on the history, newest first. The model holds the
                    // changesets after each.
                    let mut model = PathIndex::at(tip, tip_gen, &lands.used);
                    for (n, &(base, base_gen, _)) in chain.iter().enumerate() {
                        if let Some((id, generation, cs)) = n.checked_sub(1).map(|m| &chain[m]) {
                            let cost = model.cost([(*generation, cs.changes())]);
                            let add = |m: &mut PathIndex| m.add(*id, *generation, cs.changes());
                            assert!(model.grow(&lands, cost, add));
                        }
                        let walked = touched_by(chain[..n].iter().map(|(.., cs)| cs), &land);
                        let indexed: Vec<RepoPath> = (land.keys())
                            .filter(|path| model.touches(path.as_bytes(), base_gen))
                            .cloned()
                            .collect();
                        assert_eq!(indexed, walked);
                        let in_history = check_history(c, tip, base, base_gen, &land, &deadline)?;
                        assert_eq!(in_history, Checked::Paths(walked.clone()));
                        let checked = index.check(&tight, c, repo, base, &land, &deadline)?;
                        assert_eq!(checked, Checked::Paths(walked));
                    }
                    let off = ids.iter().filter(|&&id| chain.iter().all(|(on, ..)| *on != id));
                    for &base in off {
                        let base_gen = c.generation(repo, base)?;
                        let in_history = check_history(c, tip, base, base_gen, &land, &deadline)?;
                        assert_eq!(in_history, Checked::NotOnHistory);
                        let checked = index.check(&tight, c, repo, base, &land, &deadline)?;
                        assert_eq!(checked, Checked::NotOnHistory);
                    }
                    Ok(())
                })
                .unwrap();
            }
        }
    }

    #[test]
    fn the_path_index_stays_bounded_under_many_lands() {
        let f = fixture();
        let bound = 2 * cycle();
        for n in 0..10 * cycle() {
            f.fill(&format!("d{n}/f"), 1);
            within_budget(&f);
            let sizes = f.index(|index| (index.chain.len(), index.files.len(), index.dirs.len()));
            assert!(
                sizes.0 <= bound && sizes.1 <= bound && sizes.2 <= bound,
                "after {} lands: (chain, files, dirs) = {sizes:?}",
                n + 1
            );
        }
    }

    /// `n` puts below `dir`: `dir/f0` to `dir/f{n-1}`.
    fn wide(dir: &str, n: usize) -> Vec<Change> {
        (0..n).map(|i| put(&format!("{dir}/f{i}"), "x")).collect()
    }

    /// Checks that every path index counts its bytes right, and that all of
    /// them stay within the shared budget. Returns the floor of `main`.
    fn within_budget(f: &Fixture) -> ChangesetId {
        f.held();
        f.index(|index| index.floor)
    }

    #[test]
    fn the_path_indexes_of_all_refs_share_one_budget() {
        use LandState::{Conflict, Landed};
        // The index of a ref after one land of this many new paths takes
        // about a ninth of the budget, so twelve refs spend it.
        const WIDE: usize = 100;
        let f = fixture();
        let changes = wide("r0", WIDE).into_iter().collect();
        let one = PathIndex::at(f.root, 0, &Arc::default()).cost([(1, &changes)]);
        assert!(8 * one < MAX_INDEX_BYTES && 10 * one > MAX_INDEX_BYTES);
        let names: Vec<String> = (0..12).map(|i| format!("r{i}")).collect();
        for name in &names {
            let moved = f.store.write(|c| c.cas_ref(f.repo, name, None, f.root));
            assert!(moved.unwrap());
        }
        let land_wide = |name: &str| {
            let cs = f.commit(f.root, &wide(name, WIDE));
            assert_eq!(f.land_on(name, cs, name).state, Landed);
        };

        // One land on each ref, in order, until the budget is spent. A second
        // land on r0 makes r1 the least recently used, so its index goes
        // first.
        let mut rest = names.iter();
        land_wide(rest.next().unwrap());
        land_wide(rest.next().unwrap());
        let again = f.commit(f.root, &[put("r0/again", "x")]);
        assert_eq!(f.land_on("r0", again, "again").state, Landed);
        let (last, held) = loop {
            let name = rest.next().expect("the lands spend the budget");
            land_wide(name);
            let held = f.held();
            if held.values().any(|&h| !h) {
                break (name, held);
            }
        };
        let dropped: Vec<&String> = held.iter().filter(|&(_, &h)| !h).map(|(n, _)| n).collect();
        assert_eq!(dropped, ["r1"]);

        // A land that rewrites every path of the last ref's index adds only
        // a chain entry. It takes only that, so it drops nothing.
        let tip = f.store.read(|c| c.get_ref(f.repo, last)).unwrap();
        let rewrite: Vec<Change> = (0..WIDE)
            .map(|i| put(&format!("{last}/f{i}"), "y"))
            .collect();
        let out = f.land_on(last, f.commit(tip.unwrap().target, &rewrite), "rewrite");
        assert_eq!(out.state, Landed);
        assert_eq!(f.held(), held);

        // An index whose ref's lock is held is in use, so the next one goes.
        let busy = f.slot("r0");
        let lock = busy.lock().unwrap();
        assert!(rest.len() >= 2);
        rest.for_each(|name| land_wide(name));
        drop(lock);
        let held = f.held();
        let kept = |name: &str| held.get(name) == Some(&true);
        assert!(kept("r0") && !kept("r2"), "{held:?}");
        let newest = &names[names.len() - 3..];
        assert!(newest.iter().all(|name| kept(name)), "{held:?}");
        // The slot of r1 held no index and no land used it, so an eviction
        // removed it.
        assert!(!held.contains_key("r1"), "{held:?}");

        // A land on each ref, on a base before its wide land, sees that
        // land's changes, whether its index was dropped or not.
        for name in &names {
            let mine = format!("{name}/f7");
            let cs = f.commit(f.root, &[put(&mine, "mine")]);
            let out = f.land_on(name, cs, &format!("{name} conflict"));
            assert_eq!(out.state, Conflict, "{name}: {}", out.detail);
            assert_eq!(out.conflict_paths, [RepoPath::new(mine.as_str()).unwrap()]);
            let cs = f.commit(f.root, &[put(&format!("{name}/g"), "mine")]);
            let out = f.land_on(name, cs, &format!("{name} land"));
            assert_eq!(out.state, Landed, "{name}: {}", out.detail);
            f.held();
        }
    }

    #[test]
    fn an_advance_starts_over_when_a_busy_ref_holds_the_budget() {
        let f = fixture();
        assert!(
            f.store
                .write(|c| c.cas_ref(f.repo, "big", None, f.root))
                .unwrap()
        );
        // main's index has its tip at L.
        let l = f.commit(f.root, &[put("m/seed", "x")]);
        let l = f.land(l, "seed").result.unwrap();
        // big holds most of the budget.
        let flat: Vec<Change> = (0..600).map(|j| put(&format!("big{j:05}"), "x")).collect();
        let cs = f.commit(f.root, &flat);
        assert_eq!(f.land_on("big", cs, "big").state, LandState::Landed);
        assert!(f.lands.used.load(Ordering::Acquire) > MAX_INDEX_BYTES / 2);
        // An import moves main past L with a changeset that does not fit in
        // what is free.
        let free = MAX_INDEX_BYTES - f.lands.used.load(Ordering::Acquire);
        let generation = f.generation(l) + 1;
        let mut n = 10;
        while f.index(|index| index.cost([(generation, &wide("imp", n).into_iter().collect())]))
            <= free
        {
            n += 10;
        }
        f.move_main(f.commit(l, &wide("imp", n)));
        // big is in use, so nothing can be dropped, and the index starts over
        // at the head. A land on L still sees the import's change.
        let busy = f.slot("big");
        let _lock = busy.lock().unwrap();
        let out = f.land(f.commit(l, &[put("imp/f7", "mine")]), "mine");
        assert_eq!(out.state, LandState::Conflict, "{}", out.detail);
        assert_eq!(out.conflict_paths, [RepoPath::new("imp/f7").unwrap()]);
    }

    #[test]
    fn concurrent_lands_on_many_refs_keep_the_byte_count_exact() {
        let f = Arc::new(fixture());
        let refs: Vec<String> = (0..8).map(|t| format!("t{t}")).collect();
        for name in &refs {
            assert!(
                f.store
                    .write(|c| c.cas_ref(f.repo, name, None, f.root))
                    .unwrap()
            );
        }
        let threads: Vec<_> = refs
            .iter()
            .cloned()
            .map(|name| {
                let f = Arc::clone(&f);
                std::thread::spawn(move || {
                    for i in 0..30 {
                        let head = f.store.read(|c| c.get_ref(f.repo, &name));
                        let head = head.unwrap().unwrap().target;
                        let dir = format!("{name}/w{i}");
                        let cs = f.commit(head, &wide(&dir, 15 + (i % 4) * 10));
                        let out = f.land_on(&name, cs, &format!("{dir}a"));
                        assert_eq!(out.state, LandState::Landed);
                        // A land on the old head of a path that the wide land put.
                        let cs = f.commit(head, &[put(&format!("{dir}/f3"), "mine")]);
                        let out = f.land_on(&name, cs, &format!("{dir}b"));
                        assert_eq!(out.state, LandState::Conflict, "{name} {i}: {}", out.detail);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        f.held();
    }

    #[test]
    fn the_path_index_stays_within_its_byte_budget() {
        use LandState::{Conflict, Landed};
        // Three lands of this many new paths fill the budget, before a prune
        // cycle ends.
        const WIDE: usize = 400;
        let one = added_bytes(&wide("d0", WIDE).into_iter().collect());
        assert!(3 * one > MAX_INDEX_BYTES && PRUNE_EVERY > 3);
        let f = fixture();
        // The index of another ref. A restart of main's index does not need
        // its room, so it stays.
        assert!(
            f.store
                .write(|c| c.cas_ref(f.repo, "other", None, f.root))
                .unwrap()
        );
        f.land_on("other", f.commit(f.root, &[put("o", "x")]), "other");
        let mut history = vec![f.root];
        let mut restarts = 0;
        for n in 0..8 {
            let cs = f.commit(f.main().target, &wide(&format!("d{n}"), WIDE));
            assert_eq!(f.land(cs, &format!("d{n}")).state, Landed);
            history.push(cs);
            // A prune moves the floor to an older base, never to the tip.
            restarts += usize::from(within_budget(&f) == cs);
        }
        assert!(restarts > 0, "a land moved the floor up to the tip");
        assert!(f.held()["other"]);

        // Bases below the floor. Each check reaches back while the index has
        // room, and checks the rest in the path history.
        let base = history[1];
        assert!(f.generation(base) < f.generation(within_budget(&f)));
        let cases = [
            ("d5/f3", Conflict), // the land of d5 put it
            ("d6", Conflict),    // the land of d6 put paths below it
            ("d0/f3", Landed),   // only the base put it
        ];
        for (path, state) in cases {
            let out = f.land(f.commit(base, &[put(path, "mine")]), &format!("old {path}"));
            assert_eq!(out.state, state, "{path}: {}", out.detail);
            if state == Conflict {
                assert_eq!(out.conflict_paths, [RepoPath::new(path).unwrap()]);
            }
            within_budget(&f);
        }

        // An import moves main by more than the budget holds. The index
        // starts over at the new head and still sees the imported changes.
        let before = f.main().target;
        for n in 0..3 {
            f.move_main(f.commit(f.main().target, &wide(&format!("e{n}"), WIDE)));
        }
        let out = f.land(f.commit(before, &[put("e1/f7", "mine")]), "after import");
        assert_eq!(out.state, Conflict, "{}", out.detail);
        assert_eq!(out.conflict_paths, [RepoPath::new("e1/f7").unwrap()]);
        within_budget(&f);
    }

    #[test]
    fn pruning_keeps_the_oldest_base_that_a_cycle_used() {
        let (f, history) = pruned_history();
        let root = history[0];
        let out = f.land(f.commit(root, &[put("a/x", "mine")]), "old");
        assert_eq!(out.state, LandState::Conflict);
        assert_eq!(f.index(|index| index.floor), root);
        // The cycle that used root keeps it, so the next land on root does
        // not walk the history again.
        f.fill("g", cycle());
        assert_eq!(f.index(|index| index.floor), root, "the cycle used root");
        // The next cycle does not use it.
        f.fill("h", cycle());
        assert_ne!(f.index(|index| index.floor), root, "root is no longer used");
    }

    /// Length of the side chain in the merge tests.
    const SIDE: usize = 100;

    /// A history whose head is a merge, from [`merged`].
    struct Merged {
        f: Fixture,
        b: ChangesetId,
        side: Vec<ChangesetId>,
        head: ChangesetId,
    }

    /// The base of a land in the merge tests.
    #[derive(Clone, Copy, Debug)]
    enum Base {
        Root,
        B,
        /// The n-th changeset of the side chain, from 1.
        Side(usize),
        Head,
    }

    impl Merged {
        fn id(&self, base: Base) -> ChangesetId {
            match base {
                Base::Root => self.f.root,
                Base::B => self.b,
                Base::Side(n) => self.side[n - 1],
                Base::Head => self.head,
            }
        }
    }

    /// `main` goes root → B, where B puts `a/x`. With `warm`, a land then
    /// builds main's path index at B. Then an import moves `main` to a merge
    /// M of B and a side chain S1..S100. S1 puts `c`; each later one rewrites
    /// `side/f`.
    ///
    /// ```text
    /// b_first:   root ─ B ───────────────── M   M = [B, S100]
    ///                   └─ S1 ─ … ─ S100 ──┘    changes: side/f (keeps B's c)
    ///
    /// !b_first:  root ─ S1 ─ … ─ S100 ─ M      M = [S100, B]
    ///               └─ B ──────────────┘       changes: a/x
    /// ```
    fn merged(b_first: bool, warm: bool) -> Merged {
        let f = fixture();
        let b = f.commit(f.root, &[put("a/x", "B")]);
        f.move_main(b);
        if warm {
            let out = f.land(f.commit(f.root, &[put("a/x", "warm")]), "warm");
            assert_eq!(out.state, LandState::Conflict);
        }
        let fork = if b_first { b } else { f.root };
        let side = f
            .store
            .write(|c| {
                let mut chain = Vec::with_capacity(SIDE);
                let mut parent = fork;
                for n in 1..=SIDE {
                    let change = if n == 1 {
                        put("c", "side")
                    } else {
                        put("side/f", &n.to_string())
                    };
                    parent = c.insert_changeset(f.repo, &changeset(vec![parent], &[change]))?;
                    chain.push(parent);
                }
                Ok(chain)
            })
            .unwrap();
        let tip = side[SIDE - 1];
        let merge = if b_first {
            changeset(vec![b, tip], &[put("side/f", &SIDE.to_string())])
        } else {
            changeset(vec![tip, b], &[put("a/x", "B")])
        };
        let head = f.insert(&merge);
        f.move_main(head);
        Merged { f, b, side, head }
    }

    /// Lands a put of each case's path on its base, each on a fresh history,
    /// with the path index both warmed before the merge and built from
    /// scratch after it. A conflict names exactly the put path.
    fn check_merge_cases(b_first: bool, cases: &[(Base, &str, LandState)]) {
        for warm in [false, true] {
            for &(base, path, state) in cases {
                let case = format!("b_first={b_first} warm={warm} {base:?} {path}");
                let m = merged(b_first, warm);
                let change = put(path, "mine");
                let cs = m.f.commit(m.id(base), std::slice::from_ref(&change));
                let out = m.f.land(cs, "k");
                assert_eq!(out.state, state, "{case}: {}", out.detail);
                let conflicts: &[RepoPath] = if state == LandState::Conflict {
                    &[change.0]
                } else {
                    &[]
                };
                assert_eq!(out.conflict_paths, conflicts, "{case}");
                if state == LandState::Landed {
                    let tip = out.result.unwrap();
                    assert_eq!(m.f.main().target, tip, "{case}");
                    let rebased = m.f.store.read(|c| c.get_changeset(m.f.repo, tip));
                    assert_eq!(rebased.unwrap().parents(), [m.head], "{case}");
                    assert_eq!(
                        m.f.file(tip, path),
                        change
                            .1
                            .map(|state| Entry::File(FileEntry::new(state, "mine".len() as u64))),
                        "{case}"
                    );
                }
            }
        }
    }

    #[test]
    fn lands_across_a_merge_on_the_first_parent_history() {
        use LandState::{Conflict, Landed, Rejected};
        check_merge_cases(
            true,
            &[
                (Base::Root, "a/x", Conflict),    // B changed it
                (Base::Root, "side/f", Conflict), // M changed it against B
                (Base::Root, "c", Landed),        // only S1 changed it; M kept B's
                (Base::B, "a/x", Landed),         // B's own change is not after B
                (Base::B, "side/f", Conflict),
                (Base::B, "side/g", Landed), // a sibling of a changed file
                (Base::B, "c", Landed),
                (Base::Head, "side/f", Landed),
                (Base::Side(50), "b/y", Rejected), // not on the first-parent history
            ],
        );
    }

    #[test]
    fn lands_after_a_merge_that_made_the_old_tip_a_second_parent() {
        use LandState::{Conflict, Landed, Rejected};
        check_merge_cases(
            false,
            &[
                (Base::B, "b/y", Rejected),    // B left the first-parent history
                (Base::Root, "c", Conflict),   // S1 is on the first-parent history now
                (Base::Root, "a/x", Conflict), // M changed it against S100
                (Base::Root, "b/y", Landed),
                (Base::Side(50), "side/f", Conflict), // S51..S100 rewrote it
                (Base::Side(100), "side/f", Landed),  // M changed only a/x
            ],
        );
    }

    #[test]
    fn concurrent_lands_all_land_exactly_once() {
        let f = Arc::new(fixture());
        let threads: Vec<_> = (0..16)
            .map(|t| {
                let f = Arc::clone(&f);
                std::thread::spawn(move || {
                    (0..25)
                        .map(|i| {
                            let cs = f.commit(f.root, &[put(&format!("t{t}/f{i}"), "x")]);
                            f.land(cs, &format!("{t}-{i}"))
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let outcomes: Vec<LandOutcome> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        assert!(outcomes.iter().all(|o| o.state == LandState::Landed));
        assert_eq!(f.main().version, 1 + 400);
        // Every landed result is on main's first-parent history, once.
        let history: Vec<ChangesetId> = crate::read::log(&f.store, "r", f.main().target, 1000)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(history.len(), 401);
        let mut results: Vec<_> = outcomes.iter().map(|o| o.result.unwrap()).collect();
        results.sort();
        let mut landed: Vec<_> = history[..400].to_vec();
        landed.sort();
        assert_eq!(results, landed);
        assert!(f.file(f.main().target, "t15/f24").is_some());
    }

    #[test]
    fn finished_lands_expire_after_the_retention_period() {
        use LandState::{Conflict, Landed, Pending, Rejected};
        let f = fixture();
        let landed = f.land(f.commit(f.root, &[put("a/x", "1")]), "landed");
        let conflict = f.land(f.commit(f.root, &[put("a/x", "2")]), "conflict");
        let missing = LandRequest {
            target_ref: "nope".into(),
            ..req(f.root, "rejected")
        };
        let rejected = f.lands.land(&f.store, &caller(), &missing).unwrap();
        let states = [landed.state, conflict.state, rejected.state];
        assert_eq!(states, [Landed, Conflict, Rejected]);
        // All three expired; landed is the oldest, rejected the newest.
        for (extra, out) in [(3, &landed), (2, &conflict), (1, &rejected)] {
            f.age(out.land_id, LAND_RETENTION_SECS + extra);
        }

        // A land that a crash left pending. Recording it expires the two
        // oldest lands, the limit for one land.
        let cs = f.commit(f.root, &[put("b/y", "pending")]);
        let (_, pending) = f
            .store
            .write(|c| c.record_land(&caller(), &req(cs, "pending"), None))
            .unwrap();
        assert_eq!(pending.state, Pending);
        assert!(f.gone(&landed) && f.gone(&conflict));
        assert_eq!(f.get(rejected.land_id).unwrap(), rejected);

        // The next land expires the third. A land that is almost expired
        // stays, and so does the pending one.
        let recent = f.land(f.commit(f.main().target, &[put("c", "1")]), "recent");
        assert!(f.gone(&rejected));
        f.age(recent.land_id, LAND_RETENTION_SECS - 60);
        f.land(f.commit(f.main().target, &[put("d", "1")]), "next");
        assert_eq!(stored(&f.get(recent.land_id).unwrap()), stored(&recent));
        assert_eq!(stored(&f.get(pending.land_id).unwrap()), stored(&pending));
    }

    #[test]
    fn expiry_never_gives_out_a_land_id_twice() {
        let f = fixture();
        let first = f.land(f.commit(f.root, &[put("f1", "x")]), "1");
        f.age(first.land_id, LAND_RETENTION_SECS + 1);
        // The only land expired, but it is the newest, so it stays.
        f.store.write(|c| c.expire_lands()).unwrap();
        assert_eq!(stored(&f.get(first.land_id).unwrap()), stored(&first));
        // The next land gets a higher ID, and then the first one expires.
        let second = f.land(f.commit(f.root, &[put("f2", "x")]), "2");
        assert!(second.land_id > first.land_id);
        assert!(f.gone(&first));
    }

    #[test]
    fn a_retry_after_its_land_expired_does_not_land_twice() {
        let f = fixture();
        // Main moves past the base, so this land rebases.
        f.move_main(f.commit(f.root, &[put("b/y", "moved")]));
        let rebased = f.commit(f.root, &[put("a/x", "mine")]);
        let first_rebased = f.land(rebased, "rebased");
        // These two land on the head, so each lands as itself.
        let direct = f.commit(f.main().target, &[put("c", "mine")]);
        let first_direct = f.land(direct, "direct");
        let empty = f.commit(f.main().target, &[]);
        let first_empty = f.land(empty, "empty");
        assert_ne!(first_rebased.result, Some(rebased));
        assert_eq!(first_direct.result, Some(direct));
        assert_eq!(first_empty.result, Some(empty));
        let first = [first_rebased, first_direct, first_empty];

        // The rows expire. Each new land expires at most 2.
        for out in &first {
            f.age(out.land_id, LAND_RETENTION_SECS + 1);
        }
        f.fill("t", 2);
        assert!(first.iter().all(|out| f.gone(out)));

        // A retry with the same key is a new land. Its own landed copy
        // changed each of its paths after its base, so it conflicts.
        let before = f.main();
        for (cs, key, path) in [(rebased, "rebased", "a/x"), (direct, "direct", "c")] {
            let out = f.land(cs, key);
            assert_eq!(out.state, LandState::Conflict, "{key}: {}", out.detail);
            assert_eq!(out.conflict_paths, [RepoPath::new(path).unwrap()], "{key}");
            assert!(first.iter().all(|old| old.land_id != out.land_id));
        }
        assert_eq!(f.main(), before, "nothing landed twice");

        // A changeset with no changes has nothing to conflict, so it lands
        // again: one more empty changeset, and no file changes.
        let out = f.land(empty, "empty");
        assert_eq!(out.state, LandState::Landed);
        let tip = out.result.unwrap();
        assert_ne!(tip, empty);
        let deadline = crate::deadline::Deadline::new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        );
        let tree = |cs| tree::root_tree(&f.store, f.repo, cs, &deadline).unwrap();
        assert_eq!(tree(tip), tree(before.target));
    }

    #[test]
    fn a_retry_within_the_window_returns_the_stored_outcome() {
        let f = fixture();
        let cs = f.commit(f.root, &[put("a/x", "1")]);
        let first = f.land(cs, "key");
        assert_eq!(first.state, LandState::Landed);

        // The land is 60 s inside the window. Lands after it run expiry, and
        // it stays: a retry with the same key still returns the stored
        // outcome, field for field, with the same land ID.
        f.age(first.land_id, LAND_RETENTION_SECS - 60);
        f.fill("t", 3);
        let retry = f.try_land(cs, "key").unwrap();
        assert_eq!(stored(&retry), stored(&first));
        assert!(!retry.moved, "the retry moved nothing itself");
    }

    #[test]
    fn a_retry_after_the_window_is_a_new_land() {
        let f = fixture();
        let cs = f.commit(f.root, &[put("a/x", "1")]);
        let first = f.land(cs, "key");

        // The land is past the window, so the next land expires it. The key
        // is gone: a retry with the same key is a new land with a new ID,
        // checked like any other. Its own landed copy changed its paths
        // after its base, so it comes back as a conflict, and main stays.
        f.age(first.land_id, LAND_RETENTION_SECS + 60);
        f.fill("t", 1);
        assert!(f.gone(&first));
        let main_before = f.main();
        let retry = f.land(cs, "key");
        assert!(retry.land_id > first.land_id);
        assert_eq!(retry.state, LandState::Conflict);
        assert_eq!(retry.conflict_paths, [RepoPath::new("a/x").unwrap()]);
        assert_eq!(f.main(), main_before);
    }

    #[test]
    fn a_version_1_database_upgrades_and_keeps_its_lands() {
        let dir = tempfile::tempdir().unwrap();
        let (child, pending_cs) = {
            let c = Connection::open(dir.path().join("scion.db")).unwrap();
            let v1 = include_str!("../migrations/0001_init.sql");
            c.execute_batch(&format!("BEGIN; {v1} PRAGMA user_version = 1; COMMIT;"))
                .unwrap();
            let mut tx = SqliteTx(&c);
            let repo = tx.create_repo("r").unwrap();
            let insert =
                |tx: &mut SqliteTx<'_>, cs: &Changeset| tx.insert_changeset(repo, cs).unwrap();
            let root = insert(&mut tx, &changeset(vec![], &[put("a/x", "1")]));
            let child = insert(&mut tx, &changeset(vec![root], &[put("a/x", "2")]));
            let pending_cs = insert(&mut tx, &changeset(vec![root], &[put("b", "3")]));
            assert!(tx.cas_ref(repo, "main", None, child).unwrap());
            c.execute(
                "INSERT INTO lands
                     (repo_id, caller, request_key, target_ref, input_cs, state, result_cs,
                      detail, conflict_paths)
                 VALUES (?1, 't', 'landed', 'main', ?2, 'landed', ?2, '', x''),
                        (?1, 't', 'conflict', 'main', ?3, 'conflict', NULL, 'clash', ?4),
                        (?1, 't', 'pending', 'main', ?3, 'pending', NULL, '', x'')",
                params![
                    repo.0,
                    child.as_bytes(),
                    pending_cs.as_bytes(),
                    b"a/x".as_slice()
                ],
            )
            .unwrap();
            (child, pending_cs)
        };

        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        // The fixture wrote no blob files; the pending land's tree build
        // reads their lengths, so store them now.
        store_blobs(&store);
        let version: u32 = store
            .read(|c| {
                c.sqlite()
                    .unwrap()
                    .pragma_query_value(None, "user_version", |row| row.get(0))
                    .map_err(crate::error::from_sqlite)
            })
            .unwrap();
        assert_eq!(version, crate::store::SCHEMA_VERSION);
        // Finished lands start their retention at the upgrade; the pending
        // one has no finish time.
        let rows: Vec<(String, Option<bool>)> = store
            .read(|c| {
                let sql = "SELECT state, abs(finished_at - unixepoch()) < 60 FROM lands
                           ORDER BY land_id";
                let mut stmt = c
                    .sqlite()
                    .unwrap()
                    .prepare(sql)
                    .map_err(crate::error::from_sqlite)?;
                let rows = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(crate::error::from_sqlite)?;
                rows.collect::<rusqlite::Result<_>>()
                    .map_err(crate::error::from_sqlite)
            })
            .unwrap();
        let want = [
            ("landed", Some(true)),
            ("conflict", Some(true)),
            ("pending", None),
        ];
        assert_eq!(rows, want.map(|(state, now)| (state.to_owned(), now)));

        // The lands are 1, 2 and 3, in insert order.
        let get = |id| get_land(&store, &caller(), "r", id).unwrap();
        let landed = get(1);
        let pair = (landed.state, landed.result);
        assert_eq!(pair, (LandState::Landed, Some(child)));
        let conflict = get(2);
        assert_eq!(conflict.state, LandState::Conflict);
        assert_eq!(conflict.conflict_paths, [RepoPath::new("a/x").unwrap()]);
        assert_eq!(conflict.detail, "clash");

        // The pending land finishes, and the old finished ones stay.
        let out = Lands::default()
            .land(&store, &caller(), &req(pending_cs, "pending"))
            .unwrap();
        assert_eq!((out.land_id, out.state), (3, LandState::Landed));
        assert_eq!((get(1), get(2)), (landed, conflict));
    }

    #[test]
    fn conflict_paths_round_trip() {
        let paths = vec![RepoPath::new("a").unwrap(), RepoPath::new("b/c").unwrap()];
        assert_eq!(decode_paths(&encode_paths(&paths)), Some(paths));
        assert_eq!(decode_paths(&[]), Some(vec![]));
        assert_eq!(decode_paths(b"a\0\0b"), None, "an empty path is not a path");
    }

    #[test]
    fn a_pending_land_cap_counts_rows_so_a_restart_keeps_the_count() {
        let f = fixture();
        let cs = f.commit(f.root, &[put("a/x", "new")]);
        // `record` is where the quota lives: it counts the pending rows of
        // the caller in the store, and adds one new row under its limit.
        let record_of = |key: &str, limit: Option<usize>| {
            f.store
                .write(|c| c.record_land(&caller(), &req(cs, key), limit))
        };
        let first = record_of("k1", Some(1)).unwrap();
        assert_eq!(first.1.state, LandState::Pending);
        // A different key for the same principal is over the cap. The error
        // names its flag.
        let second = record_of("k2", Some(1)).unwrap_err();
        assert!(matches!(second, Error::ResourceExhausted(_)), "{second:?}");
        assert!(
            second
                .to_string()
                .contains("--max-principal-pending-lands 1")
        );
        // A retry of an existing key adds no row: it may always finish.
        let again = record_of("k1", Some(1)).unwrap();
        assert_eq!(again.1.land_id, first.1.land_id);
        // Another principal counts its own pending lands.
        let theirs = f
            .store
            .write(|c| c.record_land(&Caller("other".into()), &req(cs, "k3"), Some(1)))
            .unwrap();
        assert_eq!(theirs.1.state, LandState::Pending);
        // The count comes from the store's rows, not from memory of the
        // `Lands` that recorded them, so it is right after a restart: a
        // fresh `Lands` with the same cap refuses a new key too, because
        // the row of `k1` is still pending.
        let restarted = f
            .store
            .write(|c| c.record_land(&caller(), &req(cs, "k4"), Some(1)));
        assert!(matches!(restarted, Err(Error::ResourceExhausted(_))));
    }

    #[test]
    fn the_sweep_rejects_only_pending_lands_past_the_cutoff() {
        let f = fixture();
        // The audit file lives in its own temp dir, so two test runs
        // never share it.
        let audit_dir = tempfile::tempdir().unwrap();
        let audit_path = audit_dir.path().join("audit.log");
        let audit = crate::audit::Audit::open(&audit_path).unwrap();
        let cs = f.commit(f.root, &[put("a/x", "new")]);
        let lands = Lands::new(Some(8), None);
        let recorded = f
            .store
            .write(|c| c.record_land(&caller(), &req(cs, "old"), None))
            .unwrap();
        assert_eq!(recorded.1.state, LandState::Pending);

        // A fresh pending land stays, even at a cutoff of now.
        let rejected = lands.sweep_stale(&f.store, 60, &audit).unwrap();
        assert_eq!(rejected, 0);

        // The row is old now: the sweep rejects it, and the detail says
        // why, and that a retry needs a new request key.
        f.store
            .write(|c| {
                c.sqlite()
                    .unwrap()
                    .execute(
                        "UPDATE lands SET created_at = unixepoch() - 100000
                     WHERE state = 'pending'",
                        [],
                    )
                    .map_err(crate::error::from_sqlite)?;
                Ok(())
            })
            .unwrap();
        let rejected = lands.sweep_stale(&f.store, 60, &audit).unwrap();
        assert_eq!(rejected, 1);
        let (state, detail): (String, String) = f
            .store
            .read(|c| {
                c.sqlite()
                    .unwrap()
                    .query_row(
                        "SELECT state, detail FROM lands WHERE request_key = 'old'",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(crate::error::from_sqlite)
            })
            .unwrap();
        assert_eq!(state, "rejected");
        assert!(detail.contains("stale"), "{detail}");
        assert!(detail.contains("retry with a new request key"), "{detail}");

        // The same request key cannot resume a rejected land, so the
        // client retries with a new key, which counts as a new land.
        let resumed = f
            .store
            .write(|c| c.record_land(&caller(), &req(cs, "old"), None))
            .unwrap();
        assert_eq!(resumed.1.state, LandState::Rejected);

        // The sweep answered the land's intent: one result line, why
        // stale, joined to the intent by the request key and the land id.
        let lines: Vec<serde_json::Value> = String::from_utf8(std::fs::read(&audit_path).unwrap())
            .unwrap()
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let results: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|line| line["event"] == "land" && line["outcome"] == "failed")
            .collect();
        assert_eq!(results.len(), 1, "one result line for one intent");
        assert_eq!(results[0]["why"], "stale");
        // The request id is the sweep's own fresh id: the intent's
        // request id is not stored, so the line joins the intent on the
        // request key, not on request_id.
        assert_eq!(results[0]["request_key"], "old");
        assert_ne!(results[0]["request_id"], "old");
        assert_eq!(results[0]["land_id"], recorded.1.land_id);
    }

    /// Pass 5, W1: a request key is scoped to one caller in one repo. B
    /// reusing A's key gets its own land, not A's result; neither caller
    /// can read the other's land by its id.
    #[test]
    fn another_principals_key_and_land_id_are_not_shared() {
        let f = fixture();
        let a_cs = f.commit(f.root, &[put("a/x", "a")]);
        let a_land = f.land(a_cs, "shared");
        assert_eq!(a_land.state, LandState::Landed);

        // B lands its own changeset under A's key: the lookup names B, so
        // this is a new land, and it lands on the head A left.
        let head = f.main().target;
        let b_cs = f.commit(head, &[put("b/y", "b")]);
        let b_out = Lands::default()
            .land(&f.store, &Caller("b".into()), &req(b_cs, "shared"))
            .unwrap();
        assert_eq!(b_out.state, LandState::Landed, "{}", b_out.detail);
        assert_ne!(b_out.land_id, a_land.land_id, "two rows, one per caller");

        // A land's id answers only to its own caller.
        f.get(a_land.land_id)
            .unwrap_or_else(|e| panic!("A reads its land: {e:?}"));
        let b_gets_a = get_land(&f.store, &Caller("b".into()), "r", a_land.land_id);
        assert!(matches!(b_gets_a, Err(Error::NotFound(_))), "{b_gets_a:?}");
        let a_gets_b = get_land(&f.store, &caller(), "r", b_out.land_id);
        assert!(matches!(a_gets_b, Err(Error::NotFound(_))), "{a_gets_b:?}");
    }

    /// Pass 5, W4: the ref CAS names a version, not a target, so an ABA
    /// (main moves X -> Y -> X) still refuses a CAS that plans at the old
    /// version: the version moved twice even though the target came back.
    #[test]
    fn a_cas_at_a_version_which_moved_and_moved_back_refuses() {
        let f = fixture();
        let head = f.main();
        assert_eq!(head.version, 1);
        let y = f.commit(f.root, &[put("b/y", "y")]);
        assert!(
            f.store
                .write(|c| c.cas_ref(f.repo, "main", Some(head.version), y))
                .unwrap()
        );
        // Back to the same target X: the version still moves.
        assert!(
            f.store
                .write(|c| c.cas_ref(f.repo, "main", Some(2), head.target))
                .unwrap()
        );
        assert_eq!(f.main().target, head.target, "X again, by target");
        assert_eq!(f.main().version, 3, "but a new version");
        // A land that planned at version 1 refuses to move the ref, whatever
        // it names as the new target.
        let cs = f.commit(head.target, &[put("a/x", "later")]);
        assert!(
            !f.store
                .write(|c| c.cas_ref(f.repo, "main", Some(head.version), cs))
                .unwrap()
        );
        assert_eq!(f.main().version, 3, "the refused CAS moved nothing");
    }
}

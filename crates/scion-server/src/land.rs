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
//!      pending
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError};

use hashlink::LruCache;
use rusqlite::{Connection, OptionalExtension, params};
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};

use crate::auth::Caller;
use crate::db::{self, RepoId};
use crate::deadline::Deadline;
use crate::error::{Error, Result};
use crate::store::Store;
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
const LAND_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;

/// Most expired lands that one new land deletes. A land adds one row, so
/// expiry keeps up, and no land does much work after a long pause. Tests use
/// a small batch, so that they reach the limit.
const EXPIRE_PER_LAND: i64 = if cfg!(test) { 2 } else { 100 };

/// How far, in generations, the ref moves in one prune cycle of a
/// [`PathIndex`]. Tests use a short cycle, so that they prune.
const PRUNE_EVERY: i64 = if cfg!(test) { 4 } else { 10_000 };

/// A base more than this many generations below the tip is checked without
/// growing the [`PathIndex`], so it never holds the floor down. One prune
/// cycle, so bases keep at most about three cycles of history in the index.
/// A client lands soon after it reads the head, so an older base is rare.
/// Each land on such a base walks the history from the floor back to the
/// base, up to [`MAX_WALK`] changesets. Tests use a depth longer than the
/// history in their pruned index.
const MAX_DEPTH: i64 = if cfg!(test) { 16 } else { PRUNE_EVERY };

/// Most changesets that one land walks below the floor of the
/// [`PathIndex`]. A land whose base is further back is rejected, and the
/// client must rebase it. Tests use a walk longer than their other walks.
// SHORTCUT: each land on an old base walks the history again, and holds the
// database lock while it walks, so a client that lands on such bases makes
// other requests wait. Ceiling: a base more than this far back is rejected.
// Upgrade (issue #38): per-path history in the database, so the check reads
// only the paths that the land changes.
const MAX_WALK: usize = if cfg!(test) { 1000 } else { 100_000 };

// A base within `MAX_DEPTH` of the tip is never too old: the walk from the
// floor to it takes at most `MAX_DEPTH` steps.
const _: () = assert!(MAX_DEPTH.unsigned_abs() <= MAX_WALK as u64);

/// Most bytes that the [`PathIndex`]es of all refs hold together, counted by
/// [`entry_bytes`]: about 1M entries of short paths. An index that would go
/// over first drops the indexes of other refs that no land uses, least
/// recently used first ([`Lands::reserve`]). If that is not enough, an
/// advance or a land moves its floor up to the tip, and a reach back to a
/// base stops, and [`touched_below`] checks the rest. Tests use a small
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
    /// request key. Most reasons hold forever, but a base that is too old to
    /// check depends on what the path index holds at the time, so the same
    /// base can pass later under a new key.
    Rejected,
}

impl LandState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Landed => "landed",
            Self::Conflict => "conflict",
            Self::Rejected => "rejected",
        }
    }

    fn parse(s: &str) -> Result<Self> {
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
    /// [`Error::DeadlineExceeded`], and the land stays pending.
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
    /// Why it conflicted or was rejected.
    pub detail: String,
    /// Paths that conflict.
    pub conflict_paths: Vec<RepoPath>,
}

/// Per-ref state, guarded by the ref's lock.
type RefSlot = Arc<Mutex<Option<PathIndex>>>;

/// Serializes lands per ref and keeps each ref's [`PathIndex`].
pub struct Lands {
    /// The slot of each ref that holds a path index or that a land uses,
    /// the least recently landed on first. [`Lands::evict`] removes the
    /// others.
    refs: Mutex<LruCache<(RepoId, String), RefSlot>>,
    /// What all path indexes hold, by [`entry_bytes`]: the sum of their
    /// `bytes`. At most [`MAX_INDEX_BYTES`].
    used: Arc<AtomicUsize>,
    /// Indexes that [`Lands::evict`] dropped. Their bytes are back in
    /// `used`. A land frees them after its database lock and ref lock.
    evicted: Mutex<Vec<PathIndex>>,
}

impl Default for Lands {
    fn default() -> Self {
        Self {
            refs: Mutex::new(LruCache::new_unbounded()),
            used: Arc::default(),
            evicted: Mutex::default(),
        }
    }
}

impl Lands {
    /// Lands a changeset. A conflict or rejection is an outcome, not an error.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the repo or changeset does not exist.
    /// [`Error::InvalidArgument`] for a malformed request, or a request key
    /// reused for a different land. [`Error::Aborted`] if other writers kept
    /// moving the ref, and [`Error::DeadlineExceeded`] if the plan ran past
    /// the request's deadline. The land then stays pending, and a retry with
    /// the same request key resumes it. Store errors otherwise.
    pub fn land(&self, store: &Store, caller: &Caller, req: &LandRequest) -> Result<LandOutcome> {
        let outcome = self.land_committed(store, caller, req);
        // An index can hold up to the whole budget, and freeing it takes a
        // while, so it happens here, outside every lock.
        let evicted = mem::take(&mut *self.evicted.lock().unwrap_or_else(PoisonError::into_inner));
        drop(evicted);
        let outcome = outcome?;
        // Lands on one ref commit one at a time. Waiting for the flush after
        // the ref lock is released lets them share it.
        store.flush()?;
        Ok(outcome)
    }

    /// [`Lands::land`], up to commits that are not durable yet. Its reads do
    /// not wait for a flush either: `land` flushes before it replies.
    fn land_committed(
        &self,
        store: &Store,
        caller: &Caller,
        req: &LandRequest,
    ) -> Result<LandOutcome> {
        check_name("target_ref", &req.target_ref)?;
        check_name("request_key", &req.request_key)?;
        let (repo, recorded) = store.commit(|c| record(c, caller, req))?;
        if recorded.state != LandState::Pending {
            return Ok(recorded);
        }

        // SHORTCUT: an in-process lock serializes lands per ref, so this works
        // with one server process. Upgrade: a single lander per ref that
        // rebases a batch and moves the ref once for all of them.
        let slot = self.slot(repo, &req.target_ref);
        let mut index = slot.lock().unwrap_or_else(PoisonError::into_inner);

        // A call with the same key may have finished the land while this one
        // waited for the lock.
        let land_id = recorded.land_id;
        let row = store.read_unflushed(|c| load(c, repo, caller, land_id))?;
        if row.state != LandState::Pending {
            return Ok(row);
        }
        let result = finish(self, store, repo, land_id, req, &mut index).map_err(|e| match e {
            Error::DeadlineExceeded(why) => Error::DeadlineExceeded(format!(
                "{why}; the land stays pending, retry with the same request key"
            )),
            e => e,
        });
        // The index may be ahead of the database; rebuild it next time. A lost
        // race wrote nothing, so the index is still good. So is an index that
        // a deadline stopped: an advance adds nothing until it reaches the
        // tip, and a reach adds one whole changeset at a time, so a retry
        // goes on from where the reach stopped.
        if result
            .as_ref()
            .is_err_and(|e| !matches!(e, Error::Aborted(_) | Error::DeadlineExceeded(_)))
        {
            *index = None;
        }
        result
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
        let slots: Vec<RefSlot> = {
            let mut refs = self.lock_refs();
            // No land can clone a slot while `refs` is locked, so a slot that
            // only `refs` holds is not in use.
            refs.retain(|_, slot| {
                Arc::strong_count(slot) > 1
                    || !matches!(slot.try_lock(), Ok(index) if index.is_none())
            });
            refs.iter().map(|(_, slot)| Arc::clone(slot)).collect()
        };
        let free = MAX_INDEX_BYTES - self.used.load(Ordering::Acquire);
        let short = bytes.saturating_sub(free);
        let mut idle = Vec::new();
        let mut freed = 0;
        for slot in &slots {
            if freed >= short {
                break;
            }
            let index = match slot.try_lock() {
                Ok(index) => index,
                Err(TryLockError::Poisoned(e)) => e.into_inner(),
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
pub fn get_land(store: &Store, caller: &Caller, repo: &str, land_id: i64) -> Result<LandOutcome> {
    store.read(|c| {
        let repo = db::repo_id(c, repo)?;
        load(c, repo, caller, land_id)
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

/// Inserts a land, or finds the land for the same request key. A land to a
/// ref that does not exist is recorded as rejected at once, so only real refs
/// ever get a lock slot in [`Lands`]. A new land also expires old ones.
fn record(c: &Connection, caller: &Caller, req: &LandRequest) -> Result<(RepoId, LandOutcome)> {
    let repo = db::repo_id(c, &req.repo)?;
    db::generation(c, repo, req.changeset)?;
    let existing = c
        .query_row(
            "SELECT land_id, input_cs, target_ref FROM lands
             WHERE repo_id = ?1 AND caller = ?2 AND request_key = ?3",
            params![repo.0, caller.0, req.request_key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let land_id = if let Some((land_id, input, target)) = existing {
        if input != req.changeset.as_bytes() || target != req.target_ref {
            return Err(Error::InvalidArgument(format!(
                "request key {:?} was already used for a different land",
                req.request_key
            )));
        }
        land_id
    } else {
        let (state, detail) = if db::get_ref(c, repo, &req.target_ref)?.is_some() {
            (LandState::Pending, String::new())
        } else {
            let detail = format!("ref {:?} does not exist", req.target_ref);
            (LandState::Rejected, detail)
        };
        c.prepare_cached(
            "INSERT INTO lands
                 (repo_id, caller, request_key, target_ref, input_cs, state, detail, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7,
                 CASE ?6 WHEN 'pending' THEN NULL ELSE unixepoch() END)",
        )?
        .execute(params![
            repo.0,
            caller.0,
            req.request_key,
            req.target_ref,
            req.changeset.as_bytes(),
            state.as_str(),
            detail
        ])?;
        let land_id = c.last_insert_rowid();
        expire(c)?;
        land_id
    };
    Ok((repo, load(c, repo, caller, land_id)?))
}

/// Deletes up to [`EXPIRE_PER_LAND`] lands that finished more than
/// [`LAND_RETENTION_SECS`] ago, oldest first. A pending land has no
/// `finished_at`, so it never expires. It runs in the transaction that
/// records a new land, so the flush before that land's reply covers it.
///
/// The newest land always stays. `land_id` has no AUTOINCREMENT: a new row
/// gets the highest ID plus one. So keeping the newest row means a land ID
/// is never given out twice.
fn expire(c: &Connection) -> Result<()> {
    let deleted = c
        .prepare_cached(
            "DELETE FROM lands WHERE land_id IN (
                 SELECT land_id FROM lands
                 WHERE finished_at < unixepoch() - ?1
                     AND land_id < (SELECT max(land_id) FROM lands)
                 ORDER BY finished_at LIMIT ?2)",
        )?
        .execute(params![LAND_RETENTION_SECS, EXPIRE_PER_LAND])?;
    if deleted > 0 {
        tracing::debug!(deleted, "expired finished lands");
    }
    Ok(())
}

/// A land that `caller` started in `repo`.
fn load(c: &Connection, repo: RepoId, caller: &Caller, land_id: i64) -> Result<LandOutcome> {
    let (state, result, detail, paths) = c
        .query_row(
            "SELECT state, result_cs, detail, conflict_paths FROM lands
             WHERE repo_id = ?1 AND land_id = ?2 AND caller = ?3",
            params![repo.0, land_id, caller.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| Error::NotFound(format!("land {land_id}")))?;
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
fn set_state(
    c: &Connection,
    repo: RepoId,
    land_id: i64,
    state: LandState,
    result: Option<ChangesetId>,
    detail: String,
    conflict_paths: Vec<RepoPath>,
) -> Result<LandOutcome> {
    let updated = c
        .prepare_cached(
            "UPDATE lands SET state = ?3, result_cs = ?4, detail = ?5, conflict_paths = ?6,
                 finished_at = unixepoch()
             WHERE repo_id = ?1 AND land_id = ?2 AND state = 'pending'",
        )?
        .execute(params![
            repo.0,
            land_id,
            state.as_str(),
            result.as_ref().map(ChangesetId::as_bytes),
            detail,
            encode_paths(&conflict_paths)
        ])?;
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

/// What step 4 decided.
enum Plan {
    Reject(String),
    Conflict(String, Vec<RepoPath>),
    Land {
        rebased: Changeset,
        head: db::RefState,
    },
}

/// Runs steps 4 and 5 for a pending land. The caller holds the ref's lock.
///
/// Imports do not take that lock, and may run in another process, so the ref
/// can move between the plan and the transaction. The land is then planned
/// again from the new head, and that plan advances the index to it: each
/// plan reads the head from the database.
fn finish(
    lands: &Lands,
    store: &Store,
    repo: RepoId,
    land_id: i64,
    req: &LandRequest,
    index: &mut Option<PathIndex>,
) -> Result<LandOutcome> {
    let mut attempt = 1;
    loop {
        let plan = store.read_unflushed(|c| plan(lands, c, repo, req, index))?;
        match settle(lands, store, repo, land_id, &req.target_ref, plan, index) {
            Err(Error::Aborted(detail)) if attempt < MAX_ATTEMPTS => {
                tracing::info!(land_id, attempt, %detail, "planning the land again");
                attempt += 1;
            }
            outcome => return outcome,
        }
    }
}

/// Step 4: decides the land against the ref's current head.
fn plan(
    lands: &Lands,
    c: &Connection,
    repo: RepoId,
    req: &LandRequest,
    index: &mut Option<PathIndex>,
) -> Result<Plan> {
    let target = &req.target_ref;
    let cs = db::get_changeset(c, repo, req.changeset)?;
    let [base] = cs.parents[..] else {
        return Ok(Plan::Reject(
            "a land needs a changeset with exactly one parent".into(),
        ));
    };
    let Some(head) = db::get_ref(c, repo, target)? else {
        return Ok(Plan::Reject(format!("ref {target:?} does not exist")));
    };
    let index = match index {
        Some(index) => index,
        None => index.insert(PathIndex::new(c, repo, head.target, &lands.used)?),
    };
    index.advance_to(lands, c, repo, head.target, &req.deadline)?;
    let conflicts = match index.check(lands, c, repo, base, &cs.changes, &req.deadline)? {
        Checked::Paths(conflicts) => conflicts,
        Checked::NotOnHistory => {
            return Ok(Plan::Reject(format!(
                "base {base} is not on the first-parent history of {target:?}"
            )));
        }
        Checked::TooOld => {
            return Ok(Plan::Reject(format!(
                "base {base} is too old: it is not within {MAX_WALK} changesets of the \
                 head of {target:?}; rebase onto a newer head and land again"
            )));
        }
    };
    if !conflicts.is_empty() {
        let detail = format!(
            "{} path(s) changed on {target:?} after the base",
            conflicts.len()
        );
        return Ok(Plan::Conflict(detail, conflicts));
    }
    let rebased = Changeset {
        parents: vec![head.target],
        ..cs
    };
    Ok(Plan::Land { rebased, head })
}

/// Step 5: records what the plan decided. If the ref moved since the plan,
/// writes nothing and returns [`Error::Aborted`].
fn settle(
    lands: &Lands,
    store: &Store,
    repo: RepoId,
    land_id: i64,
    target: &str,
    plan: Plan,
    index: &mut Option<PathIndex>,
) -> Result<LandOutcome> {
    let (rebased, head) = match plan {
        Plan::Reject(detail) => {
            return store.commit(|c| {
                set_state(c, repo, land_id, LandState::Rejected, None, detail, vec![])
            });
        }
        Plan::Conflict(detail, paths) => {
            return store
                .commit(|c| set_state(c, repo, land_id, LandState::Conflict, None, detail, paths));
        }
        Plan::Land { rebased, head } => (rebased, head),
    };

    // See `tree::store_changeset`. The land's flush covers these steps.
    tree::fill_root_tree(store, repo, head.target)?;
    let landed = store.commit(|c| {
        let (id, _) = tree::store_changeset(c, repo, &rebased)?;
        if !db::cas_ref(c, repo, target, Some(head.version), id)? {
            // The error rolls back the transaction, rebased changeset included.
            return Err(Error::Aborted(format!(
                "ref {target:?} moved during the land; retry with the same request key"
            )));
        }
        let generation = db::generation(c, repo, id)?;
        let outcome = set_state(
            c,
            repo,
            land_id,
            LandState::Landed,
            Some(id),
            String::new(),
            vec![],
        )?;
        Ok((outcome, generation))
    });
    match landed {
        Ok((outcome, generation)) => {
            if let (Some(index), Some(id)) = (index.as_mut(), outcome.result) {
                index.record(lands, id, generation, &rebased.changes);
            }
            tracing::info!(land_id, id = ?outcome.result, target = %target, "landed");
            Ok(outcome)
        }
        // The path check passed, but the changes still clash with the head's
        // tree (for example a delete of a file the head never had). The
        // transaction rolled back; record the conflict.
        Err(Error::Apply(e)) => {
            let detail = format!("does not apply to {target:?}: {e}");
            store.commit(|c| set_state(c, repo, land_id, LandState::Conflict, None, detail, vec![]))
        }
        Err(e) => Err(e),
    }
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
        c: &Connection,
        repo: RepoId,
        head: ChangesetId,
        used: &Arc<AtomicUsize>,
    ) -> Result<Self> {
        Ok(Self::at(head, db::generation(c, repo, head)?, used))
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

    /// Indexes the changesets from the current tip forward to `head`. Starts
    /// over at `head` if `head` does not descend from the tip (the ref moved
    /// to other history), if `head` is more than [`MAX_DEPTH`] changesets
    /// ahead, or if those changesets would not fit in [`MAX_INDEX_BYTES`].
    /// An index at the head is always correct, and a longer walk would hold
    /// the database lock for little gain.
    fn advance_to(
        &mut self,
        lands: &Lands,
        c: &Connection,
        repo: RepoId,
        head: ChangesetId,
        deadline: &Deadline,
    ) -> Result<()> {
        if head == self.tip {
            return Ok(());
        }
        let tip_gen = db::generation(c, repo, self.tip)?;
        let mut walked: Vec<(ChangesetId, i64, BTreeMap<RepoPath, Option<FileState>>)> = Vec::new();
        // The most that the walked changesets can add, to stop the walk early.
        let mut most = 0;
        let mut steps = 0;
        let mut cur = Some(head);
        while let Some(id) = cur {
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
                break;
            }
            deadline.check()?;
            steps += 1;
            if steps > MAX_DEPTH {
                tracing::info!(
                    steps,
                    "the ref moved far; the path index starts over at the head"
                );
                break;
            }
            let generation = db::generation(c, repo, id)?;
            if generation <= tip_gen {
                break;
            }
            let cs = db::get_changeset(c, repo, id)?;
            most += added_bytes(&cs.changes);
            if self.bytes + most > MAX_INDEX_BYTES {
                tracing::info!(
                    generation,
                    "the path indexes are full; it starts over at the head"
                );
                break;
            }
            cur = cs.parents.first().copied();
            walked.push((id, generation, cs.changes));
        }
        *self = Self::new(c, repo, head, &self.used)?;
        Ok(())
    }

    /// The paths in `changes` that changed on the history after `base`, if
    /// `base` is on the history. Then prunes.
    ///
    /// A base below the floor is reached: the changesets from the floor back
    /// to it are added to the index, while they fit in [`MAX_INDEX_BYTES`].
    /// [`touched_below`] checks the rest without keeping them. It checks all
    /// of them for a base more than [`MAX_DEPTH`] below the tip, and such a
    /// base does not count as used for pruning.
    fn check(
        &mut self,
        lands: &Lands,
        c: &Connection,
        repo: RepoId,
        base: ChangesetId,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
        deadline: &Deadline,
    ) -> Result<Checked> {
        let base_gen = db::generation(c, repo, base)?;
        let tip_gen = match self.chain.last_key_value() {
            Some((&generation, _)) => generation,
            None => db::generation(c, repo, self.tip)?,
        };
        let deep = tip_gen - base_gen > MAX_DEPTH;
        let mut below = HashSet::new();
        if base != self.floor && self.chain.get(&base_gen) != Some(&base) {
            // A deep base does not grow the index: the walk checks it all.
            let mut walk = if deep {
                Walk::Stopped
            } else {
                self.reach(lands, c, repo, base, base_gen, deadline)?
            };
            if walk == Walk::Stopped {
                (walk, below) =
                    touched_below(c, repo, self.floor, base, base_gen, changes, deadline)?;
            }
            match walk {
                Walk::Reached => {}
                Walk::Missed => return Ok(Checked::NotOnHistory),
                Walk::Stopped => return Ok(Checked::TooOld),
            }
        }
        // The index holds the changesets after the floor, and `below` the paths
        // that older ones touched, so together they see every change after
        // the base.
        let conflicts = changes
            .keys()
            .filter(|path| {
                let path = path.as_bytes();
                below.contains(path) || self.touches(path, base_gen)
            })
            .cloned()
            .collect();
        if !deep {
            self.oldest_base = self.oldest_base.min(base_gen);
        }
        self.prune();
        Ok(Checked::Paths(conflicts))
    }

    /// Walks from the floor back to `base`, adding each changeset and moving
    /// the floor down past it. Stops at the first changeset that does not fit
    /// in [`MAX_INDEX_BYTES`], which is then the floor. The caller has checked
    /// that `base` is not the floor and not in the index.
    fn reach(
        &mut self,
        lands: &Lands,
        c: &Connection,
        repo: RepoId,
        base: ChangesetId,
        base_gen: i64,
        deadline: &Deadline,
    ) -> Result<Walk> {
        walk_back(
            c,
            repo,
            self.floor,
            base,
            base_gen,
            deadline,
            |id, generation, cs| {
                let cost = self.cost([(generation, &cs.changes)]);
                if !self.grow(lands, cost, |index| index.add(id, generation, &cs.changes)) {
                    return false;
                }
                if let Some(&parent) = cs.parents.first() {
                    self.floor = parent;
                }
                true
            },
        )
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
fn added_bytes(changes: &BTreeMap<RepoPath, Option<FileState>>) -> usize {
    let paths = changes.keys().map(|path| {
        let path = path.as_bytes();
        entry_bytes(path) + ancestors(path).map(entry_bytes).sum::<usize>()
    });
    entry_bytes(&[]) + paths.sum::<usize>()
}

/// What [`PathIndex::check`] found.
enum Checked {
    /// The base is on the history, and these paths of the land changed
    /// after it.
    Paths(Vec<RepoPath>),
    /// The base is not on the first-parent history.
    NotOnHistory,
    /// The base is more than [`MAX_WALK`] changesets below the floor.
    TooOld,
}

/// How a [`walk_back`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Walk {
    /// It reached the base: the base is on the history.
    Reached,
    /// The base is not on the history.
    Missed,
    /// The visit stopped it before it reached the base.
    Stopped,
}

/// Walks first parents from `from` back to `base`, and passes each changeset
/// after `base` to `visit`, newest first, while `visit` returns `true`.
/// Fails with [`Error::DeadlineExceeded`] once `deadline` has passed.
fn walk_back(
    c: &Connection,
    repo: RepoId,
    from: ChangesetId,
    base: ChangesetId,
    base_gen: i64,
    deadline: &Deadline,
    mut visit: impl FnMut(ChangesetId, i64, &Changeset) -> bool,
) -> Result<Walk> {
    let mut cur = from;
    loop {
        deadline.check()?;
        let generation = db::generation(c, repo, cur)?;
        if generation <= base_gen {
            return Ok(Walk::Missed);
        }
        let cs = db::get_changeset(c, repo, cur)?;
        if !visit(cur, generation, &cs) {
            return Ok(Walk::Stopped);
        }
        match cs.parents.first() {
            Some(&parent) if parent == base => return Ok(Walk::Reached),
            Some(&parent) => cur = parent,
            None => return Ok(Walk::Missed),
        }
    }
}

/// For a base that the index cannot reach: walks the history from `floor`
/// back to `base`, and returns each path in `land` that the walked
/// changesets touched, by the rule of [`PathIndex::touches`]. Keeps only the
/// land's own paths and directories, so memory follows the land, not the
/// depth. Stops after [`MAX_WALK`] changesets.
fn touched_below<'a>(
    c: &Connection,
    repo: RepoId,
    floor: ChangesetId,
    base: ChangesetId,
    base_gen: i64,
    land: &'a BTreeMap<RepoPath, Option<FileState>>,
    deadline: &Deadline,
) -> Result<(Walk, HashSet<&'a [u8]>)> {
    let paths: HashSet<&[u8]> = land.keys().map(RepoPath::as_bytes).collect();
    let parents: HashSet<&[u8]> = paths.iter().flat_map(|p| ancestors(p)).collect();
    // What `files` and `dirs` of an index over the walk would hold, kept only
    // where `touches` reads them for the land's paths.
    let mut files: HashSet<&[u8]> = HashSet::new();
    let mut dirs: HashSet<&[u8]> = HashSet::new();
    let mut walked = 0;
    let walk = walk_back(c, repo, floor, base, base_gen, deadline, |_, _, cs| {
        if walked == MAX_WALK {
            return false;
        }
        walked += 1;
        for path in cs.changes.keys() {
            let path = path.as_bytes();
            if let Some(&key) = paths.get(path).or_else(|| parents.get(path)) {
                files.insert(key);
            }
            dirs.extend(ancestors(path).filter_map(|dir| paths.get(dir)));
        }
        true
    })?;
    if walk == Walk::Stopped {
        tracing::info!(base_gen, walked, "a base is too old to check");
    } else {
        tracing::debug!(base_gen, walked, ?walk, "checked an old base");
    }
    let hits = paths.into_iter().filter(|&path| {
        files.contains(path) || dirs.contains(path) || ancestors(path).any(|d| files.contains(d))
    });
    Ok((walk, hits.collect()))
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
fn encode_paths(paths: &[RepoPath]) -> Vec<u8> {
    paths
        .iter()
        .map(RepoPath::as_bytes)
        .collect::<Vec<_>>()
        .join(&0)
}

fn decode_paths(bytes: &[u8]) -> Option<Vec<RepoPath>> {
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
    use std::time::Instant;

    use super::*;
    use crate::store::FSYNC_LEAVES_DRIVE_CACHE;
    use scion_core::{BlobId, Entry, IdentityTime};

    type Change = (RepoPath, Option<FileState>);
    type Edits<'a> = &'a [Change];

    struct Fixture {
        _dir: tempfile::TempDir,
        store: Store,
        repo: RepoId,
        root: ChangesetId,
        lands: Lands,
    }

    fn author() -> IdentityTime {
        IdentityTime {
            name: b"t".to_vec(),
            email: b"t@x".to_vec(),
            unix_secs: 0,
            tz_offset_minutes: 0,
        }
    }

    fn put(path: &str, content: &str) -> Change {
        let state = FileState::Regular {
            content: BlobId::of(content.as_bytes()),
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
        let store = Store::open(dir.path()).unwrap();
        let (repo, root) = store
            .write(|c| {
                let repo = db::create_repo(c, "r")?;
                let root = Changeset {
                    parents: vec![],
                    author: author(),
                    committer: None,
                    message: b"root".to_vec(),
                    changes: BTreeMap::from([put("a/x", "1"), put("b/y", "2"), put("c", "3")]),
                };
                let id = db::insert_changeset(c, repo, &root)?;
                assert!(db::cas_ref(c, repo, "main", None, id)?);
                Ok((repo, id))
            })
            .unwrap();
        Fixture {
            _dir: dir,
            store,
            repo,
            root,
            lands: Lands::default(),
        }
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
        Changeset {
            parents,
            author: author(),
            committer: None,
            message: format!("{changes:?}").into_bytes(),
            changes: changes.iter().cloned().collect(),
        }
    }

    impl Fixture {
        fn commit(&self, base: ChangesetId, changes: Edits<'_>) -> ChangesetId {
            self.insert(&changeset(vec![base], changes))
        }

        fn insert(&self, cs: &Changeset) -> ChangesetId {
            self.store
                .write(|c| db::insert_changeset(c, self.repo, cs))
                .unwrap()
        }

        /// Moves `main` to `to` the way an import does: outside lands.
        fn move_main(&self, to: ChangesetId) {
            let head = self.main();
            let moved = self
                .store
                .write(|c| db::cas_ref(c, self.repo, "main", Some(head.version), to))
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
                    Ok(c.query_row(sql, [], |row| row.get(0))?)
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
                .write(|c| Ok(c.execute(sql, params![land_id, secs])?))
                .unwrap();
            assert_eq!(aged, 1);
        }

        fn main(&self) -> db::RefState {
            self.store
                .read(|c| db::get_ref(c, self.repo, "main"))
                .unwrap()
                .unwrap()
        }

        fn file(&self, cs: ChangesetId, path: &str) -> Option<Entry> {
            let root = tree::root_tree(&self.store, self.repo, cs).unwrap();
            self.store
                .read(|c| tree::lookup(c, root, &RepoPath::new(path).unwrap()))
                .unwrap()
        }

        fn generation(&self, cs: ChangesetId) -> i64 {
            self.store
                .read(|c| db::generation(c, self.repo, cs))
                .unwrap()
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
        let rebased = f.store.read(|c| db::get_changeset(c, f.repo, tip)).unwrap();
        assert_eq!(rebased.parents, vec![one]);
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
        assert_eq!(f.land(cs, "k"), first);
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

    #[test]
    fn a_crash_at_any_step_lands_exactly_once_on_retry() {
        for crash in [Crash::AfterRecord, Crash::AfterPlan, Crash::RefMoved] {
            let f = fixture();
            // Main moves past the base, so the land must rebase.
            f.move_main(f.commit(f.root, &[put("b/y", "first")]));
            let cs = f.commit(f.root, &[put("a/x", "new")]);
            let req = req(cs, "k");

            // The first attempt runs the steps of `Lands::land` by hand.
            let (repo, recorded) = f.store.write(|c| record(c, &caller(), &req)).unwrap();
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
                    let kept = f.store.read(|c| db::has_changeset(c, repo, rebased));
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
            let landed = f.store.read(|c| db::get_changeset(c, repo, after.target));
            assert_eq!(landed.unwrap().parents, vec![before.target], "{crash:?}");
            assert_eq!(fresh.land(&f.store, &caller(), &req).unwrap(), out);
            assert_eq!(f.landed_count(), 1, "{crash:?}");
        }
    }

    #[test]
    fn get_land_shows_only_the_callers_own_lands() {
        let f = fixture();
        let out = f.land(f.commit(f.root, &[put("a/x", "new")]), "k");
        assert_eq!(
            get_land(&f.store, &caller(), "r", out.land_id).unwrap(),
            out
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
        // A's a/x and F2's f2 are below the floor, so only the walk sees
        // them. The last F's change is still in the index.
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
                .read(|c| Ok(c.query_row(sql, [key], |row| row.get(0))?))
                .unwrap();
            assert_eq!(row, "pending", "{key}");
            // The index stays, so a retry goes on from where the plan stopped.
            f.index(|_| ());
            let out = f.land(cs, key);
            assert_eq!(out.state, state, "{key}: {}", out.detail);
        }
    }

    #[test]
    fn a_base_beyond_the_walk_limit_is_rejected() {
        use LandState::{Conflict, Landed, Rejected};
        // An import moves main to root ─ C1 ─ … ─ Cn, where Ci puts `ci`.
        let f = fixture();
        let n = MAX_WALK + 1;
        let mut history = vec![f.root];
        f.store
            .write(|c| {
                for i in 1..=n {
                    let cs = changeset(vec![history[i - 1]], &[put(&format!("c{i}"), "x")]);
                    history.push(db::insert_changeset(c, f.repo, &cs)?);
                }
                Ok(())
            })
            .unwrap();
        f.move_main(history[n]);
        // A land on the head builds the index there, so Cn is the floor.
        let out = f.land(f.commit(history[n], &[put("warm", "x")]), "warm");
        assert_eq!(out.state, Landed);
        assert_eq!(f.index(|index| index.floor), history[n]);

        // The walk from the floor reaches C1 with its last step, which visits
        // C2, and C2 put `c2`.
        let out = f.land(f.commit(history[1], &[put("c2", "mine")]), "c1");
        assert_eq!(out.state, Conflict, "{}", out.detail);
        assert_eq!(out.conflict_paths, [RepoPath::new("c2").unwrap()]);

        // Root is one more step back, so the client must rebase.
        let before = f.main();
        let out = f.land(f.commit(f.root, &[put("b/y", "mine")]), "root");
        assert_eq!(out.state, Rejected);
        assert!(out.detail.contains("too old"), "{}", out.detail);
        assert_eq!(f.main(), before);
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
            let moved = f
                .store
                .write(|c| db::cas_ref(c, f.repo, name, None, f.root));
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
        let tip = f.store.read(|c| db::get_ref(c, f.repo, last)).unwrap();
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
                .write(|c| db::cas_ref(c, f.repo, "big", None, f.root))
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
                    .write(|c| db::cas_ref(c, f.repo, name, None, f.root))
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
                        let head = f.store.read(|c| db::get_ref(c, f.repo, &name));
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
                .write(|c| db::cas_ref(c, f.repo, "other", None, f.root))
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
        // room, and walks the rest.
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
                    parent = db::insert_changeset(c, f.repo, &changeset(vec![parent], &[change]))?;
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
                    let rebased = m.f.store.read(|c| db::get_changeset(c, m.f.repo, tip));
                    assert_eq!(rebased.unwrap().parents, vec![m.head], "{case}");
                    assert_eq!(m.f.file(tip, path), change.1.map(Entry::File), "{case}");
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
            .write(|c| record(c, &caller(), &req(cs, "pending")))
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
        assert_eq!(f.get(recent.land_id).unwrap(), recent);
        assert_eq!(f.get(pending.land_id).unwrap(), pending);
    }

    #[test]
    fn expiry_never_gives_out_a_land_id_twice() {
        let f = fixture();
        let first = f.land(f.commit(f.root, &[put("f1", "x")]), "1");
        f.age(first.land_id, LAND_RETENTION_SECS + 1);
        // The only land expired, but it is the newest, so it stays.
        f.store.write(expire).unwrap();
        assert_eq!(f.get(first.land_id).unwrap(), first);
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
        let tree = |cs| tree::root_tree(&f.store, f.repo, cs).unwrap();
        assert_eq!(tree(tip), tree(before.target));
    }

    #[test]
    fn a_version_1_database_upgrades_and_keeps_its_lands() {
        let dir = tempfile::tempdir().unwrap();
        let (child, pending_cs) = {
            let c = Connection::open(dir.path().join("scion.db")).unwrap();
            let v1 = include_str!("../migrations/0001_init.sql");
            c.execute_batch(&format!("BEGIN; {v1} PRAGMA user_version = 1; COMMIT;"))
                .unwrap();
            let repo = db::create_repo(&c, "r").unwrap();
            let insert = |cs: &Changeset| db::insert_changeset(&c, repo, cs).unwrap();
            let root = insert(&changeset(vec![], &[put("a/x", "1")]));
            let child = insert(&changeset(vec![root], &[put("a/x", "2")]));
            let pending_cs = insert(&changeset(vec![root], &[put("b", "3")]));
            assert!(db::cas_ref(&c, repo, "main", None, child).unwrap());
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

        let store = Store::open(dir.path()).unwrap();
        let version: u32 = store
            .read(|c| Ok(c.pragma_query_value(None, "user_version", |row| row.get(0))?))
            .unwrap();
        assert_eq!(version, crate::store::SCHEMA_VERSION);
        // Finished lands start their retention at the upgrade; the pending
        // one has no finish time.
        let rows: Vec<(String, Option<bool>)> = store
            .read(|c| {
                let sql = "SELECT state, abs(finished_at - unixepoch()) < 60 FROM lands
                           ORDER BY land_id";
                let mut stmt = c.prepare(sql)?;
                let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
                Ok(rows.collect::<rusqlite::Result<_>>()?)
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
}

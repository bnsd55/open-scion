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
//!      no path changed on the ref after the base
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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use rusqlite::{Connection, OptionalExtension, params};
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};

use crate::auth::Caller;
use crate::db::{self, RepoId};
use crate::error::{Error, Result};
use crate::store::Store;
use crate::tree;

/// Longest accepted ref name or request key, in bytes.
const MAX_NAME_BYTES: usize = 256;

/// Plans per land call when other writers keep moving the ref. After the
/// last one the call fails with [`Error::Aborted`] and the land stays pending.
// SHORTCUT: imports move refs without the land lock, so a land retries a few
// times, then hands the retry to the client. Ceiling: a ref that an import
// moves faster than a land commits starves its lands. Upgrade: route every
// ref move through the per-ref lander, so lands never race.
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

/// Where a land stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LandState {
    /// Recorded, not finished.
    Pending,
    /// The ref points at the rebased changeset.
    Landed,
    /// A changed path also changed on the ref after the base.
    Conflict,
    /// The land can never succeed.
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
#[derive(Default)]
pub struct Lands {
    refs: Mutex<HashMap<(RepoId, String), RefSlot>>,
}

impl Lands {
    /// Lands a changeset. A conflict or rejection is an outcome, not an error.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if the repo or changeset does not exist.
    /// [`Error::InvalidArgument`] for a malformed request, or a request key
    /// reused for a different land. [`Error::Aborted`] if other writers kept
    /// moving the ref; the land stays pending and a retry with the same
    /// request key resumes it. Store errors otherwise.
    pub fn land(&self, store: &Store, caller: &Caller, req: &LandRequest) -> Result<LandOutcome> {
        let outcome = self.land_committed(store, caller, req)?;
        // Lands on one ref commit one at a time. Waiting for the flush after
        // the ref lock is released lets them share it.
        store.flush()?;
        Ok(outcome)
    }

    /// [`Lands::land`], up to commits that are not durable yet.
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
        let row = store.read(|c| load(c, repo, caller, land_id))?;
        if row.state != LandState::Pending {
            return Ok(row);
        }
        let result = finish(store, repo, land_id, req, &mut index);
        // The index may be ahead of the database; rebuild it next time. A lost
        // race wrote nothing, so the index is still good.
        if result
            .as_ref()
            .is_err_and(|e| !matches!(e, Error::Aborted(_)))
        {
            *index = None;
        }
        result
    }

    fn slot(&self, repo: RepoId, name: &str) -> RefSlot {
        let mut refs = self.refs.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(refs.entry((repo, name.to_owned())).or_default())
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
/// `finished_at`, so it never expires.
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
/// Imports do not take that lock, so the ref can move between the plan and
/// the transaction. The land is then planned again from the new head, and
/// that plan advances the index to it.
fn finish(
    store: &Store,
    repo: RepoId,
    land_id: i64,
    req: &LandRequest,
    index: &mut Option<PathIndex>,
) -> Result<LandOutcome> {
    let mut attempt = 1;
    loop {
        let plan = store.read(|c| plan(c, repo, req, index))?;
        match settle(store, repo, land_id, &req.target_ref, plan, index) {
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
        None => index.insert(PathIndex::new(c, repo, head.target)?),
    };
    index.advance_to(c, repo, head.target)?;
    let Some(base_gen) = index.reach(c, repo, base)? else {
        return Ok(Plan::Reject(format!(
            "base {base} is not on the first-parent history of {target:?}"
        )));
    };
    let conflicts = index.conflicts(&cs.changes, base_gen);
    index.prune(base_gen);
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
                index.record(id, generation, &rebased.changes);
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
/// forward as the ref moves and backward when a land has an older base.
/// Each [`PRUNE_EVERY`] generations, [`PathIndex::prune`] moves `floor` up to
/// the oldest base that the cycle used, so memory follows the bases in use.
// SHORTCUT: a land with an old base keeps the index back to that base until
// the next cycle ends. Ceiling: a client that lands on a very old base in
// every cycle keeps that much history in memory. Upgrade: check bases older
// than a fixed depth with a walk from the database that keeps no entries.
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
}

impl PathIndex {
    /// An empty index at `head`. Its first prune cycle starts there.
    fn new(c: &Connection, repo: RepoId, head: ChangesetId) -> Result<Self> {
        let head_gen = db::generation(c, repo, head)?;
        Ok(Self {
            tip: head,
            floor: head,
            chain: BTreeMap::new(),
            files: HashMap::new(),
            dirs: HashMap::new(),
            cycle_start: head_gen,
            oldest_base: head_gen,
        })
    }

    /// Indexes the changesets from the current tip forward to `head`. If
    /// `head` does not descend from the tip (the ref moved to other history),
    /// starts over at `head`.
    fn advance_to(&mut self, c: &Connection, repo: RepoId, head: ChangesetId) -> Result<()> {
        if head == self.tip {
            return Ok(());
        }
        let tip_gen = db::generation(c, repo, self.tip)?;
        let mut walked = Vec::new();
        let mut cur = Some(head);
        while let Some(id) = cur {
            if id == self.tip {
                for (id, generation, changes) in walked {
                    self.add(id, generation, &changes);
                }
                self.tip = head;
                return Ok(());
            }
            let generation = db::generation(c, repo, id)?;
            if generation <= tip_gen {
                break;
            }
            let cs = db::get_changeset(c, repo, id)?;
            cur = cs.parents.first().copied();
            walked.push((id, generation, cs.changes));
        }
        *self = Self::new(c, repo, head)?;
        Ok(())
    }

    /// The generation of `base` if it is on the indexed history, extending
    /// the index backward if needed. `None` if `base` is not on the history.
    fn reach(&mut self, c: &Connection, repo: RepoId, base: ChangesetId) -> Result<Option<i64>> {
        let base_gen = db::generation(c, repo, base)?;
        if base == self.floor || self.chain.get(&base_gen) == Some(&base) {
            return Ok(Some(base_gen));
        }
        let mut cur = self.floor;
        loop {
            let generation = db::generation(c, repo, cur)?;
            if generation <= base_gen {
                return Ok(None);
            }
            let cs = db::get_changeset(c, repo, cur)?;
            self.add(cur, generation, &cs.changes);
            match cs.parents.first() {
                Some(&parent) => {
                    self.floor = parent;
                    if parent == base {
                        return Ok(Some(base_gen));
                    }
                    cur = parent;
                }
                None => return Ok(None),
            }
        }
    }

    /// Paths in `changes` that touch a path changed after `base_gen`: the same
    /// path, a directory above it, or anything below it.
    fn conflicts(
        &self,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
        base_gen: i64,
    ) -> Vec<RepoPath> {
        let after =
            |map: &HashMap<Vec<u8>, i64>, key: &[u8]| map.get(key).is_some_and(|&g| g > base_gen);
        changes
            .keys()
            .filter(|path| {
                let path = path.as_bytes();
                after(&self.files, path)
                    || after(&self.dirs, path)
                    || ancestors(path).any(|dir| after(&self.files, dir))
            })
            .cloned()
            .collect()
    }

    /// Notes that a land used `base_gen`. Once the tip is [`PRUNE_EVERY`]
    /// generations past the cycle start, moves `floor` up to the oldest base
    /// the cycle used, drops what is now below it, and starts a new cycle.
    ///
    /// A later base below the new floor is still checked in full:
    /// [`Self::reach`] adds the dropped changesets back from the database.
    fn prune(&mut self, base_gen: i64) {
        self.oldest_base = self.oldest_base.min(base_gen);
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
            tracing::debug!(floor_gen, kept = self.chain.len(), "pruned the path index");
        }
        self.cycle_start = tip_gen;
        self.oldest_base = tip_gen;
    }

    /// Records a changeset that just became the tip.
    fn record(
        &mut self,
        id: ChangesetId,
        generation: i64,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
    ) {
        self.add(id, generation, changes);
        self.tip = id;
    }

    fn add(
        &mut self,
        id: ChangesetId,
        generation: i64,
        changes: &BTreeMap<RepoPath, Option<FileState>>,
    ) {
        self.chain.insert(generation, id);
        for path in changes.keys() {
            let path = path.as_bytes();
            bump(&mut self.files, path, generation);
            for dir in ancestors(path) {
                bump(&mut self.dirs, dir, generation);
            }
        }
    }
}

fn bump(map: &mut HashMap<Vec<u8>, i64>, key: &[u8], generation: i64) {
    let slot = map.entry(key.to_vec()).or_insert(generation);
    *slot = (*slot).max(generation);
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
            let slot = self.lands.slot(self.repo, "main");
            let index = slot.lock().unwrap();
            read(index.as_ref().expect("main has a path index"))
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
                let plan = f.store.read(|c| plan(c, repo, &req, &mut index)).unwrap();
                let Plan::Land { rebased, .. } = &plan else {
                    panic!("{crash:?}: the land should plan to land");
                };
                let rebased = rebased.id();
                if crash == Crash::RefMoved {
                    f.move_main(f.commit(f.main().target, &[put("c", "import")]));
                    let err = settle(&f.store, repo, recorded.land_id, "main", plan, &mut index)
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
        assert_eq!(slots.keys().collect::<Vec<_>>(), [&(f.repo, "main".into())]);
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

    /// `main` goes root ─ A ─ F1 ─ F2 ─ …, three prune cycles of F's, each
    /// landed on the head. A puts `a/x`, and each Fi puts `fi`. By the end,
    /// the index has pruned A and the first F's. Returns the fixture and the
    /// history, root first.
    fn pruned_history() -> (Fixture, Vec<ChangesetId>) {
        let f = fixture();
        let a = f.commit(f.root, &[put("a/x", "A")]);
        assert_eq!(f.land(a, "a").state, LandState::Landed);
        let mut history = vec![f.root, a];
        history.extend(f.fill("f", 3 * cycle()));
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
    fn the_path_index_stays_bounded_under_many_lands() {
        let f = fixture();
        let bound = 2 * cycle();
        for n in 0..10 * cycle() {
            f.fill(&format!("d{n}/f"), 1);
            let sizes = f.index(|index| (index.chain.len(), index.files.len(), index.dirs.len()));
            assert!(
                sizes.0 <= bound && sizes.1 <= bound && sizes.2 <= bound,
                "after {} lands: (chain, files, dirs) = {sizes:?}",
                n + 1
            );
        }
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
        assert_eq!(version, 2);
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

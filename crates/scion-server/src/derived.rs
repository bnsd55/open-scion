//! Derived data: values computed from changesets, not part of them.
//!
//! One mechanism for every kind. A [`Kind`] has a name, a version, and a
//! function from a changeset and its parents' values to a value. A kind reads
//! the value of its first parent only, or of every parent
//! ([`Kind::ALL_PARENTS`]). A value is computed when it is first needed and
//! stored by (kind, version, changeset). A kind that must always be ready,
//! such as the root tree, also stores its value when the changeset is written.
//!
//! A history without values is derived in steps of at most [`STEP`]
//! changesets, parents first. Each step is its own transaction, so other
//! requests run between steps, and a crash loses at most one step. A value
//! is stored only after the values of the parents that its kind reads. So
//! each changeset with a value has older changesets with values, back to the
//! roots, through those parents.
//!
//! Each version has its own rows. So a version bump makes the old rows
//! unused, and nothing rewrites them: the new version derives its values
//! again. Each step also removes some rows of older versions of its kind.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::Instant;

use scion_core::{Changeset, ChangesetId};

use crate::deadline::Deadline;
use crate::error::{Error, Result};
use crate::meta::{MetaTx, RepoId};
use crate::store::Store;

/// Most changesets that one step walks or derives, and most rows of older
/// versions that one step removes. A step holds the write connection, so
/// this bounds how long other writes wait. Tests use a small step, so that a
/// short history takes several steps.
pub const STEP: usize = if cfg!(test) { 2 } else { 100 };

/// How many steps derive `changesets` changesets.
#[cfg(test)]
pub(crate) fn steps(changesets: usize) -> u64 {
    changesets.div_ceil(STEP) as u64
}

/// One kind of derived data.
pub trait Kind {
    /// The kind's name in the `derived` table.
    const NAME: &'static str;
    /// Bump it when [`Kind::derive`] gives a different answer. Rows of other
    /// versions are never read. Rows of older versions are removed. A kind
    /// of the server is listed in `store::build`, so that processes of two
    /// builds do not share a data directory.
    const VERSION: u32;
    /// Whether [`Kind::derive`] reads the value of every parent. If not, it
    /// reads the first parent's value only, and only first-parent history is
    /// derived. A changeset stores only its changes against its first
    /// parent, so a kind over snapshots, such as the root tree, needs no
    /// more.
    const ALL_PARENTS: bool = false;
    /// The value for one changeset.
    type Value;

    /// The value for changeset `id`, from the changeset and from the values
    /// for its parents, in parent order: every parent if
    /// [`Kind::ALL_PARENTS`], else the first parent only. Empty for a root.
    /// Writes that it makes, such as trees, go in the caller's transaction.
    ///
    /// It may also read the stored value of an older changeset that it
    /// reaches through those parents ([`load`]). Each one has a value.
    ///
    /// # Errors
    ///
    /// Database errors, or [`Error::Corrupt`] if the stored changeset does
    /// not fit its parents' values.
    fn derive(
        c: &mut dyn MetaTx,
        id: ChangesetId,
        changeset: &Changeset,
        parents: Vec<Self::Value>,
    ) -> Result<Self::Value>;

    /// The stored bytes of a value.
    fn encode(value: &Self::Value) -> Vec<u8>;

    /// The value in stored bytes, or `None` if they are not a value.
    fn decode(bytes: &[u8]) -> Option<Self::Value>;
}

/// The value of `K` for a changeset that `repo` may see, derived as
/// [`fill`] does if needed. Returns once the value, and what it names, is
/// durable.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`], or of [`Store::flush`].
pub fn get<K: Kind>(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    get_until::<K>(store, repo, cs, None)
}

/// [`get`], but if it derives, it stops between steps once `deadline` has
/// passed. The steps before it stay, so the next call goes on from there.
///
/// # Errors
///
/// As [`get`], and [`Error::DeadlineExceeded`].
pub fn get_by<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    deadline: &Deadline,
) -> Result<K::Value> {
    get_until::<K>(store, repo, cs, Some(deadline))
}

fn get_until<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    deadline: Option<&Deadline>,
) -> Result<K::Value> {
    let known = store.read(|c| {
        require(c, repo, cs)?;
        load::<K>(c, cs)
    })?;
    if let Some(value) = known {
        return Ok(value);
    }
    // A value can name rows that the derivation wrote, such as trees, and
    // the caller can send it to a client. So it waits for one full flush,
    // shared with concurrent commits. An error is an answer too, so it
    // waits as well: the steps before the error committed.
    let value = in_one_walk::<K>(store, repo, cs, deadline);
    store.flush()?;
    value
}

/// The walks each test kind owned, for the tests: the flight makes N cold
/// reads of one history own one walk.
#[cfg(test)]
static WALKS_OWNED: Mutex<Vec<(&'static str, usize)>> = Mutex::new(Vec::new());

/// The calls of each test kind that passed the stored-value check, for the
/// tests: a walk whose first derive waits for the count knows its callers
/// are all inside.
#[cfg(test)]
static WALKS_POLLED: Mutex<Vec<(&'static str, usize)>> = Mutex::new(Vec::new());

/// Whether the first derive of a kind waits for its callers to poll.
#[cfg(test)]
static POLLED_GATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
fn walks_owned_reset(kind: &'static str) {
    let mut owned = WALKS_OWNED.lock().unwrap_or_else(PoisonError::into_inner);
    owned.retain(|(name, _)| *name != kind);
    owned.push((kind, 0));
}

#[cfg(test)]
fn walks_owned(kind: &str) -> usize {
    WALKS_OWNED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(name, _)| *name == kind)
        .map_or(0, |(_, n)| *n)
}

#[cfg(test)]
fn walks_owned_add(kind: &'static str) {
    let mut owned = WALKS_OWNED.lock().unwrap_or_else(PoisonError::into_inner);
    match owned.iter_mut().find(|(name, _)| *name == kind) {
        Some((_, n)) => *n += 1,
        None => owned.push((kind, 1)),
    }
}

#[cfg(test)]
fn walks_polled_reset(kind: &'static str) {
    let mut polled = WALKS_POLLED.lock().unwrap_or_else(PoisonError::into_inner);
    polled.retain(|(name, _)| *name != kind);
    polled.push((kind, 0));
}

#[cfg(test)]
fn walks_polled(kind: &str) -> usize {
    WALKS_POLLED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(name, _)| *name == kind)
        .map_or(0, |(_, n)| *n)
}

#[cfg(test)]
fn walks_polled_add(kind: &'static str) {
    let mut polled = WALKS_POLLED.lock().unwrap_or_else(PoisonError::into_inner);
    match polled.iter_mut().find(|(name, _)| *name == kind) {
        Some((_, n)) => *n += 1,
        None => polled.push((kind, 1)),
    }
}
static FLIGHTS: OnceLock<Mutex<HashMap<FlightKey, Arc<Flight>>>> = OnceLock::new();

/// A walk of a kind, version and repo in flight.
struct Flight {
    /// How many walks of this key have ended. A waiter that wakes and sees
    /// the count unchanged keeps waiting.
    walks: Mutex<u64>,
    /// Woken when a walk ends, for whatever reason.
    woken: Condvar,
}

/// The cap on derive walks in flight, across all kinds and repos. Each walk
/// holds the write connection in turns, so walks beyond the cap only repeat
/// work and starve writes ([#142]).
const WALK_CAP: usize = 8;

/// Walks in flight, with the condvar a caller over the cap waits on.
static WALKS: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

thread_local! {
    /// The cap this test's caller sees, instead of [`WALK_CAP`]. A test
    /// must not lower the cap of another test that runs beside it.
    #[cfg(test)]
    static CAP_OVERRIDE: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
fn set_walk_cap(cap: usize) {
    CAP_OVERRIDE.with(|overridden| overridden.set(Some(cap)));
}

/// The cap on walks in flight, for this caller.
fn walk_cap() -> usize {
    #[cfg(test)]
    if let Some(cap) = CAP_OVERRIDE.with(Cell::get) {
        return cap;
    }
    WALK_CAP
}

/// Held while a walk derives. Drop it, and a waiting caller takes it.
struct Permit;

impl Drop for Permit {
    fn drop(&mut self) {
        let mut in_flight = WALKS.0.lock().unwrap_or_else(PoisonError::into_inner);
        *in_flight -= 1;
        WALKS.1.notify_all();
    }
}

/// Takes a walk permit, or waits for one until the deadline, then gives up
/// with [`Error::ResourceExhausted`]. Without a deadline it waits.
fn take_walk_permit(deadline: Option<&Deadline>) -> Result<Permit> {
    let mut in_flight = WALKS.0.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        if *in_flight < walk_cap() {
            *in_flight += 1;
            return Ok(Permit);
        }
        match deadline {
            None => {
                in_flight = WALKS
                    .1
                    .wait(in_flight)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            Some(deadline) => {
                let budget = deadline.at().saturating_duration_since(Instant::now());
                let (guard, result) = WALKS
                    .1
                    .wait_timeout_while(in_flight, budget, |in_flight| *in_flight >= walk_cap())
                    .unwrap_or_else(PoisonError::into_inner);
                in_flight = guard;
                if result.timed_out() {
                    return Err(Error::ResourceExhausted(
                        "no derive walk free before the deadline".to_owned(),
                    ));
                }
            }
        }
    }
}

/// Which walk a caller joins: kind, version and repo. Not the changeset:
/// the walks of tip-1, tip-2 and so on overlap on the same old range, so
/// flights per changeset would still walk it once per caller.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct FlightKey {
    kind: &'static str,
    version: u32,
    repo: i64,
}

impl FlightKey {
    fn of<K: Kind>(repo: RepoId) -> Self {
        Self {
            kind: K::NAME,
            version: K::VERSION,
            repo: repo.0,
        }
    }
}

/// The flights map, made on first use.
fn flights() -> &'static Mutex<HashMap<FlightKey, Arc<Flight>>> {
    FLIGHTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Joins the walk of `key`, or owns it: the caller that finds no flight
/// running owns the next walk, and the others wait for it.
fn join<K: Kind>(repo: RepoId) -> (Arc<Flight>, bool) {
    let mut flights = flights().lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(flight) = flights.get(&FlightKey::of::<K>(repo)) {
        return (Arc::clone(flight), false);
    }
    let flight = Arc::new(Flight {
        walks: Mutex::new(0),
        woken: Condvar::new(),
    });
    flights.insert(FlightKey::of::<K>(repo), Arc::clone(&flight));
    (flight, true)
}

/// Records that the walk `flight` owned ended, and retires the flight:
/// the next caller owns a fresh walk, even while this flight's waiters
/// still wake and read its rows. A waiter that finds the value missing
/// takes the permit again and owns the next walk itself.
fn landed(flight: &Arc<Flight>) {
    let mut flights = flights().lock().unwrap_or_else(PoisonError::into_inner);
    flights.retain(|_, running| !Arc::ptr_eq(running, flight));
    let mut walks = flight.walks.lock().unwrap_or_else(PoisonError::into_inner);
    *walks += 1;
    flight.woken.notify_all();
}

/// Waits for the walk of `flight` to end, until the deadline. False when
/// the deadline passed first.
fn waited(flight: &Arc<Flight>, before: u64, deadline: Option<&Deadline>) -> bool {
    let mut walks = flight.walks.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        if *walks > before {
            return true;
        }
        match deadline {
            None => {
                walks = flight
                    .woken
                    .wait(walks)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            Some(deadline) => {
                let budget = deadline.at().saturating_duration_since(Instant::now());
                let (guard, result) = flight
                    .woken
                    .wait_timeout_while(walks, budget, |walks| *walks <= before)
                    .unwrap_or_else(PoisonError::into_inner);
                walks = guard;
                if result.timed_out() {
                    return false;
                }
            }
        }
    }
}

/// Fills `K` for `cs` in at most one walk per kind, version and repo: a
/// caller that finds a walk of its key running waits for it, then reads
/// again; a caller that finds none owns the next walk. The owner stops at a
/// step boundary once its deadline has passed; the rows it stored stay, and
/// a waiter that finds no value takes over the next walk.
fn in_one_walk<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    deadline: Option<&Deadline>,
) -> Result<K::Value> {
    loop {
        // The value may be there already: a walk of this key that ended
        // while this caller waited stored it, or it was never missing.
        #[cfg(test)]
        walks_polled_add(K::NAME);
        let known = store.read(|c| load::<K>(c, cs))?;
        if let Some(value) = known {
            return Ok(value);
        }
        // A walk needs a permit: walks beyond the cap only repeat work, and
        // starve the writes ([#142]). Wait for one until the deadline.
        let _permit = take_walk_permit(deadline)?;
        let (flight, owner) = join::<K>(repo);
        if owner {
            #[cfg(test)]
            walks_owned_add(K::NAME);
            let value = fill_until::<K>(store, repo, cs, deadline);
            landed(&flight);
            return value;
        }
        let before = *flight.walks.lock().unwrap_or_else(PoisonError::into_inner);
        if !waited(&flight, before, deadline) {
            return Err(Error::DeadlineExceeded(
                "the call ran past its deadline".to_owned(),
            ));
        }
    }
}

/// Derives every missing value of `K` that the value for `cs` needs, in
/// steps, and returns the value for `cs`. Returns before the values are
/// durable: call [`Store::flush`] before replying with anything they name.
///
/// First it walks from `cs` through the parents that `K` reads, and stops at
/// each changeset with a stored value. It visits at most [`STEP`] changesets
/// per hold of the write connection. Then it derives the changesets without a
/// value in generation order, oldest first, at most [`STEP`] per
/// transaction. A parent's generation is below its child's, so each
/// changeset comes after its parents. Each of these transactions also
/// removes up to [`STEP`] rows of older versions of `K`. So an error or a
/// crash keeps every step before it, and the next call goes on from there.
///
/// The walk keeps the changesets without a value in memory, 40 bytes each,
/// so that it visits each one once. A walk over every parent also keeps the
/// set of the changesets it visited.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`].
pub fn fill<K: Kind>(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    fill_until::<K>(store, repo, cs, None)
}

/// [`fill`], but it stops between steps once `deadline` has passed. The
/// steps before it stay, so the next call goes on from there.
///
/// # Errors
///
/// As [`fill`], and [`Error::DeadlineExceeded`].
pub fn fill_by<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    deadline: &Deadline,
) -> Result<K::Value> {
    fill_until::<K>(store, repo, cs, Some(deadline))
}

fn fill_until<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    deadline: Option<&Deadline>,
) -> Result<K::Value> {
    let check = || deadline.map_or(Ok(()), Deadline::check);
    let pending = match walk_all::<K>(store, repo, cs, check)? {
        ControlFlow::Break(value) => return Ok(value),
        ControlFlow::Continue(pending) => pending,
    };
    derive_steps::<K>(store, repo, &pending, check)?
        .ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Fills `K` for `cs` and its history as [`fill`] does, then removes every
/// row of an older version of `K`, at most [`STEP`] rows per transaction.
/// Returns once all of it is durable.
///
/// # Errors
///
/// As [`fill`] and [`Store::flush`].
pub fn backfill<K: Kind>(store: &Store, repo: RepoId, cs: ChangesetId) -> Result<()> {
    fill::<K>(store, repo, cs)?;
    // A short step means that no older row is left.
    while store.commit(|c| remove_old::<K>(c))? == STEP {}
    store.flush()
}

/// Derives the value of `K` for `cs` inside the caller's transaction, and
/// stores every value it derives. It walks and derives as [`fill`] does,
/// but with no bound, in one transaction.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`].
pub fn derive<K: Kind>(c: &mut dyn MetaTx, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    let mut walk = match Walk::start::<K>(c, repo, cs)? {
        ControlFlow::Break(value) => return Ok(value),
        ControlFlow::Continue(walk) => walk,
    };
    walk.step::<K>(c, usize::MAX)?;
    derive_chain::<K>(c, repo, &walk.into_pending())?
        .ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Stores the value of `K` for `cs`. A second write must agree with the
/// first.
///
/// # Errors
///
/// [`Error::Corrupt`] if a different value is already stored.
pub fn put<K: Kind>(c: &mut dyn MetaTx, cs: ChangesetId, value: &K::Value) -> Result<()> {
    c.put_derived(K::NAME, K::VERSION, cs, &K::encode(value))
}

/// For a kind that is stored when a changeset is written: derives and stores
/// the value of `K` for `id`, which the caller's transaction just stored, if
/// each parent that `K` reads has a value. Otherwise it stores nothing, and
/// [`fill`] derives the value when it is first needed. So a write never
/// derives a long history. Like each step of [`fill`], it also removes a few
/// rows of older versions of `K`.
///
/// # Errors
///
/// Errors of [`Kind::derive`].
pub fn on_write<K: Kind>(c: &mut dyn MetaTx, id: ChangesetId, changeset: &Changeset) -> Result<()> {
    if load::<K>(c, id)?.is_none()
        && let Some(parents) = parent_values::<K>(c, changeset)?
    {
        let value = K::derive(c, id, changeset, parents)?;
        put::<K>(c, id, &value)?;
    }
    remove_old::<K>(c)?;
    Ok(())
}

/// The walk of [`fill`]: the stored value for `cs`, or else the changesets
/// to derive, parents first. Runs `check` before each step after the first.
fn walk_all<K: Kind>(
    store: &Store,
    repo: RepoId,
    cs: ChangesetId,
    check: impl Fn() -> Result<()>,
) -> Result<ControlFlow<K::Value, Vec<ChangesetId>>> {
    let first = store.read_unflushed(|c| {
        Ok(match Walk::start::<K>(c, repo, cs)? {
            ControlFlow::Continue(mut walk) => {
                walk.step::<K>(c, STEP)?;
                ControlFlow::Continue(walk)
            }
            stored @ ControlFlow::Break(_) => stored,
        })
    })?;
    let mut walk = match first {
        ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
        ControlFlow::Continue(walk) => walk,
    };
    while !walk.next.is_empty() {
        check()?;
        store.read_unflushed(|c| walk.step::<K>(c, STEP))?;
    }
    Ok(ControlFlow::Continue(walk.into_pending()))
}

/// Where a walk of [`fill`] stands.
#[derive(Debug)]
struct Walk {
    /// Changesets to visit, with their generation.
    next: Vec<(ChangesetId, i64)>,
    /// Changesets without a stored value, with their generation.
    pending: Vec<(i64, ChangesetId)>,
    /// Changesets visited. Only a walk over every parent keeps them: a
    /// first-parent walk reaches each changeset once.
    seen: HashSet<ChangesetId>,
}

impl Walk {
    /// The stored value for `cs`, or a walk that starts there.
    ///
    /// Rows are keyed by changeset alone, so this checks the repo: a stored
    /// value must not leak to a repo that cannot see the changeset. Every
    /// parent of a changeset in a repo is in that repo too, so the rest of
    /// the walk stays in it.
    fn start<K: Kind>(
        c: &mut dyn MetaTx,
        repo: RepoId,
        cs: ChangesetId,
    ) -> Result<ControlFlow<K::Value, Self>> {
        let generation = c.generation(repo, cs)?;
        if let Some(value) = load::<K>(c, cs)? {
            return Ok(ControlFlow::Break(value));
        }
        Ok(ControlFlow::Continue(Self {
            next: vec![(cs, generation)],
            pending: Vec::new(),
            seen: HashSet::new(),
        }))
    }

    /// Visits at most `limit` changesets. Each one without a stored value
    /// goes to `pending`, and the walk goes on to the parents that `K`
    /// reads.
    fn step<K: Kind>(&mut self, c: &mut dyn MetaTx, limit: usize) -> Result<()> {
        for _ in 0..limit {
            let Some((id, generation)) = self.next.pop() else {
                break;
            };
            if K::ALL_PARENTS && !self.seen.insert(id) {
                continue;
            }
            if c.get_derived(K::NAME, K::VERSION, id)?.is_some() {
                continue;
            }
            self.pending.push((generation, id));
            let mut parents = c.parents(id, generation)?;
            if !K::ALL_PARENTS {
                parents.truncate(1);
            }
            self.next.extend(parents);
        }
        Ok(())
    }

    /// The changesets without a value, in generation order, so that each
    /// comes after its parents.
    fn into_pending(mut self) -> Vec<ChangesetId> {
        self.pending.sort_unstable();
        self.pending.into_iter().map(|(_, id)| id).collect()
    }
}

/// The derivation of [`fill`]: derives `pending`, parents first, at most
/// [`STEP`] changesets per transaction. Returns the value for the last one.
/// Runs `check` before each step.
fn derive_steps<K: Kind>(
    store: &Store,
    repo: RepoId,
    pending: &[ChangesetId],
    check: impl Fn() -> Result<()>,
) -> Result<Option<K::Value>> {
    let mut value = None;
    for step in pending.chunks(STEP) {
        check()?;
        value = store.commit(|c| {
            let value = derive_chain::<K>(c, repo, step)?;
            remove_old::<K>(c)?;
            Ok(value)
        })?;
    }
    Ok(value)
}

/// Derives and stores the value of `K` for each of `ids`, in order, from
/// the stored values of their parents. Each parent that `K` reads has a
/// stored value already, or comes earlier in `ids`. A value that is already
/// stored, for example by a concurrent [`fill`], is read, not derived
/// again. Returns the value for the last one.
fn derive_chain<K: Kind>(
    c: &mut dyn MetaTx,
    repo: RepoId,
    ids: &[ChangesetId],
) -> Result<Option<K::Value>> {
    let mut value = None;
    for &id in ids {
        if let Some(known) = load::<K>(c, id)? {
            value = Some(known);
            continue;
        }
        let changeset = c.get_changeset(repo, id)?;
        let parents = parent_values::<K>(c, &changeset)?.ok_or_else(|| {
            Error::Corrupt(format!(
                "derived {} v{} is missing for a parent of {id}",
                K::NAME,
                K::VERSION
            ))
        })?;
        let next = K::derive(c, id, &changeset, parents)?;
        put::<K>(c, id, &next)?;
        value = Some(next);
    }
    Ok(value)
}

/// The stored values of `K` for the parents of `changeset` that `K` reads,
/// in parent order, or `None` if one of them has no value.
fn parent_values<K: Kind>(
    c: &mut dyn MetaTx,
    changeset: &Changeset,
) -> Result<Option<Vec<K::Value>>> {
    let read = if K::ALL_PARENTS { usize::MAX } else { 1 };
    let mut values = Vec::new();
    for &parent in changeset.parents.iter().take(read) {
        match load::<K>(c, parent)? {
            Some(value) => values.push(value),
            None => return Ok(None),
        }
    }
    Ok(Some(values))
}

/// Removes up to [`STEP`] rows of older versions of `K` in the caller's
/// transaction, and returns how many. Each step of [`fill`] calls it, and
/// so does each write of an eager kind, so old rows go away as new ones
/// come. Rows of a newer version stay: a newer build that ran on this data
/// may run again, and it reads them.
///
/// # Errors
///
/// Database errors.
pub fn remove_old<K: Kind>(c: &mut dyn MetaTx) -> Result<usize> {
    let removed = c.delete_derived_below(K::NAME, K::VERSION, STEP)?;
    if removed > 0 {
        tracing::debug!(
            kind = K::NAME,
            removed,
            "removed derived rows of older versions"
        );
    }
    Ok(removed)
}

/// The stored value of `K` for `cs`, if any. It does not check a repo: the
/// caller reaches `cs` from a changeset whose repo it checked.
///
/// # Errors
///
/// [`Error::Corrupt`] if the stored bytes are not a value.
pub(crate) fn load<K: Kind>(c: &mut dyn MetaTx, cs: ChangesetId) -> Result<Option<K::Value>> {
    c.get_derived(K::NAME, K::VERSION, cs)?
        .map(|bytes| {
            K::decode(&bytes).ok_or_else(|| {
                Error::Corrupt(format!("derived {} v{} for {cs}", K::NAME, K::VERSION))
            })
        })
        .transpose()
}

fn require(c: &mut dyn MetaTx, repo: RepoId, cs: ChangesetId) -> Result<()> {
    if c.has_changeset(repo, cs)? {
        Ok(())
    } else {
        Err(Error::NotFound(format!("changeset {cs}")))
    }
}

#[cfg(test)]
mod tests {
    use crate::db::SqliteTx;
    use rusqlite::Connection;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use scion_core::{BlobId, FileState, IdentityTime, RepoPath};

    use super::*;
    use crate::db::testing;
    use crate::store::{FSYNC_LEAVES_DRIVE_CACHE, SCHEMA_VERSION, migrate};
    use crate::tree::{self, RootTree};

    thread_local! {
        /// Calls to a test kind's `derive` on this thread.
        static DERIVES: Cell<usize> = const { Cell::new(0) };
        /// A changeset whose derivation fails on this thread.
        static FAIL_AT: Cell<Option<ChangesetId>> = const { Cell::new(None) };
    }

    /// A test kind: how many paths the first-parent history has changed, up
    /// to and including this changeset. Not stored at write time, and its
    /// value is not 32 bytes. Version 2 gives a new answer: it counts puts
    /// only.
    struct ChangedPaths<const VERSION: u32>;

    impl<const V: u32> Kind for ChangedPaths<V> {
        const NAME: &'static str = "changed_paths";
        const VERSION: u32 = V;
        type Value = u64;

        fn derive(
            _: &mut dyn MetaTx,
            id: ChangesetId,
            cs: &Changeset,
            parents: Vec<u64>,
        ) -> Result<u64> {
            if FAIL_AT.get() == Some(id) {
                return Err(Error::Corrupt(format!("injected failure at {id}")));
            }
            DERIVES.set(DERIVES.get() + 1);
            assert!(parents.len() <= 1, "a first-parent kind gets one value");
            let counted = cs
                .changes
                .values()
                .filter(|state| V == 1 || state.is_some());
            Ok(parents.first().copied().unwrap_or(0) + counted.count() as u64)
        }

        fn encode(count: &u64) -> Vec<u8> {
            count.to_le_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> Option<u64> {
            bytes.try_into().ok().map(u64::from_le_bytes)
        }
    }

    type ChangedPathsV1 = ChangedPaths<1>;
    type ChangedPathsV2 = ChangedPaths<2>;

    /// A test kind over every parent: the changeset, then the lists of its
    /// parents in parent order, each changeset once, at its first place. So
    /// the value depends on every parent's value and on their order.
    struct Ancestry;

    impl Kind for Ancestry {
        const NAME: &'static str = "ancestry";
        const VERSION: u32 = 1;
        const ALL_PARENTS: bool = true;
        type Value = Vec<ChangesetId>;

        fn derive(
            _: &mut dyn MetaTx,
            id: ChangesetId,
            cs: &Changeset,
            parents: Vec<Vec<ChangesetId>>,
        ) -> Result<Vec<ChangesetId>> {
            DERIVES.set(DERIVES.get() + 1);
            assert_eq!(parents.len(), cs.parents.len(), "a value for each parent");
            Ok(ancestry(id, parents))
        }

        fn encode(ids: &Vec<ChangesetId>) -> Vec<u8> {
            ids.iter().flat_map(|id| *id.as_bytes()).collect()
        }

        fn decode(bytes: &[u8]) -> Option<Vec<ChangesetId>> {
            bytes
                .chunks(32)
                .map(|id| ChangesetId::try_from(id).ok())
                .collect()
        }
    }

    /// Derives of a flight-test kind, across all threads.
    static COLD_DERIVES: AtomicUsize = AtomicUsize::new(0);
    /// Milliseconds that one derive of the flight kind sleeps.
    static COLD_SLEEP: AtomicUsize = AtomicUsize::new(0);

    /// A first-parent test kind for the flight tests: 1 + the parent's
    /// value. Each derive sleeps, so a test can hold walks open.
    struct Cold<const VERSION: u32>;

    impl<const V: u32> Kind for Cold<V> {
        const NAME: &'static str = "cold";
        const VERSION: u32 = V;
        type Value = u64;

        fn derive(
            _: &mut dyn MetaTx,
            _: ChangesetId,
            _: &Changeset,
            parents: Vec<u64>,
        ) -> Result<u64> {
            std::thread::sleep(Duration::from_millis(
                COLD_SLEEP.load(Ordering::Relaxed) as u64
            ));
            // A test that sets the gate makes the first derive wait for
            // every caller of the round to have looked for the value, so
            // the round's ownership is decided while they are all inside.
            if POLLED_GATE.load(Ordering::SeqCst) && COLD_DERIVES.load(Ordering::Relaxed) == 0 {
                for _ in 0..400 {
                    if walks_polled(Self::NAME) >= 4 {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            COLD_DERIVES.fetch_add(1, Ordering::Relaxed);
            Ok(parents.first().copied().unwrap_or(0) + 1)
        }

        fn encode(count: &u64) -> Vec<u8> {
            count.to_le_bytes().to_vec()
        }

        fn decode(bytes: &[u8]) -> Option<u64> {
            bytes.try_into().ok().map(u64::from_le_bytes)
        }
    }

    /// The rule of [`Ancestry`], shared with the model in the tests.
    fn ancestry(id: ChangesetId, parents: Vec<Vec<ChangesetId>>) -> Vec<ChangesetId> {
        let mut seen = HashSet::new();
        std::iter::once(id)
            .chain(parents.into_iter().flatten())
            .filter(|id| seen.insert(*id))
            .collect()
    }

    /// The changes of one changeset: `(path, true)` puts a file, `(path,
    /// false)` deletes it.
    type Edits = &'static [(&'static str, bool)];

    /// A first-parent chain. Changed paths: 2, 4, 5. Puts: 2, 3, 4.
    const HISTORY: [Edits; 3] = [
        &[("a", true), ("b", true)],
        &[("a", false), ("c", true)],
        &[("d", true)],
    ];

    /// A first-parent chain of `n` changesets that put and delete `a` in
    /// turn. Changed paths: 1, 2, 3, ... Puts: 1, 1, 2, 2, ...
    fn chain(n: usize) -> Vec<Edits> {
        const PUT: Edits = &[("a", true)];
        const DELETE: Edits = &[("a", false)];
        (0..n)
            .map(|i| if i % 2 == 0 { PUT } else { DELETE })
            .collect()
    }

    /// A new store with `history` in repo `r`, as [`store_chain`] stores it.
    fn store_with(history: &[Edits]) -> (tempfile::TempDir, Store, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store.write(|c| store_chain(c, history)).unwrap();
        (dir, store, repo, ids)
    }

    /// Stores `history` in a new repo `r` through the one write path, which
    /// stores root trees and the commit graph, and nothing else. Returns the
    /// IDs, oldest first.
    fn store_chain(c: &mut dyn MetaTx, history: &[Edits]) -> Result<(RepoId, Vec<ChangesetId>)> {
        chain_with(c, history, |c, repo, cs| {
            Ok(tree::store_changeset(c, repo, cs)?.0)
        })
    }

    /// Stores `history` in a new repo `r` with `store`. Returns the IDs,
    /// oldest first.
    fn chain_with(
        c: &mut dyn MetaTx,
        history: &[Edits],
        store: impl Fn(&mut dyn MetaTx, RepoId, &Changeset) -> Result<ChangesetId>,
    ) -> Result<(RepoId, Vec<ChangesetId>)> {
        let repo = c.create_repo("r")?;
        let mut ids: Vec<ChangesetId> = Vec::new();
        for edits in history {
            let changes = edits
                .iter()
                .map(|&(path, put)| {
                    let state = put.then(|| FileState::Regular {
                        content: BlobId::of(path.as_bytes()),
                        executable: false,
                    });
                    (RepoPath::new(path).unwrap(), state)
                })
                .collect();
            let cs = Changeset {
                parents: ids.last().copied().into_iter().collect(),
                author: IdentityTime {
                    name: b"a".to_vec(),
                    email: b"a@x".to_vec(),
                    unix_secs: 0,
                    tz_offset_minutes: 0,
                },
                committer: None,
                message: vec![],
                changes,
            };
            ids.push(store(c, repo, &cs)?);
        }
        Ok((repo, ids))
    }

    /// Every `derived` row, as (kind, version, changeset, value).
    fn rows(c: &Connection) -> Vec<(String, u32, Vec<u8>, Vec<u8>)> {
        let mut stmt = c
            .prepare("SELECT kind, version, cs, value FROM derived ORDER BY kind, version, cs")
            .unwrap();
        stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    /// The rows of kind `name` at `version`, as (changeset, value).
    fn rows_of(store: &Store, name: &str, version: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let all = store.read(|c| Ok(rows(c))).unwrap();
        all.into_iter()
            .filter(|(kind, v, ..)| kind == name && *v == version)
            .map(|(_, _, cs, value)| (cs, value))
            .collect()
    }

    /// The rows of the test kind at `version`, as (changeset, value).
    fn kind_rows(store: &Store, version: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        rows_of(store, ChangedPathsV1::NAME, version)
    }

    #[test]
    fn a_new_kind_is_derived_on_first_read_and_stored() {
        let (_dir, store, repo, ids) = store_with(&HISTORY);
        assert!(kind_rows(&store, 1).is_empty(), "nothing derived yet");

        DERIVES.set(0);
        let start = store.blobs().full_flushes();
        let flushes = || store.blobs().full_flushes() - start;
        assert_eq!(get::<ChangedPathsV1>(&store, repo, ids[2]).unwrap(), 5);
        assert_eq!(DERIVES.get(), 3, "each changeset once, from the root");
        // None where the commit's own fsync flushes the drive cache.
        let one = u64::from(FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(flushes(), one, "durable before get returns");
        let counts: Vec<u64> = ids
            .iter()
            .map(|&cs| get::<ChangedPathsV1>(&store, repo, cs).unwrap())
            .collect();
        assert_eq!(counts, [2, 4, 5]);
        assert_eq!(DERIVES.get(), 3, "later reads use the stored values");
        assert_eq!(flushes(), one, "and run no flush");
        let stored = kind_rows(&store, 1);
        assert_eq!(stored.len(), 3);
        assert!(stored.iter().all(|(_, value)| value.len() == 8));

        // A stored value does not leak to a repo without the changeset.
        let other = store.write(|c| c.create_repo("other")).unwrap();
        let tip = ids[2];
        assert!(matches!(
            get::<ChangedPathsV1>(&store, other, tip),
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            store.commit(|c| derive::<ChangedPathsV1>(c, other, tip)),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn a_version_bump_derives_every_value_again() {
        let (_dir, store, repo, ids) = store_with(&HISTORY);
        let tip = ids[2];
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 5);

        DERIVES.set(0);
        assert_eq!(get::<ChangedPathsV2>(&store, repo, tip).unwrap(), 4);
        assert_eq!(DERIVES.get(), 3, "version 2 derives every value again");
        assert_eq!(kind_rows(&store, 2).len(), 3);
        assert!(
            kind_rows(&store, 1).is_empty(),
            "its steps removed version 1"
        );
    }

    #[test]
    fn a_version_2_database_upgrades_and_keeps_its_derived_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (repo, ids, before) = {
            let c = Connection::open(dir.path().join("scion.db")).unwrap();
            migrate(&c, 2).unwrap();
            // The write path of version 2 stored root trees only.
            let mut tx = SqliteTx(&c);
            let (repo, ids) = chain_with(&mut tx, &HISTORY, |c, repo, cs| {
                let id = c.insert_changeset(repo, cs)?;
                tree::derive_root_tree(c, repo, id)?;
                Ok(id)
            })
            .unwrap();
            (repo, ids, rows(&c))
        };
        // The rows that version 2 holds: a root tree for each changeset.
        // Their kind and version must stay, or every root tree is derived
        // again.
        assert_eq!(before.len(), 3);
        assert!(
            before
                .iter()
                .all(|(kind, v, ..)| kind == RootTree::NAME && *v == RootTree::VERSION)
        );

        let store = Store::open(dir.path()).unwrap();
        let version: u32 = store
            .read(|c| Ok(c.pragma_query_value(None, "user_version", |row| row.get(0))?))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        assert_eq!(store.read(|c| Ok(rows(c))).unwrap(), before);
        // Each root tree is read from its copied row.
        for &cs in &ids {
            let tree = tree::root_tree(&store, repo, cs).unwrap();
            let row = (
                RootTree::NAME.to_owned(),
                RootTree::VERSION,
                cs.as_bytes().to_vec(),
                tree.as_bytes().to_vec(),
            );
            assert!(before.contains(&row), "{cs}");
        }
        // The table now holds a value that is not 32 bytes.
        assert_eq!(get::<ChangedPathsV1>(&store, repo, ids[2]).unwrap(), 5);
    }

    /// How many root tree rows the store holds.
    fn root_trees(store: &Store) -> usize {
        let all = store.read(|c| Ok(rows(c))).unwrap();
        all.iter()
            .filter(|(kind, ..)| kind == RootTree::NAME)
            .count()
    }

    #[test]
    fn a_long_history_is_derived_in_steps() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let tip = ids[6];

        // One step of the walk goes through at most STEP changesets.
        let walk = store
            .read(|c| {
                let ControlFlow::Continue(mut walk) = Walk::start::<ChangedPathsV1>(c, repo, tip)?
                else {
                    panic!("no value is stored yet");
                };
                walk.step::<ChangedPathsV1>(c, STEP)?;
                Ok(walk)
            })
            .unwrap();
        let next: Vec<ChangesetId> = walk.next.iter().map(|&(id, _)| id).collect();
        assert_eq!(next, [ids[6 - STEP]]);
        let newest: Vec<ChangesetId> = ids.iter().rev().take(STEP).copied().collect();
        let pending: Vec<ChangesetId> = walk.pending.iter().map(|&(_, id)| id).collect();
        assert_eq!(pending, newest);

        DERIVES.set(0);
        let commits = store.commits();
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 7);
        assert_eq!(DERIVES.get(), 7, "each changeset once");
        assert_eq!(store.commits() - commits, steps(7), "one per step");
        // Each step went on from the value of the step before it.
        for (count, &cs) in (1..).zip(&ids) {
            assert_eq!(get::<ChangedPathsV1>(&store, repo, cs).unwrap(), count);
        }
        assert_eq!(DERIVES.get(), 7);
    }

    #[test]
    fn an_error_in_a_step_keeps_the_steps_before_it() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let tip = ids[6];
        // The third step fails, as a crash after the second commit would.
        // The error waits until the steps before it are durable.
        FAIL_AT.set(Some(ids[2 * STEP]));
        let flushes = store.blobs().full_flushes();
        let failed = get::<ChangedPathsV1>(&store, repo, tip);
        FAIL_AT.set(None);
        assert!(matches!(failed, Err(Error::Corrupt(_))), "{failed:?}");
        let one = u64::from(FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(store.blobs().full_flushes() - flushes, one);

        // Only the rows of the first two steps, each with its right value.
        let mut kept: Vec<(Vec<u8>, Vec<u8>)> = (1_u64..)
            .zip(&ids[..2 * STEP])
            .map(|(count, cs)| (cs.as_bytes().to_vec(), count.to_le_bytes().to_vec()))
            .collect();
        kept.sort();
        assert_eq!(kind_rows(&store, 1), kept);

        DERIVES.set(0);
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 7);
        assert_eq!(DERIVES.get(), 3, "the next read derives only the rest");
    }

    #[test]
    fn a_step_reads_the_values_that_another_fill_stored() {
        let (_dir, store, repo, ids) = store_with(&chain(5));
        let tip = ids[4];
        // One fill walks. Another fill derives the history before the first
        // one's steps run.
        let walked = walk_all::<ChangedPathsV1>(&store, repo, tip, || Ok(()));
        let ControlFlow::Continue(pending) = walked.unwrap() else {
            panic!("no value is stored yet");
        };
        assert_eq!(pending, ids);
        DERIVES.set(0);
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 5);
        assert_eq!(DERIVES.get(), 5);

        let late = derive_steps::<ChangedPathsV1>(&store, repo, &pending, || Ok(())).unwrap();
        assert_eq!(late, Some(5));
        assert_eq!(DERIVES.get(), 5, "the late steps derive nothing again");
    }

    #[test]
    fn a_fill_with_a_deadline_stops_between_steps() {
        let past = Deadline::new(std::time::Instant::now());
        // A history of one step is walked in one go, and the deadline stops
        // the first derivation step. A longer one stops in its walk.
        for n in [STEP, 7] {
            let (_dir, store, repo, ids) = store_with(&chain(n));
            let tip = ids[n - 1];
            DERIVES.set(0);
            let late = get_by::<ChangedPathsV1>(&store, repo, tip, &past);
            assert!(
                matches!(late, Err(Error::DeadlineExceeded(_))),
                "{n}: {late:?}"
            );
            let late = fill_by::<ChangedPathsV1>(&store, repo, tip, &past);
            assert!(
                matches!(late, Err(Error::DeadlineExceeded(_))),
                "{n}: {late:?}"
            );
            assert_eq!(DERIVES.get(), 0, "{n}");
            assert!(kind_rows(&store, 1).is_empty(), "{n}");
            assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), n as u64);
        }
    }

    #[test]
    fn a_step_removes_rows_of_older_versions_only() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        assert_eq!(get::<ChangedPathsV1>(&store, repo, ids[6]).unwrap(), 7);
        assert_eq!(kind_rows(&store, 1).len(), 7);
        assert_eq!(root_trees(&store), 7);

        // The first step of version 2 derives two values and removes two
        // rows of version 1.
        assert_eq!(get::<ChangedPathsV2>(&store, repo, ids[1]).unwrap(), 1);
        assert_eq!(kind_rows(&store, 2).len(), STEP);
        assert_eq!(kind_rows(&store, 1).len(), 7 - STEP);

        // Version 1 removes neither its own rows nor the rows of version 2,
        // which is newer. No step removes the rows of another kind.
        assert_eq!(
            store.commit(|c| remove_old::<ChangedPathsV1>(c)).unwrap(),
            0
        );
        assert_eq!(kind_rows(&store, 1).len(), 7 - STEP);
        assert_eq!(kind_rows(&store, 2).len(), STEP);
        assert_eq!(root_trees(&store), 7);
    }

    #[test]
    fn a_backfill_derives_a_history_and_removes_every_older_row() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let tip = ids[6];
        backfill::<ChangedPathsV2>(&store, repo, tip).unwrap();
        assert_eq!(kind_rows(&store, 2).len(), 7);
        // A build with version 1 derives its own rows and keeps version 2.
        assert_eq!(get::<ChangedPathsV1>(&store, repo, tip).unwrap(), 7);
        assert_eq!(kind_rows(&store, 1).len(), 7);
        assert_eq!(kind_rows(&store, 2).len(), 7);

        // Back on version 2, nothing is left to derive. The backfill removes
        // every row of version 1, at most STEP per transaction, and returns
        // once that is durable.
        DERIVES.set(0);
        let commits = store.commits();
        let flushes = store.blobs().full_flushes();
        backfill::<ChangedPathsV2>(&store, repo, tip).unwrap();
        assert_eq!(DERIVES.get(), 0);
        assert!(kind_rows(&store, 1).is_empty(), "version 1 is removed");
        assert_eq!(store.commits() - commits, steps(7));
        let one = u64::from(FSYNC_LEAVES_DRIVE_CACHE);
        assert_eq!(store.blobs().full_flushes() - flushes, one);
        assert_eq!(kind_rows(&store, 2).len(), 7);
        assert_eq!(root_trees(&store), 7);
        assert_eq!(get::<ChangedPathsV2>(&store, repo, tip).unwrap(), 4);
    }

    #[test]
    fn a_write_removes_root_trees_of_older_versions() {
        let (_dir, store, repo, ids) = store_with(&chain(7));
        let older = "INSERT INTO derived (kind, version, cs, value)
                     SELECT kind, 0, cs, value FROM derived WHERE kind = ?1";
        let added = store
            .write(|c| Ok(c.execute(older, [RootTree::NAME])?))
            .unwrap();
        assert_eq!(added, 7);

        let cs = Changeset {
            parents: vec![ids[6]],
            ..store.read(|c| c.get_changeset(repo, ids[0])).unwrap()
        };
        store
            .write(|c| tree::store_changeset(c, repo, &cs))
            .unwrap();
        // The new row of version 1, and seven rows of version 0 less one
        // step.
        assert_eq!(root_trees(&store), 7 + 1 + 7 - STEP);
    }

    /// A new store with the graph `parents` in repo `r`, with no derived
    /// rows. Each changeset puts one file.
    fn store_graph(parents: &[Vec<usize>]) -> (tempfile::TempDir, Store, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (repo, ids) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                Ok((repo, testing::insert_graph(c, repo, parents)?))
            })
            .unwrap();
        (dir, store, repo, ids)
    }

    /// The value of [`Ancestry`] for each changeset of `parents`, by the
    /// same rule, without the store.
    fn ancestry_model(parents: &[Vec<usize>], ids: &[ChangesetId]) -> Vec<Vec<ChangesetId>> {
        let mut model: Vec<Vec<ChangesetId>> = Vec::new();
        for (i, of) in parents.iter().enumerate() {
            let values = of.iter().map(|&p| model[p].clone()).collect();
            model.push(ancestry(ids[i], values));
        }
        model
    }

    /// Two roots, a criss-cross (3 and 4 merge 1 and 2 in both orders), and
    /// a merge of the second root.
    fn criss_cross() -> Vec<Vec<usize>> {
        vec![
            vec![],
            vec![0],
            vec![0],
            vec![1, 2],
            vec![2, 1],
            vec![3, 4],
            vec![],
            vec![5, 6],
        ]
    }

    #[test]
    fn a_kind_over_every_parent_derives_the_graph_parents_first() {
        let parents = criss_cross();
        let (_dir, store, repo, ids) = store_graph(&parents);
        let model = ancestry_model(&parents, &ids);
        let tip = ids[7];

        // One step of the walk visits at most STEP changesets.
        let walk = store
            .read(|c| {
                let ControlFlow::Continue(mut walk) = Walk::start::<Ancestry>(c, repo, tip)? else {
                    panic!("no value is stored yet");
                };
                walk.step::<Ancestry>(c, STEP)?;
                Ok(walk)
            })
            .unwrap();
        assert_eq!(walk.pending.len(), STEP);

        DERIVES.set(0);
        let commits = store.commits();
        assert_eq!(get::<Ancestry>(&store, repo, tip).unwrap(), model[7]);
        assert_eq!(DERIVES.get(), 8, "each changeset once");
        assert_eq!(store.commits() - commits, steps(8), "one per step");
        for (i, &cs) in ids.iter().enumerate() {
            assert_eq!(get::<Ancestry>(&store, repo, cs).unwrap(), model[i]);
        }
        assert_eq!(DERIVES.get(), 8, "later reads use the stored values");

        // A first-parent kind derives only the first-parent history: 7, 5,
        // 3, 1, 0.
        DERIVES.set(0);
        get::<ChangedPathsV1>(&store, repo, tip).unwrap();
        assert_eq!(DERIVES.get(), 5);
        assert_eq!(kind_rows(&store, 1).len(), 5);
    }

    #[test]
    fn a_write_stores_a_value_only_when_its_parents_have_one() {
        let parents = criss_cross();
        let (_dir, store, repo, ids) = store_graph(&parents);
        let model = ancestry_model(&parents, &ids);
        let stored = |cs| store.read(|c| load::<Ancestry>(c, cs)).unwrap();

        // The parents of 3 have no value yet.
        let on_write = |cs: ChangesetId| {
            store
                .write(|c| {
                    let cs_row = c.get_changeset(repo, cs)?;
                    on_write::<Ancestry>(c, cs, &cs_row)
                })
                .unwrap();
        };
        on_write(ids[3]);
        assert_eq!(stored(ids[3]), None);
        // A root needs no other value, and then 1 and 2 have what they need.
        on_write(ids[0]);
        on_write(ids[1]);
        on_write(ids[2]);
        on_write(ids[3]);
        for i in 0..4 {
            assert_eq!(stored(ids[i]).as_ref(), Some(&model[i]), "{i}");
        }
        assert_eq!(stored(ids[4]), None);
    }

    proptest::proptest! {
        /// On any graph, a kind over every parent gets the value of each
        /// parent, in order, and derives each ancestor once, in steps.
        #[test]
        fn a_kind_over_every_parent_matches_a_model(
            parents in testing::graphs(24),
            starts in proptest::collection::vec(proptest::prelude::any::<proptest::sample::Index>(), 1..4),
        ) {
            let (_dir, store, repo, ids) = store_graph(&parents);
            let model = ancestry_model(&parents, &ids);
            let mut derived: HashMap<ChangesetId, usize> = HashMap::new();
            for start in starts {
                let i = start.index(ids.len());
                let missing = model[i].iter().filter(|id| !derived.contains_key(id)).count();
                DERIVES.set(0);
                let commits = store.commits();
                proptest::prop_assert_eq!(&get::<Ancestry>(&store, repo, ids[i]).unwrap(), &model[i]);
                proptest::prop_assert_eq!(DERIVES.get(), missing);
                proptest::prop_assert_eq!(store.commits() - commits, steps(missing));
                derived.extend(model[i].iter().map(|&id| (id, i)));
            }
        }
    }

    /// The flight tests share the counters and sleeps, so they run one at a
    /// time.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// A second repo with a chain of `n`, beside the one `store_with` made.
    /// Its author differs, so its changeset IDs differ from the first repo's:
    /// a changeset ID is the hash of its content, and equal content would
    /// make the two repos share their derived rows.
    fn another_chain(store: &Store, history: &[Edits]) -> (RepoId, Vec<ChangesetId>) {
        store
            .write(|c| {
                let repo = c.create_repo("s")?;
                let mut ids: Vec<ChangesetId> = Vec::new();
                for edits in history {
                    let changes = edits
                        .iter()
                        .map(|&(path, put)| {
                            let state = put.then(|| FileState::Regular {
                                content: BlobId::of(path.as_bytes()),
                                executable: false,
                            });
                            (RepoPath::new(path).unwrap(), state)
                        })
                        .collect();
                    let cs = Changeset {
                        parents: ids.last().copied().into_iter().collect(),
                        author: IdentityTime {
                            name: b"b".to_vec(),
                            email: b"b@x".to_vec(),
                            unix_secs: 0,
                            tz_offset_minutes: 0,
                        },
                        committer: None,
                        message: vec![],
                        changes,
                    };
                    ids.push(tree::store_changeset(c, repo, &cs)?.0);
                }
                Ok((repo, ids))
            })
            .unwrap()
    }

    #[test]
    fn n_cold_reads_derive_once() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        COLD_SLEEP.store(50, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(12));
        let tip = *ids.last().unwrap();
        // Four cold reads of the tip at once, all starting together: they
        // join one walk, which derives each changeset once. The gate in
        // the first derive holds the walk until all four callers have
        // looked for the value, so the round's ownership is decided with
        // them all inside: a caller that walked on its own would own
        // another walk, and load cannot change that count.
        walks_owned_reset("cold");
        walks_polled_reset("cold");
        POLLED_GATE.store(true, Ordering::SeqCst);
        let at_the_gate = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    at_the_gate.wait();
                    assert_eq!(get::<Cold<1>>(&store, repo, tip).unwrap(), 12);
                });
            }
        });
        POLLED_GATE.store(false, Ordering::SeqCst);
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
        assert_eq!(
            walks_owned("cold"),
            1,
            "the four cold reads did not share one walk"
        );
        COLD_SLEEP.store(0, Ordering::Relaxed);
    }

    #[test]
    fn a_caller_over_the_cap_waits_then_succeeds() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        COLD_SLEEP.store(100, Ordering::Relaxed);
        // The owner's walk sleeps in every derive: 12 of them, so the one
        // permit stays taken for more than a second, whatever the machine's
        // load does to a thread's start.
        let (_dir, store, repo, ids) = store_with(&chain(12));
        let (repo2, ids2) = another_chain(&store, &chain(3));
        let tip1 = *ids.last().unwrap();
        let tip2 = *ids2.last().unwrap();

        // The first caller walks, and holds the one permit while it does.
        let owner = std::thread::scope(|scope| {
            let owner = scope.spawn(|| {
                set_walk_cap(1);
                get::<Cold<1>>(&store, repo, tip1).unwrap()
            });
            // The real condition, not a clock: the owner's walk has stored
            // the second changeset, so it holds the one permit with 10
            // sleeping derives to go, more than a second of walking. Every
            // walk takes a permit from the one counter, so while the owner
            // walks, no walk is free, whatever else runs.
            let second = ids[1];
            let mut walking = false;
            for _ in 0..1_000 {
                let stored = store
                    .read(|c| load::<Cold<1>>(c, second))
                    .expect("the store reads");
                if stored.is_some() {
                    walking = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(walking, "the owner never got past its first derive");
            // A caller of another repo waits for the permit, then walks.
            let waited = scope.spawn(|| {
                set_walk_cap(1);
                let at = std::time::Instant::now();
                assert_eq!(get::<Cold<1>>(&store, repo2, tip2).unwrap(), 3);
                at.elapsed() >= Duration::from_millis(500)
            });
            // A caller with a deadline gives up: the one walk is taken, and
            // the owner's walk outlasts this deadline by far.
            scope.spawn(|| {
                set_walk_cap(1);
                let deadline = Deadline::new(Instant::now() + Duration::from_millis(200));
                assert!(
                    matches!(
                        get_by::<Cold<1>>(&store, repo2, ids2[0], &deadline),
                        Err(Error::ResourceExhausted(_))
                    ),
                    "the caller with a deadline did not give up"
                );
            });
            owner.join().unwrap();
            waited.join().unwrap()
        });
        assert!(owner, "the waiting caller walked after the first one");
        COLD_SLEEP.store(0, Ordering::Relaxed);
    }

    #[test]
    fn a_waiter_that_leaves_does_not_trap_the_next_caller() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        COLD_SLEEP.store(100, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(6));
        let tip = *ids.last().unwrap();
        // The owner's walk stops at a step boundary past its deadline, with
        // the last changesets still missing.
        let owner = std::thread::scope(|scope| {
            let owner = scope.spawn(|| {
                let deadline = Deadline::new(Instant::now() + Duration::from_millis(300));
                get_by::<Cold<1>>(&store, repo, tip, &deadline)
            });
            // A waiter of a changeset the stopped walk DID store: it wakes
            // when the walk ends, reads its value, and leaves without
            // walking. A caller that comes after it must not join the flight
            // it held: that walk is over, and the new caller owns a fresh
            // one. A flight that a waiter's exit leaves behind would hold
            // the new caller until its deadline, with nobody to wake it.
            let waiter = scope.spawn(|| {
                let deadline = Deadline::new(Instant::now() + Duration::from_secs(5));
                get_by::<Cold<1>>(&store, repo, ids[1], &deadline).unwrap()
            });
            let owner = owner.join().unwrap();
            let waiter = waiter.join().unwrap();
            // The next caller reads the tip, which the stopped walk did not
            // store: it owns a fresh walk and finishes the chain.
            let deadline = Deadline::new(Instant::now() + Duration::from_secs(5));
            let value = get_by::<Cold<1>>(&store, repo, tip, &deadline).unwrap();
            assert_eq!(value, 6);
            (owner, waiter)
        });
        assert!(
            matches!(owner.0, Err(Error::DeadlineExceeded(_))),
            "{owner:?}"
        );
        assert_eq!(owner.1, 2);
        COLD_SLEEP.store(0, Ordering::Relaxed);
    }

    #[test]
    fn a_stopped_walk_hands_over() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        COLD_SLEEP.store(50, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(12));
        let tip = *ids.last().unwrap();
        // The owner's walk stops at a step boundary when its deadline passes;
        // the rows it stored stay. The waiter then walks only what is left.
        let owner = std::thread::scope(|scope| {
            let owner = scope.spawn(|| {
                let deadline = Deadline::new(Instant::now() + Duration::from_millis(200));
                get_by::<Cold<1>>(&store, repo, tip, &deadline)
            });
            std::thread::sleep(Duration::from_millis(50));
            let waiter = scope.spawn(|| get::<Cold<1>>(&store, repo, tip).unwrap());
            let owner = owner.join().unwrap();
            assert_eq!(waiter.join().unwrap(), 12);
            owner
        });
        assert!(
            matches!(owner, Err(Error::DeadlineExceeded(_))),
            "{owner:?}"
        );
        // Every changeset was derived once: the walk resumed, it did not redo.
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
    }
}

//! Derived data: values computed from changesets, not part of them.
//!
//! One mechanism for every kind. A kind has a name, a version, and a
//! function from a changeset and its parents' values to a value. A kind reads
//! the value of its first parent only, or of every parent
//! (`ALL_PARENTS`). A value is computed when it is first needed and
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

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Instant;

use scion_core::{Changeset, ChangesetId};

use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::error::{Error, Result};
use crate::meta::Meta as _;
use crate::meta::{MetaTx, RepoId};

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
pub(crate) trait Kind {
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
    /// reaches through those parents (`load`). Each one has a value.
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
        lens: crate::tree::BlobLens<'_>,
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
/// [`Kind::derive`], or of [`Store::flush`](crate::store::Store::flush).
/// PROBE BRANCH ONLY: a breadcrumb for the hang hunt. Prints one line per
/// call when SCION_PROBE is set; silent otherwise.
fn probe(site: &str, msg: impl std::fmt::Display) {
    if std::env::var_os("SCION_PROBE").is_some() {
        let t = std::thread::current();
        eprintln!("[probe] {site} tid={:?} name={:?} {msg}", t.id(), t.name());
    }
}

pub(crate) fn get<K: Kind>(store: &Backend, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    get_until::<K>(store, repo, cs, None)
}

/// [`get`], but if it derives, it stops between steps once `deadline` has
/// passed. The steps before it stay, so the next call goes on from there.
///
/// # Errors
///
/// As [`get`], and [`Error::DeadlineExceeded`].
pub(crate) fn get_by<K: Kind>(
    store: &Backend,
    repo: RepoId,
    cs: ChangesetId,
    deadline: &Deadline,
) -> Result<K::Value> {
    get_until::<K>(store, repo, cs, Some(deadline))
}

fn get_until<K: Kind>(
    store: &Backend,
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

/// The derive walks of one [`Store`]: which are in flight, and the cap on
/// how many may be. Every `Store` in the process keeps its own, so repo 1
/// of one store never joins the walk of repo 1 of another.
pub(crate) struct DeriveFlights {
    /// The walks in flight, one per kind, version and repo: N cold reads
    /// of one history wait for one walk instead of repeating its work
    /// ([#142]).
    flights: Mutex<HashMap<FlightKey, Arc<Flight>>>,
    /// Walks in flight, with the condvar a caller over the cap waits on.
    in_flight: Mutex<usize>,
    woken: Condvar,
    /// The cap on walks in flight. Each walk holds the write connection in
    /// turns, so walks beyond the cap only repeat work and starve writes.
    walks_cap: usize,
}

impl DeriveFlights {
    pub(crate) fn new(walks_cap: usize) -> Self {
        Self {
            flights: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(0),
            woken: Condvar::new(),
            walks_cap,
        }
    }
}

/// A walk of a kind, version and repo in flight.
struct Flight {
    /// How many walks of this key have ended. A waiter that wakes and sees
    /// the count unchanged keeps waiting.
    walks: Mutex<u64>,
    /// Woken when a walk ends, for whatever reason.
    woken: Condvar,
}

/// Held while a walk derives. Drop it, and a waiting owner takes it.
struct Permit<'a> {
    in_flight: &'a Mutex<usize>,
    woken: &'a Condvar,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *in_flight -= 1;
        self.woken.notify_all();
    }
}

/// Takes a walk permit, or waits for one until the deadline, then gives up
/// with [`Error::ResourceExhausted`]. Without a deadline it waits.
fn take_walk_permit<'a>(
    walks: &'a DeriveFlights,
    deadline: Option<&Deadline>,
) -> Result<Permit<'a>> {
    probe("derive.permit", "enter");
    let mut in_flight = walks
        .in_flight
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    loop {
        if *in_flight < walks.walks_cap {
            *in_flight += 1;
            return Ok(Permit {
                in_flight: &walks.in_flight,
                woken: &walks.woken,
            });
        }
        match deadline {
            None => {
                in_flight = walks
                    .woken
                    .wait(in_flight)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            Some(deadline) => {
                let budget = deadline.at().saturating_duration_since(Instant::now());
                let (guard, result) = walks
                    .woken
                    .wait_timeout_while(in_flight, budget, |in_flight| {
                        *in_flight >= walks.walks_cap
                    })
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
/// Joins the walk of `key`, or owns it: the caller that finds no flight
/// running owns the next walk, and the others wait for it.
fn join<K: Kind>(walks: &DeriveFlights, repo: RepoId) -> (Arc<Flight>, bool) {
    let mut flights = walks.flights.lock().unwrap_or_else(PoisonError::into_inner);
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
/// owns the next walk itself.
fn retire(walks: &DeriveFlights, flight: &Arc<Flight>) {
    let mut flights = walks.flights.lock().unwrap_or_else(PoisonError::into_inner);
    flights.retain(|_, running| !Arc::ptr_eq(running, flight));
    let mut walks = flight.walks.lock().unwrap_or_else(PoisonError::into_inner);
    *walks += 1;
    flight.woken.notify_all();
}

/// Waits for the walk of `flight` to end, until the deadline. False when
/// the deadline passed first.
fn waited(flight: &Arc<Flight>, before: u64, deadline: Option<&Deadline>) -> bool {
    let mut walks = flight.walks.lock().unwrap_or_else(PoisonError::into_inner);
    probe("derive.waited", format!("enter before={before}"));
    loop {
        if *walks > before {
            probe("derive.waited", format!("exit true walks={walks}"));
            return true;
        }
        match deadline {
            None => {
                walks = flight
                    .woken
                    .wait(walks)
                    .unwrap_or_else(PoisonError::into_inner);
                probe("derive.waited", format!("woken walks={walks}"));
            }
            Some(deadline) => {
                let budget = deadline.at().saturating_duration_since(Instant::now());
                let (guard, result) = flight
                    .woken
                    .wait_timeout_while(walks, budget, |walks| *walks <= before)
                    .unwrap_or_else(PoisonError::into_inner);
                walks = guard;
                if result.timed_out() {
                    probe("derive.waited", "exit false (timeout)");
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
    store: &Backend,
    repo: RepoId,
    cs: ChangesetId,
    deadline: Option<&Deadline>,
) -> Result<K::Value> {
    let walks = store.derive_flights();
    loop {
        // The value may be there already: a walk of this key that ended
        // while this caller waited stored it, or it was never missing.
        let known = store.read(|c| load::<K>(c, cs))?;
        if let Some(value) = known {
            probe("derive.walk", "value known");
            return Ok(value);
        }
        // Join first: a waiter holds no walk permit, so callers that wait
        // for a running walk never take a permit from a walk of another
        // repo or kind ([#142]).
        let (flight, owner) = join::<K>(walks, repo);
        probe("derive.walk", format!("joined owner={owner}"));
        if owner {
            // The walk's permit: walks beyond the cap only repeat work,
            // and starve the writes. Wait for one until the deadline.
            let _permit = take_walk_permit(walks, deadline)?;
            probe("derive.walk", "permit taken, walking");
            let value = fill_until::<K>(store, repo, cs, deadline);
            probe("derive.walk", "walk done, retiring");
            retire(walks, &flight);
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
/// durable: call [`Store::flush`](crate::store::Store::flush) before replying with anything they name.
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
pub(crate) fn fill<K: Kind>(store: &Backend, repo: RepoId, cs: ChangesetId) -> Result<K::Value> {
    fill_until::<K>(store, repo, cs, None)
}

/// [`fill`], but it stops between steps once `deadline` has passed. The
/// steps before it stay, so the next call goes on from there.
///
/// # Errors
///
/// As [`fill`], and [`Error::DeadlineExceeded`].
pub(crate) fn fill_by<K: Kind>(
    store: &Backend,
    repo: RepoId,
    cs: ChangesetId,
    deadline: &Deadline,
) -> Result<K::Value> {
    fill_until::<K>(store, repo, cs, Some(deadline))
}

fn fill_until<K: Kind>(
    store: &Backend,
    repo: RepoId,
    cs: ChangesetId,
    deadline: Option<&Deadline>,
) -> Result<K::Value> {
    let check = || deadline.map_or(Ok(()), Deadline::check);
    let pending = match walk_all::<K>(store, repo, cs, check)? {
        ControlFlow::Break(value) => return Ok(value),
        ControlFlow::Continue(pending) => pending,
    };
    derive_steps::<K>(store, repo, &pending, deadline, check)?
        .ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Fills `K` for `cs` and its history, then removes every
/// row of an older version of `K`, at most [`STEP`] rows per transaction.
/// Returns once all of it is durable.
///
/// # Errors
///
/// Errors as for the fill, and of [`Store::flush`](crate::store::Store::flush).
/// Derives the kind of `cs`, and removes the rows of older versions.
/// Returns the kind's name, which the `derive` command reports. The kinds
/// a user derives are the ones the server ships, each with its own `pub`
/// fn below, so the crate-private trait never shows in a signature.
pub(crate) fn backfill<K: Kind>(
    store: &Backend,
    repo: &str,
    cs: ChangesetId,
) -> Result<&'static str> {
    use crate::meta::Meta as _;
    let repo = store.read(|c| c.repo_id(repo))?;
    fill::<K>(store, repo, cs)?;
    // A short step means that no older row is left.
    while store.commit(|c| remove_old::<K>(c))? == STEP {}
    store.flush()?;
    Ok(K::NAME)
}

/// Derives the value of `K` for `cs` inside the caller's transaction, and
/// stores every value it derives. It walks and derives as [`fill`] does,
/// but with no bound, in one transaction.
///
/// # Errors
///
/// [`Error::NotFound`] if `repo` does not have `cs`. Errors of
/// [`Kind::derive`].
pub(crate) fn derive<K: Kind>(
    c: &mut dyn MetaTx,
    repo: RepoId,
    cs: ChangesetId,
    lens: crate::tree::BlobLens<'_>,
) -> Result<K::Value> {
    let mut walk = match Walk::start::<K>(c, repo, cs)? {
        ControlFlow::Break(value) => return Ok(value),
        ControlFlow::Continue(walk) => walk,
    };
    walk.step::<K>(c, usize::MAX)?;
    derive_chain::<K>(c, repo, &walk.into_pending(), lens)?
        .ok_or_else(|| Error::NotFound(format!("changeset {cs}")))
}

/// Stores the value of `K` for `cs`. A second write must agree with the
/// first.
///
/// # Errors
///
/// [`Error::Corrupt`] if a different value is already stored.
pub(crate) fn put<K: Kind>(c: &mut dyn MetaTx, cs: ChangesetId, value: &K::Value) -> Result<()> {
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
pub(crate) fn on_write<K: Kind>(
    c: &mut dyn MetaTx,
    id: ChangesetId,
    changeset: &Changeset,
    lens: crate::tree::BlobLens<'_>,
) -> Result<()> {
    if load::<K>(c, id)?.is_none()
        && let Some(parents) = parent_values::<K>(c, changeset)?
    {
        let value = K::derive(c, id, changeset, parents, lens)?;
        put::<K>(c, id, &value)?;
    }
    remove_old::<K>(c)?;
    Ok(())
}

/// The walk of [`fill`]: the stored value for `cs`, or else the changesets
/// to derive, parents first. Runs `check` before each step after the first.
fn walk_all<K: Kind>(
    store: &Backend,
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
    store: &Backend,
    repo: RepoId,
    pending: &[ChangesetId],
    deadline: Option<&Deadline>,
    check: impl Fn() -> Result<()>,
) -> Result<Option<K::Value>> {
    let mut value = None;
    for step in pending.chunks(STEP) {
        check()?;
        value = store.commit(|c| {
            let value = derive_chain::<K>(c, repo, step, &mut crate::tree::lens_of(store))?;
            remove_old::<K>(c)?;
            Ok(value)
        })?;
        // A writer that waited for the connection while this step held it
        // gets in before the next step takes it again: the walk waits
        // while writers wait, so the mutex handoff is deterministic, not
        // a race the walker wins under load ([#142]).
        store.wait_for_writers(deadline)?;
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
    lens: crate::tree::BlobLens<'_>,
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
        let next = K::derive(c, id, &changeset, parents, lens)?;
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
pub(crate) fn remove_old<K: Kind>(c: &mut dyn MetaTx) -> Result<usize> {
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
    use crate::backend::Backend;
    use crate::db::SqliteTx;
    use crate::store::Store;
    use rusqlite::Connection;
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
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
            _lens: crate::tree::BlobLens<'_>,
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
            _lens: crate::tree::BlobLens<'_>,
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

    /// Milliseconds that one derive of the flight kind sleeps, after the
    /// gate. Only the land test sets it: its walkers hold the write
    /// connection while they sleep, so a write that arrives then has to
    /// wait for the walkers' transactions to end.
    static COLD_SLEEP: AtomicUsize = AtomicUsize::new(0);

    /// How many derives of the flight kind have entered the gate. A test
    /// that closes the gate waits for a count, so it knows its walking
    /// owners are inside, with their walk permits taken.
    static COLD_ENTERED: AtomicUsize = AtomicUsize::new(0);

    /// The derive count that a writer's transaction saw when it got the
    /// write connection: the writer-priority test reads it to tell a walk
    /// that waited from a walk that re-took the connection first.
    static DERIVES_AT_WRITE: AtomicUsize = AtomicUsize::new(0);

    /// The gate of the flight tests: a closed gate holds each derive until
    /// the test releases it, one release per derive, and an open gate lets
    /// every derive go at once. A test that closes it holds the walking
    /// owner inside its derive, with the walk permit taken, so the other
    /// callers' orders are forced, not timed.
    static COLD_GATE: Mutex<(bool, u64)> = Mutex::new((true, 0));
    /// Woken when the gate opens or releases.
    static COLD_GATE_WOKEN: Condvar = Condvar::new();

    /// Closes the gate, and empties its releases.
    fn gate_close() {
        *COLD_GATE.lock().unwrap_or_else(PoisonError::into_inner) = (false, 0);
        COLD_ENTERED.store(0, Ordering::SeqCst);
    }

    /// Opens the gate when dropped, even on a panic: a test that fails
    /// must not leave walkers blocked at the gate, or the failing test
    /// would hang instead of reporting.
    struct GateGuard;

    impl Drop for GateGuard {
        fn drop(&mut self) {
            gate_open();
        }
    }

    /// Closes the gate, and returns the guard that opens it again.
    fn gate_closed() -> GateGuard {
        gate_close();
        GateGuard
    }

    /// Lets `n` derives through.
    fn gate_release(n: u64) {
        let mut gate = COLD_GATE.lock().unwrap_or_else(PoisonError::into_inner);
        gate.1 += n;
        COLD_GATE_WOKEN.notify_all();
    }

    /// Opens the gate for good, and empties its releases.
    fn gate_open() {
        let mut gate = COLD_GATE.lock().unwrap_or_else(PoisonError::into_inner);
        *gate = (true, 0);
        COLD_GATE_WOKEN.notify_all();
    }

    /// Waits until `n` derives of the flight kind are inside the gate,
    /// with a generous ceiling: a bug is a timeout, not a hang without a
    /// bound.
    fn wait_entered(n: usize) {
        for _ in 0..12_000 {
            if COLD_ENTERED.load(Ordering::SeqCst) >= n {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!(
            "only {} walks reached the gate",
            COLD_ENTERED.load(Ordering::SeqCst)
        );
    }

    /// A first-parent test kind for the flight tests: 1 + the parent's
    /// value. Each derive passes the gate, so a test walks a history one
    /// derive at a time.
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
            _: crate::tree::BlobLens<'_>,
        ) -> Result<u64> {
            COLD_ENTERED.fetch_add(1, Ordering::SeqCst);
            probe("derive.gate", "entered the gate");
            let mut gate = COLD_GATE.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if gate.0 {
                    break;
                }
                if gate.1 > 0 {
                    gate.1 -= 1;
                    break;
                }
                gate = COLD_GATE_WOKEN
                    .wait(gate)
                    .unwrap_or_else(PoisonError::into_inner);
                probe("derive.gate", "woken at the gate");
            }
            drop(gate);
            probe("derive.gate", "through the gate");
            std::thread::sleep(Duration::from_millis(
                COLD_SLEEP.load(Ordering::Relaxed) as u64
            ));
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
    fn store_with(history: &[Edits]) -> (tempfile::TempDir, Backend, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        let (repo, ids) = store.write(|c| store_chain(c, history)).unwrap();
        (dir, store, repo, ids)
    }

    /// [`store_with`] on the backend the tests run on: the Postgres test
    /// schemas on the Postgres job, the data directory's store otherwise.
    fn store_with_here(
        history: &[Edits],
    ) -> (tempfile::TempDir, Backend, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        let (repo, ids) = store.write(|c| store_chain(c, history)).unwrap();
        (dir, store, repo, ids)
    }

    /// The body of [`n_cold_reads_derive_once`], on the store given: the
    /// Postgres variant runs it against the database, the SQLite one
    /// against the data directory, and both read the same in-memory
    /// flights.
    fn n_cold_reads_derive_once_on(store: &Backend, repo: RepoId, tip: ChangesetId) {
        let at_the_gate = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let _gate = gate_closed();
            for _ in 0..8 {
                scope.spawn(|| {
                    at_the_gate.wait();
                    assert_eq!(get::<Cold<1>>(store, repo, tip).unwrap(), 12);
                });
            }
            wait_entered(1);
            for _ in 0..2_000 {
                if joined_on(store, repo) >= 9 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(joined_on(store, repo), 9, "the seven callers never joined");
            let walks = store.derive_flights();
            assert_eq!(
                *walks
                    .in_flight
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                1,
                "a caller that only waits holds a walk permit"
            );
            gate_open();
        });
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
    }

    /// Backends `history` in a new repo `r` through the one write path, which
    /// stores root trees and the commit graph, and nothing else. Returns the
    /// IDs, oldest first.
    fn store_chain(c: &mut dyn MetaTx, history: &[Edits]) -> Result<(RepoId, Vec<ChangesetId>)> {
        // The tests store no blob files, so a test lens answers 0 for
        // everything: the trees it builds are only compared to themselves.
        chain_with(c, history, |c, repo, cs| {
            Ok(tree::store_changeset(c, repo, cs, &mut tree::lens_none())?.0)
        })
    }

    /// Backends `history` in a new repo `r` with `store`. Returns the IDs,
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
    fn rows_of(store: &Backend, name: &str, version: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let all = store.read(|c| Ok(rows(c.sqlite().unwrap()))).unwrap();
        all.into_iter()
            .filter(|(kind, v, ..)| kind == name && *v == version)
            .map(|(_, _, cs, value)| (cs, value))
            .collect()
    }

    /// The rows of the test kind at `version`, as (changeset, value).
    fn kind_rows(store: &Backend, version: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
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
            store.commit(|c| derive::<ChangedPathsV1>(c, other, tip, &mut tree::lens_none())),
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
    fn a_version_2_database_upgrades_and_rederives_its_root_trees() {
        let dir = tempfile::tempdir().unwrap();
        let (repo, ids, before) = {
            let c = Connection::open(dir.path().join("scion.db")).unwrap();
            migrate(&c, 2).unwrap();
            // The write path of version 2 stored root trees only.
            let mut tx = SqliteTx(&c);
            let (repo, ids) = chain_with(&mut tx, &HISTORY, |c, repo, cs| {
                let id = c.insert_changeset(repo, cs)?;
                tree::derive_root_tree(c, repo, id, &mut tree::lens_none())?;
                Ok(id)
            })
            .unwrap();
            (repo, ids, rows(&c))
        };
        // The rows that version 2 holds: a root tree for each changeset.
        assert_eq!(before.len(), 3);
        assert!(
            before
                .iter()
                .all(|(kind, v, ..)| kind == RootTree::NAME && *v == RootTree::VERSION)
        );

        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        let version: u32 = store
            .read(|c| {
                Ok(c.sqlite()
                    .unwrap()
                    .pragma_query_value(None, "user_version", |row| row.get(0))?)
            })
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        // Migration 7 drops the old root trees: they predate the entry
        // lengths, so they are re-derived from the blob store.
        assert_eq!(
            store.read(|c| Ok(rows(c.sqlite().unwrap()))).unwrap(),
            vec![]
        );
        // The blobs the chain names, by the chain's own convention.
        for path in ["a", "b", "c", "d"] {
            store.blobs().put(path.as_bytes()).unwrap();
        }
        let mut after = Vec::new();
        for &cs in &ids {
            let tree = tree::root_tree(&store, repo, cs).unwrap();
            after.push((
                RootTree::NAME.to_owned(),
                RootTree::VERSION,
                cs.as_bytes().to_vec(),
                tree.as_bytes().to_vec(),
            ));
        }
        let mut got = store.read(|c| Ok(rows(c.sqlite().unwrap()))).unwrap();
        got.sort();
        after.sort();
        assert_eq!(got, after);
    }

    /// How many root tree rows the store holds.
    fn root_trees(store: &Backend) -> usize {
        let all = store.read(|c| Ok(rows(c.sqlite().unwrap()))).unwrap();
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

        let late = derive_steps::<ChangedPathsV1>(&store, repo, &pending, None, || Ok(())).unwrap();
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
        backfill::<ChangedPathsV2>(&store, "r", tip).unwrap();
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
        backfill::<ChangedPathsV2>(&store, "r", tip).unwrap();
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
            .write(|c| Ok(c.sqlite().unwrap().execute(older, [RootTree::NAME])?))
            .unwrap();
        assert_eq!(added, 7);

        // The chain puts "a"; its blob file makes the real lens work.
        store.blobs().put(b"a").unwrap();
        let cs = Changeset {
            parents: vec![ids[6]],
            ..store.read(|c| c.get_changeset(repo, ids[0])).unwrap()
        };
        store
            .write(|c| tree::store_changeset(c, repo, &cs, &mut tree::lens_of(&store)))
            .unwrap();
        // The new row of version 1, and seven rows of version 0 less one
        // step.
        assert_eq!(root_trees(&store), 7 + 1 + 7 - STEP);
    }

    /// A new store with the graph `parents` in repo `r`, with no derived
    /// rows. Each changeset puts one file.
    fn store_graph(
        parents: &[Vec<usize>],
    ) -> (tempfile::TempDir, Backend, RepoId, Vec<ChangesetId>) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        let (repo, ids) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                Ok((
                    repo,
                    testing::insert_graph(c.sqlite().unwrap(), repo, parents)?,
                ))
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
                    on_write::<Ancestry>(c, cs, &cs_row, &mut crate::tree::lens_of(&store))
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

    /// The flight tests share the gate, so they run one at a time.
    static SERIAL: Mutex<()> = Mutex::new(());

    /// A second repo with a chain of `n`, beside the one `store_with` made.
    /// Its author differs, so its changeset IDs differ from the first repo's:
    /// a changeset ID is the hash of its content, and equal content would
    /// make the two repos share their derived rows.
    fn another_chain(store: &Backend, name: &str, history: &[Edits]) -> (RepoId, Vec<ChangesetId>) {
        store
            .write(|c| {
                let repo = c.create_repo(name)?;
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
                            email: [b"b@", name.as_bytes(), b"@x"].concat(),
                            unix_secs: 0,
                            tz_offset_minutes: 0,
                        },
                        committer: None,
                        message: vec![],
                        changes,
                    };
                    ids.push(tree::store_changeset(c, repo, &cs, &mut tree::lens_none())?.0);
                }
                Ok((repo, ids))
            })
            .unwrap()
    }

    #[test]
    fn n_cold_reads_derive_once() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(12));
        let tip = *ids.last().unwrap();
        // Eight cold reads of the tip at once, all starting together: they
        // join one walk, which derives each changeset once. The gate holds
        // the walking owner inside its first derive until every caller has
        // looked for the value, so the round's ownership is decided with
        // them all inside: a caller that walked on its own would own
        // another walk, and load cannot change that count.
        let at_the_gate = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let _gate = gate_closed();
            for _ in 0..8 {
                scope.spawn(|| {
                    at_the_gate.wait();
                    assert_eq!(get::<Cold<1>>(&store, repo, tip).unwrap(), 12);
                });
            }
            // All eight are inside the round: the owner holds the walk at
            // the gate, and the seven others wait for it on its flight.
            // A waiter holds no walk permit: the walk counter of the store
            // still counts the owner's walk alone, so the default cap of
            // 8 keeps seven walks free for other repos and kinds ([#142]).
            wait_entered(1);
            for _ in 0..2_000 {
                if joined_on(&store, repo) >= 9 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(joined_on(&store, repo), 9, "the seven callers never joined");
            let walks = store.derive_flights();
            assert_eq!(
                *walks
                    .in_flight
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                1,
                "a caller that only waits holds a walk permit"
            );
            // Open the gate, and the one walk derives every changeset once.
            gate_open();
        });
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
    }

    #[test]
    fn n_cold_reads_derive_once_on_postgres() {
        if !Backend::test_is_postgres() {
            return;
        }
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with_here(&chain(12));
        let tip = *ids.last().unwrap();
        // The same flight, held in the Postgres backend's own walks: eight
        // cold readers of one repo join one walk, and the walk derives
        // each changeset once.
        n_cold_reads_derive_once_on(&store, repo, tip);
    }

    #[test]
    fn a_caller_over_the_cap_waits_then_succeeds() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        // A cap of one walk, and three repos: the first caller walks repo
        // one and stops inside its first derive, at the closed gate, with
        // its permit taken. The gate, not a clock, holds it there: the
        // walk's permit stays taken for as long as the test wants.
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open_with_walks(dir.path(), 1).unwrap());
        let (repo1, ids1) = another_chain(&store, "one", &chain(3));
        let (repo2, ids2) = another_chain(&store, "two", &chain(3));
        let (repo3, ids3) = another_chain(&store, "three", &chain(3));
        let dir2 = tempfile::tempdir().unwrap();
        let store2 = Backend::Sqlite(Store::open(dir2.path()).unwrap());
        let (repo_b, ids_b) = another_chain(&store2, "r", &chain(3));
        let (owner, beside) = std::thread::scope(|scope| {
            let _gate = gate_closed();
            let owner = scope.spawn(|| get::<Cold<1>>(&store, repo1, ids1[2]).unwrap());
            // The owner is provably inside its walk: its first derive ran,
            // and it holds the one permit with the gate closed. Until the
            // test opens the gate, no other walk can start, whatever the
            // machine's load does to thread starts.
            wait_entered(1);
            // A caller of ANOTHER store in this process walks beside it:
            // its store keeps its own walks and its own permit count, so
            // it enters its first derive while the first store's walk
            // holds its permit. Flights shared across stores would make it
            // wait for the first store's permit, and it would never reach
            // the gate.
            let beside = scope.spawn(|| get::<Cold<1>>(&store2, repo_b, ids_b[2]).unwrap());
            wait_entered(2);
            // A caller of repo two waits for the permit, then walks.
            let waited = scope.spawn(|| get::<Cold<1>>(&store, repo2, ids2[2]).unwrap());
            // A caller of repo three with a deadline gives up: the one
            // walk is taken, and the test holds it there.
            let short = scope.spawn(|| {
                let deadline = Deadline::new(Instant::now() + Duration::from_millis(200));
                get_by::<Cold<1>>(&store, repo3, ids3[0], &deadline)
            });
            let short = short.join().unwrap();
            assert!(
                matches!(short, Err(Error::ResourceExhausted(_))),
                "the caller with a deadline did not give up: {short:?}"
            );
            // Open the gate: the owner finishes its walk, and the waiter
            // takes the permit and walks repo two.
            gate_open();
            let walked = owner.join().unwrap();
            assert_eq!(waited.join().unwrap(), 3);
            beside.join().unwrap();
            (walked, true)
        });
        assert_eq!(owner, 3);
        assert!(beside);
    }

    /// How many Arcs the flight of `Cold<1>` for `repo` has: the map's one,
    /// the walking owner's, and one per caller that joined the flight and
    /// still waits on it. A test that must have a waiter on the flight
    /// before the walk ends polls this count.
    fn joined_on(store: &Backend, repo: RepoId) -> usize {
        store
            .derive_flights()
            .flights
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&FlightKey::of::<Cold<1>>(repo))
            .map_or(0, Arc::strong_count)
    }

    #[test]
    fn a_waiter_that_leaves_does_not_trap_the_next_caller() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(6));
        let tip = *ids.last().unwrap();
        // The owner's walk stops at a step boundary past its deadline, with
        // the last changesets still missing. The gate walks it one derive
        // at a time: the second pair of releases goes out only once the
        // waiter has joined the walk's flight and the deadline, built
        // here, has passed - the state decides the order, not a clock's
        // mercy to thread starts.
        let owner_deadline = Deadline::new(Instant::now() + Duration::from_millis(50));
        std::thread::scope(|scope| {
            let _gate = gate_closed();
            let owner = scope.spawn(|| get_by::<Cold<1>>(&store, repo, tip, &owner_deadline));
            wait_entered(1);
            // A waiter of the same repo, spawned while the owner walks: it
            // joins the walk's flight. The walk's end wakes it.
            let waiter = scope.spawn(|| {
                let deadline = Deadline::new(Instant::now() + Duration::from_secs(5));
                get_by::<Cold<1>>(&store, repo, ids[1], &deadline).unwrap()
            });
            // The waiter is provably on the flight, and the owner's
            // deadline has provably passed, before the walk is let on.
            for _ in 0..2_000 {
                if joined_on(&store, repo) >= 3 && Instant::now() >= owner_deadline.at() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(
                joined_on(&store, repo) >= 3 && Instant::now() >= owner_deadline.at(),
                "the waiter never joined the flight"
            );
            gate_release(2);
            gate_release(2);
            // A waiter of a changeset the stopped walk DID store: it wakes
            // when the walk ends, reads its value, and leaves without
            // walking. A caller that comes after it must not join the flight
            // it held: that walk is over, and the new caller owns a fresh
            // one. A flight that a waiter's exit leaves behind would hold
            // the new caller until its deadline, with nobody to wake it.
            let owner = owner.join().unwrap();
            assert!(
                matches!(owner, Err(Error::DeadlineExceeded(_))),
                "{owner:?}"
            );
            // The next caller reads the tip, which the stopped walk did not
            // store: it owns a fresh walk and finishes the chain.
            gate_open();
            let deadline = Deadline::new(Instant::now() + Duration::from_secs(5));
            let value = get_by::<Cold<1>>(&store, repo, tip, &deadline).unwrap();
            assert_eq!(value, 6);
            assert_eq!(waiter.join().unwrap(), 2);
        });
    }

    #[test]
    fn a_stopped_walk_hands_over() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(12));
        let tip = *ids.last().unwrap();
        // The owner's walk stops at a step boundary when its deadline passes;
        // the rows it stored stay. A waiter that joined the walk mid-flight
        // wakes when the walk ends, finds its value still missing, and owns
        // the next walk itself: it walks only what is left, and every
        // changeset is derived once, never redone.
        let owner_deadline = Deadline::new(Instant::now() + Duration::from_millis(50));
        std::thread::scope(|scope| {
            let _gate = gate_closed();
            let owner = scope.spawn(|| get_by::<Cold<1>>(&store, repo, tip, &owner_deadline));
            wait_entered(1);
            // The waiter joins the walk while it is still in flight, so
            // the walk's end is what wakes it. The test waits for the
            // waiter's Arc to be on the flight, and for the deadline to
            // have passed, before it lets the walk on: a waiter that
            // arrived after the walk ended would join nothing and check
            // something else.
            let waiter = scope.spawn(|| {
                let deadline = Deadline::new(Instant::now() + Duration::from_secs(5));
                get_by::<Cold<1>>(&store, repo, tip, &deadline).unwrap()
            });
            for _ in 0..2_000 {
                if joined_on(&store, repo) >= 3 && Instant::now() >= owner_deadline.at() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(
                joined_on(&store, repo) >= 3 && Instant::now() >= owner_deadline.at(),
                "the waiter never joined the flight"
            );
            gate_release(2);
            gate_release(2);
            let owner = owner.join().unwrap();
            assert!(
                matches!(owner, Err(Error::DeadlineExceeded(_))),
                "{owner:?}"
            );
            // The walk is over: the waiter owns a fresh one, and with the
            // gate open it walks the rest without waiting.
            gate_open();
            assert_eq!(waiter.join().unwrap(), 12);
        });
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
    }

    #[test]
    fn a_land_finishes_while_cold_reads_walk_deep_history() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        // A cap of two walks, and a storm of cold reads: forty repos, each
        // read by one caller. Each walk holds the write connection for two
        // sleeping derives per step, over two steps, and then it is done:
        // it never contends again. A land that arrives while the first two
        // walk queues behind at most their four transactions; the other
        // thirty-eight callers wait at the permit, and never touch the
        // write connection. Walks without a cap would all queue their
        // transactions on the write connection, and the land would wait
        // behind all of them.
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open_with_walks(dir.path(), 2).unwrap());
        let mut repos: Vec<(RepoId, ChangesetId)> = Vec::new();
        for i in 0..40 {
            let (repo, ids) = another_chain(&store, &format!("r{i}"), &chain(4));
            repos.push((repo, *ids.last().unwrap()));
        }
        COLD_SLEEP.store(100, Ordering::Relaxed);
        std::thread::scope(|scope| {
            let mut walkers: Vec<_> = repos
                .iter()
                .map(|&(repo, tip)| {
                    let store = store.clone();
                    scope.spawn(move || get::<Cold<1>>(&store, repo, tip).unwrap())
                })
                .collect();
            // The storm is provably deep: twenty walkers have entered
            // their derives. With the cap that took ten waves of walking;
            // without it, the walkers are all queued on the write
            // connection by now.
            wait_entered(20);
            let at = Instant::now();
            let (land_repo, _) = another_chain(&store, "land", &chain(1));
            let waited = at.elapsed();
            // The storm's walks take at least twenty waves of two
            // transactions, four seconds of holding; the land finishes in
            // well under the first two walkers' four transactions.
            assert!(
                waited < Duration::from_millis(1_500),
                "the land waited {waited:?}"
            );
            COLD_SLEEP.store(0, Ordering::Relaxed);
            for walker in walkers.drain(..) {
                assert_eq!(walker.join().unwrap(), 4);
            }
            let _ = land_repo;
        });
        COLD_SLEEP.store(0, Ordering::Relaxed);
    }
    #[test]
    fn a_walk_waits_while_a_writer_waits_for_the_connection() {
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with(&chain(12));
        let tip = *ids.last().unwrap();
        // The walk holds the connection inside its first step, at the
        // closed gate. A writer arrives and waits for the connection: it
        // cannot get in while the step's transaction holds it. When the
        // step ends, the walk waits while the writer waits, so the
        // writer's write runs first, and only then the walk's next derive
        // passes the gate. A walk that did not wait would take the
        // connection back at once, and the writer would wait for the
        // whole walk.
        std::thread::scope(|scope| {
            let _gate = gate_closed();
            let walker = scope.spawn(|| get::<Cold<1>>(&store, repo, tip).unwrap());
            wait_entered(1);
            // The writer reads the walk's derive count inside its own
            // transaction, right after it got the connection: that is the
            // one moment that separates the two designs. A walk that
            // waits while a writer waits has derived only the first
            // step's two changesets by then; a walk that re-took the
            // connection has derived its second step too.
            let writer = scope.spawn(|| {
                store
                    .write(|_| {
                        DERIVES_AT_WRITE
                            .store(COLD_ENTERED.load(Ordering::SeqCst), Ordering::SeqCst);
                        Ok(())
                    })
                    .unwrap();
            });
            // The writer is provably waiting: it counted itself when its
            // commit started, and it cannot have gotten in, because the
            // walk's step still holds the connection.
            for _ in 0..2_000 {
                if store.writers_waiting() > 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(store.writers_waiting() > 0, "the writer never waited");
            // Twelve releases bank the whole walk, so a walk that does not
            // wait for the writer walks on to the end and is caught by the
            // snapshot, instead of blocking at the gate forever.
            gate_release(12);
            // The writer's write finishes before the walk derives again.
            writer.join().unwrap();
            assert_eq!(
                DERIVES_AT_WRITE.load(Ordering::SeqCst),
                2,
                "the walk derived while a writer waited"
            );
            // The writer is done: the walk goes on, and the gate lets it.
            gate_open();
            assert_eq!(walker.join().unwrap(), 12);
        });
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
    }

    #[test]
    fn a_walk_waits_while_a_writer_waits_on_postgres() {
        if !Backend::test_is_postgres() {
            return;
        }
        let _serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
        COLD_DERIVES.store(0, Ordering::Relaxed);
        let (_dir, store, repo, ids) = store_with_here(&chain(12));
        let tip = *ids.last().unwrap();
        // The same writer priority, over the Postgres writer mutex: the
        // walk's step ends, it sees a writer waiting for the connection,
        // and the writer's write runs before the walk's next derive.
        std::thread::scope(|scope| {
            let _gate = gate_closed();
            let walker = scope.spawn(|| get::<Cold<1>>(&store, repo, tip).unwrap());
            wait_entered(1);
            let writer = scope.spawn(|| {
                store
                    .write(|_| {
                        DERIVES_AT_WRITE
                            .store(COLD_ENTERED.load(Ordering::SeqCst), Ordering::SeqCst);
                        Ok(())
                    })
                    .unwrap();
            });
            for _ in 0..2_000 {
                if store.writers_waiting() > 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(store.writers_waiting() > 0, "the writer never waited");
            gate_release(12);
            writer.join().unwrap();
            assert_eq!(
                DERIVES_AT_WRITE.load(Ordering::SeqCst),
                2,
                "the walk derived while a writer waited"
            );
            gate_open();
            assert_eq!(walker.join().unwrap(), 12);
        });
        assert_eq!(COLD_DERIVES.load(Ordering::Relaxed), 12);
    }
}

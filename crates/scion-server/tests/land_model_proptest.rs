//! The land model check as a randomized script: a proptest strategy
//! generates a sequence of operations against the real `scion-server` —
//! lands from several callers on a few refs, retries with the same
//! request key, twins of one key in flight together, calls dropped
//! mid-flight, a SIGKILL (plain, or at a pending land), the collector
//! (`gc`) beside the live server, bursts of k lands on one ref at once —
//! and a plain in-memory model checks what came out:
//!
//! - each ref's first-parent history is one chain, walked root→head,
//!   and every step of it is exactly one acknowledged land's result —
//!   no land applied twice;
//! - every LANDED reply's result is on the chain;
//! - one land ID per (caller, request key), and no key's state
//!   regresses;
//! - a kill kept every land that had answered LANDED before it;
//! - `scion-server fsck` sees no damage.
//!
//! The script runs as written: operations execute one at a time, and
//! the concurrent ops (`Twin`, `Burst`) await their calls together. So
//! the concurrency the workload carries is exactly the script, and a
//! shrunk script replays its shape — that is what makes proptest's
//! shrinking honest here. What still does not replay is when a reply
//! ARRIVES inside a burst; every check is therefore a property that
//! must hold under any schedule over the ledger a run produced, as in
//! `land_model.rs`, whose entry point this file replaces once it has
//! run green on main.
//!
//! Determinism: proptest's seed picks the script. A failing script
//! shrinks and lands in `proptest-regressions/land_model_proptest.txt`,
//! and the failure output names the seed: replay with
//! `PROPTEST_RNG_SEED=<seed>` and the same
//! `SCION_LANDMODEL_CASES`. CI runs one case
//! (`SCION_LANDMODEL_CASES`, default 1); the nightly `land-model` job
//! raises it to eight.

#![cfg(unix)]

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use common::*;
use proptest::prelude::*;
use scion_core::{BlobId, Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{
    self, CreateChangesetRequest, GetLandRequest, LandRequest, LandResponse, LandState,
};
use tonic::Status;

/// Callers that land. Each land names a caller; the fixture's one token
/// makes them one caller at the server, so the keys carry the identity.
const CALLERS: usize = 4;
/// A ref that does not exist, for the lands that must be rejected.
const MISSING: usize = REFS.len();

/// One operation of the script. Ops that name an earlier land do so by
/// its position in the script's ledger; the runner takes the index
/// modulo the ledger's length, so a shrunk script that deleted a
/// referenced land stays well-formed.
#[derive(Clone, Debug, PartialEq)]
enum Op {
    /// Build a changeset and land it. `stale` bases it on an older head
    /// of the ref, so lands whose base the ref moved past conflict;
    /// `shared` puts `SHARED`, so lands over one path meet.
    Land {
        caller: usize,
        target: usize,
        stale: bool,
        shared: bool,
    },
    /// Retry an earlier land with its same request key.
    Retry { caller: usize, land: usize },
    /// Two calls with the same request key, in flight at once.
    Twin { caller: usize, land: usize },
    /// A land call dropped in flight: its reply never comes.
    Cut { caller: usize, land: usize },
    /// A NEW land whose reply is dropped before it is read: the intent
    /// is in the ledger, the land stays pending with no reply, and the
    /// retry phase finishes it.
    CutFresh {
        caller: usize,
        target: usize,
        shared: bool,
        /// How long the call waits before its reply is dropped. The
        /// default 20 ms; a fast cut cancels the server mid-flow, so
        /// the land's row stays pending (M4).
        cut_ms: u64,
    },
    /// Poll an earlier land by ID.
    Poll { caller: usize, land: usize },
    /// SIGKILL the server now, and restart it.
    Kill,
    /// SIGKILL the server once a land is pending, and restart it.
    KillAtPending,
    /// Run `scion-server gc` beside the live server, grace 0.
    Gc,
    /// k new lands on one ref, in flight at once, awaited together.
    Burst { k: usize, target: usize },
    /// k new lands in flight at once; while they queue on the ref's
    /// lock their rows sit pending, so the kill lands mid-queue and
    /// leaves pending rows that never decided (M4). The replies are
    /// dropped, not read.
    BurstCut { k: usize, target: usize },
    /// Retry what never answered, then check the model.
    Check,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Weights: lands dominate; the races and the crashes appear.
        6 => (any::<usize>(), any::<usize>(), any::<bool>(), any::<bool>()).prop_map(
            |(c, t, stale, shared)| Op::Land {
                caller: c % CALLERS,
                target: t % (REFS.len() + 1),
                stale,
                shared
            }
        ),
        1 => (any::<usize>(), any::<usize>())
            .prop_map(|(c, l)| Op::Retry { caller: c % CALLERS, land: l }),
        1 => (any::<usize>(), any::<usize>())
            .prop_map(|(c, l)| Op::Twin { caller: c % CALLERS, land: l }),
        1 => (any::<usize>(), any::<usize>())
            .prop_map(|(c, l)| Op::Cut { caller: c % CALLERS, land: l }),
        1 => (any::<usize>(), any::<usize>())
            .prop_map(|(c, l)| Op::Poll { caller: c % CALLERS, land: l }),
        1 => (any::<usize>(), any::<usize>(), any::<bool>())
            .prop_map(|(c, t, shared)| Op::CutFresh {
                caller: c % CALLERS,
                target: t % (REFS.len() + 1),
                shared,
                cut_ms: 20,
            }),
        1 => Just(Op::Kill),
        1 => Just(Op::KillAtPending),
        1 => Just(Op::Gc),
        2 => (2usize..=8, any::<usize>()).prop_map(|(k, t)| {
            if k % 3 == 0 {
                Op::BurstCut {
                    k,
                    target: t % (REFS.len() + 1),
                }
            } else {
                Op::Burst {
                    k,
                    target: t % (REFS.len() + 1),
                }
            }
        }),
        2 => Just(Op::Check),
    ]
}

fn script_strategy() -> impl Strategy<Value = Vec<Op>> {
    // The per-PR run is one script, and it must fit the job's budget on
    // every seed: a script's cost is its length plus its restart-type
    // ops, each of which can wait ~10 s and then boot a server, and a
    // seed that draws many of both ran past the CI job's 30 minutes.
    // The per-PR run therefore draws a shorter script and caps the
    // restart-type ops (the overflow becomes a cheap collector sweep,
    // which keeps that coverage alive); the nightly run keeps the full
    // range, its own job.
    let cases = std::env::var("SCION_LANDMODEL_CASES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1);
    if cases == 1 {
        prop::collection::vec(op_strategy(), 8..=40)
            .prop_map(|mut script| {
                let mut restarts = 0;
                for op in &mut script {
                    if matches!(op, Op::Kill | Op::KillAtPending) {
                        restarts += 1;
                        if restarts > 3 {
                            *op = Op::Gc;
                        }
                    }
                }
                script
            })
            .boxed()
    } else {
        prop::collection::vec(op_strategy(), 8..=120).boxed()
    }
}

/// The script of one corpus file: one op per line, whitespace split.
fn parse_script(text: &str) -> Vec<Op> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let part: Vec<&str> = line.split_whitespace().collect();
            let num = |s: &str| {
                s.parse::<usize>().unwrap_or_else(|e| {
                    panic!("a corpus line the runner cannot parse ({line}): {e}")
                })
            };
            match part.as_slice() {
                ["land", caller, target, stale, shared] => Op::Land {
                    caller: num(caller),
                    target: num(target),
                    stale: *stale == "stale" || *stale == "1",
                    shared: *shared == "shared" || *shared == "1",
                },
                ["retry", caller, land] => Op::Retry {
                    caller: num(caller),
                    land: num(land),
                },
                ["twin", caller, land] => Op::Twin {
                    caller: num(caller),
                    land: num(land),
                },
                ["cut", caller, land] => Op::Cut {
                    caller: num(caller),
                    land: num(land),
                },
                ["cut-fresh", caller, target, shared] => Op::CutFresh {
                    caller: num(caller),
                    target: num(target),
                    shared: *shared == "shared" || *shared == "1",
                    cut_ms: 20,
                },
                ["cut-fast", caller, target, shared] => Op::CutFresh {
                    caller: num(caller),
                    target: num(target),
                    shared: *shared == "shared" || *shared == "1",
                    // Long enough for the land's row to be committed
                    // pending, short enough that the cancel lands
                    // mid-flow and the row never decides.
                    cut_ms: 8,
                },
                ["poll", caller, land] => Op::Poll {
                    caller: num(caller),
                    land: num(land),
                },
                ["kill"] => Op::Kill,
                ["kill-pending"] => Op::KillAtPending,
                ["gc"] => Op::Gc,
                ["burst", k, target] => Op::Burst {
                    k: num(k),
                    target: num(target),
                },
                ["burst-cut", k, target] => Op::BurstCut {
                    k: num(k),
                    target: num(target),
                },
                ["check"] => Op::Check,
                other => panic!("a corpus line the runner does not know: {other:?}"),
            }
        })
        .collect()
}

/// One land the script sent, with every reply the run saw for it.
struct Rec {
    caller: usize,
    key: String,
    /// The changeset as the client built it: its paths and file states
    /// are what the rebase rule moves onto the ref.
    original: Changeset,
    id: ChangesetId,
    base: ChangesetId,
    target: String,
    /// The ref's head as this call's own reply settled: the head the
    /// server saw when it decided this land, or the closest a batched
    /// reply can name (see the Twin and Burst arms). A conflict reply
    /// is judged against this head, not against today's.
    decided_head: Option<ChangesetId>,
    /// This land's step in the M2 folder-became-file pair, if it plays
    /// one: 0 puts the child, 1 makes the folder a file, 2 re-puts the
    /// child on a stale base.
    m2_phase: Option<usize>,
    replies: Vec<(&'static str, LandResponse)>,
}

impl Rec {
    fn paths(&self) -> Vec<RepoPath> {
        self.original.changes().keys().cloned().collect()
    }

    fn request(&self) -> LandRequest {
        LandRequest {
            repo: REPO.into(),
            changeset_id: self.id.as_bytes().to_vec(),
            target_ref: self.target.clone(),
            request_key: self.key.clone(),
        }
    }

    /// The last final state any reply named. A pending reply only waits;
    /// once a land ends, every later reply agrees with it (K1).
    fn final_reply(&self) -> Option<&LandResponse> {
        self.replies
            .iter()
            .rev()
            .map(|(_, reply)| reply)
            .find(|reply| reply.state() != LandState::Pending)
    }

    fn is_final(&self) -> bool {
        self.final_reply().is_some()
    }
}

/// The lands a script sent, in the order it sent them: the ledger the
/// checks read. The runner takes land indices modulo its length.
type Ledger = Vec<Rec>;

/// One call, with a ceiling: a call that hangs past 60 s is a bug, and
/// the script must fail, not the CI job's timeout.
async fn ceiling<T>(what: &str, fut: impl Future<Output = Result<T, Status>>) -> T {
    tokio::time::timeout(Duration::from_secs(60), fut)
        .await
        .unwrap_or_else(|_| panic!("{what}: the call hung past 60 s"))
        .unwrap_or_else(|status| panic!("{what}: {status:?}"))
}

/// One change for a land: one or two own paths, and `SHARED` when the op
/// says so, so lands over one path meet.
async fn build_changes(
    scion: &mut Scion,
    op: usize,
    j: usize,
    shared: bool,
    m2_phase: Option<usize>,
    put: &mut HashMap<BlobId, Vec<u8>>,
) -> BTreeMap<RepoPath, Option<FileState>> {
    let mut rng = common::Rng::new((op as u64) << 8 | j as u64 | 1);
    let mut changes = BTreeMap::new();
    // M2: every third non-shared land of caller 1 plays one step of a
    // folder-became-file pair. Phase 0 puts `m2/child`; phase 1 (a
    // fresh base, so its parent's tree holds the child) deletes the
    // child and makes `m2` itself a file; phase 2 re-puts the child on
    // a stale base - under a folder the ref turned into a file - and
    // must conflict.
    if let Some(phase) = m2_phase {
        let mut child = |content: Option<FileState>| {
            changes.insert(RepoPath::new("m2/child").unwrap(), content);
        };
        if phase == 2 {
            // A different child than phase 0 put: the ref deleted
            // `m2/child` and made `m2` a file after this land's stale
            // base, so only the folder-file rule sees the collision.
            let len = 1 + rng.below(64);
            let data = rng.bytes(len);
            let id = put_blob(scion, &data).await.unwrap();
            put.insert(id, data);
            changes.insert(
                RepoPath::new("m2/child2").unwrap(),
                Some(FileState::Regular {
                    content: id,
                    executable: false,
                }),
            );
            return changes;
        }
        if phase == 1 {
            child(None);
            let data = rng.bytes(12);
            let id = put_blob(scion, &data).await.unwrap();
            put.insert(id, data);
            changes.insert(
                RepoPath::new("m2").unwrap(),
                Some(FileState::Regular {
                    content: id,
                    executable: false,
                }),
            );
        } else {
            let len = 1 + rng.below(64);
            let data = rng.bytes(len);
            let id = put_blob(scion, &data).await.unwrap();
            put.insert(id, data);
            child(Some(FileState::Regular {
                content: id,
                executable: false,
            }));
        }
        return changes;
    }
    // Every fourth put is executable: the file states a land carries
    // must survive the server's read and re-encode exactly, mode
    // included (M5). The ordinal spans the whole script, so a corpus
    // of eight ops meets one.
    let executable = |i: usize| (op + j + i) % 4 == 3;
    for i in 0..=rng.below(1) {
        let len = 1 + rng.below(64);
        let data = rng.bytes(len);
        let id = put_blob(scion, &data).await.unwrap();
        put.insert(id, data);
        changes.insert(
            RepoPath::new(format!("o{op}/f{j}-{i}")).unwrap(),
            Some(FileState::Regular {
                content: id,
                executable: executable(i),
            }),
        );
    }
    if shared {
        let data = rng.bytes(16);
        let id = put_blob(scion, &data).await.unwrap();
        put.insert(id, data);
        changes.insert(
            RepoPath::new(SHARED).unwrap(),
            Some(FileState::Regular {
                content: id,
                executable: executable(2),
            }),
        );
    }
    changes
}

/// Sends one land: builds the changeset on `base`, creates it, and calls
/// land. The intent enters the ledger before the call, so a reply a
/// future kill would cut leaves a pending land the retry phase finishes.
/// The M2 step this land plays, if any: the due step reads off the
/// ledger, so the callers can force the bases the steps need.
fn m2_phase_for(state: &mut Run, caller: usize, target: usize, shared: bool) -> Option<usize> {
    // The trio stays on one ref: the second step deletes the child the
    // first put, so its base must carry that put - a trio on another
    // ref would base on a tree without it.
    // A missing ref resolves nowhere: its lands are rejected on the ref,
    // and a rejected step would still consume the trio's count.
    if !(caller == 1 && !shared && target != MISSING) {
        return None;
    }
    let m2_ref = *state.m2_ref.get_or_insert(target);
    if target != m2_ref {
        return None;
    }
    // Which step is due reads off the ledger, not a counter: a step
    // whose forerunner never landed - a killed or conflicted put -
    // would ask the server to delete or overwrite a path its tree does
    // not have, and the refusal is the model lying, not the server.
    // Once the file landed and the stale re-put was answered, the
    // folder is gone for good on this ref: the trio is done, and a
    // put under the file would be a format refusal, not a conflict.
    let name = target_name(target);
    let mut put_landed = false;
    let mut file_landed = false;
    let mut replied_after_file = false;
    for rec in &state.lands {
        if rec.target != name || rec.m2_phase.is_none() {
            continue;
        }
        let Some(reply) = rec.final_reply() else {
            continue;
        };
        match (rec.m2_phase, reply.state()) {
            (Some(0), LandState::Landed) => put_landed = true,
            (Some(1), LandState::Landed) => file_landed = true,
            (Some(2), LandState::Landed | LandState::Conflict) if file_landed => {
                replied_after_file = true;
            }
            _ => {}
        }
    }
    match (put_landed, file_landed, replied_after_file) {
        (_, true, true) => None,
        (_, true, false) => Some(2),
        (true, false, _) => Some(1),
        _ => Some(0),
    }
}

async fn send_land(
    state: &mut Run,
    caller: usize,
    target: usize,
    base: ChangesetId,
    shared: bool,
    m2_phase: Option<usize>,
) -> usize {
    let op = state.ops;
    let key = format!("c{caller}-o{op}-{j}", j = state.lands.len());
    let changes = build_changes(
        &mut state.scion,
        op,
        state.lands.len(),
        shared,
        m2_phase,
        &mut state.put,
    )
    .await;
    let original = Changeset::new(vec![base], author(), None, key.as_bytes().to_vec(), changes)
        .expect("changeset fits the format");
    let (bytes, id) = original.encode_with_id();
    let reply = ceiling(
        &format!("CreateChangeset {key}"),
        state.scion.create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        }),
    )
    .await
    .into_inner();
    assert_eq!(changeset_id(&reply.changeset_id), id);
    let target_name = target_name(target);
    let rec = Rec {
        caller,
        key,
        original,
        id,
        base,
        target: target_name.to_owned(),
        decided_head: None,
        m2_phase,
        replies: Vec::new(),
    };
    state.lands.push(rec);
    state.lands.len() - 1
}

/// Records, on every land the op touched, the ref's head right after
/// the reply: what a later conflict check judges the reply against.
async fn note_head(state: &mut Run, target: &str, idxs: &[usize]) {
    let head = resolve_ref(&mut state.objects, target).await.ok();
    for idx in idxs {
        state.lands[*idx].decided_head = head;
    }
}

fn target_name(target: usize) -> &'static str {
    if target == MISSING {
        "no-such-ref"
    } else {
        REFS[target % REFS.len()]
    }
}

/// The runner's mutable state.
struct Run {
    /// The ref the M2 trio plays on: the first caller-1 non-shared land
    /// sets it; lands on other refs keep the plain shape, so the trio's
    /// steps never split across refs.
    m2_ref: Option<usize>,
    scion: Scion,
    objects: Objects,
    lands: Ledger,
    /// Heads the run has seen, per ref: the oldest first, so `stale`
    /// lands have a base the ref may have moved past.
    heads: HashMap<&'static str, Vec<ChangesetId>>,
    /// Lands that had answered LANDED before each kill: the durability
    /// check holds them against the chain after the restart.
    acked_before_kill: Vec<(usize, ChangesetId)>,
    /// The script killed the server at least once.
    killed: bool,
    /// The collector ran while the ledger held at least one land.
    gc_beside_a_live_land: bool,
    /// The collector ran while a land sat pending (M4).
    gc_during_pending: bool,
    /// The bytes of every blob this script put, by ID: the read-back
    /// check compares a landed tree's files to what the client sent.
    put: HashMap<BlobId, Vec<u8>>,
    ops: usize,
}

/// What one script's run saw, so the corpus guard can name a kind of
/// operation the scripts stopped exercising.
#[derive(Default, Debug)]
struct RunSummary {
    landed: usize,
    conflict: usize,
    rejected: usize,
    /// A land whose first call never answered, finished by the retry
    /// phase, in a script that killed the server.
    retried_after_kill: bool,
    gc_beside_a_live_land: bool,
    /// A land that carried an executable file (M5): the corpus and the
    /// strategy must keep putting modes, or a mode-dropping mutation
    /// survives an empty comparison.
    executable_lands: usize,
    /// The collector ran while a land sat pending (M4): its blobs were
    /// still needed by a land the retry phase would finish.
    gc_during_pending: bool,
}

/// Runs the whole script against one real server, then checks.
#[allow(clippy::too_many_lines)] // one arm per op: the dispatch is the table
async fn run_script(script: &[Op]) -> RunSummary {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let mut server = Server::start(&data, &log);
    let (scion, objects) = connect(&server.url).await.unwrap();
    let mut state = Run {
        scion,
        objects,
        lands: Vec::new(),
        heads: HashMap::new(),
        acked_before_kill: Vec::new(),
        killed: false,
        gc_beside_a_live_land: false,
        gc_during_pending: false,
        put: HashMap::new(),
        m2_ref: None,
        ops: 0,
    };

    // A wall-clock budget far under the CI job cap, enforced by a
    // watchdog thread. The between-ops check cannot fire on an op that
    // blocks inside one call - a land RPC runs to its 600 s client
    // deadline, and the gc subprocess has no ceiling at all - so the
    // watchdog, which sees real time whatever the executor does, kills
    // the process with the seed and the current op. libtest captures
    // print! and eprint!, but a raw write to the stderr file handle,
    // and the abort that follows, reach the CI log.
    let budget = Duration::from_secs(
        std::env::var("SCION_LANDMODEL_BUDGET_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(240),
    );
    let started = std::time::Instant::now();
    let ordinal = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let watchdog = Watchdog {
        script: script.to_vec(),
        seed: seed(),
        started,
        budget,
        ordinal: Arc::clone(&ordinal),
        stop: Arc::clone(&stop),
    };
    std::thread::spawn(move || watchdog.run());

    for op in script {
        state.ops += 1;
        let mut err = std::io::stderr().lock();
        let _ = writeln!(
            err,
            "{} seed {} op {}: {op:?}",
            humantime_millis(started.elapsed()),
            seed(),
            state.ops
        );
        let _ = err.flush();
        ordinal.store(state.ops, Ordering::Relaxed);
        assert!(
            started.elapsed() < budget,
            "the script ran past its wall-clock budget of {budget:?} after \
             {} ops; the seed and the script are printed above; a run this \
             slow is itself a finding, not just a harness failure",
            state.ops
        );
        match op {
            Op::Land {
                caller,
                target,
                stale: _,
                shared,
            } => {
                let name = target_name(*target);
                let head = match resolve_ref(&mut state.objects, name).await {
                    Ok(head) => Some(head),
                    Err(_) if *target == MISSING => None,
                    Err(status) => panic!("ResolveRef {name}: {status:?}"),
                };
                let m2_phase = m2_phase_for(&mut state, *caller, *target, *shared);
                let base = match head {
                    Some(head) => {
                        state.heads.entry(name).or_default().push(head);
                        let heads = &state.heads[name];
                        // The trio's steps ride the base their tree
                        // facts hold: the put and the delete ride the
                        // fresh head (the delete needs the landed put
                        // in its tree), and the third step re-puts a
                        // new child under the folder the second turned
                        // into a file, so it needs a base from before
                        // that: a fresh base would hold a file where
                        // it puts.
                        if m2_phase == Some(2) { heads[0] } else { head }
                    }
                    // The missing ref resolves nowhere; base on a real
                    // head, so the land is rejected on the ref, not on
                    // the changeset.
                    None => resolve_ref(&mut state.objects, REFS[0]).await.unwrap(),
                };
                let idx = send_land(&mut state, *caller, *target, base, *shared, m2_phase).await;
                let request = state.lands[idx].request();
                let reply = ceiling(
                    &format!("land {}", state.lands[idx].key),
                    state.scion.land(request),
                )
                .await
                .into_inner();
                state.lands[idx].replies.push(("land", reply));
                note_head(&mut state, name, &[idx]).await;
            }
            Op::CutFresh {
                caller,
                target,
                shared,
                cut_ms,
            } => {
                let name = target_name(*target);
                let head = match resolve_ref(&mut state.objects, name).await {
                    Ok(head) => Some(head),
                    Err(_) if *target == MISSING => None,
                    Err(status) => panic!("ResolveRef {name}: {status:?}"),
                };
                let m2_phase = m2_phase_for(&mut state, *caller, *target, *shared);
                let base = match head {
                    Some(head) => {
                        state.heads.entry(name).or_default().push(head);
                        let heads = &state.heads[name];
                        // The trio's third step re-puts a new child
                        // under the folder the second turned into a
                        // file: only a base from before that change
                        // holds a folder there.
                        if m2_phase == Some(2) { heads[0] } else { head }
                    }
                    None => resolve_ref(&mut state.objects, REFS[0]).await.unwrap(),
                };
                let idx = send_land(&mut state, *caller, *target, base, *shared, m2_phase).await;
                let request = state.lands[idx].request();
                {
                    let fut = state.scion.land(request);
                    tokio::pin!(fut);
                    // The reply is dropped, not read: the land stays
                    // pending with no reply, and the retry phase
                    // finishes it.
                    let _ = tokio::time::timeout(Duration::from_millis(*cut_ms), fut).await;
                }
                note_head(&mut state, name, &[idx]).await;
            }
            Op::Retry { caller: _, land } => {
                if state.lands.is_empty() {
                    continue;
                }
                let idx = land % state.lands.len();
                let request = state.lands[idx].request();
                let reply = ceiling("retry", state.scion.land(request))
                    .await
                    .into_inner();
                state.lands[idx].replies.push(("retry", reply));
            }
            Op::Twin { caller: _, land } => {
                if state.lands.is_empty() {
                    continue;
                }
                let idx = land % state.lands.len();
                let request = state.lands[idx].request();
                let (mut a, mut b) = (state.scion.clone(), state.scion.clone());
                let (ra, rb) = tokio::join!(
                    ceiling("twin a", a.land(request.clone())),
                    ceiling("twin b", b.land(request)),
                );
                state.lands[idx].replies.push(("twin", ra.into_inner()));
                state.lands[idx].replies.push(("twin", rb.into_inner()));
            }
            Op::Cut { caller: _, land } => {
                if state.lands.is_empty() {
                    continue;
                }
                let idx = land % state.lands.len();
                let request = state.lands[idx].request();
                let fut = state.scion.land(request);
                tokio::pin!(fut);
                // The reply is dropped, not awaited: the land stays
                // pending, and the retry phase of the next Check
                // finishes it.
                let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
            }
            Op::Poll { caller: _, land } => {
                if state.lands.is_empty() {
                    continue;
                }
                let idx = land % state.lands.len();
                let Some(reply) = state.lands[idx]
                    .final_reply()
                    .filter(|reply| !reply.land_id.is_empty())
                    .cloned()
                else {
                    continue;
                };
                let request = GetLandRequest {
                    repo: REPO.into(),
                    land_id: reply.land_id.clone(),
                };
                let polled = ceiling("GetLand", state.scion.get_land(request))
                    .await
                    .into_inner();
                assert_eq!(
                    polled.land_id, reply.land_id,
                    "GetLand answered another land"
                );
                state.lands[idx].replies.push(("poll", polled));
                let name = state.lands[idx].target.clone();
                note_head(&mut state, &name, &[idx]).await;
            }
            Op::Kill | Op::KillAtPending => {
                // The lands that had answered LANDED before the kill:
                // the durability check holds them against the chain.
                state
                    .acked_before_kill
                    .extend(state.lands.iter().enumerate().filter_map(|(idx, rec)| {
                        let reply = rec
                            .replies
                            .iter()
                            .rev()
                            .map(|(_, reply)| reply)
                            .find(|reply| reply.state() == LandState::Landed)?;
                        Some((idx, changeset_id(&reply.result_changeset_id)))
                    }));
                if *op == Op::KillAtPending {
                    // A crash that meets a pending row. The store may be
                    // Postgres in the parity job, where the SQLite file
                    // does not exist; then wait a moment and kill. The
                    // wait can miss: a land can pass through pending and
                    // finish between two polls of the file. A missed row
                    // is logged, and the kill still runs - it is the
                    // plain kill then, which the checks already hold.
                    let db = data.join("scion.db");
                    if db.exists() {
                        let conn = rusqlite::Connection::open(&db).unwrap();
                        let saw = wait_until_or_timeout("a land pending in the database", || {
                            pending_lands(&conn) > 0
                        })
                        .await;
                        if !saw {
                            println!(
                                "seed {}: kill-pending saw no pending row; \
                                 the land finished between two polls, and \
                                 the kill is a plain kill",
                                seed()
                            );
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                }
                state.killed = true;
                drop(server); // SIGKILL
                server = common::start_after_kill(&data, &log);
                let (scion, objects) = connect(&server.url).await.unwrap();
                state.scion = scion;
                state.objects = objects;
            }
            Op::Gc => {
                if !state.lands.is_empty() {
                    state.gc_beside_a_live_land = true;
                    // A land with no final reply is still pending in the
                    // database: gc ran during its life (M4).
                    if state.lands.iter().any(|rec| !rec.is_final()) {
                        state.gc_during_pending = true;
                    }
                }
                // A ceiling: gc runs beside a live server and waits on
                // its store; without a bound it was the one op that
                // could block past the script's own budget.
                {
                    let db = data.join("scion.db");
                    if db.exists() {
                        let conn = rusqlite::Connection::open(&db).unwrap();
                        println!(
                            "gc: pending rows in the database before the sweep: {}",
                            pending_lands(&conn)
                        );
                    }
                }
                let mut gc = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"));
                gc.arg("--data").arg(&data);
                // On Postgres the gc subprocess reads the store through
                // the URL, the way the server does: the schema follows
                // the store, not the path.
                if scion_server::Backend::test_is_postgres() {
                    gc.arg("--postgres-url").arg(common::postgres_url(&data));
                }
                let mut child = gc
                    .args(["gc", "--grace-secs", "0"])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .unwrap();
                let give_up = std::time::Instant::now() + Duration::from_secs(60);
                let status = loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        break status;
                    }
                    assert!(
                        std::time::Instant::now() < give_up,
                        "gc beside a live server never ended within 60 s"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                };
                let out = child.wait_with_output().unwrap();
                let err = String::from_utf8_lossy(&out.stderr);
                let pruned: Vec<&str> = err
                    .lines()
                    .filter(|line| {
                        line.contains("pruned")
                            || line.contains("swept")
                            || line.contains("M4 MUTANT")
                            || line.contains("pending rows")
                    })
                    .collect();
                for line in pruned.iter().take(12) {
                    println!("gc: {line}");
                }
                assert!(status.success(), "gc failed: {err}");
            }
            Op::Burst { k, target } => {
                let name = target_name(*target);
                let base = match resolve_ref(&mut state.objects, name).await {
                    Ok(head) => {
                        state.heads.entry(name).or_default().push(head);
                        head
                    }
                    // A missing ref resolves nowhere; base on a real
                    // head, so the lands are rejected on the ref, not
                    // on the changeset.
                    Err(_) => resolve_ref(&mut state.objects, REFS[0]).await.unwrap(),
                };
                let mut idxs = Vec::new();
                for j in 0..*k {
                    let idx =
                        send_land(&mut state, j % CALLERS, *target, base, j % 2 == 0, None).await;
                    idxs.push(idx);
                }
                let mut calls = tokio::task::JoinSet::new();
                for idx in idxs {
                    let request = state.lands[idx].request();
                    let mut conn = state.scion.clone();
                    calls.spawn(async move { (idx, conn.land(request).await) });
                }
                while let Some(done) = calls.join_next().await {
                    let (idx, reply) = done.unwrap();
                    let reply = reply
                        .unwrap_or_else(|s| panic!("burst: {s:?}"))
                        .into_inner();
                    state.lands[idx].replies.push(("burst", reply));
                }
            }
            Op::BurstCut { k, target } => {
                // The lands go in flight together and queue on the ref's
                // lock, so their rows sit pending while the head of the
                // queue runs. The kill lands mid-queue: the running flow
                // aborts, the queued rows never decide, and the retry
                // phase finishes them against a repo whose collector may
                // have eaten their blobs (M4). The replies are dropped,
                // not read.
                let name = target_name(*target);
                let base = match resolve_ref(&mut state.objects, name).await {
                    Ok(head) => {
                        state.heads.entry(name).or_default().push(head);
                        head
                    }
                    Err(_) => resolve_ref(&mut state.objects, REFS[0]).await.unwrap(),
                };
                let mut idxs = Vec::new();
                for j in 0..*k {
                    let idx =
                        send_land(&mut state, j % CALLERS, *target, base, j % 2 == 0, None).await;
                    idxs.push(idx);
                }
                let mut calls = tokio::task::JoinSet::new();
                for idx in idxs {
                    let request = state.lands[idx].request();
                    let mut conn = state.scion.clone();
                    calls.spawn(async move { (idx, conn.land(request).await) });
                }
                let db = data.join("scion.db");
                if db.exists() {
                    let conn = rusqlite::Connection::open(&db).unwrap();
                    let saw =
                        wait_until_or_timeout("a queued land pending in the database", || {
                            pending_lands(&conn) > 0
                        })
                        .await;
                    if !saw {
                        println!(
                            "seed {}: burst-cut saw no pending row; the queue drained before the kill",
                            seed()
                        );
                    }
                }
                state.killed = true;
                drop(server); // SIGKILL
                server = common::start_after_kill(&data, &log);
                let (scion, objects) = connect(&server.url).await.unwrap();
                state.scion = scion;
                state.objects = objects;
                // The in-flight calls die with the old server; their
                // tasks are dropped unread.
            }
            Op::Check => check(&mut state).await,
            // A skip op whose ledger was empty: nothing to do.
        }
    }
    // The model holds at the end, whatever the script did.
    check(&mut state).await;
    let mut summary = RunSummary {
        gc_during_pending: state.gc_during_pending,
        retried_after_kill: state.killed
            && state.lands.iter().any(|rec| {
                rec.replies
                    .first()
                    .is_some_and(|(source, _)| *source == "retry")
            }),
        gc_beside_a_live_land: state.gc_beside_a_live_land,
        ..RunSummary::default()
    };
    for rec in &state.lands {
        let states: Vec<String> = rec
            .replies
            .iter()
            .map(|(source, reply)| format!("{source}={:?}", reply.state()))
            .collect();
        println!("land {}: {}", rec.key, states.join(" "));
    }
    for rec in &state.lands {
        if rec.original.changes().values().any(|state| {
            matches!(
                state,
                Some(scion_core::FileState::Regular {
                    executable: true,
                    ..
                })
            )
        }) {
            summary.executable_lands += 1;
        }
        match rec.final_reply().map(LandResponse::state) {
            Some(LandState::Landed) => summary.landed += 1,
            Some(LandState::Conflict) => summary.conflict += 1,
            Some(LandState::Rejected) => summary.rejected += 1,
            _ => {}
        }
    }
    // fsck sees no damage. It is read-only and safe beside a live
    // server, and the retry phase of the final Check has settled every
    // land, so nothing is pending to look like a problem. On Postgres
    // the fsck subprocess reads the store through the URL, the way the
    // server does: the schema follows the store, not the path.
    let mut fsck = std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"));
    fsck.arg("--data").arg(&data);
    if scion_server::Backend::test_is_postgres() {
        fsck.arg("--postgres-url").arg(common::postgres_url(&data));
    }
    let out = fsck.arg("fsck").output().unwrap();
    assert!(
        out.status.success(),
        "fsck saw damage:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    drop(server);
    stop.store(true, Ordering::Relaxed);
    summary
}

/// The wall-clock guard of one script run: every 5 s it checks the
/// budget, and when a run passes it, names the seed and the op that was
/// current, straight to the stderr file handle, and aborts the process.
/// It exists because a budget checked between ops cannot see an op that
/// blocks inside one call: a land RPC runs to its 600 s deadline, and a
/// `gc` subprocess has no ceiling at all.
struct Watchdog {
    script: Vec<Op>,
    seed: u64,
    started: std::time::Instant,
    budget: Duration,
    ordinal: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl Watchdog {
    fn run(self) {
        while !self.stop.load(Ordering::Relaxed) {
            if self.started.elapsed() >= self.budget {
                let at = self.ordinal.load(Ordering::Relaxed);
                let op = self.script.get(at.saturating_sub(1));
                let mut err = std::io::stderr().lock();
                let _ = writeln!(
                    err,
                    "the random case passed its budget of {budget:?} at op {at} \
                     (seed {s}): {op:?}; aborting so the cap cannot hide it",
                    at = at,
                    budget = self.budget,
                    s = self.seed,
                    op = op.map_or_else(String::new, |op| format!("{op:?}")),
                );
                let _ = err.flush();
                std::process::abort();
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    }
}

/// The elapsed time as `1m23.4s`, for a log line a human scans.
fn humantime_millis(d: Duration) -> String {
    format!("{}.{:02}s", d.as_secs(), d.subsec_millis() / 10)
}

/// Every landed land's own files read back, byte for byte, from its
/// result: the tree the server built names the client's blobs, and a
/// collector that removed one makes the read refuse (M4), and a store
/// that lost a byte makes it differ.
async fn check_files(state: &mut Run) {
    for rec in &state.lands {
        let Some(reply) = rec.final_reply() else {
            continue;
        };
        if reply.state() != LandState::Landed {
            continue;
        }
        let result = changeset_id(&reply.result_changeset_id);
        for (path, file) in rec.original.changes() {
            let Some(FileState::Regular { content, .. }) = file else {
                continue;
            };
            let want = state
                .put
                .get(content)
                .unwrap_or_else(|| panic!("{}: the client never put {content}", rec.key));
            let request = v1::ReadFileRequest {
                repo: REPO.into(),
                changeset_id: result.as_bytes().to_vec(),
                path: path.as_bytes().to_vec(),
            };
            let mut got = state
                .scion
                .read_file(request)
                .await
                .unwrap_or_else(|s| {
                    panic!(
                        "{}: ReadFile {path:?} at {result}: {s:?} - the file the land put is gone",
                        rec.key
                    )
                })
                .into_inner();
            let mut got_bytes = Vec::new();
            while let Some(part) = got.message().await.unwrap() {
                got_bytes.extend_from_slice(&part.data);
            }
            assert_eq!(
                &got_bytes, want,
                "{}: ReadFile {path:?} at {result} returned other bytes than the client put",
                rec.key
            );
        }
    }
}

/// The retry phase and the model checks of one `Check` op.
async fn check(state: &mut Run) {
    finish_lands(&mut state.scion, &mut state.lands).await;
    check_keys(&state.lands);
    for target in REFS {
        let (mut scion, mut objects) = (state.scion.clone(), state.objects.clone());
        check_chain(&mut scion, &mut objects, target, &state.lands).await;
        check_conflict(&mut scion, &mut objects, target, &state.lands).await;
        check_durability(
            &mut scion,
            &mut objects,
            target,
            &state.lands,
            &state.acked_before_kill,
        )
        .await;
    }
    check_get_land(&state.scion, &state.lands).await;
    check_files(state).await;
}

/// Retries every land that has no final reply, two calls at once for a
/// land whose reply a kill cut: calls with the same key wait for each
/// other and return the same land. Appends the replies.
#[allow(clippy::needless_range_loop)] // the ledger's length is read while replies land
async fn finish_lands(scion: &mut Scion, lands: &mut Ledger) {
    let mut calls = tokio::task::JoinSet::new();
    for idx in 0..lands.len() {
        if lands[idx].is_final() {
            continue;
        }
        let request = lands[idx].request();
        let mut retry_conn = scion.clone();
        calls.spawn(async move {
            let reply = retry_conn
                .land(request)
                .await
                .unwrap_or_else(|s| panic!("the retry of land {idx}: {s:?}"));
            (idx, reply.into_inner())
        });
        // The twin of a cut call: in flight at the same time.
        if lands[idx].replies.is_empty() {
            let request = lands[idx].request();
            let mut twin_conn = scion.clone();
            calls.spawn(async move {
                let reply = twin_conn
                    .land(request)
                    .await
                    .unwrap_or_else(|s| panic!("the twin retry of land {idx}: {s:?}"));
                (idx, reply.into_inner())
            });
        }
    }
    let mut replies = Vec::new();
    while let Some(done) = calls.join_next().await {
        replies.push(done.unwrap());
    }
    for (idx, reply) in replies {
        lands[idx].replies.push(("retry", reply));
    }
}

/// K1: one land ID per land, one final state per key, no regression.
/// Prints every panic to the raw stderr handle - the one libtest
/// cannot capture - before the harness holds the test's output, so a
/// panic is in the CI log even when the job is cancelled before the
/// summary runs.
fn install_panic_hook() {
    static HOOK: OnceLock<()> = OnceLock::new();
    HOOK.get_or_init(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            use std::io::Write as _;
            let _ = writeln!(
                std::io::stderr().lock(),
                "PANIC: {}",
                info.payload()
                    .downcast_ref::<String>()
                    .map_or("panic", String::as_str)
            );
            default(info);
        }));
    });
}

/// Writes a check's failure to the raw stderr handle - the one libtest
/// cannot capture - before panicking, so the message is in the CI log
/// even when the job is cancelled before the harness prints the panic.
fn fail_check(message: &str) -> ! {
    panic!("{message}");
}

/// A check's assert: on failure, the message reaches the raw stderr
/// before the panic.
macro_rules! check_assert {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            fail_check(&format!($($arg)*));
        }
    };
}

fn check_keys(lands: &Ledger) {
    let mut land_ids: HashMap<String, (usize, String)> = HashMap::new();
    for rec in lands {
        check_assert!(
            !rec.replies.is_empty(),
            "{:?}: no reply ever came for its key",
            (rec.caller, &rec.key)
        );
        let mut seen_id: Option<&str> = None;
        let mut seen_final: Option<LandState> = None;
        for (source, reply) in &rec.replies {
            let owner = land_ids
                .entry(reply.land_id.clone())
                .or_insert_with(|| (rec.caller, rec.key.clone()));
            if *owner != (rec.caller, rec.key.clone()) {
                fail_check(&format!("land ID {} went to two lands", reply.land_id));
            }
            match seen_id {
                Some(before) => check_assert!(
                    before == reply.land_id.as_str(),
                    "{:?}: replies with its key name two lands: {before} and {} ({source})",
                    (rec.caller, &rec.key),
                    reply.land_id
                ),
                None => seen_id = Some(reply.land_id.as_str()),
            }
            let state = reply.state();
            match (seen_final, state) {
                (None, LandState::Pending) => {}
                (None, _) => seen_final = Some(state),
                (Some(before), LandState::Pending) => panic!(
                    "{:?}: a {source} reply went back to pending after {before:?}",
                    (rec.caller, &rec.key)
                ),
                (Some(before), _) => check_assert!(
                    before == state,
                    "{:?}: two final states for one land ({source})",
                    (rec.caller, &rec.key)
                ),
            }
        }
    }
}

/// A ref's first-parent chain, walked root-first: the root, then each
/// landed changeset with the index of the land that produced it.
struct Chain {
    steps: Vec<(ChangesetId, Changeset, Option<usize>)>,
    /// Position of each changeset ID in `steps`.
    at: HashMap<ChangesetId, usize>,
}

impl Chain {
    async fn walk(scion: &mut Scion, objects: &mut Objects, target: &str, lands: &Ledger) -> Self {
        let results: HashMap<ChangesetId, usize> = lands
            .iter()
            .enumerate()
            .filter_map(|(idx, rec)| {
                let reply = rec.final_reply()?;
                (reply.state() == LandState::Landed)
                    .then(|| (changeset_id(&reply.result_changeset_id), idx))
            })
            .collect();
        let head = resolve_ref(objects, target).await.unwrap();
        let mut raw = Vec::new();
        let mut at = head;
        loop {
            let (changeset, id) = get_changeset(scion, at).await;
            let parent = changeset.parents().first().copied();
            raw.push((id, changeset));
            match parent {
                Some(p) => at = p,
                None => break, // the repo's root changeset, now last
            }
        }
        raw.reverse();
        let mut steps = Vec::new();
        let mut at = HashMap::new();
        for (id, changeset) in raw {
            let land = results.get(&id).copied();
            at.insert(id, steps.len());
            steps.push((id, changeset, land));
        }
        Self { steps, at }
    }
}

/// K2: each ref's chain is constructible by the model's rebase rule, and
/// each landed result is the change its land sent, rebased onto its
/// parent; every land that replied LANLED is on it, exactly once.
async fn check_chain(scion: &mut Scion, objects: &mut Objects, target: &str, lands: &Ledger) {
    let chain = Chain::walk(scion, objects, target, lands).await;
    for (i, (id, changeset, land)) in chain.steps.iter().enumerate() {
        if i == 0 {
            if !(land.is_none()) {
                fail_check("{target}: the root {id} is a land's result");
            }
            continue;
        }
        let rec_idx = land.unwrap_or_else(|| {
            panic!("{target}: {id} is on the ref, but no land replied LANDED with it")
        });
        let rec = &lands[rec_idx];
        check_assert!(
            rec.target == target,
            "{:?}: its result is on {target}, but it was sent to {}",
            (rec.caller, &rec.key),
            rec.target
        );
        // R3: the result is the original change, rebased onto this parent.
        let want = rec
            .original
            .clone()
            .with_parents(changeset.parents().to_vec())
            .unwrap();
        check_assert!(
            &want == changeset,
            "{:?}: the landed result",
            (rec.caller, &rec.key)
        );
    }
    // Every land that replied LANDED for this ref is on it, exactly once:
    // the chain holds one step per land, and no step twice.
    let landed = lands
        .iter()
        .filter(|rec| {
            rec.target == target
                && rec
                    .final_reply()
                    .is_some_and(|reply| reply.state() == LandState::Landed)
        })
        .count();
    check_assert!(
        chain.steps.len() - 1 == landed,
        "{target}: {landed} lands replied LANDED, the chain holds {}",
        chain.steps.len() - 1
    );
    // R4: the chain is constructible. Walking root-first, each step's
    // land has its base on the chain before it, and no path of the land
    // changed on the chain between its base and the head it landed on.
    for i in 1..chain.steps.len() {
        let (id, _, land) = &chain.steps[i];
        let rec = &lands[land.expect("mapped above")];
        let base_at = if rec.base == chain.steps[0].0 {
            0
        } else {
            *chain.at.get(&rec.base).unwrap_or_else(|| {
                panic!(
                    "{:?}: the base {} of {id} is not on {target}'s history",
                    (rec.caller, &rec.key),
                    rec.base
                )
            })
        };
        check_assert!(
            base_at < i,
            "{:?}: the base of {id} is not before it on {target}",
            (rec.caller, &rec.key)
        );
        let moved: BTreeSet<RepoPath> = chain.steps[base_at + 1..i]
            .iter()
            .flat_map(|(_, cs, _)| cs.changes().keys().cloned())
            .collect();
        let clash: Vec<RepoPath> = rec
            .paths()
            .into_iter()
            .filter(|p| moved.contains(p))
            .collect();
        check_assert!(
            clash.is_empty(),
            "{:?}: {target} moved {clash:?} between its base and its landing, so it must conflict",
            (rec.caller, &rec.key)
        );
    }
}

/// K3: every CONFLICT reply names paths of its land that the ref really
/// moved after the land's base.
async fn check_conflict(scion: &mut Scion, objects: &mut Objects, target: &str, lands: &Ledger) {
    for rec in lands {
        if rec.target != target {
            continue;
        }
        let Some(reply) = rec.final_reply() else {
            continue;
        };
        // M2: a folder that became a file (or was pruned) after the
        // base moves every path under it. A land that writes or deletes
        // such a path must conflict.
        let head = resolve_ref(objects, target).await.unwrap();
        let mut at = head;
        let mut moved_after_base = BTreeSet::new();
        loop {
            if at == rec.base {
                break;
            }
            let (changeset, _) = get_changeset(scion, at).await;
            moved_after_base.extend(changeset.changes().keys().cloned());
            match changeset.parents().first().copied() {
                Some(parent) => at = parent,
                None => panic!(
                    "{:?}: the base {} is not on {target}'s history",
                    (rec.caller, &rec.key),
                    rec.base
                ),
            }
        }
        let under_moved = rec.original.changes().keys().any(|path| {
            let bytes = path.as_bytes();
            moved_after_base.iter().any(|m| {
                let m = m.as_bytes();
                bytes.len() > m.len() && bytes.starts_with(m) && bytes[m.len()] == b'/'
            })
        });
        if under_moved && rec.m2_phase == Some(2) {
            assert_eq!(
                reply.state(),
                LandState::Conflict,
                "{:?}: {target} made a file of the land's folder after the base, so the land must conflict",
                (rec.caller, &rec.key)
            );
        }
        if reply.state() != LandState::Conflict {
            continue;
        }
        let named: BTreeSet<RepoPath> = reply
            .conflict_paths
            .iter()
            .map(|p| RepoPath::new(p.as_slice()).unwrap())
            .collect();
        let mine: BTreeSet<RepoPath> = rec.paths().into_iter().collect();
        check_assert!(
            named.is_subset(&mine),
            "{:?}: conflict paths {named:?} are not a subset of the land's",
            (rec.caller, &rec.key)
        );
        // The protocol allows a CONFLICT with no paths when the land's
        // changes do not apply to the head's tree; the detail then says
        // why. A CONFLICT with paths must name a path the ref really
        // moved after the base, as of the decided head.
        if named.is_empty() {
            check_assert!(
                !reply.detail.is_empty(),
                "{:?}: a CONFLICT with no paths must say why in the detail",
                (rec.caller, &rec.key)
            );
            continue;
        }
        // A named path counts as moved when the ref moved it, or moved
        // a folder over it: a folder turned into a file or pruned moves
        // every path under it too.
        let hit: Vec<&RepoPath> = named
            .iter()
            .filter(|p| {
                moved_after_base.contains(*p)
                    || moved_after_base.iter().any(|m| {
                        let m = m.as_bytes();
                        p.as_bytes().len() > m.len()
                            && p.as_bytes().starts_with(m)
                            && p.as_bytes()[m.len()] == b'/'
                    })
            })
            .collect();
        check_assert!(
            !hit.is_empty(),
            "{:?}: conflict over {named:?}, but {target} moved none of them after {}",
            (rec.caller, &rec.key),
            rec.base
        );
    }
}

/// K4: `GetLand` answers for every land ID the server issued, with the
/// state the replies hold.
async fn check_get_land(scion: &Scion, lands: &Ledger) {
    for rec in lands {
        let Some(reply) = rec.final_reply() else {
            continue;
        };
        assert_ne!(
            reply.land_id,
            "",
            "{:?}: the land never got an ID",
            (rec.caller, &rec.key)
        );
        let request = GetLandRequest {
            repo: REPO.into(),
            land_id: reply.land_id.clone(),
        };
        let mut own = scion.clone();
        let got = own
            .get_land(request)
            .await
            .unwrap_or_else(|s| panic!("{:?}: GetLand: {s:?}", (rec.caller, &rec.key)))
            .into_inner();
        assert_eq!(got.land_id, reply.land_id);
        check_assert!(
            got.state() == reply.state(),
            "{:?}: GetLand answers another state",
            (rec.caller, &rec.key)
        );
        if reply.state() == LandState::Landed {
            check_assert!(
                changeset_id(&got.result_changeset_id) == changeset_id(&reply.result_changeset_id),
                "{:?}: GetLand names another result",
                (rec.caller, &rec.key)
            );
        }
    }
}

/// K5: durability across each kill. Every land that replied LANDED
/// before a kill is still on its ref's history after the restart,
/// unchanged (its ID is its content).
async fn check_durability(
    scion: &mut Scion,
    objects: &mut Objects,
    target: &str,
    lands: &Ledger,
    acked_before_kill: &[(usize, ChangesetId)],
) {
    let chain = Chain::walk(scion, objects, target, lands).await;
    for (idx, result) in acked_before_kill
        .iter()
        .filter(|(idx, _)| lands[*idx].target == target)
    {
        let rec = &lands[*idx];
        check_assert!(
            chain.at.contains_key(result),
            "{:?}: its land's result {result} left {target} across the restart",
            (rec.caller, &rec.key)
        );
    }
}

/// `GetChangeset` as one call: the changeset and the ID it hashes to.
async fn get_changeset(scion: &mut Scion, id: ChangesetId) -> (Changeset, ChangesetId) {
    let request = v1::GetChangesetRequest {
        repo: REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
    };
    let mut stream = scion
        .get_changeset(request)
        .await
        .unwrap_or_else(|s| panic!("GetChangeset {id}: {s:?}"))
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(part) = stream.message().await.unwrap() {
        bytes.extend_from_slice(&part.changeset);
    }
    Changeset::decode_with_id(&bytes).unwrap()
}

/// A restart after a kill waits for the killed server's store lock. The
/// lock outlives the SIGKILL by a moment - on Postgres the advisory
/// lock dies with the killed server's session only when the database
/// ends that backend - so the model's restarts go through
/// `start_after_kill`, which retries ONLY the holder refusal, bounded.
/// The race itself is timing-shaped, so this test holds the lock for
/// real: a live server keeps it, and the restart succeeds the moment
/// the holder dies, however long that took. Any other refusal fails at
/// once.
#[test]
fn a_restart_waits_for_the_killed_servers_store_lock() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let log = dir.path().join("server.log");
    std::fs::create_dir_all(&data).unwrap();
    let holder = Server::start(&data, &log);
    // The restart races a lock the holder keeps. On its own thread:
    // the retry loop runs while this thread waits, then kills.
    let (data, log) = (data.clone(), log.clone());
    let waiter = std::thread::spawn(move || {
        let server = common::start_after_kill(&data, &log);
        // The check runs while this thread's server still lives: the
        // restart the retry landed answers the store's reads.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (scion, mut objects) = connect(&server.url).await.unwrap();
            drop(scion);
            // A read that names a repo the fresh store does not hold:
            // the answer is the store's own NotFound, which proves the
            // restarted server answers, whatever it holds.
            let status = resolve_ref(&mut objects, REFS[0])
                .await
                .expect_err("the fresh store holds no refs yet");
            assert_eq!(status.code(), tonic::Code::NotFound, "{status}");
        });
    });
    std::thread::sleep(std::time::Duration::from_millis(300));
    drop(holder); // SIGKILL the holder; the waiter's next try must land
    waiter.join().unwrap();
}

/// The checked-in seed corpus: each file is one script, run first, so a
/// regression here reads like a sentence.
#[test]
fn the_seed_corpus_holds() {
    install_panic_hook();
    // Postgres pays per-op commits and a full server restart per script:
    // the whole corpus measured 1m53s under it on the runner, and a
    // per-PR share that keeps every outcome kind - kill, twin, gc, a
    // conflict, a missing ref - is these six files, under one minute.
    // The nightly land-model workflow runs the whole corpus there
    // (SCION_LANDMODEL_CASES set).
    let short_on_postgres = ["02", "03", "05", "06", "08", "10"];
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/land_model_corpus");
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "txt") {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres")
                && std::env::var("SCION_LANDMODEL_CASES").is_err()
                && !short_on_postgres.iter().any(|p| name.starts_with(p))
            {
                continue;
            }
            names.push(path);
        }
    }
    names.sort();
    if names.is_empty() {
        fail_check("the corpus is empty");
    }
    let (mut landed, mut conflict, mut rejected) = (0, 0, 0);
    let (mut retried_after_kill, mut gc_beside) = (false, false);
    let (mut executable_lands, mut gc_during_pending) = (0, false);
    for path in names {
        let text = std::fs::read_to_string(&path).unwrap();
        let script = parse_script(&text);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let summary = rt.block_on(run_script(&script));
        landed += summary.landed;
        conflict += summary.conflict;
        rejected += summary.rejected;
        retried_after_kill |= summary.retried_after_kill;
        gc_beside |= summary.gc_beside_a_live_land;
        executable_lands += summary.executable_lands;
        gc_during_pending |= summary.gc_during_pending;
    }
    // The corpus must keep making every kind of outcome: a parser or a
    // strategy change cannot empty it without this sounding.
    if landed == 0 {
        fail_check("the corpus never saw a LANDED land");
    }
    if conflict == 0 {
        fail_check("the corpus never saw a CONFLICT");
    }
    if rejected == 0 {
        fail_check("the corpus never saw a REJECTED land");
    }
    if !retried_after_kill {
        fail_check("the corpus never finished a land by the retry phase after a kill");
    }
    if !gc_beside {
        fail_check("the corpus never ran the collector beside a live land");
    }
    if executable_lands == 0 {
        fail_check(
            "the corpus never put an executable file, so a mode-dropping \
             mutation would survive an empty comparison",
        );
    }
    if !gc_during_pending {
        fail_check(
            "the corpus never ran the collector while a land sat pending, \
             so a collector that eats a pending land's blobs would survive \
             an empty read-back",
        );
    }
}

/// The random case runs on a hand-built runner, not the `proptest!`
/// macro: the macro re-applies every `PROPTEST_*` env var over the
/// config it is handed (`contextualize_config`), so CI's workspace-wide
/// `PROPTEST_CASES=500` multiplied this server-backed case to 500
/// scripts and the job died at its cap with no failure (run
/// 39926739912: 120 scripts in 24 minutes, then the kill). The runner
/// built here answers to nothing but this file's own numbers. The same
/// shape already guards `access.rs`'s server-backed cases.
#[test]
fn the_model_holds_across_a_random_script() {
    install_panic_hook();
    // Postgres pays per-op commits and full server restarts: the
    // whole suite under it ran past this job's 30 minutes, and the
    // nightly workflow carries the full random coverage there
    // instead (8 cases, its own job). The per-PR Postgres share
    // stays the corpus, measured at about 2 minutes.
    if std::env::var("SCION_TEST_BACKEND").as_deref() == Ok("postgres")
        && std::env::var("SCION_LANDMODEL_CASES").is_err()
    {
        println!(
            "skipping the random case on Postgres: the nightly land-model workflow carries it there (SCION_LANDMODEL_CASES forces it on)"
        );
        return;
    }
    let cases = cases_for_this_run();
    let mut runner = proptest::test_runner::TestRunner::new(prop_config());
    let strategy = script_strategy();
    // Counted, not configured: the runner replays the persisted failure
    // cases through the same closure, so the real count is what the log
    // must state.
    let ran = std::sync::atomic::AtomicU32::new(0);
    let verdict = runner.run(&strategy, |script| {
        ran.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Printed before the case runs, so a failure names its seed and
        // its script in the log even if shrinking then eats the cap.
        println!("seed: {}", seed());
        println!("script: {script:?}");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(run_script(&script));
        Ok(())
    });
    if let Err(failure) = verdict {
        fail_check(&format!("the random case failed: {failure}"));
    }
    let ran = ran.load(std::sync::atomic::Ordering::Relaxed);
    if ran == 0 {
        fail_check("the random case ran no script at all");
    }
    if ran < cases {
        fail_check(&format!(
            "the random case ran {ran} scripts but {cases} cases were asked for"
        ));
    }
    println!("the random case ran {ran} scripts (replays included), {cases} case(s) asked for");
}

/// The case count this file runs: one per PR, `SCION_LANDMODEL_CASES`
/// for a nightly or a manual run.
fn cases_for_this_run() -> u32 {
    std::env::var("SCION_LANDMODEL_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// One case per CI run; `SCION_LANDMODEL_CASES` raises it for a nightly
/// or a manual run. Shrinking happens only on failure.
fn prop_config() -> proptest::test_runner::Config {
    let cases = cases_for_this_run();
    // The shrunk scripts land in this exact file, relative to the
    // crate: the default persistence looks for a lib.rs or main.rs to
    // anchor on, and an integration test has neither.
    std::fs::create_dir_all("proptest-regressions").unwrap();
    proptest::test_runner::Config {
        cases,
        rng_seed: proptest::test_runner::RngSeed::Fixed(seed()),
        // Shrinking reruns many real-server scripts - one run costs
        // tens of seconds - and proptest's max_shrink_time did not
        // bound it in CI: a single failing case ran 37 shrinks for 21
        // minutes and the job was cancelled before the harness printed
        // the panic. Eight iterations end a failure in minutes; the
        // regression file keeps the seed and the input for a local
        // rerun.
        max_shrink_time: 60_000,
        // A per-PR run (one case) shrinks not at all: proptest's
        // iteration budget did not hold on CI - a failing case ran 25
        // shrinks for 23 minutes with iters 8 and the time belt set -
        // and the runner exits before its shrink loop when this is 0.
        // The nightly run (more cases) shrinks with the caps on.
        max_shrink_iters: if cases == 1 { 0 } else { 8 },
        failure_persistence: Some(Box::new(
            proptest::test_runner::FileFailurePersistence::Direct(
                "proptest-regressions/land_model_proptest.txt",
            ),
        )),
        ..proptest::test_runner::Config::default()
    }
}

/// The seed the random case runs on: `SCION_LANDMODEL_SEED` pins a run
/// for reproduction; otherwise the run draws one and the test prints it,
/// so a failure names the seed that produced it.
static SEED: OnceLock<u64> = OnceLock::new();

fn seed() -> u64 {
    *SEED.get_or_init(|| {
        std::env::var("SCION_LANDMODEL_SEED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(1, |d| u64::try_from(d.as_millis()).unwrap_or(1))
            })
    })
}

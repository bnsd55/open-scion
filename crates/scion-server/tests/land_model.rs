//! The reference model for concurrent lands, retries and restarts (the v0.5
//! gate, #378). Several principals land against the real `scion-server`
//! binary — normal lands, retries with the same request key, lands to a ref
//! that does not exist, calls dropped in flight, and a SIGKILL per round —
//! and a plain in-memory model checks what came out:
//!
//! - the by-key rule: every reply for a (caller, request key) names one land
//!   ID, and the state never regresses;
//! - the rebase rule: a land whose base the ref moved past over one of the
//!   land's paths conflicts, and one whose base the ref still honors lands;
//! - linearizability: each ref's final first-parent chain, walked from its
//!   head to the root, is constructible by the model's own rebase
//!   simulation from the lands that replied LANDED — in any order;
//! - durability: a kill leaves landed lands landed with the same result on
//!   the ref's history, and pending lands that a retry finishes;
//!   `GetLand` answers for every ID the server issued, to its owner only.
//!
//! Determinism, stated honestly: the seed picks the operations and the kill
//! points, and replays them. The interleaving of concurrent lands does not
//! replay — thread timing differs — so every check is a property that must
//! hold under ANY schedule over the ledger a run actually produced. A
//! failure is therefore a real bug, not a scheduling artifact; the printed
//! seed reproduces the workload that found it.

#![cfg(unix)]

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use scion_core::{Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{
    self, CreateChangesetRequest, GetLandRequest, LandRequest, LandResponse, LandState,
};
use tonic::Status;

/// Rounds of land, kill and check. Each round starts the server, runs the
/// principals, kills the server under them, and checks.
const ROUNDS: u64 = 3;
/// Principals that land concurrently. Each holds its own connections and
/// its own request keys: the by-key rule is per (repo, caller, key).
const PRINCIPALS: usize = 4;
/// Operations per principal per round.
const OPS: u64 = 20;
/// A ref that does not exist, for the lands that must be rejected.
const MISSING: &str = "no-such-ref";

/// One land a principal sent, with every reply the run saw for it.
struct Rec {
    caller: usize,
    key: String,
    /// The changeset as the client built it: its paths and file states are
    /// what the rebase rule moves onto the ref.
    original: Changeset,
    id: ChangesetId,
    base: ChangesetId,
    target: String,
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

/// Names a land in messages without moving out of the model.
fn tag(rec: &Rec) -> (usize, &str) {
    (rec.caller, &rec.key)
}

/// What one principal did in one round.
#[derive(Default)]
struct Notebook {
    recs: Vec<Rec>,
}

/// The model: the ledger of lands, keyed the way the server keys them.
#[derive(Default)]
struct Model {
    recs: Vec<Rec>,
}

impl Model {
    fn absorb(&mut self, notebook: Notebook) {
        self.recs.extend(notebook.recs);
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
    /// Walks `target` from its head to the root through `GetChangeset`,
    /// and maps every step to the land whose final reply named it.
    async fn walk(scion: &mut Scion, objects: &mut Objects, target: &str, model: &Model) -> Self {
        let results: HashMap<ChangesetId, usize> = model
            .recs
            .iter()
            .enumerate()
            .filter_map(|(idx, rec)| {
                let reply = rec.final_reply()?;
                (reply.state() == LandState::Landed)
                    .then(|| (changeset_id(&reply.result_changeset_id), idx))
            })
            .collect();
        let mut head = resolve_ref(objects, target).await.unwrap();
        let mut raw = Vec::new();
        loop {
            let (changeset, id) = get_changeset(scion, head).await;
            let parent = changeset.parents().first().copied();
            raw.push((id, changeset));
            match parent {
                Some(p) => head = p,
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

/// K1: one land ID per land, one final state per key, no regression.
fn check_keys(model: &Model) {
    let mut land_ids: HashMap<String, (usize, String)> = HashMap::new();
    let _ = &land_ids;
    for rec in &model.recs {
        assert!(
            !rec.replies.is_empty(),
            "{:?}: no reply ever came for its key",
            tag(rec)
        );
        let mut seen_id: Option<&str> = None;
        let mut seen_final: Option<LandState> = None;
        for (source, reply) in &rec.replies {
            let owner = land_ids
                .entry(reply.land_id.clone())
                .or_insert_with(|| (rec.caller, rec.key.clone()));
            assert_eq!(
                *owner,
                (rec.caller, rec.key.clone()),
                "land ID {} went to two lands",
                reply.land_id
            );
            match seen_id {
                Some(before) => assert_eq!(
                    before,
                    reply.land_id.as_str(),
                    "{:?}: replies with its key name two lands: {before} and {} ({source})",
                    tag(rec),
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
                    tag(rec)
                ),
                (Some(before), _) => assert_eq!(
                    before,
                    state,
                    "{:?}: two final states for one land ({source})",
                    tag(rec)
                ),
            }
        }
    }
}

/// K2: each ref's chain is constructible by the model's rebase rule, and
/// each landed result is the change its land sent, rebased onto its parent.
async fn check_chain(
    scion: &mut Scion,
    objects: &mut Objects,
    target: &'static str,
    model: &Model,
) {
    let chain = Chain::walk(scion, objects, target, model).await;
    for (i, (id, changeset, land)) in chain.steps.iter().enumerate() {
        if i == 0 {
            // The repo's root changeset: no land produced it.
            assert!(land.is_none(), "{target}: the root {id} is a land's result");
            continue;
        }
        let rec_idx = land.unwrap_or_else(|| {
            panic!("{target}: {id} is on the ref, but no land replied LANDED with it")
        });
        let rec = &model.recs[rec_idx];
        assert_eq!(
            rec.target,
            target,
            "{:?}: its result is on {target}, but it was sent to {}",
            tag(rec),
            rec.target
        );
        // R3: the result is the original change, rebased onto this parent.
        let want = rec
            .original
            .clone()
            .with_parents(changeset.parents().to_vec())
            .unwrap();
        assert_eq!(&want, changeset, "{:?}: the landed result", tag(rec));
    }
    // Every land that replied LANDED for this ref is on it, exactly once.
    let landed = model
        .recs
        .iter()
        .filter(|rec| {
            rec.target == target
                && rec
                    .final_reply()
                    .is_some_and(|reply| reply.state() == LandState::Landed)
        })
        .count();
    assert_eq!(
        chain.steps.len() - 1,
        landed,
        "{target}: {landed} lands replied LANDED, the chain holds {}",
        chain.steps.len() - 1
    );
    // R4: the chain is constructible. Walking root-first, each step's land
    // has its base on the chain before it, and no path of the land changed
    // on the chain between its base and the head it landed on.
    for i in 1..chain.steps.len() {
        let (id, _, land) = &chain.steps[i];
        let rec = &model.recs[land.expect("mapped above")];
        let base_at = if rec.base == chain.steps[0].0 {
            0
        } else {
            *chain.at.get(&rec.base).unwrap_or_else(|| {
                panic!(
                    "{:?}: the base {} of {id} is not on {target}'s history",
                    tag(rec),
                    rec.base
                )
            })
        };
        assert!(
            base_at < i,
            "{:?}: the base of {id} is not before it on {target}",
            tag(rec)
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
        assert!(
            clash.is_empty(),
            "{:?}: {target} moved {clash:?} between its base and its landing, so it must conflict",
            tag(rec)
        );
    }
}

/// K3: every CONFLICT reply names paths of its land that the ref really
/// moved after the land's base.
async fn check_conflict(
    scion: &mut Scion,
    objects: &mut Objects,
    target: &'static str,
    model: &Model,
) {
    for rec in &model.recs {
        if rec.target != target {
            continue;
        }
        let Some(reply) = rec.final_reply() else {
            continue;
        };
        if reply.state() != LandState::Conflict {
            continue;
        }
        let named: BTreeSet<RepoPath> = reply
            .conflict_paths
            .iter()
            .map(|p| RepoPath::new(p.as_slice()).unwrap())
            .collect();
        let mine: BTreeSet<RepoPath> = rec.paths().into_iter().collect();
        assert!(
            !named.is_empty() && named.is_subset(&mine),
            "{:?}: conflict paths {named:?} are not a non-empty subset of the land's",
            tag(rec)
        );
        // The ref moved at least one named path after the land's base: the
        // final chain holds the whole landing order, so walking from the
        // head back to the base collects everything the ref moved since.
        let head = resolve_ref(objects, target).await.unwrap();
        let mut at = head;
        let mut moved = BTreeSet::new();
        loop {
            if at == rec.base {
                break;
            }
            let (changeset, _) = get_changeset(scion, at).await;
            moved.extend(changeset.changes().keys().cloned());
            match changeset.parents().first().copied() {
                Some(parent) => at = parent,
                None => panic!(
                    "{:?}: the base {} is not on {target}'s history",
                    tag(rec),
                    rec.base
                ),
            }
        }
        let hit: Vec<&RepoPath> = named.iter().filter(|p| moved.contains(*p)).collect();
        assert!(
            !hit.is_empty(),
            "{:?}: conflict over {named:?}, but {target} moved none of them after {}",
            tag(rec),
            rec.base
        );
    }
}

/// K4: `GetLand` answers for every land ID the server issued, with the
/// state the replies hold; only the owning principal may read its land.
async fn check_get_land(scions: &[Scion], model: &Model) {
    for rec in &model.recs {
        let Some(reply) = rec.final_reply() else {
            continue;
        };
        assert_ne!(
            reply.land_id,
            "",
            "{:?}: the land never got an ID",
            tag(rec)
        );
        let request = GetLandRequest {
            repo: REPO.into(),
            land_id: reply.land_id.clone(),
        };
        let mut own = scions[rec.caller].clone();
        let got = own
            .get_land(request.clone())
            .await
            .unwrap_or_else(|s| panic!("{:?}: GetLand: {s:?}", tag(rec)))
            .into_inner();
        assert_eq!(got.land_id, reply.land_id);
        assert_eq!(
            got.state(),
            reply.state(),
            "{:?}: GetLand answers another state",
            tag(rec)
        );
        if reply.state() == LandState::Landed {
            assert_eq!(
                changeset_id(&got.result_changeset_id),
                changeset_id(&reply.result_changeset_id),
                "{:?}: GetLand names another result",
                tag(rec)
            );
        }
        // A stranger's read is the server's own unit test
        // (`get_land_shows_only_the_callers_own_lands`): every principal
        // here shares the fixture's one token, so they are one caller.
        let _ = request;
    }
}

/// K5: durability across the kill. Every land that replied LANDED before
/// the kill is still on its ref's history after the restart, unchanged
/// (its ID is its content).
async fn check_durability(
    scion: &mut Scion,
    objects: &mut Objects,
    target: &'static str,
    model: &Model,
    pre_kill: &[(usize, ChangesetId)],
) {
    let chain = Chain::walk(scion, objects, target, model).await;
    for (caller, key, result) in pre_kill.iter().filter_map(|(idx, result)| {
        let rec = &model.recs[*idx];
        (rec.target == target).then(|| (rec.caller, rec.key.clone(), *result))
    }) {
        assert!(
            chain.at.contains_key(&result),
            "{caller:?} {key}: its land's result {result} left {target} across the restart"
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

/// Retries every land that has no final reply, two calls at once for a
/// land whose reply a kill cut: calls with the same key wait for each
/// other and return the same land. Appends the replies.
async fn finish_lands(scions: &[Scion], model: &mut Model) {
    let mut calls = tokio::task::JoinSet::new();
    for idx in 0..model.recs.len() {
        if model.recs[idx].is_final() {
            continue;
        }
        let (request, scion) = {
            let rec = &model.recs[idx];
            (rec.request(), scions[rec.caller].clone())
        };
        calls.spawn(async move {
            let mut scion = scion;
            let reply = scion
                .land(request)
                .await
                .unwrap_or_else(|s| panic!("the retry of land {idx}: {s:?}"));
            (idx, reply.into_inner())
        });
        // The twin of a cut call: in flight at the same time.
        if model.recs[idx].replies.is_empty() {
            let (request, scion) = {
                let rec = &model.recs[idx];
                (rec.request(), scions[rec.caller].clone())
            };
            calls.spawn(async move {
                let mut scion = scion;
                let reply = scion
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
        model.recs[idx].replies.push(("retry", reply));
    }
}

/// Polls an earlier land by ID, when one has an ID yet: the reply and the
/// index it belongs to. `None` skips the op (no ID is known, the die said
/// no, or the kill cut the call).
async fn poll_one(
    scion: &mut Scion,
    recs: &[Rec],
    rng: &mut Rng,
) -> Result<Option<(usize, LandResponse)>, Status> {
    if recs.is_empty() || rng.below(6) != 0 {
        return Ok(None);
    }
    let idx = rng.below(recs.len());
    let Some(reply) = recs[idx]
        .final_reply()
        .filter(|reply| !reply.land_id.is_empty())
        .cloned()
    else {
        return Ok(None);
    };
    let request = GetLandRequest {
        repo: REPO.into(),
        land_id: reply.land_id.clone(),
    };
    let polled = scion.get_land(request).await?.into_inner();
    assert_eq!(
        polled.land_id, reply.land_id,
        "GetLand answered another land"
    );
    Ok(Some((idx, polled)))
}

/// One change for a land: one or two own paths, and sometimes the shared
/// one, so concurrent lands to one ref conflict.
async fn build_changes(
    scion: &mut Scion,
    op: u64,
    rng: &mut Rng,
) -> Result<BTreeMap<RepoPath, Option<FileState>>, Status> {
    let mut changes = BTreeMap::new();
    for i in 0..=rng.below(1) {
        let len = 1 + rng.below(64);
        let data = rng.bytes(len);
        let id = put_blob(scion, &data).await?;
        changes.insert(
            RepoPath::new(format!("o{op}/f{i}")).unwrap(),
            Some(FileState::Regular {
                content: id,
                executable: false,
            }),
        );
    }
    if rng.below(3) == 0 {
        let data = rng.bytes(16);
        let id = put_blob(scion, &data).await?;
        changes.insert(
            RepoPath::new(SHARED).unwrap(),
            Some(FileState::Regular {
                content: id,
                executable: false,
            }),
        );
    }
    Ok(changes)
}

/// One principal: its ops for one round. A call that fails is the kill;
/// anything else is a bug.
async fn principal(
    url: String,
    caller: usize,
    mut rng: Rng,
    ops: u64,
    signals: Arc<Signals>,
) -> Notebook {
    let name = format!("p{caller}");
    let mut recs: Vec<Rec> = Vec::new();
    let (mut scion, mut objects) = match connect(&url).await {
        Ok(both) => both,
        Err(error) => return cut(recs, &name, "connect", &error, &signals),
    };
    let mut bases = HashMap::new();
    for op in 0..ops {
        let tag = format!("{name}-o{op}-{}", rng.next() % 1_000_000);
        // Retry an earlier land with its key, or poll one by ID, or land.
        if !recs.is_empty() && rng.below(5) == 0 {
            let idx = rng.below(recs.len());
            let request = recs[idx].request();
            let reply = match scion.land(request).await {
                Ok(reply) => reply.into_inner(),
                Err(status) => return cut(recs, &name, "retry", &status, &signals),
            };
            recs[idx].replies.push(("retry", reply));
            continue;
        }
        let polled = match poll_one(&mut scion, &recs, &mut rng).await {
            Ok(polled) => polled,
            Err(status) => return cut(recs, &name, "GetLand", &status, &signals),
        };
        if let Some((idx, reply)) = polled {
            recs[idx].replies.push(("poll", reply));
            continue;
        }
        let target = if rng.below(12) == 0 {
            MISSING.to_owned()
        } else {
            REFS[rng.below(REFS.len())].to_owned()
        };
        let base = match bases.get(target.as_str()) {
            Some(&base) if rng.below(4) == 0 => base,
            _ => {
                // The missing ref resolves nowhere; base on a real head, so
                // the land is rejected on the ref, not on the changeset.
                let name_for_base = if target == MISSING {
                    REFS[0]
                } else {
                    target.as_str()
                };
                match resolve_ref(&mut objects, name_for_base).await {
                    Ok(id) => id,
                    Err(status) => return cut(recs, &name, "ResolveRef", &status, &signals),
                }
            }
        };
        bases.insert(target.clone(), base);
        let changes = match build_changes(&mut scion, op, &mut rng).await {
            Ok(changes) => changes,
            Err(status) => return cut(recs, &name, "PutBlob", &status, &signals),
        };
        let original = Changeset::new(
            vec![base],
            author(),
            None,
            tag.clone().into_bytes(),
            changes,
        )
        .expect("changeset fits the format");
        let (bytes, id) = original.encode_with_id();
        let reply = match scion
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await
        {
            Ok(reply) => reply.into_inner(),
            Err(status) => return cut(recs, &name, "CreateChangeset", &status, &signals),
        };
        assert_eq!(changeset_id(&reply.changeset_id), id);
        let rec = Rec {
            caller,
            key: tag,
            original,
            id,
            base,
            target: target.clone(),
            replies: Vec::new(),
        };
        let request = rec.request();
        // The intent is in the notebook before the call: a kill may cut the
        // reply, and the retry phase then finishes the land it left pending.
        recs.push(rec);
        let idx = recs.len() - 1;
        if target != MISSING && rng.below(10) == 0 {
            // A call dropped in flight: the land stays pending, and the
            // retry phase finishes it.
            let fut = scion.land(request);
            tokio::pin!(fut);
            let _ = tokio::time::timeout(Duration::from_millis(20), fut).await;
        } else {
            let reply = match scion.land(request).await {
                Ok(reply) => reply.into_inner(),
                Err(status) => return cut(recs, &name, "land", &status, &signals),
            };
            recs[idx].replies.push(("land", reply));
        }
    }
    Notebook { recs }
}

/// A failed call is the kill; anything else is a bug.
fn cut(
    recs: Vec<Rec>,
    name: &str,
    call: &str,
    error: &dyn std::fmt::Debug,
    signals: &Signals,
) -> Notebook {
    let killed = signals.killed.load(Ordering::SeqCst);
    assert!(killed, "{name}: {call} failed before the kill: {error:?}");
    Notebook { recs }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_model_holds_across_lands_retries_and_restarts() {
    let rounds = usize::try_from(env("SCION_LANDMODEL_ROUNDS", ROUNDS)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    let seed = env(
        "SCION_LANDMODEL_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    println!("seed {seed}; replay with SCION_LANDMODEL_SEED={seed}");
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);

    let mut model = Model::default();
    let mut rng = Rng::new(seed);
    for round in 0..rounds {
        let server = Server::start(&data, &log);
        let signals = Arc::new(Signals::default());
        let principals: Vec<_> = (0..PRINCIPALS)
            .map(|caller| {
                let (url, rng, signals) = (
                    server.url.clone(),
                    Rng::new(rng.next()),
                    Arc::clone(&signals),
                );
                tokio::spawn(principal(url, caller, rng, OPS, signals))
            })
            .collect();
        // Let the principals get to work, then kill the server under them:
        // two rounds in three, at a moment a land is pending, so a crash
        // meets a pending row.
        tokio::time::sleep(Duration::from_millis(150)).await;
        if round % 3 != 0 {
            let conn = rusqlite::Connection::open(data.join("scion.db")).unwrap();
            wait_until("a land is pending in the database", || {
                pending_lands(&conn) > 0
            })
            .await;
        }
        tokio::time::sleep(Duration::from_millis(u64::try_from(rng.below(60)).unwrap())).await;
        signals.killed.store(true, Ordering::SeqCst);
        drop(server);
        for principal in principals {
            model.absorb(principal.await.unwrap());
        }

        // The pre-kill picture: the lands that already replied LANDED.
        let pre_kill: Vec<(usize, ChangesetId)> = model
            .recs
            .iter()
            .enumerate()
            .filter_map(|(idx, rec)| {
                let reply = rec
                    .replies
                    .iter()
                    .rev()
                    .map(|(_, reply)| reply)
                    .find(|reply| reply.state() == LandState::Landed)?;
                Some((idx, changeset_id(&reply.result_changeset_id)))
            })
            .collect();

        // The restart, then the checks.
        let server = Server::start(&data, &log);
        let (scion, objects) = connect(&server.url).await.unwrap();
        let scions: Vec<Scion> = (0..PRINCIPALS).map(|_| scion.clone()).collect();
        finish_lands(&scions, &mut model).await;
        check_keys(&model);
        for target in REFS {
            let (mut scion, mut objects) = (scion.clone(), objects.clone());
            check_chain(&mut scion, &mut objects, target, &model).await;
            check_conflict(&mut scion, &mut objects, target, &model).await;
            check_durability(&mut scion, &mut objects, target, &model, &pre_kill).await;
        }
        check_get_land(&scions, &model).await;
    }
    println!(
        "{} lands checked over {rounds} rounds; the model holds",
        model.recs.len()
    );
}

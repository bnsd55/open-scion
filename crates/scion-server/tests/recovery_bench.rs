//! The measured recovery time of `docs/operations.md`, section 7: a
//! store of a stated size (repos, changesets, blob bytes, and pending
//! lands), a server killed with SIGKILL mid-land, then the time from
//! the restart until readiness answers `SERVING` and the first read
//! succeeds. A measurement, not a gate: plain `cargo test` skips it,
//! and the `recovery time` CI job runs it with `--ignored --nocapture`
//! on a fixed runner label, which is the machine class the docs quote.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::{Bearer, Caller};
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, LandRequest, PutBlobRequest, ResolveRefRequest};
use scion_server::test_support::backend::from_store;
use scion_server::test_support::land::LandState;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::store::Store;
use tonic::transport::Endpoint;
use tonic_health::pb::health_client::HealthClient;

/// The stated size of the measured store, the restore bench's own: repos
/// and changesets per repo, and the content bytes of each changeset's
/// own file.
const REPOS: u8 = 20;
const CHANGESETS_PER_REPO: u8 = 50;
const FILE_BYTES: usize = 32 * 1024;
/// Pending lands built into the store before the crash: the recovery
/// starts over rows a retry would have to finish.
const PENDING_LANDS: u8 = 5;
/// How long lands fly before the kill: long enough for several lands to
/// sit between the pending row and the landed one, short enough that
/// the whole measure stays minutes.
const LAND_STORM_SECS: u64 = 2;
/// A recovery of this store past this bound means something changed in
/// the restart path, not in the machine.
const RECOVERY_BOUND_SECS: u64 = 60;
/// The bearer token both servers take and the clients send.
const TOKEN: &str = "recovery-bench-token";
/// The port both servers take, one after the other.
const PORT: u16 = 17910;
/// The health service's readiness name; the health service takes no
/// token.
const READINESS_SERVICE: &str = "scion.v1.Scion";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement for the recovery-time job: run with --ignored --nocapture"]
async fn a_restart_after_a_kill_recovers_in_a_stated_time() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let store = from_store(Store::open(&data).unwrap());

    // The build, the restore bench's shape: REPOS repos of
    // CHANGESETS_PER_REPO changesets each, one file per changeset, every
    // chain deriving as it grows. Its size is what the recovery starts
    // from, so the number the docs quote names what was recovered.
    let mut blob_bytes: u64 = 0;
    let build_started = Instant::now();
    let mut tips: Vec<ChangesetId> = Vec::new();
    for repo in 0..REPOS {
        let name = format!("repo-{repo:03}");
        let mut parent = None;
        let mut ref_version: Option<i64> = None;
        let mut tip = None;
        for step in 0..CHANGESETS_PER_REPO {
            let bytes = vec![b'a' + (step % 26); FILE_BYTES];
            let blob = store.blobs().put(&bytes).unwrap();
            blob_bytes += bytes.len() as u64;
            let cs = chain_changeset(&name, step, blob, parent);
            let id = store
                .write(|c| {
                    let repo_id = c.create_repo(&name)?;
                    c.add_repo_blob(repo_id, blob)?;
                    let id = c.insert_changeset(repo_id, &cs)?;
                    assert!(c.cas_ref(repo_id, "main", ref_version, id)?);
                    Ok(id)
                })
                .unwrap();
            ref_version = Some(ref_version.map_or(1, |v| v + 1));
            parent = Some(id);
            tip = Some(id);
        }
        tips.push(tip.expect("the repo has changesets"));
    }
    let changesets = u64::from(REPOS) * u64::from(CHANGESETS_PER_REPO);
    let build = build_started.elapsed();

    // The pending lands, built in: one per the first PENDING_LANDS
    // repos, recorded the way a land records itself before it works, so
    // the store the crash hits carries rows a retry would have to
    // finish.
    for (repo_at, tip) in tips.iter().enumerate().take(usize::from(PENDING_LANDS)) {
        let req = scion_server::test_support::land::LandRequest {
            repo: format!("repo-{repo_at:03}"),
            changeset: *tip,
            target_ref: "main".into(),
            request_key: format!("pending-{repo_at}"),
            deadline: scion_server::test_support::deadline::Deadline::new(
                Instant::now() + Duration::from_secs(600),
            ),
        };
        store
            .commit(|c| {
                let (_, outcome) = c.record_land(&Caller("bench@x".to_string()), &req, None)?;
                assert_eq!(
                    outcome.state,
                    LandState::Pending,
                    "the built land records as pending"
                );
                Ok(())
            })
            .unwrap();
    }

    // The seam's handle closes before the server opens the same files:
    // the test's own writer must not hold the store the server wants.
    drop(store);
    std::thread::sleep(std::time::Duration::from_millis(300));

    // The port is the test's own: a rerun on a development machine must
    // not talk to a server a panicking earlier run left behind. On the
    // runner the port is fresh and this kills nothing.
    Command::new("sh")
        .arg("-c")
        .arg(format!(
            "lsof -t -iTCP:{PORT} -sTCP:LISTEN | xargs kill -9 2> /dev/null || true"
        ))
        .output()
        .expect("the port frees");

    // The first server, held by a guard: a panicking test kills its
    // servers on the way out, so none survives to answer a later run.
    let mut server = ServerGuard(start_server_named(dir.path(), "server-1.log"));
    wait_for_serving(dir.path()).await;

    // Lands in flight, through the wire, then SIGKILL mid-land. The kill
    // is the point of the measure: the runbook's crash, at a moment the
    // lands are between the pending row and the landed one.
    let lander = tokio::spawn(land_loop());
    tokio::time::sleep(Duration::from_secs(LAND_STORM_SECS)).await;
    lander.abort();
    kill_nine(&mut server.0);
    let _ = server.0.wait();
    let pending_after_crash = pending_lands(&data);

    // The measure: from the restart's command to readiness answering
    // SERVING and the first read succeeding.
    let started = Instant::now();
    let mut recovered = ServerGuard(start_server_named(dir.path(), "server-2.log"));
    wait_for_serving(dir.path()).await;
    first_read_succeeds().await;
    let recovery = started.elapsed();
    kill_nine(&mut recovered.0);
    let _ = recovered.0.wait();

    println!(
        "recovery: {:.1} s (build {:.1} s) for {REPOS} repos, {changesets} changesets, \
         {} MiB blob bytes, {PENDING_LANDS} pending lands built in, \
         {pending_after_crash} pending left by the crash",
        recovery.as_secs_f64(),
        build.as_secs_f64(),
        blob_bytes / (1024 * 1024),
    );
    assert!(
        recovery.as_secs() < RECOVERY_BOUND_SECS,
        "the recovery took {recovery:?}, past the {RECOVERY_BOUND_SECS} s bound: something \
         changed in the restart path, not in the machine"
    );
}

/// The server as the runbook starts it: a real process on the data
/// directory, its logs beside the store, on the fixed port.
fn start_server_named(work: &std::path::Path, log: &str) -> Child {
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(work.join(log))
        .expect("the log opens");
    let err = out.try_clone().expect("the log clones");
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(work.join("data"))
        .arg("serve")
        .arg("--listen")
        .arg(format!("127.0.0.1:{PORT}"))
        .arg("--token")
        .arg(TOKEN)
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .expect("the scion-server binary spawns")
}

/// SIGKILL: the kill the runbook cannot catch, mid-land by the loop.
fn kill_nine(child: &mut Child) {
    child.kill().expect("the server dies to SIGKILL");
}

/// A spawned server the test cannot leak: on panic or on success, the
/// drop kills the child, so no run leaves a listener on the port that a
/// later run would take for its own.
struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Polls the health service until readiness answers SERVING. The
/// service takes no token; a poll before the port opens errors, and the
/// loop reads that as not yet.
async fn wait_for_serving(work: &std::path::Path) {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{PORT}"))
        .unwrap()
        .connect_lazy();
    let mut health = HealthClient::new(endpoint);
    let deadline = Instant::now() + Duration::from_secs(RECOVERY_BOUND_SECS);
    let mut first = true;
    loop {
        assert!(
            Instant::now() < deadline,
            "the server never answered SERVING inside the bound"
        );
        if let Ok(status) = health
            .check(tonic::Request::new(tonic_health::pb::HealthCheckRequest {
                service: READINESS_SERVICE.to_string(),
            }))
            .await
        {
            if status.into_inner().status
                == tonic_health::pb::health_check_response::ServingStatus::Serving as i32
            {
                return;
            }
        } else if first {
            // One look at the server's own words before the loop goes
            // quiet: a startup that failed names its reason here.
            first = false;
            if let Ok(log) = std::fs::read_to_string(work.join("server.log")) {
                panic!(
                    "the server never came up; its log: {}",
                    &log[..log.len().min(2000)]
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The first read after the restart: the ref resolves, through the wire,
/// with the token a client sends.
async fn first_read_succeeds() {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{PORT}"))
        .unwrap()
        .connect_lazy();
    let bearer = Bearer::new(TOKEN).expect("the token is a header");
    let mut client = ScionClient::with_interceptor(endpoint, bearer);
    let answer = client.resolve_ref(ResolveRefRequest {
        repo: "repo-000".into(),
        name: "main".into(),
    });
    let answer = answer
        .await
        .expect("the first read after the recovery answers");
    assert_eq!(
        answer.into_inner().changeset_id.len(),
        32,
        "the first read resolves a changeset"
    );
}

/// Lands in flight, through the wire: a loop of blob, changeset, land,
/// distinct keys, so the kill catches lands between the pending row and
/// the landed one. The loop dies with the process it talks to.
async fn land_loop() {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{PORT}"))
        .unwrap()
        .connect_lazy();
    let bearer = Bearer::new(TOKEN).expect("the token is a header");
    let mut client = ScionClient::with_interceptor(endpoint, bearer);
    let mut n = 0u64;
    loop {
        // The ref's tip, rebased by every land before this one.
        let Ok(answer) = client
            .resolve_ref(ResolveRefRequest {
                repo: "repo-000".into(),
                name: "main".into(),
            })
            .await
        else {
            return;
        };
        let tip: [u8; 32] = answer
            .into_inner()
            .changeset_id
            .try_into()
            .expect("32 bytes");
        let Ok(put) = client
            .put_blob(tokio_stream::once(PutBlobRequest {
                repo: "repo-000".into(),
                data: vec![b'x'; 1024],
            }))
            .await
        else {
            return;
        };
        let blob = BlobId::from_bytes(put.into_inner().blob_id.try_into().expect("32 bytes"));
        let cs = Changeset::new(
            vec![ChangesetId::from_bytes(tip)],
            IdentityTime::new(b"repo-000".to_vec(), b"bench@x".to_vec(), 0, 0)
                .expect("identity fits the format"),
            None,
            format!("recovery storm {n}").into_bytes(),
            [(
                RepoPath::new(format!("s{n:05}").into_bytes()).unwrap(),
                Some(FileState::Regular {
                    content: blob,
                    executable: false,
                }),
            )]
            .into(),
        )
        .expect("changeset fits the format");
        let (bytes, id) = cs.encode_with_id();
        if client
            .create_changeset(CreateChangesetRequest {
                repo: "repo-000".into(),
                changeset: bytes,
            })
            .await
            .is_err()
        {
            return;
        }
        let _ = client
            .land(LandRequest {
                repo: "repo-000".into(),
                changeset_id: id.as_bytes().to_vec(),
                target_ref: "main".into(),
                request_key: format!("storm-{n}"),
            })
            .await;
        n += 1;
    }
}

/// The pending lands the crash left, counted straight from the meta
/// database while no server holds it.
fn pending_lands(data: &std::path::Path) -> u64 {
    let db = rusqlite::Connection::open(data.join("scion.db")).expect("the meta opens");
    let pending: u64 = db
        .query_row(
            "SELECT count(*) FROM lands WHERE state = 'pending'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .expect("the lands table reads")
        .unsigned_abs();
    pending
}

/// The changeset `step` of a chain in `name`: one file backed by `blob`,
/// on `parent`, with a message that names it.
fn chain_changeset(name: &str, step: u8, blob: BlobId, parent: Option<ChangesetId>) -> Changeset {
    Changeset::new(
        parent.into_iter().collect(),
        IdentityTime::new(name.as_bytes().to_vec(), b"bench@x".to_vec(), 0, 0)
            .expect("identity fits the format"),
        None,
        format!("{name} step {step}").into_bytes(),
        [(
            RepoPath::new(format!("f{step:03}").into_bytes()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format")
}

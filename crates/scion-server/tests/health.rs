//! Health suite: the real `scion-server` binary answers `grpc.health.v1`
//! `Watch` with `SERVING` once the store is open. The named services are
//! readiness: on shutdown they flip to `NOT_SERVING` before the drain,
//! while the server still answers on the open watch. The empty name is
//! liveness: it stays `SERVING` while the process answers, so a supervisor
//! does not kill a draining server. The drain holds until the in-flight
//! calls end, and its bound cuts them and exits 1 past it.
#![cfg(unix)]

mod common;

use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;

use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{CreateChangesetRequest, LandRequest, LandState, PutBlobRequest};

/// A running `scion-server`, and the watcher of its overall health.
struct Server {
    child: Child,
    watcher: Option<tonic::Streaming<tonic_health::pb::HealthCheckResponse>>,
    named: Option<tonic::Streaming<tonic_health::pb::HealthCheckResponse>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The address of the newest `serving` line in the log, if there is one.
fn bound_addr(log: &Path) -> Option<SocketAddr> {
    let text = fs::read_to_string(log).ok()?;
    let line = text.lines().rev().find(|line| line.contains("serving"))?;
    line.split("listen=")
        .nth(1)?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Starts the binary with extra `serve` flags, on `data`, logging to `log`.
fn spawn_with(extra: &[&str], data: &Path, log: &Path) -> Server {
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    cmd.arg("--data").arg(data);
    // Postgres mode: the same schema the test helpers filled (the marker
    // file in the data directory makes a plain SQLite start refuse).
    if scion_server::Backend::test_is_postgres() {
        cmd.arg("--postgres-url").arg(common::postgres_url(data));
    }
    let child = cmd
        .args(["serve", "--token", common::TOKEN, "--listen", "127.0.0.1:0"])
        .args(extra)
        // Plain log lines, so the address parses without escape codes.
        .env("NO_COLOR", "1")
        // The write park of a blob whose file is missing: a test parks a
        // writer of the real process with a park file.
        .env("SCION_TEST_PARK_WRITES", "1")
        .env("RUST_LOG", "info")
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file)
        .spawn()
        .unwrap();
    Server {
        child,
        watcher: None,
        named: None,
    }
}

/// Waits until the server has bound its port, then opens a watch on its
/// overall health, which answers `SERVING` at once.
async fn wait_serving(server: &mut Server, log: &Path) {
    let started = std::time::Instant::now();
    let addr = loop {
        if let Some(status) = server.child.try_wait().ok().flatten() {
            // The server's own log names the refusal; a silent exit code
            // is not debuggable.
            let tail = fs::read_to_string(log).unwrap_or_default();
            let last: Vec<&str> = tail.lines().rev().take(6).collect();
            panic!(
                "the server exited early: {status}; log tail: {}",
                last.into_iter().rev().collect::<Vec<_>>().join(" | ")
            );
        }
        if let Some(addr) = bound_addr(log) {
            break addr;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "timed out");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("the server listens");
    let mut client = HealthClient::new(channel);
    let watcher = client
        .watch(tonic::Request::new(HealthCheckRequest {
            service: String::new(),
        }))
        .await
        .expect("the health service answers")
        .into_inner();
    // A watch of one named service answers `SERVING` too, on its own
    // connection, so the test does not depend on how one connection
    // multiplexes two idle streams.
    let addr2 = bound_addr(log).expect("the address again");
    let channel2 = Channel::from_shared(format!("http://{addr2}"))
        .unwrap()
        .connect()
        .await
        .expect("the server listens again");
    let mut client2 = HealthClient::new(channel2);
    let named = client2
        .watch(tonic::Request::new(HealthCheckRequest {
            service: "scion.v1.Scion".to_owned(),
        }))
        .await
        .expect("the named service answers")
        .into_inner();
    let mut named = named;
    let first = common::ceiling("the first named status", named.message())
        .await
        .unwrap()
        .expect("a status");
    assert_eq!(first.status(), ServingStatus::Serving);
    server.watcher = Some(watcher);
    server.named = Some(named);
}

/// Sends `SIGINT`, as Ctrl-C does.
fn interrupt(child: &Child) {
    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .output()
        .expect("kill runs");
}

/// Sends `SIGTERM`, as an operator's stop does.
fn terminate(child: &Child) {
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .output()
        .expect("kill runs");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_serves_until_shutdown_starts() {
    let data = tempfile::tempdir().unwrap();
    let log_dir = data.path().join("log");
    fs::create_dir(&log_dir).unwrap();
    let log = log_dir.join("server.log");
    let mut server = spawn_with(&[], data.path(), &log);
    wait_serving(&mut server, &log).await;
    let mut watcher = server.watcher.take().unwrap();

    // The first status is `SERVING`: the store is open.
    let first = common::ceiling("the first overall status", watcher.message())
        .await
        .unwrap()
        .expect("a status");
    assert_eq!(first.status(), ServingStatus::Serving);

    // Shutdown starts: the named service flips to `NOT_SERVING` on its
    // open stream, while the server still answers. The overall watch
    // carries no update: liveness stays `SERVING` through the drain, so
    // a supervisor does not kill a draining server.
    interrupt(&server.child);
    let mut named = server.named.take().unwrap();
    let update = common::ceiling("the named NOT_SERVING update", named.message())
        .await
        .expect("the named watch stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
    // The overall watch carries no update within this wait: liveness
    // stays `SERVING` through the drain, so a supervisor does not kill a
    // draining server. The named watch above proves the drain is on.
    let quiet = tokio::time::timeout(Duration::from_millis(500), watcher.message()).await;
    assert!(
        quiet.is_err(),
        "liveness flipped during the drain: {quiet:?}"
    );
}

/// The drain holds while a call is open, and a second signal ends it: the
/// in-flight calls are cut, the log says so, and the process exits 1.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_signal_ends_the_drain_of_an_open_call() {
    let data = tempfile::tempdir().unwrap();
    let log_dir = data.path().join("log");
    fs::create_dir(&log_dir).unwrap();
    let log = log_dir.join("server.log");
    let mut server = spawn_with(&[], data.path(), &log);
    wait_serving(&mut server, &log).await;
    let mut named = server.named.take().unwrap();

    // The first signal starts the drain: the named watch sees
    // `NOT_SERVING` and stays open, so the server keeps running. (Its
    // first status, `SERVING`, wait_serving already read.)
    interrupt(&server.child);
    let update = common::ceiling("the NOT_SERVING update", named.message())
        .await
        .expect("the named watch stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        server.child.try_wait().unwrap().is_none(),
        "the drain ended while the watch was open"
    );

    // The second signal cuts the drain: the process ends promptly, with a
    // non-zero exit code, and the log names the cut.
    interrupt(&server.child);
    let started = std::time::Instant::now();
    let status = loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the second signal did not end the drain within 5 s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(status.code(), Some(1), "the cut is a non-zero exit");
    let logged = fs::read_to_string(&log).unwrap_or_default();
    assert!(
        logged.contains("a second signal ended the drain"),
        "the log does not name the cut"
    );
}

/// Waits until the child has ended, and returns its exit code.
async fn exit_code(child: &mut Child, within: Duration, why: &'static str) -> i32 {
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code().expect("the child ended by a signal");
        }
        assert!(started.elapsed() < within, "{why}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Sends one chunk of `data` and holds the stream open: the call is in
/// flight until the sender's half is dropped. Returns the task of its
/// reply.
fn stalled_upload(
    scion: &mut common::Scion,
    data: &'static [u8],
) -> tokio::task::JoinHandle<Result<tonic::Response<scion_proto::v1::PutBlobResponse>, tonic::Status>>
{
    let parts = vec![PutBlobRequest {
        repo: common::REPO.into(),
        data: data.to_vec(),
    }];
    let mut stalled = scion.clone();
    // The stream ends at once, so the server starts its write; a park
    // file (below) holds that write, and with it the call.
    tokio::spawn(async move { stalled.put_blob(tokio_stream::iter(parts)).await })
}

/// Waits for the server to bind its port, and returns its URL.
async fn wait_url(log: &Path) -> String {
    let started = std::time::Instant::now();
    let addr = loop {
        if let Some(addr) = bound_addr(log) {
            break addr;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the server never bound its port"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    format!("http://{addr}")
}

/// The drain finishes its in-flight calls, and the store keeps the
/// documented promise: readiness is false before the drain, an upload
/// that was answered stays durable over the restart, and a land that was
/// answered holds while a cut one resumes by its request key. A clean
/// drain exits 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_finishes_its_in_flight_calls_and_the_store_keeps_the_promise() {
    let data = tempfile::tempdir().unwrap();
    let log_dir = data.path().join("log");
    let hooks = data.path().join("hooks");
    fs::create_dir(&log_dir).unwrap();
    fs::create_dir(&hooks).unwrap();
    let log = log_dir.join("server.log");
    common::create_repo(data.path());
    let mut server = spawn_with(&["--drain-timeout-secs", "30"], data.path(), &log);
    wait_serving(&mut server, &log).await;
    let url = wait_url(&log).await;
    let (mut scion, mut objects) = common::connect(&url).await.unwrap();
    let base = common::resolve_ref(&mut objects, "main").await.unwrap();
    let uploaded = BlobId::of(b"in flight");

    // The upload parks server-side on its write, so the call is in
    // flight for sure while the signal lands.
    let park = hooks.join(format!("park-{uploaded}"));
    let parked = hooks.join(format!("parked-{uploaded}"));
    fs::write(&park, b"").unwrap();
    let put = stalled_upload(&mut scion, b"in flight");
    let started = std::time::Instant::now();
    while !parked.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the upload never reached its park"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A land is sent at the same moment as the signal: it is either
    // answered (so it must hold) or cut (so a retry with the same
    // request key must resume it).
    let changeset = Changeset::new(
        vec![base],
        common::author(),
        None,
        b"drain land".to_vec(),
        std::iter::once((
            RepoPath::new("f.txt").unwrap(),
            Some(FileState::Regular {
                content: uploaded,
                executable: false,
            }),
        ))
        .collect(),
    )
    .expect("changeset fits the format");
    let (bytes, id) = changeset.encode_with_id();
    let cs_bytes = bytes.clone();
    let mut landing = scion.clone();
    let land = tokio::spawn(async move {
        landing
            .create_changeset(CreateChangesetRequest {
                repo: common::REPO.into(),
                changeset: bytes,
            })
            .await?;
        landing
            .land(LandRequest {
                repo: common::REPO.into(),
                changeset_id: id.as_bytes().to_vec(),
                target_ref: "main".into(),
                request_key: "drain-0".into(),
            })
            .await
    });
    terminate(&server.child);

    // Readiness flips on the open named watch; liveness stays.
    let mut watcher = server.watcher.take().unwrap();
    let mut named = server.named.take().unwrap();
    // The overall watch's first status is still queued; read it.
    let first = common::ceiling("the first overall status", watcher.message())
        .await
        .unwrap()
        .expect("a status");
    assert_eq!(first.status(), ServingStatus::Serving);
    let update = common::ceiling("the named NOT_SERVING update", named.message())
        .await
        .expect("the named watch stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
    let quiet = tokio::time::timeout(Duration::from_millis(500), watcher.message()).await;
    assert!(
        quiet.is_err(),
        "liveness flipped during the drain: {quiet:?}"
    );

    // The watches go, so they hold nothing: the drain may end.
    drop(named);
    drop(watcher);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The park lifts: the upload finishes, and the drain answers it.
    fs::remove_file(&park).unwrap();
    let blob = common::ceiling("the stalled upload's reply", put)
        .await
        .unwrap()
        .expect("the upload answers during the drain")
        .into_inner()
        .blob_id;
    assert_eq!(BlobId::try_from(blob.as_slice()).unwrap(), uploaded);

    // The land's reply decides which promise applies.
    let land = common::ceiling("the land's reply during the drain", land)
        .await
        .unwrap();
    let answered = land.is_ok();

    // The drain ends, and the process exits 0 within its bound.
    let code = exit_code(
        &mut server.child,
        Duration::from_secs(35),
        "the drain never ended",
    )
    .await;
    assert_eq!(code, 0, "a clean drain exits 0");

    after_the_restart(data.path(), &log, answered, id, cs_bytes).await;
}

/// After a restart on the same data directory: an answered land is at the
/// ref; a cut one is resumed by its request key; and the store is clean.
async fn after_the_restart(
    data: &Path,
    log: &Path,
    answered: bool,
    id: ChangesetId,
    cs_bytes: Vec<u8>,
) {
    let again = common::Server::start(data, log);
    let (mut scion, mut objects) = common::connect(&again.url).await.unwrap();
    if answered {
        assert_eq!(
            common::resolve_ref(&mut objects, "main").await.unwrap(),
            id,
            "the answered land holds over the restart"
        );
    } else {
        // The cut reach the changeset's create too: both are idempotent,
        // so the retry re-creates the changeset and lands with the same
        // request key.
        scion
            .create_changeset(CreateChangesetRequest {
                repo: common::REPO.into(),
                changeset: cs_bytes,
            })
            .await
            .unwrap();
        let resumed = scion
            .land(LandRequest {
                repo: common::REPO.into(),
                changeset_id: id.as_bytes().to_vec(),
                target_ref: "main".into(),
                request_key: "drain-0".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resumed.state(), LandState::Landed, "the cut land resumes");
        assert_eq!(
            common::resolve_ref(&mut objects, "main").await.unwrap(),
            id,
            "the resumed land is at the ref"
        );
    }
    let mut fsck = Command::new(env!("CARGO_BIN_EXE_scion-server"));
    fsck.arg("--data").arg(data);
    // On Postgres the store lives in the database; a bare fsck would
    // read the data directory's SQLite and report what is not there.
    if scion_server::Backend::test_is_postgres() {
        fsck.arg("--postgres-url").arg(common::postgres_url(data));
    }
    let clean = fsck.arg("fsck").output().unwrap();
    assert_eq!(clean.status.code(), Some(0), "fsck is clean");
    drop(again);
}
/// The drain bound cuts the calls that run past it: the process exits 1
/// and the log names the cut, and a later retry works against a restart.
#[tokio::test(flavor = "multi_thread")]
async fn the_drain_bound_cuts_the_calls_past_it() {
    let data = tempfile::tempdir().unwrap();
    let log_dir = data.path().join("log");
    fs::create_dir(&log_dir).unwrap();
    let log = log_dir.join("server.log");
    common::create_repo(data.path());
    let mut server = spawn_with(&["--drain-timeout-secs", "1"], data.path(), &log);
    wait_serving(&mut server, &log).await;
    let url = wait_url(&log).await;
    let (mut scion, _objects) = common::connect(&url).await.unwrap();

    // An upload whose write never lands: no park file is ever lifted.
    let put = stalled_upload(&mut scion, b"never answered");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The stop: readiness flips on the open named watch.
    terminate(&server.child);
    let mut named = server.named.take().unwrap();
    let update = common::ceiling("the named NOT_SERVING update", named.message())
        .await
        .expect("the named watch stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);

    // The bound cuts the call: exit 1, and the log names the cut.
    let code = exit_code(
        &mut server.child,
        Duration::from_secs(10),
        "the bound never fired",
    )
    .await;
    assert_eq!(code, 1, "a cut drain exits non-zero");
    let logged = fs::read_to_string(&log).unwrap();
    assert!(
        logged.contains("the drain timeout passed"),
        "the log does not name the bound"
    );
    drop(put);

    // The unacknowledged upload left no damage, and a retry works.
    let again = common::Server::start(data.path(), &log);
    let (mut scion, _objects) = common::connect(&again.url).await.unwrap();
    let blob = common::put_blob(&mut scion, b"retry").await.unwrap();
    assert_eq!(blob, BlobId::of(b"retry"));
    drop(again);
}

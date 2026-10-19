//! Health suite: the real `scion-server` binary answers `grpc.health.v1`
//! `Watch` with `SERVING` once the store is open, and with `NOT_SERVING`
//! when shutdown starts, while the server still answers on the open watch;
//! a second signal while the drain runs ends it, cutting the open calls.
#![cfg(unix)]

mod common;

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use tempfile::TempDir;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// A running `scion-server`, and the watcher of its overall health.
struct Server {
    child: Child,
    watcher: Option<tonic::Streaming<tonic_health::pb::HealthCheckResponse>>,
    named: Option<tonic::Streaming<tonic_health::pb::HealthCheckResponse>>,
    _data: TempDir,
    log: PathBuf,
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

/// Starts the real binary on a fresh data directory.
fn spawn() -> Server {
    let data = tempfile::tempdir().unwrap();
    let dir = data.path().join("log");
    fs::create_dir(&dir).unwrap();
    let log = dir.join("server.log");
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(data.path())
        .args(["serve", "--token", "t", "--listen", "127.0.0.1:0"])
        // Plain log lines, so the address parses without escape codes.
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file)
        .spawn()
        .unwrap();
    Server {
        child,
        watcher: None,
        named: None,
        _data: data,
        log,
    }
}

/// Waits until the server has bound its port, then opens a watch on its
/// overall health, which answers `SERVING` at once.
async fn wait_serving(server: &mut Server, log: &Path) {
    let started = std::time::Instant::now();
    let addr = loop {
        if let Some(status) = server.child.try_wait().ok().flatten() {
            panic!("the server exited early: {status}");
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

#[tokio::test(flavor = "multi_thread")]
async fn the_server_serves_until_shutdown_starts() {
    let mut server = spawn();
    let log = server.log.clone();
    wait_serving(&mut server, &log).await;
    let mut watcher = server.watcher.take().unwrap();

    // The first status is `SERVING`: the store is open.
    let first = common::ceiling("the first overall status", watcher.message())
        .await
        .unwrap()
        .expect("a status");
    assert_eq!(first.status(), ServingStatus::Serving);

    // Shutdown starts: the watcher sees `NOT_SERVING` on its open stream,
    // while the server still answers. A named service flips too.
    interrupt(&server.child);
    let update = common::ceiling("the NOT_SERVING update", watcher.message())
        .await
        .expect("the watcher stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
    let mut named = server.named.take().unwrap();
    let update = common::ceiling("the named NOT_SERVING update", named.message())
        .await
        .expect("the named watch stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
}

/// The drain holds while a call is open, and a second signal ends it: the
/// in-flight calls are cut, the log says so, and the process exits 1.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_signal_ends_the_drain_of_an_open_call() {
    let mut server = spawn();
    let log = server.log.clone();
    wait_serving(&mut server, &log).await;
    let mut watcher = server.watcher.take().unwrap();

    // The first signal starts the drain: the open watch sees NOT_SERVING
    // and stays open, so the server keeps running.
    interrupt(&server.child);
    let update = common::ceiling("the NOT_SERVING update", watcher.message())
        .await
        .expect("the watcher stays open through the drain")
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

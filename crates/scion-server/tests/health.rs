//! Health suite: the real `scion-server` binary answers `grpc.health.v1`
//! `Watch` with `SERVING` once the store is open, and with `NOT_SERVING`
//! when shutdown starts, while the server still answers on the open watch.
#![cfg(unix)]

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
    server.watcher = Some(watcher);
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
    let first = watcher.message().await.unwrap().expect("a status");
    assert_eq!(first.status(), ServingStatus::Serving);

    // Shutdown starts: the watcher sees `NOT_SERVING` on its open stream,
    // while the server still answers.
    interrupt(&server.child);
    let update = watcher
        .message()
        .await
        .expect("the watcher stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
}

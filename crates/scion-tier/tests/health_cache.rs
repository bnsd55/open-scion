//! Health suite for the cache: the real `scion-cache` binary answers
//! `grpc.health.v1` `Watch` with `SERVING` once its store and its upstream
//! channel are ready, and with `NOT_SERVING` when shutdown starts, while it
//! still answers on the open watch.
#![cfg(unix)]

use tempfile::TempDir;

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// A running `scion-cache`, and the watcher of its overall health.
struct Cache {
    child: Child,
    watcher: Option<tonic::Streaming<tonic_health::pb::HealthCheckResponse>>,
    _data: TempDir,
    log: PathBuf,
}

impl Drop for Cache {
    fn drop(&mut self) {
        // `Child::kill` sends SIGKILL.
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// A free loopback port: one that the OS just gave back.
fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// Starts the real cache binary, which needs an upstream to even start.
fn spawn() -> Cache {
    let upstream = free_port();
    let listen = free_port();
    let data = tempfile::tempdir().unwrap();
    let log = data.path().join("cache.log");
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_scion-cache"))
        .args([
            "--listen",
            &listen.to_string(),
            "--insecure-listen",
            "--token",
            "t",
            "--upstream-token",
            "t",
            "--upstream",
            &format!("http://{upstream}"),
        ])
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .stdout(log_file.try_clone().unwrap())
        .stderr(log_file)
        .spawn()
        .unwrap();
    Cache {
        child,
        watcher: None,
        _data: data,
        log,
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

/// Waits until the cache has bound its port, then opens a watch on its
/// overall health, which answers `SERVING` at once.
async fn wait_serving(cache: &mut Cache) {
    let started = std::time::Instant::now();
    let addr = loop {
        if let Some(status) = cache.child.try_wait().ok().flatten() {
            panic!("the cache exited early: {status}");
        }
        if let Some(addr) = bound_addr(&cache.log) {
            break addr;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out; log: {:?}",
            fs::read_to_string(&cache.log).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("the cache listens");
    let mut client = HealthClient::new(channel);
    let watcher = client
        .watch(tonic::Request::new(HealthCheckRequest {
            service: String::new(),
        }))
        .await
        .expect("the health service answers")
        .into_inner();
    cache.watcher = Some(watcher);
}

/// Awaits `fut`, or panics naming `step`: a cache that never answers a
/// step fails the test in seconds, not at the CI cancel.
async fn ceiling<T>(step: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .unwrap_or_else(|_| panic!("the step {step} never finished within 30 s"))
}

/// Sends `SIGINT`, as Ctrl-C does.
fn interrupt(child: &Child) {
    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .output()
        .expect("kill runs");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_cache_serves_until_shutdown_starts() {
    let mut cache = spawn();
    wait_serving(&mut cache).await;
    let mut watcher = cache.watcher.take().unwrap();

    // The first status is `SERVING`: the store and the upstream channel are
    // open.
    let first = ceiling("the first status", watcher.message())
        .await
        .unwrap()
        .expect("a status");
    assert_eq!(first.status(), ServingStatus::Serving);

    // Shutdown starts: the watcher sees `NOT_SERVING` on its open stream,
    // while the cache still answers.
    interrupt(&cache.child);
    let update = ceiling("the NOT_SERVING update", watcher.message())
        .await
        .expect("the watcher stays open through the drain")
        .expect("a status update");
    assert_eq!(update.status(), ServingStatus::NotServing);
}

//! The cache's serving line names the address the OS bound, not the one
//! the flag asked for: a starter that passes `--listen 127.0.0.1:0` reads
//! the real port from the log and connects, the way the release smoke
//! parses the server's line.

use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// A running `scion-cache`.
struct Cache {
    child: Child,
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

/// Starts the real cache binary on the port the OS chooses. The cache
/// needs an upstream to even start; a dead one keeps the test off the
/// network.
fn spawn() -> Cache {
    let upstream = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
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
            "127.0.0.1:0",
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

#[test]
fn the_serving_line_names_the_bound_port() {
    let mut cache = spawn();
    let started = Instant::now();
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
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(
        addr.port() != 0,
        "the serving line must name the port the OS bound"
    );
    // The logged port accepts connections: the log's port is the real
    // one, so a starter can connect after reading the line.
    TcpStream::connect(addr).expect("the cache serves on the logged port");
}

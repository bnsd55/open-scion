//! The daemon refuses a data directory that does not exist, not
//! creates one: a typo in `--data`, or a run from the wrong folder,
//! must not make a new empty store and serve from it. Red before the
//! rule: the daemon's default `scion-daemon-data` was created in
//! `crates/scion-tier/` by a test window that dropped `--data`, and
//! the daemon served from it.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Starts the daemon from a working directory that has no data
/// directory in it, with the caller's `RUST_LOG` pinned out.
fn spawn(cwd: &std::path::Path, args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .args(args)
        .current_dir(cwd)
        .env("NO_COLOR", "1")
        .env_remove("RUST_LOG")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
}

/// The refusal path: the daemon must exit soon with the refusal, and
/// the missing directory must still be missing. A daemon that keeps
/// running has created the directory and serves from it.
fn refuses(cwd: &std::path::Path, missing: &str, args: &[&str]) {
    let args = [&["--token", "t", "--upstream-token", "t"], args].concat();
    let mut child = spawn(cwd, &args);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!(
                "{args:?} never refused: it created {} and serves from it",
                cwd.join(missing).display()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let stderr = child.wait_with_output().unwrap().stderr;
    assert_ne!(status.code(), Some(0), "{args:?} proceeded");
    assert!(
        !cwd.join(missing).exists(),
        "{args:?} created {}",
        cwd.join(missing).display()
    );
    assert!(
        String::from_utf8_lossy(&stderr).contains("does not exist; create it or pass --data"),
        "{args:?} did not name the refusal: {}",
        String::from_utf8_lossy(&stderr)
    );
}

#[test]
fn a_daemon_refuses_a_missing_data_directory_and_creates_nothing() {
    // An explicit path that does not exist.
    let cwd = tempfile::tempdir().unwrap();
    refuses(cwd.path(), "missing", &["--data", "missing"]);

    // The default `scion-daemon-data` in the cwd: a run from the wrong
    // folder refuses, wherever it runs.
    let empty = tempfile::tempdir().unwrap();
    refuses(empty.path(), "scion-daemon-data", &[]);
}

#[test]
fn a_daemon_serves_a_data_directory_that_exists() {
    // With the directory present, the daemon opens it and serves: the
    // refusal is for the missing directory only. The socket's birth is
    // the proof it got past the store.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("daemon");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
    // The socket must sit inside a short path: macOS's sun_path caps
    // at 104 bytes, and a deep tempdir busts it. Give the socket an
    // explicit short path through /tmp.
    let short = std::env::temp_dir().join(format!("c78-{}", std::process::id()));
    let _ = std::fs::remove_file(&short);
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .args([
            "--token",
            "t",
            "--upstream-token",
            "t",
            "--data",
            data.to_str().unwrap(),
            "--socket",
            short.to_str().unwrap(),
        ])
        .current_dir(dir.path())
        .env("NO_COLOR", "1")
        .env_remove("RUST_LOG")
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut served = false;
    loop {
        if short.exists() {
            served = true;
            break;
        }
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
        if Instant::now() > deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&short);
    assert!(
        served,
        "the daemon never served a data directory that exists"
    );
}

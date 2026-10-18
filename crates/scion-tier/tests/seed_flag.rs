//! `scion-daemon` refuses to seed from a snapshot unless the operator
//! says, with `--seed-full-repo`, that the snapshot is a full-repo
//! credential: every user of the daemon may read its repo whole.

use std::io::BufRead as _;
use std::path::Path;
use std::process::Command;

use scion_core::BlobId;
use scion_proto::{Bytes, object};
use scion_tier::{Key, snapshot};
use tempfile::TempDir;

/// A snapshot of one blob, as a scion-cache writes it.
fn snapshot_of(dir: &Path) -> std::path::PathBuf {
    let data = b"seed me\n";
    let objects = [(
        Key::Blob(BlobId::of(data)),
        Bytes::from(object::pack(data).unwrap()),
    )];
    let path = dir.join("r.scnsnap");
    snapshot::write(&path, "r", &objects).unwrap();
    path
}

/// Runs `scion-daemon` with `--seed`, with `--seed-full-repo` when asked.
/// A refused seed exits at once; a seeded daemon serves until it is
/// stopped, so the caller passes `flag: false` here.
fn daemon(data: &Path, snapshot: &Path, flag: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scion-daemon"));
    command
        .arg("--data")
        .arg(data)
        .arg("--upstream")
        .arg("http://127.0.0.1:1")
        .arg("--token")
        .arg("t")
        .arg("--upstream-token")
        .arg("t")
        .arg("--seed")
        .arg(snapshot);
    if flag {
        command.arg("--seed-full-repo");
    }
    command.output().expect("scion-daemon runs")
}

#[test]
fn seeding_without_the_flag_is_refused() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_of(dir.path());
    let out = daemon(&dir.path().join("data"), &snapshot, false);
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--seed-full-repo"), "{err}");
    assert!(err.contains("full-repo credential"), "{err}");
    // Refused before anything was stored: no data directory was created.
    assert!(!dir.path().join("data").exists(), "{err}");
}

/// `--seed-full-repo` alone seeds nothing either: it says something about
/// the `--seed` files, so clap refuses it for the missing `--seed <FILE>`.
#[test]
fn the_flag_without_a_seed_is_refused() {
    let dir = TempDir::new().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .arg("--data")
        .arg(dir.path().join("data"))
        .arg("--upstream")
        .arg("http://127.0.0.1:1")
        .arg("--token")
        .arg("t")
        .arg("--upstream-token")
        .arg("t")
        .arg("--seed-full-repo")
        .output()
        .expect("scion-daemon runs");
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.lines().any(|line| line.trim() == "--seed <FILE>"),
        "the missing --seed is not what clap refused: {err}"
    );
    assert!(
        !err.contains("--seed-full-repo is required"),
        "clap must not ask for the flag: {err}"
    );
}

/// With `--seed-full-repo`, the seed really goes through: the daemon
/// reports it, then serves. The process is killed by its own PID.
#[test]
fn the_flag_lets_a_seed_through() {
    let dir = TempDir::new().unwrap();
    let snapshot = snapshot_of(dir.path());
    let mut child = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .arg("--data")
        .arg(dir.path().join("data"))
        .arg("--upstream")
        .arg("http://127.0.0.1:1")
        .arg("--token")
        .arg("t")
        .arg("--upstream-token")
        .arg("t")
        .arg("--seed")
        .arg(&snapshot)
        .arg("--seed-full-repo")
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("scion-daemon runs");
    // The child is stopped by its own handle only, never by a pattern.
    let stderr = child.stderr.take().expect("the stderr is piped");
    let mut lines = std::io::BufReader::new(stderr).lines();
    let mut seeded = false;
    for line in &mut lines {
        if line
            .as_deref()
            .is_ok_and(|line| line.contains("seeded the store from a snapshot"))
        {
            seeded = true;
            break;
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(seeded, "the daemon never reported the seed");
}

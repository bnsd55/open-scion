//! `scion-daemon` refuses to seed from a snapshot unless the operator
//! says, with `--seed-full-repo`, that the snapshot is a full-repo
//! credential: every user of the daemon may read its repo whole.

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

/// The flag alone seeds nothing either: it says something about the
/// `--seed` files, so clap refuses it without them.
#[test]
fn the_flag_without_a_seed_is_refused() {
    let dir = TempDir::new().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-daemon"))
        .arg("--data")
        .arg(dir.path().join("data"))
        .arg("--token")
        .arg("t")
        .arg("--upstream-token")
        .arg("t")
        .arg("--seed-full-repo")
        .output()
        .expect("scion-daemon runs");
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("--seed"), "{err}");
}

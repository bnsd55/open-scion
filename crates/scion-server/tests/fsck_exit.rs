//! The exit codes `scion-server fsck` promises in 1.x, as one table:
//! 0 when it saw no damage, 1 when it did, 2 when it could not run.
//! Every row of the table is a line here, so a release that changes a
//! code fails this test before it ships (docs/stability.md, 1.x, and
//! docs/operations.md, the fsck section).

#![cfg(unix)]

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use scion_server::test_support::store::Store;

/// The `scion-server fsck` exit-code table.
#[test]
fn the_exit_codes_of_fsck() {
    // 0: a clean directory.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let _blob = store.blobs().put(b"clean\n").unwrap();
    drop(store);
    assert_eq!(
        fsck(dir.path()).status.code(),
        Some(0),
        "a clean store exits 0"
    );

    // 1: damage. A blob file whose bytes no longer hash to its name is
    // the smallest damage there is.
    let objects = dir.path().join("objects/blob");
    let fan = fs::read_dir(&objects)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let file = fs::read_dir(&fan).unwrap().next().unwrap().unwrap().path();
    let mut torn = fs::read(&file).unwrap();
    let last = torn.len() - 1;
    torn[last] ^= 0xff;
    fs::write(&file, torn).unwrap();
    assert_eq!(
        fsck(dir.path()).status.code(),
        Some(1),
        "a damaged store exits 1"
    );

    // 2: cannot run. A database no store can open cannot be checked.
    let broken = tempfile::tempdir().unwrap();
    fs::write(broken.path().join("scion.db"), b"not a database at all\n").unwrap();
    assert_eq!(
        fsck(broken.path()).status.code(),
        Some(2),
        "a store that cannot open exits 2"
    );
}

/// Runs `scion-server --data <dir> fsck` and returns its status.
fn fsck(data: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .args(["--data"])
        .arg(data)
        .arg("fsck")
        .env("NO_COLOR", "1")
        .output()
        .unwrap()
}

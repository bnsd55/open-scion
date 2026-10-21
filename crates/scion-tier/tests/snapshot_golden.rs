//! The snapshot file's bytes, as the machine form of stability.md's
//! snapshot promise: the file today's writer produces for one fixed
//! object set is byte-for-byte the checked-in golden, and the golden
//! seeds back the same objects into an empty store. A changed file is
//! a format change: stability.md's rule is a new magic beside
//! `scnsnap1\n`, read beside it, never a rewrite of v1.
//!
//! Updating on purpose: run this test with
//! `SCION_UPDATE_SNAPSHOT_GOLDEN=1`, commit the changed
//! `tests/snapshot_v1.golden`, and name the break in a changes/
//! fragment. A diff in that file is a format change; no other edit
//! may touch it.

use std::fs;
use std::os::unix::fs::PermissionsExt as _;

use scion_core::BlobId;
use scion_proto::Bytes;
use scion_proto::runtime::object;
use scion_tier::DiskStore;
use scion_tier::{Key, Store as _, snapshot};

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/snapshot_v1.golden");
const REPO: &str = "MAIN";

/// The fixed object set the golden pins: two blobs, in their stored
/// form (one zstd frame each).
fn objects() -> Vec<(Key, Bytes)> {
    let a = b"snapshot golden a\n";
    let b = b"snapshot golden b\n";
    vec![
        (
            Key::Blob(BlobId::of(a)),
            Bytes::from(object::pack(a).unwrap()),
        ),
        (
            Key::Blob(BlobId::of(b)),
            Bytes::from(object::pack(b).unwrap()),
        ),
    ]
}

/// The snapshot the writer produces for the fixed object set, compared
/// against the checked-in golden - or re-pinned when the run says so.
#[test]
fn the_written_snapshot_is_the_golden() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("MAIN.scnsnap");
    snapshot::write(&path, REPO, &objects()).unwrap();
    let written = fs::read(&path).unwrap();
    assert!(
        written.starts_with(b"scnsnap1\n"),
        "the magic is the format's version: a changed one is a new format beside v1"
    );
    if std::env::var_os("SCION_UPDATE_SNAPSHOT_GOLDEN").is_some() {
        fs::write(GOLDEN, &written).unwrap();
    }
    let golden = fs::read(GOLDEN).unwrap();
    assert_eq!(
        written, golden,
        "the snapshot bytes changed: a format change is a new magic beside \
         scnsnap1 (stability.md) and a changes/ fragment naming the break; \
         to re-pin on purpose run with SCION_UPDATE_SNAPSHOT_GOLDEN=1"
    );
}

/// The golden seeds back exactly the objects it pins: the reader of a
/// release N file is the reader of release N+1.
#[test]
fn the_golden_seeds_back_the_same_objects() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let file = dir.path().join("seed.scnsnap");
    fs::write(&file, fs::read(GOLDEN).unwrap()).unwrap();
    let store = DiskStore::open(&dir.path().join("store"), usize::MAX).unwrap();
    let seeded = snapshot::seed(&store, &file)
        .unwrap()
        .expect("the golden is a snapshot");
    assert_eq!(seeded.repo, REPO);
    assert_eq!(seeded.stored, 2, "both golden objects seed");
    assert_eq!(seeded.skipped, 0);
    assert!(!seeded.damaged);
    for (key, frame) in objects() {
        let stored = store.get(key, frame.len()).unwrap().unwrap();
        assert_eq!(stored, frame, "the stored form is the golden's");
    }
}

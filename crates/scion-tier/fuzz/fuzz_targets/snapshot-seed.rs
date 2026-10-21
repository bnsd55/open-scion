//! The snapshot seed is a file an operator drops beside the store: the
//! reader must take any bytes calmly, keep only objects whose bytes match
//! their ID, and skip or mark damaged the rest. Never a panic, and nothing
//! stored that does not verify.

#![no_main]

use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;

use libfuzzer_sys::fuzz_target;
use scion_tier::DiskStore;
use scion_tier::snapshot;

fuzz_target!(|data: &[u8]| {
    // The checked open refuses a file whose parent is group- or
    // others-writable, so the temp dir is private whatever the umask of
    // the run.
    let work = tempfile::tempdir().expect("temp dir");
    #[cfg(unix)]
    std::fs::set_permissions(work.path(), Permissions::from_mode(0o700))
        .expect("make the temp dir private");
    let file = work.path().join("snapshot");
    std::fs::write(&file, data).expect("write the seed file");
    let store = DiskStore::open(&work.path().join("store"), usize::MAX).expect("an empty store");
    if let Ok(Some(seeded)) = snapshot::seed(&store, &file) {
        // Whatever the file held, the store keeps only objects whose
        // bytes match their ID: seed checked each one before the put.
        // A snapshot whose header and repo name parse but that holds no
        // records seeds an all-zero report - that is a calm take, not a
        // failure. Any file with at least one record lands in one of
        // the three buckets.
        let zero_records = data.len() == 9 + 2 + u16::from_le_bytes([data[9], data[10]]) as usize;
        assert!(
            zero_records || seeded.stored + seeded.skipped + usize::from(seeded.damaged) >= 1,
            "a snapshot with records seeds at least one bucket: {seeded:?}"
        );
    }});

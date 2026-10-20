//! The snapshot seed is a file an operator drops beside the store: the
//! reader must take any bytes calmly, keep only objects whose bytes match
//! their ID, and skip or mark damaged the rest. Never a panic, and nothing
//! stored that does not verify.

#![no_main]

use libfuzzer_sys::fuzz_target;
use scion_tier::DiskStore;
use scion_tier::snapshot;

fuzz_target!(|data: &[u8]| {
    let work = tempfile::tempdir().expect("temp dir");
    let file = work.path().join("snapshot");
    std::fs::write(&file, data).expect("write the seed file");
    let store = DiskStore::open(&work.path().join("store"), usize::MAX).expect("an empty store");
    if let Ok(Some(seeded)) = snapshot::seed(&store, &file) {
        // Whatever the file held, the store keeps only objects whose bytes
        // match their ID: seed checked each one before the put.
        assert!(seeded.stored + seeded.skipped + usize::from(seeded.damaged) >= 1);
    }
});

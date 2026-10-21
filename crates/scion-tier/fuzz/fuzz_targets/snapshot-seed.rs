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
        // Every object record the file holds is accounted exactly once:
        // stored, skipped for an ID its bytes do not match, or the one
        // damaged mark where the framing tore. The count here reads the
        // same framing, parsed on its own, not through seed's code. A
        // valid snapshot may hold zero records, so zero is a fine count.
        assert_eq!(
            seeded.stored + seeded.skipped + usize::from(seeded.damaged),
            object_records(data),
            "seed's report must cover every record the file frames"
        );
    }
});

/// The object records `data` frames, read independently of `seed`: the
/// magic and repo name header, then per record its 37-byte head and the
/// payload the head's size names. A head or a payload that ends past the
/// file is the one record `seed` marks damaged.
fn object_records(data: &[u8]) -> usize {
    const MAGIC: &[u8; 9] = b"scnsnap1\n";
    const HEAD: usize = 1 + 32 + 4;
    let mut rest = match data.get(..MAGIC.len() + 2) {
        Some(head) if head.starts_with(MAGIC) => {
            let len = usize::from(u16::from_le_bytes([
                data[MAGIC.len()],
                data[MAGIC.len() + 1],
            ]));
            match data.get(MAGIC.len() + 2..MAGIC.len() + 2 + len) {
                Some(_) => MAGIC.len() + 2 + len,
                // The name ends past the file: seed refuses the whole
                // file, so the report (and this count) never runs.
                None => return usize::MAX,
            }
        }
        // Not a snapshot: seed refuses it.
        _ => return usize::MAX,
    };
    let mut records = 0;
    while !rest_data(data, rest).is_empty() {
        let Some(head) = rest_data(data, rest).get(..HEAD) else {
            records += 1;
            break;
        };
        let size = u32::from_le_bytes([head[33], head[34], head[35], head[36]]) as usize;
        rest += HEAD;
        if rest_data(data, rest).len() < size {
            records += 1;
            break;
        }
        rest += size;
        records += 1;
    }
    records
}

fn rest_data(data: &[u8], from: usize) -> &[u8] {
    data.get(from..).unwrap_or(&[])
}

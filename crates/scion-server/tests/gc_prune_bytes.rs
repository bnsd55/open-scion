//! The collector's prune bounds its body fetches by bytes: the lengths
//! come first, then one fetch per byte-budget chunk, so a chunk's memory
//! is the byte budget plus one body, never a row count times the largest
//! body. The guard runs gc on a fixture whose bodies sum to several
//! budgets, with a small test budget, and checks three things: the pruned
//! rows are exactly the dead rows the fixture made (an independent count,
//! not the same code path), the new changesets' puts survive, and the
//! body fetches gc really made are more than one — counted at the
//! `stored_changeset_bodies` call site, so a wiring that bypasses the
//! byte-budget chunker (or the old single row-chunked fetch) fails here,
//! not only in the chunker's own unit test.

use std::collections::BTreeMap;
use std::time::Duration;

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};

use scion_server::gc::{self, Stage};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, DEFAULT_GRACE_SECS};

/// A changeset on no parent that puts `blob`, with `padding` bytes of
/// message, so the fixture controls the body's length.
fn cs_put(blob: BlobId, padding: usize) -> Changeset {
    Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        vec![b'x'; padding],
        BTreeMap::from([(
            RepoPath::new("pinned.txt").unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]),
    )
    .expect("the changeset builds")
}

#[test]
fn the_prune_fetches_bodies_in_byte_budget_chunks() {
    // Six bodies of about 400 bytes against a 512-byte budget: the chunks
    // hold one body each, so six unseen changesets take at least six
    // fetches where the old single row-chunked fetch took one.
    Backend::test_set_gc_body_budget(512);
    let dir = tempfile::TempDir::new().unwrap();
    let store = Backend::open_test_shared(dir.path()).unwrap();
    let repo_a = store.write(|c| c.create_repo("gc-a")).unwrap();
    let repo_b = store.write(|c| c.create_repo("gc-b")).unwrap();
    let old = std::time::Duration::from_secs(8 * 24 * 60 * 60);

    // Repo A: one dead row, so its prune runs and the callback that
    // follows it fires before repo B's prune starts.
    let dead_a = store.blobs().put(b"dead a\n" as &[u8]).unwrap();
    store.write(|c| c.add_repo_blob(repo_a, dead_a)).unwrap();
    age_file(&store, dead_a, old);

    // The changesets land after the snapshot walk, at the callback: the
    // walk's `seen` would otherwise name them, and repo B's prune would
    // never fetch their bodies. One of them puts the blob of a dead row,
    // so the re-read's fresh-protection must keep that row alive.
    let seeded = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seed_flag = seeded.clone();
    let store_for_seed = store.clone();
    let mut seed: Box<dyn FnMut(Stage)> = Box::new(move |stage| {
        if stage == Stage::RepoRows && !seed_flag.swap(true, std::sync::atomic::Ordering::SeqCst) {
            for i in 0..6 {
                let put = store_for_seed
                    .blobs()
                    .put(format!("live {i}\n").as_bytes())
                    .unwrap();
                store_for_seed
                    .write(|c| c.insert_changeset(repo_b, &cs_put(put, 400)))
                    .unwrap();
            }
            // The fresh-protected row: a dead row (aged, so it is a
            // candidate) that the last unseen changeset puts. The
            // snapshot walk never saw that changeset, so only the re-read
            // can keep the row.
            let pinned = store_for_seed
                .blobs()
                .put(b"pinned by a new changeset\n" as &[u8])
                .unwrap();
            store_for_seed
                .write(|c| c.add_repo_blob(repo_b, pinned))
                .unwrap();
            age_file(&store_for_seed, pinned, old);
            store_for_seed
                .write(|c| c.insert_changeset(repo_b, &cs_put(pinned, 400)))
                .unwrap();
        }
    });

    // Repo B's three plain dead rows: aged candidates no changeset puts.
    let dead_b: Vec<BlobId> = (0..3)
        .map(|i| {
            store
                .blobs()
                .put(format!("dead b {i}\n").as_bytes())
                .unwrap()
        })
        .collect();
    for blob in &dead_b {
        store.write(|c| c.add_repo_blob(repo_b, *blob)).unwrap();
        age_file(&store, *blob, old);
    }

    let report = gc::run(
        &store,
        Duration::from_secs(DEFAULT_GRACE_SECS),
        false,
        &mut |stage| seed(stage),
    )
    .unwrap();

    // Equality, against an independent count: repo A's dead row went, and
    // repo B lost only its three plain dead rows (the fixture below), so
    // three rows pruned in total. The pinned row and the puts stay.
    let b_rows = store.read(|c| c.repo_blob_ids(repo_b)).unwrap();
    let a_rows = store.read(|c| c.repo_blob_ids(repo_a)).unwrap();
    eprintln!(
        "the prune fetched the bodies in {} fetches; {} rows pruned;          repo A holds {} rows, repo B holds {}",
        Backend::test_gc_body_fetch_count(),
        report.rows_pruned,
        a_rows.len(),
        b_rows.len()
    );
    assert_eq!(
        report.rows_pruned, 4,
        "exactly the four plain dead rows: repo A's one, repo B's three"
    );
    assert!(a_rows.is_empty(), "repo A's dead row went");
    // The pinned row survived the prune of the very batch that could have
    // taken it, because the unseen changeset that puts it was re-read.
    let pinned = store.blobs().put(b"pinned by a new changeset\n").unwrap();
    assert!(
        b_rows.iter().any(|b| b.as_slice() == pinned.as_bytes()),
        "the fresh-protected row left the table"
    );
    // The bound on the real path: the wiring counted more than one body
    // fetch, so the byte-budget chunker is what ran. One fetch would mean
    // the old row-chunked shape came back.
    let fetches = Backend::test_gc_body_fetch_count();
    assert!(
        fetches > 1,
        "the prune made {fetches} body fetches: the byte budget did not split them"
    );
    Backend::test_set_gc_body_budget(0);
}

/// Sets the mtime of the blob file of `id` back by `age`: the candidate
/// scan reads the file's mtime, so a young file is no candidate at all.
fn age_file(store: &Backend, id: BlobId, age: Duration) {
    let (_, path) = store.blobs().location(id);
    let file = std::fs::File::options().write(true).open(&path).unwrap();
    file.set_modified(std::time::SystemTime::now() - age)
        .unwrap();
}

//! The collector's prune reads bodies and deletes rows in batches, so
//! its statements do not grow with the repo: a repo of 1,000 changesets
//! and 1,000 prunable rows costs the prune at most 20 statements more
//! than a repo of 100. The statement count is a process-wide seam, so
//! this guard owns its binary: no other test speaks SQL in its process.
//! On SQLite the count is always zero, which is why the guard skips
//! there.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};

use scion_server::gc::{self, Stage};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, DEFAULT_GRACE_SECS};

/// A changeset on no parent that puts `blob`.
fn cs_put(blob: BlobId) -> Changeset {
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
        b"gc\n".to_vec(),
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

/// Sets the mtime of the blob file of `id` back by `age`.
fn age_file(store: &Backend, id: BlobId, age: Duration) {
    let (_, path) = store.blobs().location(id);
    let file = std::fs::File::options().write(true).open(&path).unwrap();
    file.set_modified(SystemTime::now() - age).unwrap();
}

#[test]
fn the_prune_of_a_thousand_rows_costs_twenty_statements() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the statement count is a Postgres seam");
        return;
    }
    let old = Duration::from_secs(8 * 24 * 60 * 60);
    let mut counts = Vec::new();
    for n in [100, 1000] {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo("gc")).unwrap();
        for i in 0..n {
            // The changeset puts one blob: the prune reads its body. No
            // row names the put, so the candidate scan sees only the
            // prunable row.
            let put = store.blobs().put(format!("cs {i}\n").as_bytes()).unwrap();
            store
                .write(|c| c.insert_changeset(repo, &cs_put(put)))
                .unwrap();
            // The prunable row: names a blob no changeset puts.
            let dead = store
                .blobs()
                .put(format!("dead {i}\n").as_bytes())
                .unwrap();
            store.write(|c| c.add_repo_blob(repo, dead)).unwrap();
            age_file(&store, dead, old);
        }
        // The statements of one gc run's prune: the count resets when the
        // snapshot walk ends, and the first repo's prune is done when its
        // next stage note arrives.
        let prune = std::cell::Cell::new(None);
        let report = gc::run(
            &store,
            Duration::from_secs(DEFAULT_GRACE_SECS),
            false,
            &mut |stage| match stage {
                Stage::LiveSet => Backend::test_reset_statement_count(),
                Stage::RepoRows if prune.get().is_none() => {
                    prune.set(Some(Backend::test_statement_count()));
                }
                Stage::RepoRows | Stage::BlobFiles => {}
            },
        )
        .unwrap();
        assert_eq!(
            report.rows_pruned,
            u64::try_from(n).unwrap_or(u64::MAX),
            "n = {n}: {report:?}"
        );
        counts.push(prune.get().unwrap());
    }
    let [small, large] = [counts[0], counts[1]];
    eprintln!("the prune of 100 rows cost {small} statements, of 1,000 cost {large}");
    let growth = large - small;
    assert!(
        growth <= 20,
        "the prune of 1,000 rows cost {large} statements, of 100 cost {small}: \
         the difference {growth} grew with the repo"
    );
}

//! A wide changeset builds under the bound: the prefetch walks each path
//! one part per level, so the build's tree reads grow with paths x depth,
//! once. The quadratic walk this replaced re-walked every path from the
//! root at every level and scanned the next level's ids per path. The
//! shape is 100,000 files over 20,000 directories: the card's 20,000 over
//! 2,000 builds in ~330ms quadratic against ~190ms cursor on the writing
//! machine, too close for a bound that holds under load; the shape times
//! five separates them about forty to one. The
//! build runs on SQLite whatever the test backend is: this guard measures
//! CPU, not statements, and must not share the Postgres statement counter
//! with the statement guard.

use std::time::Duration;

use scion_core::{Changeset, FileState, IdentityTime, RepoPath};

use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;

const REPO: &str = "tree";

fn changeset(files: &[String]) -> Changeset {
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
        b"tree\n".to_vec(),
        files
            .iter()
            .map(|file| {
                (
                    RepoPath::new(file.as_str()).unwrap(),
                    Some(FileState::Regular {
                        content: scion_core::BlobId::of(file.as_bytes()),
                        executable: false,
                    }),
                )
            })
            .collect(),
    )
    .expect("changeset fits the format")
}

#[test]
fn a_wide_changeset_builds_under_the_bound() {
    // The public open with no URL is the data directory's SQLite store,
    // whatever SCION_TEST_BACKEND says.
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open(dir.path(), None, None).unwrap();
    let repo = store.write(|c| c.create_repo(REPO)).unwrap();
    // The base commit creates the directories, so the counted build's
    // directories are stored trees the apply has to read.
    let base = changeset(
        &(0..20_000)
            .map(|i| format!("d{i:05}/base.txt"))
            .collect::<Vec<_>>(),
    );
    let (base_id, base_root) = store
        .write(|c| tree::store_changeset(c, repo, &base, &mut |_| Ok(0)))
        .unwrap();
    let commit = changeset(
        &(0..100_000)
            .map(|i| format!("d{:05}/f{i}.txt", i % 20_000))
            .collect::<Vec<_>>(),
    )
    .with_parents(vec![base_id])
    .expect("changeset fits the format");
    let started = std::time::Instant::now();
    store
        .write(|c| tree::apply(c, Some(base_root), commit.changes(), &mut |_| Ok(0)))
        .unwrap();
    let elapsed = started.elapsed();
    eprintln!("100,000 files over 20,000 directories built in {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "the build took {elapsed:?}"
    );
}

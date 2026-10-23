//! A wide changeset's prefetch takes one resolution step per changed
//! path's directory: the walk's work grows with paths x depth, once. The
//! quadratic walk this replaced re-walked every path from the root at
//! every level, so its steps grow with paths x depth x levels, and the
//! same shape costs many times the sum of the paths' depths: at two
//! parts deep its next-level dedupe scans the whole level per changed
//! path. The shape is 100,000 files over 20,000 directories: the card's
//! 20,000 over 2,000 is too small to separate the two under load. The
//! build runs on
//! SQLite whatever the test backend is, so it never shares the Postgres
//! statement counter with the statement guard.

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
    // The walk's work: one resolution step per directory the changed
    // paths' chains cross, so the steps are at most the sum of the
    // changed paths' depths. A walk that re-walks the paths from the
    // root at every level resolves about twice that.
    let before = tree::prefetch_steps();
    store
        .write(|c| tree::apply(c, Some(base_root), commit.changes(), &mut |_| Ok(0)))
        .unwrap();
    let steps = tree::prefetch_steps() - before;
    let bound: u64 = commit
        .changes()
        .keys()
        .map(|path| path.as_bytes().split(|&b| b == b'/').count() as u64)
        .sum();
    eprintln!("the walk took {steps} steps for paths whose depths sum to {bound}");
    assert!(
        steps <= bound,
        "the walk took {steps} steps, past the {bound} the paths sum to"
    );
}

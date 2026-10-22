//! A commit's directory trees load in one batch per level, so the
//! statements of its root-tree derivation do not grow with the
//! directories the commit touches: a commit that writes files in 100
//! stored directories costs at most 4 statements more than one that
//! touches 10. The count covers one `tree::apply`, the derivation's tree
//! reads; the per-changeset writes of `store_changeset` are audit row 4,
//! a later card. The statement count is a process-wide seam, so this
//! guard owns its binary: no other test speaks SQL in its process. On
//! SQLite the count is always zero, which is why the guard skips there.

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
fn the_derivation_of_a_hundred_directories_costs_four_statements_more() {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the statement count is a Postgres seam");
        return;
    }
    let mut counts = Vec::new();
    for d in [10, 100] {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test_shared(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo(REPO)).unwrap();
        // The base commit creates the directories, so the counted commit's
        // directories are stored trees the apply has to read.
        let base = changeset(
            &(0..d)
                .map(|i| format!("d{i:03}/base.txt"))
                .collect::<Vec<_>>(),
        );
        let base_id = store
            .write(|c| Ok(tree::store_changeset(c, repo, &base, &mut |_| Ok(0))?.0))
            .unwrap();
        let (_base_id, base_root) = store
            .write(|c| tree::store_changeset(c, repo, &base, &mut |_| Ok(0)))
            .unwrap();
        let commit = changeset(
            &(0..d)
                .map(|i| format!("d{i:03}/new.txt"))
                .collect::<Vec<_>>(),
        )
        .with_parents(vec![base_id])
        .expect("changeset fits the format");
        // The statements of the commit's root-tree derivation: the count
        // resets after the base, whose cost is the same for every D. The
        // derived histories and the changeset row are a per-changeset
        // matter, not the tree read this guard pins.
        Backend::test_reset_statement_count();
        store
            .write(|c| tree::apply(c, Some(base_root), commit.changes(), &mut |_| Ok(0)))
            .unwrap();
        counts.push(Backend::test_statement_count());
    }
    let [small, large] = [counts[0], counts[1]];
    eprintln!(
        "the derivation of a commit in 10 directories cost {small} statements, in 100 cost {large}"
    );
    let growth = large - small;
    assert!(
        growth <= 4,
        "the derivation of a commit in 100 directories cost {large} statements, \
         in 10 cost {small}: the difference {growth} grew with the directories"
    );
}

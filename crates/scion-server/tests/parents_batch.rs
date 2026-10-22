//! The parents batch answers every ask: a merge asked twice in one
//! `parents_many` call gets its parents twice, in the same order. Two
//! histories can reach the same merge, so the caller may ask for the
//! same merge twice in one batch; a backend that deduplicates the asks
//! would answer the first copy with an empty list, and the walk would
//! call a valid graph corrupt.

use std::collections::BTreeMap;

use scion_core::Changeset;
use scion_server::Backend;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::{meta::MetaTx as _, seed};

fn author() -> scion_core::IdentityTime {
    scion_core::IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

#[test]
fn the_parents_batch_answers_a_merge_asked_twice() {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();

    let root = Changeset::new(vec![], author(), None, b"root".to_vec(), BTreeMap::new())
        .expect("changeset fits the format");
    let side = Changeset::new(
        vec![root_id_of(&root)],
        author(),
        None,
        b"side".to_vec(),
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    let side_id = side.encode_with_id().1;
    let merge = Changeset::new(
        vec![root_id_of(&root), side_id],
        author(),
        None,
        b"merge".to_vec(),
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    let merge_id = merge.encode_with_id().1;
    let css = vec![root, side, merge];

    let (repo, ids) = store
        .write(|c| {
            let repo = c.create_repo("r")?;
            let ids = seed::seed_file_less_ladder(c, repo, &css)?;
            Ok((repo, ids))
        })
        .unwrap();
    assert_eq!(ids.len(), css.len());

    let merge_gen = store.read(|c| c.generation(repo, merge_id)).unwrap();
    let got = store
        .read(|c| c.parents_many(&[(merge_id, merge_gen), (merge_id, merge_gen)]))
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0], got[1], "a merge asked twice gets the same parents");
    // The merge joins the root and the side, first parent first.
    assert_eq!(got[0].len(), 2, "{got:?}");
    assert_eq!(got[0][0].0, root_id_of(&css[0]));
    assert_eq!(got[0][1].0, side_id);
}

/// The id of a changeset built in memory, before the store assigns
/// anything: ids are content addresses.
fn root_id_of(cs: &Changeset) -> scion_core::ChangesetId {
    cs.encode_with_id().1
}

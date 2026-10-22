//! The statement budget of the graph walks on Postgres. A walk read one
//! node per statement; on Postgres each statement is a round trip, so a
//! deep history paid minutes for one query. The walk now reads its nodes
//! in batches and keeps them across steps; this guard pins the count so
//! it can only fall. The formulas:
//!
//! - the bulk seed: a handful of 1,000-row statements, not one per row;
//! - one ancestor walk (`Search`): the merge parents ride one
//!   `parents_many` statement per 1,000 deferred merges. What is left is
//!   the chain's node reads: a merge chain is a linked list, so the next
//!   hop's id is only known after the read, and the walk pays one
//!   statement per merge for them. That is 1 statement per merge plus
//!   the parents batches, the side-parent node batches, the descend
//!   hops, and 4 statements of transaction overhead per 1,000 reads
//!   (about 30 reads per merge here), so about 1.2 statements per
//!   merge. A fallback to one parents statement per merge would cost
//!   about 20,000 on this ladder and fail.
//! - one merge-base walk (`Paint`): unchanged by the parents batch. It
//!   still asks for each merge's parents in its own statement, and each
//!   pop reads its node alone: about 3 statements per merge.
//!
//! The test runs alone in its file: the statement counter is global to
//! the process, and the other test files are separate processes, so
//! nothing else writes between the reset and the read. On SQLite the
//! store's statements are in-process calls, which is why the test skips
//! there.
#![cfg(unix)]

use std::collections::BTreeMap;

use scion_core::Changeset;
use scion_core::ChangesetId;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::read;
use scion_server::test_support::seed;
use scion_server::{Backend, test_support::deadline::Deadline, test_support::meta::MetaTx as _};
use std::time::{Duration, Instant};

const REPO: &str = "ladder";

fn author() -> scion_core::IdentityTime {
    scion_core::IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

/// A deadline that a test does not reach.
fn later() -> Deadline {
    Deadline::new(Instant::now() + Duration::from_secs(600))
}

/// The ladder's changesets and the ids the budget test walks between:
/// the root, `merges` side branches joined by merges, and one late and
/// one early changeset off the ladder.
fn ladder_changesets(
    merges: usize,
) -> (
    Vec<Changeset>,
    Vec<ChangesetId>,
    Vec<ChangesetId>,
    ChangesetId,
    ChangesetId,
) {
    let mut css = vec![
        Changeset::new(vec![], author(), None, b"root".to_vec(), BTreeMap::new())
            .expect("changeset fits the format"),
    ];
    let mut main = vec![css[0].encode_with_id().1];
    let mut sides = Vec::new();
    for i in 0..merges {
        let last = *main.last().unwrap();
        let side = Changeset::new(
            vec![last],
            author(),
            None,
            format!("side {i}").into_bytes(),
            BTreeMap::new(),
        )
        .expect("changeset fits the format");
        let side_id = side.encode_with_id().1;
        let merge = Changeset::new(
            vec![last, side_id],
            author(),
            None,
            format!("merge {i}").into_bytes(),
            BTreeMap::new(),
        )
        .expect("changeset fits the format");
        let merge_id = merge.encode_with_id().1;
        css.push(side);
        css.push(merge);
        main.push(merge_id);
        sides.push(side_id);
    }
    let late = Changeset::new(
        vec![sides[0]],
        author(),
        None,
        b"late".to_vec(),
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    let early = Changeset::new(
        vec![main[0]],
        author(),
        None,
        b"early".to_vec(),
        BTreeMap::new(),
    )
    .expect("changeset fits the format");
    let late_id = late.encode_with_id().1;
    let early_id = early.encode_with_id().1;
    css.push(late);
    css.push(early);
    (css, main, sides, late_id, early_id)
}

#[test]
fn the_ladder_walks_stay_under_the_statement_budget() {
    if !Backend::test_is_postgres() {
        println!("skipping: Postgres only");
        return;
    }
    let merges = 10_000;
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let (css, main, sides, late, early) = ladder_changesets(merges);
    let (main, sides, late, early) = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let ids = seed::seed_file_less_ladder(c, repo, &css)?;
            assert_eq!(ids.len(), css.len());
            Ok((main, sides, late, early))
        })
        .unwrap();
    let tip = *main.last().unwrap();

    // The seed's budget: the 20,001 changesets, their parent rows, their
    // repo membership, and 3 derived values each, in 1,000-row statements,
    // plus the first changeset's ordinary write. A fallback to one
    // statement per row would cost about 360,000.
    let seed_statements = Backend::test_statement_count();
    println!("PROBE seed: {seed_statements}");
    assert!(
        seed_statements <= 500,
        "the bulk seed wrote {seed_statements} statements"
    );

    let statements = |run: &mut dyn FnMut()| {
        Backend::test_reset_statement_count();
        run();
        Backend::test_statement_count()
    };
    // One ancestor walk's budget: 1 statement per merge for the chain's
    // node reads (the chain is a linked list) plus 1/5th per merge for
    // the parents batches, the side-parent node batches, the descend
    // hops, and the steps' transaction overhead, and k = 1,000 for the
    // single-node loads off the chain. Measured 11,155 and 10,124 here.
    let search_cap = u64::try_from(merges).unwrap() + u64::try_from(merges).unwrap() / 5 + 1_000;
    // One merge-base walk's budget: 2 node reads per merge (the merge
    // and its side parent, one statement each) and 1 parents statement
    // per merge, plus the steps' overhead. The parents batch does not
    // touch the Paint walk; measured 30,141 here.
    let paint_cap = 3 * u64::try_from(merges).unwrap() + 2_000;
    let not_ancestor = statements(&mut || {
        assert!(!read::is_ancestor(&store, REPO, late, tip, &later()).unwrap());
    });
    println!("PROBE not-ancestor: {not_ancestor} (cap {search_cap})");
    assert!(
        not_ancestor <= search_cap,
        "the not-ancestor walk took {not_ancestor} statements"
    );
    let ancestor = statements(&mut || {
        assert!(read::is_ancestor(&store, REPO, sides[0], tip, &later()).unwrap());
    });
    println!("PROBE ancestor: {ancestor}");
    assert!(
        ancestor <= search_cap,
        "the ancestor walk took {ancestor} statements"
    );
    for (a, b, base) in [
        (sides[0], tip, sides[0]),
        (late, tip, sides[0]),
        (tip, early, main[0]),
    ] {
        let count = statements(&mut || {
            assert_eq!(
                read::merge_base(&store, REPO, a, b, &later()).unwrap(),
                [base]
            );
        });
        assert!(
            count <= paint_cap,
            "the merge-base walk took {count} statements"
        );
    }
}

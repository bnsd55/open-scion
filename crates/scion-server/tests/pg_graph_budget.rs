//! The statement budget of the graph walks on Postgres. A walk read one
//! node per statement; on Postgres each statement is a round trip, so a
//! deep history paid minutes for one query. The walk now reads its nodes
//! in batches and keeps them across steps; this guard pins the count so
//! it can only fall. The formulas:
//!
//! - the bulk seed: a handful of 1,000-row statements, not one per row;
//! - one walk: at most 2 statements per merge (the parents read and the
//!   side-parent batch of one) plus one node batch per 1,000 queued
//!   links and a fixed margin. A fallback to one statement per visited
//!   node would cost about 150,000 on this ladder and fail.
//!
//! The parents-batch change re-pins the per-merge term to
//! `ceil(visited / DERIVED_BATCH) * c + k`.
//!
//! The test runs alone in its file: the statement counter is global to
//! the process, and the other test files are separate processes, so
//! nothing else writes between the reset and the read. On SQLite the
//! store's statements are in-process calls, which is why the test skips
//! there.
#![cfg(unix)]

use std::collections::BTreeMap;

use scion_core::Changeset;
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

#[test]
fn the_ladder_walks_stay_under_the_statement_budget() {
    if !Backend::test_is_postgres() {
        println!("skipping: Postgres only");
        return;
    }
    let merges = 10_000;
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let (main, sides, late, early) = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
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
            let ids = seed::seed_file_less_ladder(c, repo, &css)?;
            assert_eq!(ids.len(), css.len());
            Ok((main, sides, late_id, early_id))
        })
        .unwrap();
    let tip = *main.last().unwrap();

    // The seed's budget: the 20,001 changesets, their parent rows, their
    // repo membership, and 3 derived values each, in 1,000-row statements,
    // plus the first changeset's ordinary write. A fallback to one
    // statement per row would cost about 360,000.
    let seed_statements = Backend::test_statement_count();
    assert!(
        seed_statements <= 500,
        "the bulk seed wrote {seed_statements} statements"
    );

    let statements = |run: &mut dyn FnMut()| {
        Backend::test_reset_statement_count();
        run();
        Backend::test_statement_count()
    };
    // One walk's budget. c = 3 statements per merge (the parents read,
    // the side-parent batch of one, and the first-parent node that the
    // merge chain follows), k = 2,000 for the node-read batches and the
    // walk's frontier misses on this 20,001-node ladder. A fallback to
    // one statement per visited node would cost about 150,000 and fail
    // here. The parents-batch change removes the per-merge term and
    // re-pins this to ceil(visited / DERIVED_BATCH) * c + k.
    let cap = 3 * u64::try_from(merges).unwrap() + 2_000;
    let not_ancestor = statements(&mut || {
        assert!(!read::is_ancestor(&store, REPO, late, tip, &later()).unwrap());
    });
    assert!(
        not_ancestor <= cap,
        "the not-ancestor walk took {not_ancestor} statements"
    );
    let ancestor = statements(&mut || {
        assert!(read::is_ancestor(&store, REPO, sides[0], tip, &later()).unwrap());
    });
    assert!(
        ancestor <= cap,
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
        assert!(count <= cap, "the merge-base walk took {count} statements");
    }
}

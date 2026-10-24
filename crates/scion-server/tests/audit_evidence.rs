//! The audit's evidence, as tests: a tampered history node reads as
//! corrupt through the server path, and fsck reports it. The production
//! audit's row names these tests (the `NodeId::of(body) == id` check in
//! `get_history_node`, and fsck's history-node pass).

mod common;

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::test_support::deadline::Deadline;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::read;
use scion_server::test_support::tree;
use scion_server::{Backend, Error};
use std::time::{Duration, Instant};

const REPO: &str = "r";

fn regular(content: &[u8]) -> FileState {
    FileState::Regular {
        content: BlobId::of(content),
        executable: false,
    }
}

fn author() -> IdentityTime {
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

fn path(p: &str) -> RepoPath {
    RepoPath::new(p).unwrap()
}

fn later() -> Deadline {
    Deadline::new(Instant::now() + Duration::from_secs(600))
}

/// A store with two changesets, so the path history holds at least two
/// history nodes and a body swap always mismatches.
fn two_changesets(store: &Backend) -> Vec<ChangesetId> {
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let mut ids = Vec::new();
            for files in [
                vec![("a", Some(regular(b"one\n")))],
                vec![("a", Some(regular(b"two\n")))],
            ] {
                let cs = Changeset::new(
                    ids.last().copied().into_iter().collect(),
                    author(),
                    None,
                    format!("change {}", ids.len()).into_bytes(),
                    files.into_iter().map(|(p, s)| (path(p), s)).collect(),
                )
                .expect("changeset fits the format");
                ids.push(tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0);
            }
            Ok(ids)
        })
        .unwrap()
}

/// The `(id, body)` pairs of every history node, in ID order.
fn history_rows(store: &Backend) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .read(|c| c.history_node_bodies_after(&[], usize::MAX))
        .unwrap()
}

/// Overwrites every history node's body with another node's body, so
/// every row's stored bytes no longer hash to its ID. Distinct IDs always
/// mean distinct bodies (the ID is the hash of the body), so a swap with
/// a neighbour always mismatches.
fn tamper_history(store: &Backend, data: &std::path::Path, rows: &[(Vec<u8>, Vec<u8>)]) {
    assert!(rows.len() >= 2, "the fixture needs two history nodes");
    let mut swapped: Vec<(Vec<u8>, Vec<u8>)> = rows
        .iter()
        .enumerate()
        .map(|(i, (id, _))| {
            let (_, other_body) = &rows[(i + 1) % rows.len()];
            (id.clone(), other_body.clone())
        })
        .collect();
    // The last row copies the first's body, so at least one row (the
    // first) provably holds another node's bytes even in odd rotations.
    swapped.last_mut().unwrap().1.clone_from(&rows[0].1);
    if Backend::test_is_postgres() {
        let schema = Backend::test_schema(data).expect("postgres tests name a schema");
        let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("the test database's URL");
        let mut client =
            postgres::Client::connect(&url, postgres::NoTls).expect("the test database connects");
        for (id, body) in &swapped {
            client
                .execute(
                    &format!("UPDATE {schema}.history_nodes SET body = $1 WHERE id = $2"),
                    &[&body, &id],
                )
                .expect("the tamper updates the row");
        }
    } else {
        store
            .write(|c| {
                let conn = c.sqlite().expect("the sqlite seam gives the connection");
                for (id, body) in &swapped {
                    conn.execute(
                        "UPDATE history_nodes SET body = ?1 WHERE id = ?2",
                        rusqlite::params![body, id],
                    )
                    .expect("the tamper updates the row");
                }
                Ok(())
            })
            .unwrap();
    }
}

/// A path-history read through the server path answers Corrupt when a
/// history node's stored body no longer matches its ID.
#[test]
fn a_tampered_history_node_reads_as_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let ids = two_changesets(&store);
    let tip = *ids.last().unwrap();

    // The read answers whole before the tamper: the path log walks the
    // history nodes this test is about.
    let before = read::log_page(&store, REPO, tip, Some(&path("a")), 10, &later()).unwrap();
    assert_eq!(before.entries.len(), 2, "{before:?}");

    tamper_history(&store, dir.path(), &history_rows(&store));

    match read::log_page(&store, REPO, tip, Some(&path("a")), 10, &later()) {
        Err(Error::Corrupt(why)) => assert!(
            why.contains("does not match its ID"),
            "the refusal names the check: {why}"
        ),
        other => panic!("the tampered history read as {other:?}"),
    }
}

/// fsck reports a history node whose body does not match its ID.
#[test]
fn fsck_reports_a_tampered_history_node() {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let _ids = two_changesets(&store);

    tamper_history(&store, dir.path(), &history_rows(&store));

    let problems = scion_server::fsck::run(&store, scion_server::fsck::BATCH, &mut |_| {})
        .unwrap()
        .problems;
    assert!(
        problems
            .iter()
            .any(|p| p.to_string().contains("history-node-bad")
                && p.to_string().contains("body does not match its ID")),
        "fsck must report the tampered node: {problems:?}"
    );
}

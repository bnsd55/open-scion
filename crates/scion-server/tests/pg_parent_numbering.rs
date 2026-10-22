//! The numbering of a changeset's parents: from zero, as on SQLite. The
//! ordinary insert on Postgres numbered the rows from one (WITH
//! ORDINALITY starts at one), so every first-parent read (`n = 0`)
//! matched nothing on real Postgres data, and a chain fetch walked one
//! changeset and stopped. The tests pin the zero-based numbering on both
//! backends, chain a ladder through the ordinary path, migrate a store
//! that holds one-based rows, and guard the bulk seed against writing
//! rows the ordinary path never writes.

use std::collections::BTreeMap;

use scion_core::{Changeset, ChangesetId};
use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _, RepoId};
use scion_server::test_support::tree;

const REPO: &str = "r";

fn author() -> scion_core::IdentityTime {
    scion_core::IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        1_700_000_000,
        0,
    )
    .expect("identity fits the format")
}

fn is_pg() -> bool {
    Backend::test_is_postgres()
}

/// A store with one repo and its root, stored through the ordinary path,
/// so the root carries its tree and its graph node like any real first
/// commit.
fn setup(dir: &std::path::Path) -> (Backend, RepoId, ChangesetId) {
    let store = Backend::open_test(dir).unwrap();
    let (repo, root) = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let cs = Changeset::new(vec![], author(), None, b"root\n".to_vec(), BTreeMap::new())
                .expect("changeset fits the format");
            let id = tree::store_changeset(c, repo, &cs, &mut |_| Ok(0))?.0;
            assert!(c.cas_ref(repo, "main", None, id)?);
            Ok((repo, id))
        })
        .unwrap();
    (store, repo, root)
}

/// One file-less child of `base`, stored through the ordinary path.
fn child(store: &Backend, repo: RepoId, base: ChangesetId, tag: &str) -> ChangesetId {
    store
        .write(|c| {
            let cs = Changeset::new(
                vec![base],
                author(),
                None,
                format!("{tag}\n").into_bytes(),
                BTreeMap::new(),
            )
            .expect("changeset fits the format");
            tree::store_changeset(c, repo, &cs, &mut |_| Ok(0)).map(|(id, _)| id)
        })
        .unwrap()
}

/// A merge of `first` and `second`, stored through the ordinary path.
fn merge_cs(store: &Backend, repo: RepoId, first: ChangesetId, second: ChangesetId) -> ChangesetId {
    store
        .write(|c| {
            let cs = Changeset::new(
                vec![first, second],
                author(),
                None,
                b"merge\n".to_vec(),
                BTreeMap::new(),
            )
            .expect("changeset fits the format");
            tree::store_changeset(c, repo, &cs, &mut |_| Ok(0)).map(|(id, _)| id)
        })
        .unwrap()
}

/// The database, reached directly, with the store's schema selected.
fn raw_with_schema(schema: &str) -> postgres::Client {
    let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL");
    let mut db = postgres::Client::connect(url.trim_end_matches('/'), postgres::NoTls)
        .expect("the test database is up");
    db.batch_execute(&format!("SET search_path TO {schema}"))
        .expect("the schema selects");
    db
}

/// The (n, parent) pairs of one changeset, straight from Postgres.
fn pg_parent_rows(db: &mut postgres::Client, cs: ChangesetId) -> Vec<(i64, Vec<u8>)> {
    db.query(
        "SELECT n, parent FROM changeset_parents WHERE cs = $1 ORDER BY n",
        &[&cs.as_bytes().to_vec()],
    )
    .expect("the parent rows read")
    .iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

/// The (n, parent) pairs of one changeset, straight from SQLite.
fn sqlite_parent_rows(store: &Backend, cs: ChangesetId) -> Vec<(i64, Vec<u8>)> {
    store
        .read(|c| {
            let db = c.sqlite().expect("the data directory's connection");
            let mut stmt = db
                .prepare("SELECT n, parent FROM changeset_parents WHERE cs = ?1 ORDER BY n")
                .map_err(scion_server::test_support::error::from_sqlite)?;
            stmt.query_map([cs.as_bytes()], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(scion_server::test_support::error::from_sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(scion_server::test_support::error::from_sqlite)
        })
        .unwrap()
}

/// Every parent row of the store, as (n, parent) pairs, for a
/// whole-store comparison.
fn all_parent_rows(store: &Backend, dir: &std::path::Path) -> Vec<(i64, Vec<u8>)> {
    if is_pg() {
        let schema = Backend::test_schema(dir).unwrap();
        let mut db = raw_with_schema(&schema);
        db.query(
            "SELECT n, parent FROM changeset_parents ORDER BY cs, n",
            &[],
        )
        .expect("the parent rows read")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
    } else {
        store
            .read(|c| {
                let db = c.sqlite().expect("the data directory's connection");
                let mut stmt = db
                    .prepare("SELECT n, parent FROM changeset_parents ORDER BY cs, n")
                    .map_err(scion_server::test_support::error::from_sqlite)?;
                stmt.query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(scion_server::test_support::error::from_sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(scion_server::test_support::error::from_sqlite)
            })
            .unwrap()
    }
}

/// A ladder of 300 changesets goes in through the ordinary path; the
/// chain fetch from the tip returns the whole first-parent chain, not
/// just the start node.
#[test]
fn a_ladder_through_the_ordinary_path_chains() {
    if !is_pg() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, mut tip) = setup(dir.path());
    for i in 0..300 {
        tip = child(&store, repo, tip, &format!("c{i}"));
    }
    let chain = store
        .read(|c| c.get_derived_chain("commit_graph", 1, tip, 1_000))
        .unwrap();
    assert_eq!(
        chain.len(),
        301,
        "the tip and all 300 ancestors carry graph rows"
    );
    assert_eq!(chain[0].0, tip, "the chain starts at the asked node");
}

/// Both backends store a merge's first parent at n = 0 and its second at
/// n = 1: the rows are the parents' places in the array.
#[test]
fn a_merges_parents_keep_their_places_from_zero() {
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, root) = setup(dir.path());
    let first = child(&store, repo, root, "a");
    let second = child(&store, repo, root, "b");
    let m = merge_cs(&store, repo, first, second);
    let rows = if is_pg() {
        let schema = Backend::test_schema(dir.path()).unwrap();
        let mut db = raw_with_schema(&schema);
        pg_parent_rows(&mut db, m)
    } else {
        sqlite_parent_rows(&store, m)
    };
    assert_eq!(rows.len(), 2, "both parents are rows");
    assert_eq!(rows[0].0, 0, "the first parent is row zero");
    assert_eq!(rows[0].1, first.as_bytes().to_vec());
    assert_eq!(rows[1].0, 1, "the second parent is row one");
    assert_eq!(rows[1].1, second.as_bytes().to_vec());
}

/// A store whose rows the old insert wrote one-based migrates to
/// zero-based on open, and opening it again changes nothing.
#[test]
fn a_one_based_store_migrates_and_stays_migrated() {
    if !is_pg() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let schema = Backend::test_schema(dir.path()).unwrap();
    let mut db = raw_with_schema(&schema);
    db.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .unwrap();
    let old = [
        include_str!("../migrations/postgres/0001_init.sql"),
        include_str!("../migrations/postgres/0002_view_roots.sql"),
        include_str!("../migrations/postgres/0003_view_trees.sql"),
        include_str!("../migrations/postgres/0004_store_uuid.sql"),
    ];
    // The ledger first, as the runner makes it before it migrates.
    db.batch_execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
             version BIGINT PRIMARY KEY,
             name    TEXT NOT NULL
         )",
    )
    .expect("the ledger builds");
    for (i, sql) in old.iter().enumerate() {
        db.batch_execute(&format!(
            "{sql}\nINSERT INTO schema_migrations (version, name) \
             VALUES ({}, 'postgres schema')",
            i + 1
        ))
        .expect("the old schema builds");
    }
    // A root and a child, the child's parent row written one-based, the
    // way the pre-fix insert wrote every ordinary changeset.
    db.batch_execute(
        "INSERT INTO changesets (id, gen, body)
         VALUES (decode(repeat('01', 32), 'hex'), 1, 'root'),
                (decode(repeat('02', 32), 'hex'), 2, 'child');
         INSERT INTO changeset_parents (cs, n, parent)
         VALUES (decode(repeat('02', 32), 'hex'), 1, decode(repeat('01', 32), 'hex'));",
    )
    .expect("the one-based rows insert");
    drop(db);

    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let mut db = raw_with_schema(&schema);
    let version: i64 = db
        .query_one("SELECT max(version) FROM schema_migrations", &[])
        .unwrap()
        .get(0);
    assert_eq!(version, 5, "the open migrated to the current schema");
    let rows = pg_parent_rows(&mut db, ChangesetId::from_bytes([2; 32]));
    assert_eq!(
        rows,
        vec![(0, vec![1_u8; 32])],
        "the one-based row moved to zero"
    );
    drop(db);

    // Opening again runs no migration and leaves the rows alone.
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let mut db = raw_with_schema(&schema);
    let version: i64 = db
        .query_one("SELECT max(version) FROM schema_migrations", &[])
        .unwrap()
        .get(0);
    assert_eq!(version, 5, "the second open adds no version");
    let rows = pg_parent_rows(&mut db, ChangesetId::from_bytes([2; 32]));
    assert_eq!(
        rows,
        vec![(0, vec![1_u8; 32])],
        "the second open moves nothing"
    );
}

/// The bulk seed writes the same parent rows the ordinary path writes
/// for the same graph, so a statement budget that measures seeded data
/// measures data production writes.
#[test]
fn the_bulk_seed_writes_what_the_ordinary_path_writes() {
    let ladder = || {
        let root = Changeset::new(vec![], author(), None, b"root\n".to_vec(), BTreeMap::new())
            .expect("changeset fits the format");
        let a = Changeset::new(
            vec![root.encode_with_id().1],
            author(),
            None,
            b"a\n".to_vec(),
            BTreeMap::new(),
        )
        .expect("changeset fits the format");
        let b = Changeset::new(
            vec![root.encode_with_id().1],
            author(),
            None,
            b"b\n".to_vec(),
            BTreeMap::new(),
        )
        .expect("changeset fits the format");
        let m = Changeset::new(
            vec![a.encode_with_id().1, b.encode_with_id().1],
            author(),
            None,
            b"merge\n".to_vec(),
            BTreeMap::new(),
        )
        .expect("changeset fits the format");
        vec![root, a, b, m]
    };
    // The ordinary store: every changeset through the ordinary path.
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, _) = setup(dir.path());
    for cs in ladder() {
        store
            .write(|c| tree::store_changeset(c, repo, &cs, &mut |_| Ok(0)).map(|_| ()))
            .unwrap();
    }
    // The seeded store: the same changesets through the bulk seed, in a
    // fresh store, the way the budget tests seed.
    let seed_dir = tempfile::tempdir().unwrap();
    let seed_store = Backend::open_test(seed_dir.path()).unwrap();
    seed_store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.bulk_seed(repo, &ladder(), &[])?;
            Ok(())
        })
        .unwrap();
    let ordinary = all_parent_rows(&store, dir.path());
    let seeded = all_parent_rows(&seed_store, seed_dir.path());
    assert_eq!(
        ordinary.len(),
        4,
        "the graph holds four parent rows: two children and the merge's two"
    );
    assert_eq!(ordinary, seeded, "the seed writes the ordinary rows");
}

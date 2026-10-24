//! The riskiest uncovered Postgres paths (C46, from #597's coverage
//! run): the store-identity refusals at the open, the off-host
//! plaintext refusal, and the newer-schema refusal. Each fails if the
//! path breaks: a refusal that no longer fires is the bug the test
//! catches. Postgres-only: they skip on the SQLite backend, whose
//! equivalents the SQLite suites cover.

use std::path::Path;

use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};

/// The Postgres URL the tests share, from the job's environment.
fn url() -> String {
    std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the job's Postgres")
}

/// The tests get their own databases: the job's database names a store
/// of the suite's other tests, and these opens are about exactly that
/// identity. The name is this test process's own, so parallel tests
/// never share one.
fn scratch_db(tag: &str) -> (postgres::Client, String) {
    let base = url();
    let name = format!("scion_c46a_{tag}_{}", std::process::id());
    let mut maker = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    maker
        .batch_execute(&format!("DROP DATABASE IF EXISTS {name}"))
        .unwrap();
    maker
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .unwrap();
    let db_url = base
        .rsplit_once('/')
        .map_or(base.clone(), |(rest, _)| format!("{rest}/{name}"));
    (maker, db_url)
}

fn skip_on_sqlite() -> bool {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the refusal is a Postgres-open path");
        return true;
    }
    false
}

/// A marker naming Postgres and the store `uuid`: the file an open
/// writes into a served data directory.
fn write_marker(data: &Path, uuid: [u8; 16]) {
    let hex: String = uuid
        .iter()
        .fold(String::new(), |a, b| a + &format!("{b:02x}"));
    std::fs::write(data.join("backend"), format!("postgres\n{hex}\n")).unwrap();
}

#[test]
fn a_served_database_refuses_a_markerless_directory_and_a_mismatched_marker() {
    if skip_on_sqlite() {
        return;
    }
    let (_maker, url) = scratch_db("identity");

    // A served store: an open migrates, then a repo row makes the
    // schema one a server has served.
    let dir1 = tempfile::tempdir().unwrap();
    let store = match Backend::open(dir1.path(), Some(&url), None) {
        Ok(store) => store,
        Err(e) => panic!("the first open serves: {e:#}"),
    };
    store.write(|c| c.create_repo("r")).unwrap();
    drop(store);

    // A fresh directory pointed at the served database: the database
    // names a store the directory does not know, and the schema has
    // served repos, so the open refuses instead of adopting it.
    let dir2 = tempfile::tempdir().unwrap();
    let Err(refused) = Backend::open(dir2.path(), Some(&url), None) else {
        panic!("a served database refuses a marker-less directory")
    };
    assert!(
        matches!(refused, scion_server::Error::FailedPrecondition(_)),
        "{refused}"
    );
    assert!(
        format!("{refused:#}").contains("carries no marker of it"),
        "{refused:#}"
    );

    // A directory whose marker names ANOTHER store: the mismatch
    // refusal, before any row of this directory is named.
    let dir3 = tempfile::tempdir().unwrap();
    write_marker(dir3.path(), [0xaa; 16]);
    let Err(refused) = Backend::open(dir3.path(), Some(&url), None) else {
        panic!("a mismatched marker refuses the open")
    };
    let text = format!("{refused:#}");
    assert!(
        text.contains("belongs to the store") || text.contains("one was restored over the other"),
        "{text}"
    );
}

#[test]
fn an_offhost_plain_url_is_refused_before_it_connects() {
    if skip_on_sqlite() {
        return;
    }
    // A host that is not on this machine, with no sslmode: the traffic
    // would be plaintext, so the open refuses before it dials. The IP
    // is TEST-NET-3: nothing answers there, so a connect would hang -
    // the test would time out if the check did not fire first.
    let dir = tempfile::tempdir().unwrap();
    let Err(refused) = Backend::open(dir.path(), Some("postgres://203.0.113.7/scion"), None) else {
        panic!("an off-host plain URL refuses")
    };
    let text = format!("{refused:#}");
    assert!(
        text.contains("must be encrypted"),
        "the refusal names the fix: {text}"
    );

    // Loopback is exempt: the same URL shape to 127.0.0.1 gets past the
    // check and fails on the connect instead (nothing listens there),
    // which must NOT be the encryption refusal.
    let dir = tempfile::tempdir().unwrap();
    let Err(refused) = Backend::open(dir.path(), Some("postgres://127.0.0.1:1/scion"), None) else {
        panic!("a closed loopback port still refuses, on the connect")
    };
    assert!(
        !format!("{refused:#}").contains("must be encrypted"),
        "loopback must not be held to the TLS rule: {refused:#}"
    );
}

#[test]
fn a_database_of_a_newer_schema_is_refused() {
    if skip_on_sqlite() {
        return;
    }
    // The attack gets its own database: created and dropped here, so a
    // bumped schema version never meets another test.
    let (maker, db_url) = scratch_db("newer");

    // A store of this build migrates the fresh database to version 5.
    let dir = tempfile::tempdir().unwrap();
    match Backend::open(dir.path(), Some(&db_url), None) {
        Ok(_) => {}
        Err(e) => panic!("the fresh database opens: {e:#}"),
    }

    // The database is now of a newer build: max(version) above this
    // build's PG_SCHEMA_VERSION. The next open must refuse: an older
    // binary must not run its migrations over a newer store.
    {
        let mut client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();
        client
            .batch_execute("INSERT INTO schema_migrations (version, name) VALUES (99, 'future')")
            .unwrap();
    }
    let Err(refused) = Backend::open(dir.path(), Some(&db_url), None) else {
        panic!("a newer-schema database refuses the older build")
    };
    let text = format!("{refused:#}");
    assert!(
        text.contains("newer than this build"),
        "the refusal names the versions: {text}"
    );

    drop(maker);
}

//! `gc` and `fsck` refuse a Postgres-backed data directory: their queries
//! read the data directory's own database, which the Postgres backend does
//! not keep. A stray `gc` that saw an empty store would sweep every blob
//! file, so the refusal happens at the open, before gc can see anything.

use std::path::Path;
use std::process::Command;

use scion_server::Backend;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};

/// The refusal needs a directory whose marker names Postgres, which only
/// a Postgres-backed open writes.
fn skip_on_sqlite() -> bool {
    if !Backend::test_is_postgres() {
        eprintln!("skip on sqlite: the marker needs a Postgres-backed open");
        return true;
    }
    false
}

/// The blob files under the data directory.
fn blob_files(data: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for dir in std::fs::read_dir(data.join("objects/blob"))
        .into_iter()
        .flatten()
    {
        let dir = dir.unwrap();
        for file in std::fs::read_dir(dir.path()).into_iter().flatten() {
            files.push(file.unwrap().path());
        }
    }
    files
}

#[test]
fn a_gc_of_a_postgres_backed_directory_removes_no_blob_file() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo("kept")).unwrap();
    let blob = store.blobs().put(b"kept").unwrap();
    store.write(|c| c.add_repo_blob(repo, blob)).unwrap();
    let before = blob_files(dir.path());
    assert!(!before.is_empty(), "the blob made one file");

    // The collector serves every backend, in the process of this test:
    // a subprocess would open the whole database, whose one-server lock
    // and store UUID belong to no test schema. A long grace keeps
    // everything: the collector sees the row and the file, and keeps
    // both.
    let report = scion_server::gc::run(
        &store,
        std::time::Duration::from_secs(3600),
        false,
        &mut |_| {},
    )
    .unwrap();
    assert_eq!(
        report.rows_young, 1,
        "the collector saw the blob: {report:?}"
    );
    assert_eq!(report.files_young, 1, "{report:?}");
    assert_eq!(report.rows_pruned, 0, "{report:?}");
    assert_eq!(report.files_removed, 0, "{report:?}");
    assert_eq!(blob_files(dir.path()), before);
    drop(store);

    // Without --postgres-url the open is refused: the directory says its
    // metadata lives in Postgres, and no file is touched.
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(dir.path())
        .arg("gc")
        .env_remove("SCION_TEST_BACKEND")
        .env_remove("SCION_TEST_POSTGRES_URL")
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "gc without --postgres-url must be refused: {out:?}"
    );
    assert_eq!(blob_files(dir.path()), before);
}

#[test]
fn a_sqlite_open_of_a_postgres_backed_directory_is_refused() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    // A data directory whose marker names Postgres refuses a SQLite open,
    // whose empty store would look like a server with no data at all.
    let error = Backend::open_shared(dir.path(), None, None)
        .err()
        .expect("the marker refuses the SQLite backend");
    assert!(error.to_string().contains("--postgres-url"), "{error}");
}

#[test]
fn a_postgres_open_makes_the_data_directory_it_names() {
    if skip_on_sqlite() {
        return;
    }
    // The first serve on a fresh host names the marker before anything
    // else makes the directory: the open creates it, and the marker is in
    // it, so the next SQLite open is refused.
    let dir = tempfile::tempdir().unwrap();
    let fresh = dir.path().join("not-made-yet");
    let store = Backend::open_test(&fresh).unwrap();
    assert!(fresh.join("backend").exists(), "the marker names Postgres");
    drop(store);
    let Err(error) = Backend::open_shared(&fresh, None, None) else {
        panic!("the marker refuses the SQLite backend");
    };
    assert!(error.to_string().contains("--postgres-url"), "{error}");
}

/// A marker whose first line is neither `postgres` nor absent is damage:
/// read as absent, a Postgres directory would open as a fresh, empty
/// SQLite store, which looks like a server with no data at all. Both
/// directions refuse, before anything is read or written; the checks run
/// before the database is contacted, so these tests need no Postgres.
#[test]
fn a_damaged_marker_refuses_both_opens() {
    for (name, text) in [("empty", ""), ("garbage", "an old build left this\n")] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("backend"), text).unwrap();
        let Err(error) = Backend::open(dir.path(), None, None) else {
            panic!("the {name} marker refuses the SQLite open");
        };
        assert!(error.to_string().contains("damaged"), "{error}");
        // A URL is named, but the marker check precedes the database:
        // the open fails before it contacts anything.
        let Err(error) = Backend::open(dir.path(), Some("postgres://127.0.0.1:5/scion"), None)
        else {
            panic!("the {name} marker refuses the Postgres open");
        };
        assert!(error.to_string().contains("damaged"), "{error}");
    }
}

/// A write that another writer blocks answers busy in seconds, not past
/// the minute-long statement clock: the open sets a lock timeout like the
/// SQLite busy timeout, and a lock the database cannot give in it maps to
/// busy, so the client retries. The SQLite side of the row runs here too:
/// another connection of the test holds the write lock past the same
/// clock, through the busy timeout the open sets.
#[test]
fn a_write_blocked_by_another_writer_answers_busy_in_seconds() {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    if Backend::test_is_postgres() {
        // A connection of the test holds the name in an open transaction: it
        // inserted the row and never ended the transaction, so every writer
        // of the same name waits for it.
        let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
        let schema = Backend::test_schema(dir.path()).unwrap();
        let mut held = postgres::Client::connect(&url, postgres::NoTls).unwrap();
        held.batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        held.batch_execute("BEGIN; INSERT INTO repos (name) VALUES ('held');")
            .unwrap();
        let error = run_blocked_write(&store);
        let _ = held.batch_execute("ROLLBACK");
        let _ = error;
    } else {
        // A raw connection of the test holds the write lock: BEGIN
        // IMMEDIATE takes the lock the store's writer wants, and holds
        // it while the store waits out its busy timeout.
        let held = rusqlite::Connection::open(dir.path().join("scion.db")).unwrap();
        held.busy_timeout(std::time::Duration::from_millis(100))
            .unwrap();
        held.execute_batch("BEGIN IMMEDIATE").unwrap();
        let error = run_blocked_write(&store);
        let _ = held.execute_batch("ROLLBACK");
        let _ = error;
    }
}

/// The blocked write of [`a_write_blocked_by_another_writer_answers_busy_in_seconds`],
/// timed and checked for the busy answer. Returns the error so a caller
/// may roll its holder back before the store drops; the checks above
/// have already proven the answer.
fn run_blocked_write(store: &Backend) -> scion_server::Error {
    let started = std::time::Instant::now();
    let error = store
        .write(|c| c.create_repo("held").map(|_| ()))
        .expect_err("the held name refuses the write");
    let waited = started.elapsed();
    assert!(error.is_busy(), "{error:?}");
    assert!(
        waited >= std::time::Duration::from_secs(4),
        "answered after {waited:?}"
    );
    assert!(
        waited < std::time::Duration::from_secs(20),
        "answered after {waited:?}"
    );
    error
}

/// The store UUID: the first Postgres open mints one, the marker file
/// carries it, and an open of the database against the directory of
/// another store is refused instead of serving metadata whose blobs
/// are not on this disk.
#[test]
fn a_database_refuses_the_directory_of_another_store() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    drop(store);
    let marker = dir.path().join("backend");
    let text = std::fs::read_to_string(&marker).unwrap();
    let uuid = text
        .lines()
        .nth(1)
        .expect("the marker names the store UUID");
    assert_eq!(uuid.len(), 32, "{text:?}");
    // The marker of another store: database A restored with directory B.
    std::fs::write(&marker, format!("postgres\n{}\n", "0".repeat(32))).unwrap();
    let Err(error) = Backend::open_test(dir.path()) else {
        panic!("the other store's marker refuses the open");
    };
    let message = error.to_string();
    assert!(message.contains("UUID"), "{message}");
    assert!(message.contains(&"0".repeat(32)), "{message}");
    // The real marker still opens: nothing was written by the refusal.
    std::fs::write(&marker, text).expect("the marker restores");
    assert!(Backend::open_test(dir.path()).is_ok());
}

/// A process that loses the one-server lock is fenced: another process
/// holds the database now, so every read and write fails, and the server
/// exits. The takeover is real: the test kills the store's session and
/// takes the lock from its own connection, as a second server would.
#[test]
fn a_lost_server_lock_fences_the_backend() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    store
        .write(|c| c.create_repo("before").map(|_| ()))
        .unwrap();
    assert!(!Backend::fenced(&store));
    // The takeover: kill the store's session, then take the lock before
    // the store's next statement can.
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(dir.path()).unwrap();
    let key = Backend::test_server_lock(dir.path()).unwrap();
    let mut taker = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    taker
        .batch_execute(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = '{schema}' AND pid <> pg_backend_pid()"
        ))
        .unwrap();
    taker.query("SELECT pg_advisory_lock($1)", &[&key]).unwrap();
    // The first write goes through the reconnect and the lock check: it
    // answers that the lock is gone, not the connection error of the old
    // session, and the fence trips.
    let error = store
        .write(|c| c.create_repo("after").map(|_| ()))
        .expect_err("a backend that lost its lock fails writes");
    assert!(
        matches!(error, scion_server::Error::FailedPrecondition(_)),
        "{error:?}"
    );
    assert!(Backend::fenced(&store), "the fence tripped");
    // Reads fail too, though their own connections never lost anything:
    // the reader pool would otherwise serve rows of another process.
    let error = store
        .read(|c| c.repo_id("before").map(|_| ()))
        .expect_err("a fenced backend fails reads");
    assert!(
        matches!(error, scion_server::Error::FailedPrecondition(_)),
        "{error:?}"
    );
    drop(taker);
}

/// A reconnect ends the transaction of the dead session, and nothing of
/// the old transaction runs on the new session: every statement of the
/// dead transaction fails until its caller ends it, a new BEGIN opens a
/// fresh one, and the store recovers without a restart.
#[test]
fn a_reconnect_ends_the_dead_transaction_and_a_new_begin_passes() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    store
        .write(|c| c.create_repo("before").map(|_| ()))
        .unwrap();
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(dir.path()).unwrap();
    let mut killer = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    // The transaction dies mid-flight: the kill lands between its BEGIN
    // and its next statement. The lock is left alone, so the reconnect
    // recovers the store instead of fencing it.
    let error = store
        .commit(|c| -> Result<(), scion_server::Error> {
            killer
                .batch_execute(&format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE application_name = '{schema}' AND pid <> pg_backend_pid()"
                ))
                .unwrap();
            // The first statement of the dead transaction: the reconnect
            // dropped the transaction, so the statement is refused.
            let first = c.repo_id("before");
            assert!(
                matches!(&first, Err(scion_server::Error::Corrupt(message))
                    if message.contains("lost with the connection")),
                "the first statement answers that the transaction was lost: {first:?}"
            );
            // Every later statement of it too, until the caller ends it.
            let second = c.repo_id("before");
            assert!(
                matches!(&second, Err(scion_server::Error::Corrupt(message))
                    if message.contains("lost with the connection")),
                "the next statement of the dead transaction fails too: {second:?}"
            );
            Err(scion_server::Error::NotFound("ends the transaction".into()))
        })
        .expect_err("the transaction was lost");
    assert!(
        matches!(error, scion_server::Error::NotFound(_)),
        "{error:?}"
    );
    // A new transaction passes: the BEGIN starts a fresh one on the
    // fresh session, and the store recovers without a restart.
    store
        .commit(|c| c.create_repo("after").map(|_| ()))
        .expect("a new transaction runs after the dead one was ended");
    store
        .read(|c| c.repo_id("after").map(|_| ()))
        .expect("the repo the new transaction made is there");
}

/// An unreachable database is not a takeover: writes answer what a
/// client retries against, the fence stays down, and the store recovers
/// when the database comes back. Needs a database this test may stop and
/// start again: name its Docker container in `SCION_TEST_PG_CONTAINER`.
// It stops the database every test of this binary shares, so it runs
// alone: `cargo test --test backend_refusals -- --ignored`.
#[test]
#[ignore = "stops the test database every test of this binary shares"]
fn an_unreachable_database_is_not_a_takeover() {
    if skip_on_sqlite() {
        return;
    }
    let Ok(container) = std::env::var("SCION_TEST_PG_CONTAINER") else {
        eprintln!(
            "skip: stopping the database needs SCION_TEST_PG_CONTAINER, \
             the name of a Docker container this test may stop"
        );
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    store
        .write(|c| c.create_repo("before").map(|_| ()))
        .unwrap();
    let stop = Command::new("docker")
        .args(["stop", "-t", "1", &container])
        .status()
        .expect("docker runs");
    assert!(stop.success(), "docker stop failed");
    // Two writes in a row fail, and neither fences: the database is
    // unreachable, which is no proof that another process holds it.
    for _ in 0..2 {
        let error = store
            .write(|c| c.create_repo("down").map(|_| ()))
            .expect_err("a write against a database that is down fails");
        assert!(error.is_busy(), "{error:?}");
        assert!(
            !Backend::fenced(&store),
            "an unreachable database must not fence the backend"
        );
    }
    // The database comes back: the next write reconnects, takes the lock
    // again, and runs. No process restart was needed.
    let start = Command::new("docker")
        .args(["start", &container])
        .status()
        .expect("docker runs");
    assert!(start.success(), "docker start failed");
    for _ in 0..60 {
        if Command::new("docker")
            .args(["exec", &container, "pg_isready", "-U", "postgres"])
            .status()
            .is_ok_and(|out| out.success())
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    store
        .write(|c| c.create_repo("after").map(|_| ()))
        .expect("the store recovers with the database");
    assert!(!Backend::fenced(&store));
}

/// `fsck` runs on a Postgres-backed store: a clean store reports nothing,
/// and a missing blob file is found, through the same metadata interface.
#[test]
fn a_fsck_of_a_postgres_backed_store_finds_missing_blob_files() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let repo = store.write(|c| c.create_repo("fsck-pg")).unwrap();
    let blob = store.blobs().put(b"fsck me\n").unwrap();
    store.write(|c| c.add_repo_blob(repo, blob)).unwrap();
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(
        report.problems.is_empty(),
        "a clean store is clean: {:?}",
        report.problems
    );

    // The damage of the guide's example: a file that a row names is gone.
    std::fs::remove_file(store.blobs().location(blob).1).unwrap();
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(
        report
            .problems
            .iter()
            .any(|p| p.to_string().starts_with(&format!("blob-missing {blob}"))),
        "{:?}",
        report.problems
    );
}

/// One throwaway database per test, so the lock scope of a production
/// open is checked on a real open without touching the shared test
/// database's default schema.
struct Throwaway {
    url: String,
    name: String,
}

impl Throwaway {
    /// One throwaway database per test, named after the test, so
    /// parallel tests of this file do not drop each other's database.
    fn new(who: &str) -> Self {
        let base = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
        // The name carries the test, so parallel tests of this file do
        // not drop each other's database.
        let name = format!("scion_test_lockcheck_{}_{}", who, std::process::id());
        let mut admin = postgres::Client::connect(&base, postgres::NoTls).unwrap();
        admin
            .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .unwrap();
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .unwrap();
        let cut = base.rfind('/').expect("the test URL names a database");
        Self {
            url: format!("{}/{}", &base[..cut], name),
            name,
        }
    }

    /// The URL of this database, riding `schema` when one is given.
    fn url_riding(&self, schema: Option<&str>) -> String {
        match schema {
            Some(schema) => format!("{}?options=--search_path%3D{schema}", self.url),
            None => self.url.clone(),
        }
    }
}

impl Drop for Throwaway {
    fn drop(&mut self) {
        let base = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
        let Ok(mut admin) = postgres::Client::connect(&base, postgres::NoTls) else {
            return;
        };
        let _ = admin.batch_execute(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        ));
    }
}

/// The one-server lock keys on the schema the connection lands in, not
/// on the words of the URL: a plain URL and one that rides
/// `--search_path=public` land in the same schema of one database, so
/// the second open is refused, however differently the two URLs spell
/// where they go. A production behavior change: a URL that names its own
/// schema fences one schema, not the whole database.
#[test]
fn a_plain_url_and_one_that_rides_public_fence_each_other() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("fence");
    let dir_a = tempfile::tempdir().unwrap();
    let _a = Backend::open(dir_a.path(), Some(&db.url), None).expect("the plain URL opens");
    let dir_b = tempfile::tempdir().unwrap();
    let Err(error) = Backend::open(dir_b.path(), Some(&db.url_riding(Some("public"))), None) else {
        panic!("the second open of one schema is refused");
    };
    assert!(
        error.to_string().contains("another server holds"),
        "{error}"
    );
}

/// Two URLs that ride different schemas of one database open side by
/// side: each fences its own schema, which is what lets the parallel
/// tests of one database each hold one server.
#[test]
fn two_urls_that_ride_different_schemas_open_side_by_side() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("side_by_side");
    let mut admin = postgres::Client::connect(&db.url, postgres::NoTls).unwrap();
    admin
        .batch_execute("CREATE SCHEMA scion_test_lock_one; CREATE SCHEMA scion_test_lock_two;")
        .unwrap();
    let dir_a = tempfile::tempdir().unwrap();
    let a = Backend::open(
        dir_a.path(),
        Some(&db.url_riding(Some("scion_test_lock_one"))),
        None,
    )
    .expect("the first schema opens");
    let dir_b = tempfile::tempdir().unwrap();
    let b = Backend::open(
        dir_b.path(),
        Some(&db.url_riding(Some("scion_test_lock_two"))),
        None,
    )
    .expect("the second schema opens beside it");
    // Each store keeps its rows in its own schema: one repo apiece, and
    // the other schema's table stays empty.
    a.write(|c| c.create_repo("one").map(|_| ())).unwrap();
    b.write(|c| c.create_repo("two").map(|_| ())).unwrap();
    for (schema, name) in [
        ("scion_test_lock_one", "one"),
        ("scion_test_lock_two", "two"),
    ] {
        let rows = admin
            .query(
                &format!("SELECT name FROM {schema}.repos WHERE name = '{name}'"),
                &[],
            )
            .unwrap();
        assert_eq!(rows.len(), 1, "{schema} holds {name}");
    }
}

/// 16 bytes as 32 lowercase hex digits, as the marker spells them.
fn hex16_of(bytes: &[u8; 16]) -> String {
    let mut hex = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// A data directory that names a store refuses a database schema that
/// holds no store: the open would adopt the marker's UUID into an empty
/// schema and serve nothing, and every caller would read "repo not
/// found" with no error at the start. A `--postgres-url` that names the
/// wrong schema (a `search_path` typo, or a schema never served) must
/// refuse at the open.
#[test]
fn a_directory_that_names_a_store_refuses_an_empty_schema() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("empty_schema");
    let mut admin = postgres::Client::connect(&db.url, postgres::NoTls).unwrap();
    admin
        .batch_execute("CREATE SCHEMA a; CREATE SCHEMA b;")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    // Schema `a` serves once: it mints the store, the directory's marker
    // takes its UUID, and the schema holds the store's data.
    let served = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the first schema opens");
    served.write(|c| c.create_repo("r").map(|_| ())).unwrap();
    drop(served);
    let marker = std::fs::read_to_string(dir.path().join("backend")).unwrap();
    let uuid = marker.lines().nth(1).unwrap().trim().to_owned();
    assert_eq!(uuid.len(), 32, "{marker}");
    // The same directory against schema `b`, which no store ever served:
    // refuse, and name the store the directory belongs to.
    let Err(error) = Backend::open(dir.path(), Some(&db.url_riding(Some("b"))), None) else {
        panic!("the empty schema adopts the marker's store");
    };
    assert!(
        matches!(error, scion_server::Error::FailedPrecondition(_)),
        "{error:?}"
    );
    let text = error.to_string();
    assert!(text.contains("holds no store"), "{error}");
    assert!(
        text.contains(&uuid),
        "the refusal names the marker's store: {text}"
    );
    assert!(
        text.contains("search_path"),
        "the refusal says what to check: {text}"
    );
}

/// The adopt keeps its legit case: a schema that holds a store's rows
/// but no `store_uuid` row - a dump taken before the UUID existed - is
/// adopted by the marker's directory, and the reopened store serves its
/// rows and writes the UUID back.
#[test]
fn a_schema_that_holds_a_store_but_no_uuid_row_still_adopts() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("adopt");
    let mut admin = postgres::Client::connect(&db.url, postgres::NoTls).unwrap();
    admin.batch_execute("CREATE SCHEMA a;").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let served = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the first open mints");
    served.write(|c| c.create_repo("r").map(|_| ())).unwrap();
    drop(served);
    let marker = std::fs::read_to_string(dir.path().join("backend")).unwrap();
    let uuid = marker.lines().nth(1).unwrap().trim().to_owned();
    // The dump's shape: the rows stay, the `store_uuid` row goes.
    admin.batch_execute("DELETE FROM a.store_uuid;").unwrap();
    let reopened = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the dump's schema is adopted");
    assert!(reopened.read(|c| c.repo_id("r")).is_ok(), "the rows serve");
    drop(reopened);
    let row: Vec<u8> = postgres::Client::connect(&db.url_riding(Some("a")), postgres::NoTls)
        .unwrap()
        .query("SELECT uuid FROM store_uuid WHERE one = 1", &[])
        .unwrap()[0]
        .get(0);
    let mut back = [0u8; 16];
    back.copy_from_slice(&row);
    assert_eq!(
        hex16_of(&back),
        uuid,
        "the adopted identity is the marker's"
    );
}
/// A first open cut between its store's UUID row and its marker opens
/// again: the row commits early in the open, the marker is written at
/// its end, and a kill in between left a store that could never open
/// without hand repair. The store was empty - no repos row, so no blob
/// of another disk can be named - so the directory that returns takes
/// the database's store as its own, and this open writes the marker the
/// cut fell before.
#[test]
fn a_first_open_cut_before_its_marker_opens_again() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("marker_window");
    let mut admin = postgres::Client::connect(&db.url, postgres::NoTls).unwrap();
    admin.batch_execute("CREATE SCHEMA a;").unwrap();
    let dir = tempfile::tempdir().unwrap();
    // The first open mints the store's row; the cut takes the marker the
    // open writes at its end.
    let first = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the first open mints");
    drop(first);
    std::fs::remove_file(dir.path().join("backend")).unwrap();
    // The kill's shape: the row stands, the marker is gone, the schema
    // holds no repo. The next open serves, and writes the marker back.
    let again = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the first open cut before its marker opens again");
    again
        .write(|c| c.create_repo("r").map(|_| ()))
        .expect("the reopened store serves");
    drop(again);
    let row: Vec<u8> = postgres::Client::connect(&db.url_riding(Some("a")), postgres::NoTls)
        .unwrap()
        .query("SELECT uuid FROM store_uuid WHERE one = 1", &[])
        .unwrap()[0]
        .get(0);
    let mut minted = [0u8; 16];
    minted.copy_from_slice(&row);
    let marker = std::fs::read_to_string(dir.path().join("backend")).unwrap();
    let written = marker.lines().nth(1).unwrap().trim();
    assert_eq!(
        written,
        hex16_of(&minted),
        "the marker names the store the row minted"
    );
}

/// A served store whose marker went missing still refuses a directory
/// that names no store: the schema holds repos, so a fresh directory
/// here would serve metadata whose blobs are not on this disk.
#[test]
fn a_served_store_without_its_marker_refuses_a_fresh_directory() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("marker_served");
    let mut admin = postgres::Client::connect(&db.url, postgres::NoTls).unwrap();
    admin.batch_execute("CREATE SCHEMA a;").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let served = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the first open mints");
    served.write(|c| c.create_repo("r").map(|_| ())).unwrap();
    drop(served);
    std::fs::remove_file(dir.path().join("backend")).unwrap();
    let Err(error) = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None) else {
        panic!("a fresh directory serves a served store");
    };
    assert!(error.to_string().contains("carries no marker"), "{error}");
}

/// The runbook's re-pairing: a served store whose marker was lost
/// accepts a marker written back exactly as operations.md says (the
/// `printf` command shape, the 32 hex digits the refusal text names),
/// and serves again. A marker written in the wrong format is read as
/// absent, so the refusal stands and the store stays closed.
#[test]
fn a_lost_marker_written_back_as_the_runbook_says_reopens_the_store() {
    if skip_on_sqlite() {
        return;
    }
    let db = Throwaway::new("marker_repair");
    let mut admin = postgres::Client::connect(&db.url, postgres::NoTls).unwrap();
    admin.batch_execute("CREATE SCHEMA a;").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let served = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the first open mints");
    served.write(|c| c.create_repo("r").map(|_| ())).unwrap();
    drop(served);
    std::fs::remove_file(dir.path().join("backend")).unwrap();
    let Err(error) = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None) else {
        panic!("a served store refuses without its marker");
    };
    let message = error.to_string();
    assert!(message.contains("carries no marker"), "{message}");
    // The runbook: the UUID comes from the error text - the 32 hex
    // digits it names - and the marker is the printf of two lines.
    let start = message
        .find("names the store ")
        .expect("the refusal names the store")
        + "names the store ".len();
    let uuid = &message[start..start + 32];
    assert!(uuid.bytes().all(|b| b.is_ascii_hexdigit()), "{message}");
    // A marker whose UUID line is not 32 hex digits counts as no UUID
    // at all: the open refuses again with the same message, exactly as
    // it did without the file. The runbook's write is the only one that
    // reopens the store.
    let dashed = format!(
        "postgres\n{}-{}-{}-{}-{}\n",
        &uuid[..8],
        &uuid[8..12],
        &uuid[12..16],
        &uuid[16..20],
        &uuid[20..]
    );
    std::fs::write(dir.path().join("backend"), &dashed).unwrap();
    let Err(still) = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None) else {
        panic!("a marker whose UUID line is not 32 hex digits opens a store");
    };
    assert!(still.to_string().contains("carries no marker"), "{still}");
    let marker = dir.path().join("backend");
    let out = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "printf 'postgres\\n{}\\n' > {}",
            uuid,
            marker.display()
        ))
        .status()
        .expect("the runbook's printf runs");
    assert!(out.success(), "the runbook's printf wrote the marker");
    let bytes = std::fs::read(&marker).unwrap();
    assert_eq!(
        bytes,
        format!("postgres\n{uuid}\n").into_bytes(),
        "the marker's bytes are the two lines the runbook says"
    );
    let again = Backend::open(dir.path(), Some(&db.url_riding(Some("a"))), None)
        .expect("the marker the runbook wrote reopens the store");
    assert!(
        again.read(|c| c.repo_id("r")).is_ok(),
        "the repo the store served is still there"
    );
}

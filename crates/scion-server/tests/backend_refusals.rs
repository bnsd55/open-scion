//! `gc` and `fsck` refuse a Postgres-backed data directory: their queries
//! read the data directory's own database, which the Postgres backend does
//! not keep. A stray `gc` that saw an empty store would sweep every blob
//! file, so the refusal happens at the open, before gc can see anything.

use std::path::Path;
use std::process::Command;

use scion_server::Backend;
use scion_server::meta::{Meta as _, MetaTx as _};

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
    store.blobs().put(b"kept").unwrap();
    let before = blob_files(dir.path());
    assert!(!before.is_empty(), "the blob made one file");
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

    // With --postgres-url the gc runs: the collector serves every
    // backend. The blob is still in use, so no file is removed.
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_scion-server"))
        .arg("--data")
        .arg(dir.path())
        .arg("--postgres-url")
        .arg(&url)
        .arg("gc")
        .env_remove("SCION_TEST_BACKEND")
        .env_remove("SCION_TEST_POSTGRES_URL")
        .output()
        .unwrap();
    assert!(out.status.success(), "gc on Postgres must run: {out:?}");
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

/// A write that another writer blocks answers busy in seconds, not past
/// the minute-long statement clock: the open sets a lock timeout like the
/// SQLite busy timeout, and a lock the database cannot give in it maps to
/// busy, so the client retries.
#[test]
fn a_write_blocked_by_another_writer_answers_busy_in_seconds() {
    if skip_on_sqlite() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
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
    let started = std::time::Instant::now();
    let error = store
        .write(|c| c.create_repo("held").map(|_| ()))
        .expect_err("the held name refuses the write");
    let waited = started.elapsed();
    let _ = held.batch_execute("ROLLBACK");
    assert!(matches!(error, scion_server::Error::Busy), "{error:?}");
    assert!(
        waited >= std::time::Duration::from_secs(4),
        "answered after {waited:?}"
    );
    assert!(
        waited < std::time::Duration::from_secs(20),
        "answered after {waited:?}"
    );
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
        assert!(matches!(error, scion_server::Error::Busy), "{error:?}");
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

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

    // With --postgres-url the gc runs, and refuses on Postgres with one
    // line saying why. Still no file touched.
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
    assert!(
        !out.status.success(),
        "gc on Postgres must be refused: {out:?}"
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
    drop(std::fs::write(&marker, text));
    assert!(Backend::open_test(dir.path()).is_ok());
}

/// A process that loses the one-server lock is fenced: another process
/// may hold the database now, so every read and write fails, and the
/// server exits (`health_fenced`). The hook makes the writer's next
/// setups lose the lock the way a takeover does.
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
    store.arm_lock_loss();
    // The server's own sessions die: the next statement reconnects, and
    // the hook drops the lock on it, as a takeover would.
    let url = std::env::var("SCION_TEST_POSTGRES_URL").unwrap();
    let schema = Backend::test_schema(dir.path()).unwrap();
    let mut killer = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    killer
        .batch_execute(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = '{schema}' AND pid <> pg_backend_pid()"
        ))
        .unwrap();
    // A write reconnects, loses the lock, and the fence trips.
    let error = store
        .write(|c| c.create_repo("after").map(|_| ()))
        .expect_err("a fenced backend fails writes");
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
}

//! `gc` and `fsck` refuse a Postgres-backed data directory: their queries
//! read the data directory's own database, which the Postgres backend does
//! not keep. A stray `gc` that saw an empty store would sweep every blob
//! file, so the refusal happens at the open, before gc can see anything.

use std::path::Path;
use std::process::Command;

use scion_server::Backend;

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
    let error = Backend::open_shared(&fresh, None, None)
        .err()
        .expect("the marker refuses the SQLite backend");
    assert!(error.to_string().contains("--postgres-url"), "{error}");
}

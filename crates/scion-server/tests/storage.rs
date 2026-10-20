//! What a caller sees when the server's disk runs out: the blob file
//! writes answer the disk's own error, mapped in one place to a fixed
//! out-of-space reply that names no server path; nothing acknowledged is
//! lost, no file is left half-written, and the next write works once the
//! space is back.
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::store::Store;
use scion_server::{Backend, Error, StorageClass};

/// A store on the backend the tests run on.
fn store() -> (tempfile::TempDir, Backend) {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    (dir, store)
}

/// Whether the error is the disk's answer, mapped to out-of-space.
fn is_out_of_space(e: &Error) -> bool {
    matches!(e, Error::Io(io) if io.kind() == std::io::ErrorKind::StorageFull)
}

/// The file path of a blob under the data directory's `objects/blob`.
fn blob_path(data: &std::path::Path, id: scion_core::BlobId) -> std::path::PathBuf {
    let hex = id.to_string();
    data.join("objects")
        .join("blob")
        .join(&hex[..2])
        .join(&hex[2..])
}

/// Whether the blob root holds nothing but fan-out folders: no temp file
/// of a failed write was left under them.
fn blob_dirs_are_clean(data: &std::path::Path) -> bool {
    let Ok(dirs) = std::fs::read_dir(data.join("objects/blob")) else {
        return false;
    };
    let mut dirs = dirs;
    dirs.all(|dir| {
        let Ok(dir) = dir else {
            return false;
        };
        let name = dir.file_name().to_string_lossy().to_string();
        // Two hex characters, or the tmp folder of the process.
        name.len() == 2 || name == "tmp"
    })
}

#[test]
fn a_put_the_disk_refuses_answers_out_of_space_and_leaves_no_torn_file() {
    let (dir, store) = store();
    // A read works before anything is written, and a first put lands.
    let kept = store.blobs().put(b"kept\n").unwrap();
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    // The next file write is the one the disk refuses.
    store.blobs().fail_writes_after(0);
    let error = store.blobs().put(b"refused\n").unwrap_err();
    // The kind first, then the reply: the reply assert consumes the error.
    assert!(is_out_of_space(&error), "{error:?}");
    let reply = tonic::Status::from(match &error {
        Error::Io(io) => Error::Io(std::io::Error::from_raw_os_error(
            io.raw_os_error().unwrap(),
        )),
        _ => unreachable!(),
    });
    assert_eq!(reply.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        reply.message(),
        "the server is out of disk space; retry later"
    );
    drop(error);
    // No torn file: the refused blob's name never appeared, and no temp
    // file of the failed write is left behind.
    let id = scion_core::BlobId::of(b"refused\n");
    assert!(!blob_path(dir.path(), id).exists());
    assert!(blob_dirs_are_clean(dir.path()));
    // A read works right after the failure, and the next write works
    // without anything being cleared: the space came back.
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    let after = store.blobs().put(b"after\n").unwrap();
    assert_eq!(store.blobs().get(after).unwrap(), b"after\n");
}

#[test]
fn a_batch_that_loses_a_file_keeps_its_earlier_files() {
    let (dir, store) = store();
    let mut batch = store.blobs().batch();
    let one = batch.put(b"one\n").unwrap();
    // The second file write of the batch is the one the disk refuses.
    store.blobs().fail_writes_after(0);
    let error = batch.put(b"two\n").unwrap_err();
    assert!(is_out_of_space(&error), "{error:?}");
    drop(batch);
    // The first file of the batch is intact: it was fully written before
    // the failure, and no row names it yet, so a later gc sweep may take
    // it when its grace passes. The refused blob left no file.
    assert!(blob_path(dir.path(), one).exists());
    assert_eq!(store.blobs().get(one).unwrap(), b"one\n");
    let two = scion_core::BlobId::of(b"two\n");
    assert!(!blob_path(dir.path(), two).exists());
    assert!(blob_dirs_are_clean(dir.path()));
    // A read works after the failure, and the next write works.
    assert_eq!(store.blobs().get(one).unwrap(), b"one\n");
    store.blobs().put(b"after\n").unwrap();
}

/// Fills the metadata database to a file size of its current pages plus
/// `extra`, so the next write that must grow the file hits the real
/// disk-full of the database, not a hook.
fn cap_metadata(store: &Backend, extra: i64) {
    let Backend::Sqlite(store) = store else {
        panic!("the page cap is a data-directory test");
    };
    store.cap_database_pages(extra);
}

fn release_metadata(store: &Backend) {
    let Backend::Sqlite(store) = store else {
        return;
    };
    store.release_database_pages();
}

#[test]
fn a_metadata_write_that_must_grow_the_file_answers_out_of_space() {
    let (_dir, store) = store();
    if Backend::test_is_postgres() {
        // On Postgres the metadata lives in the database; its disk full is
        // the SQLSTATE 53100 arm, covered in error.rs beside the busy
        // test. The blob files of this test still run on Postgres, above.
        return;
    }
    let _repo = store.write(|c| c.create_repo("r")).unwrap();
    // One transaction that must grow the file: a thousand repos, past a
    // cap of eight pages over the current size.
    cap_metadata(&store, 8);
    let error = store
        .write(|c| {
            for i in 0..2000 {
                c.create_repo(&format!("repo-{i}"))?;
            }
            Ok(())
        })
        .unwrap_err();
    let out_of_space = matches!(
        &error,
        Error::Storage(storage) if storage.class() == StorageClass::DiskFull
    );
    assert!(out_of_space, "{error:?}");
    // The same raw error, mapped: the fixed reply, no path in it.
    let status = tonic::Status::from(Error::Io(std::io::Error::from_raw_os_error(28)));
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        status.message(),
        "the server is out of disk space; retry later"
    );
    drop(error);
    // The transaction rolled back: no repo landed, and a read works.
    assert!(store.read(|c| c.repo_id("r")).is_ok());
    assert!(
        store
            .read(|c| c.repo_id("repo-1999"))
            .unwrap_err()
            .to_string()
            .contains("not found")
    );
    // The space is back: the same write works, and the next one too.
    release_metadata(&store);
    let _repo2 = store.write(|c| c.create_repo("r2")).unwrap();
    assert!(store.read(|c| c.repo_id("repo-2000")).is_err());
}

#[test]
fn the_blob_hook_refuses_exactly_one_write_per_count() {
    let (dir, store) = store();
    // Two writes go through, the third fails, the fourth goes: a test can
    // fail a write in the middle of a sequence, not only the first.
    store.blobs().fail_writes_after(2);
    store.blobs().put(b"a\n").unwrap();
    store.blobs().put(b"b\n").unwrap();
    let error = store.blobs().put(b"c\n").unwrap_err();
    assert!(is_out_of_space(&error), "{error:?}");
    store.blobs().put(b"d\n").unwrap();
    assert_eq!(
        store.blobs().get(scion_core::BlobId::of(b"d\n")).unwrap(),
        b"d\n"
    );
    // Another open of the same directory still reads the stored files.
    drop(store);
    let again = Store::open(dir.path()).unwrap();
    assert_eq!(
        again.blobs().get(scion_core::BlobId::of(b"a\n")).unwrap(),
        b"a\n"
    );
}

#[test]
fn a_write_on_a_read_only_disk_answers_unavailable_and_leaves_no_torn_file() {
    let (dir, store) = store();
    let kept = store.blobs().put(b"kept\n").unwrap();
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    // The next file write is the one the read-only disk refuses: 30 is
    // EROFS, the same on Linux and macOS.
    store.blobs().fail_writes_after_errno(0, 30);
    let error = store.blobs().put(b"refused\n").unwrap_err();
    let read_only = matches!(
        &error,
        Error::Io(io) if io.kind() == std::io::ErrorKind::ReadOnlyFilesystem
    );
    assert!(read_only, "{error:?}");
    let reply = tonic::Status::from(match &error {
        Error::Io(io) => Error::Io(std::io::Error::from_raw_os_error(
            io.raw_os_error().unwrap(),
        )),
        _ => unreachable!(),
    });
    assert_eq!(reply.code(), tonic::Code::Unavailable);
    assert_eq!(reply.message(), "the disk is read-only; retry later");
    drop(error);
    // No torn file: the refused blob's name never appeared, and no temp
    // file of the failed write is left behind.
    let id = scion_core::BlobId::of(b"refused\n");
    assert!(!blob_path(dir.path(), id).exists());
    assert!(blob_dirs_are_clean(dir.path()));
    // The rest of the store keeps serving, and once the disk is
    // writable again the same write lands.
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    let after = store.blobs().put(b"after\n").unwrap();
    assert_eq!(store.blobs().get(after).unwrap(), b"after\n");
}

#[test]
fn a_read_on_a_failing_disk_answers_internal_and_keeps_serving_the_rest() {
    let (_dir, store) = store();
    let one = store.blobs().put(b"one\n").unwrap();
    let two = store.blobs().put(b"two\n").unwrap();
    assert_eq!(store.blobs().get(one).unwrap(), b"one\n");
    // The next file read is the one the failing disk refuses: 5 is EIO.
    store.blobs().fail_reads_after(0);
    let error = store.blobs().get(one).unwrap_err();
    let io_failure = matches!(&error, Error::Io(io) if io.raw_os_error() == Some(5));
    assert!(io_failure, "{error:?}");
    // EIO is a failing device, not a busy one: the reply is an internal
    // error, not an invitation to retry.
    let reply = tonic::Status::from(match &error {
        Error::Io(_io) => Error::Io(std::io::Error::from_raw_os_error(5)),
        _ => unreachable!(),
    });
    assert_eq!(reply.code(), tonic::Code::Internal);
    assert_eq!(reply.message(), "internal error");
    drop(error);
    // The hook is spent: the rest of the store keeps serving.
    assert_eq!(store.blobs().get(two).unwrap(), b"two\n");
    // And the failed read works once the disk recovers.
    assert_eq!(store.blobs().get(one).unwrap(), b"one\n");
}

#[test]
fn a_read_hook_refuses_exactly_one_read_per_count() {
    let (_dir, store) = store();
    let one = store.blobs().put(b"a\n").unwrap();
    // Two reads go through, the third fails, the fourth goes.
    store.blobs().fail_reads_after(2);
    assert_eq!(store.blobs().get(one).unwrap(), b"a\n");
    assert_eq!(store.blobs().get(one).unwrap(), b"a\n");
    let error = store.blobs().get(one).unwrap_err();
    assert!(
        matches!(&error, Error::Io(io) if io.raw_os_error() == Some(5)),
        "{error:?}"
    );
    assert_eq!(store.blobs().get(one).unwrap(), b"a\n");
}

#[test]
fn a_file_fsync_the_disk_refuses_answers_internal_and_leaves_no_torn_file() {
    let (dir, store) = store();
    let kept = store.blobs().put(b"kept\n").unwrap();
    // The next fsync is the temp file's: the write reached the page
    // cache, but the drive did not take it.
    store.blobs().fail_fsyncs_after(0);
    let error = store.blobs().put(b"refused\n").unwrap_err();
    assert!(
        matches!(&error, Error::Io(io) if io.raw_os_error() == Some(5)),
        "{error:?}"
    );
    drop(error);
    // No ack, and no torn file: the refused blob's name never appeared,
    // and the temp file of the failed write is gone with it.
    let id = scion_core::BlobId::of(b"refused\n");
    assert!(!blob_path(dir.path(), id).exists());
    assert!(blob_dirs_are_clean(dir.path()));
    // The rest of the store keeps serving.
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    // The drive took the data after all: the retry rewrites the blob.
    let id = store.blobs().put(b"refused\n").unwrap();
    assert_eq!(store.blobs().get(id).unwrap(), b"refused\n");
}

#[test]
fn a_retry_after_a_failed_dir_fsync_makes_the_kept_file_durable() {
    let (dir, store) = store();
    let kept = store.blobs().put(b"kept\n").unwrap();
    // The next fsync is the temp file's; the one after is the blob
    // directory's, after the rename. The drive takes the file's data but
    // refuses the directory entry's.
    store.blobs().fail_fsyncs_after(1);
    let error = store.blobs().put(b"later\n").unwrap_err();
    assert!(
        matches!(&error, Error::Io(io) if io.raw_os_error() == Some(5)),
        "{error:?}"
    );
    drop(error);
    // No ack. The file stays where the rename put it: a writer never
    // removes a name, because the same name may already hold a blob that
    // another writer acknowledged.
    let id = scion_core::BlobId::of(b"later\n");
    assert!(blob_path(dir.path(), id).exists());
    // The store keeps serving.
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    // The retry finds the file intact and makes it durable before it
    // answers: the counter shows the file fsync and the directory fsync
    // of the keep path, and the directory fsync of the batch's sync.
    let before = store.blobs().fsync_count();
    let id = store.blobs().put(b"later\n").unwrap();
    assert_eq!(
        store.blobs().fsync_count() - before,
        3,
        "the kept file was fsynced, file and directory both"
    );
    assert_eq!(store.blobs().get(id).unwrap(), b"later\n");
}

#[test]
fn a_fsync_hook_refuses_exactly_one_fsync_per_count() {
    let (_dir, store) = store();
    let one = store.blobs().put(b"a\n").unwrap();
    // One fsync goes through (the temp file's), the next fails (the
    // directory's, after the rename), and the one after goes: the count
    // runs over the file fsyncs and the directory fsyncs in order.
    store.blobs().fail_fsyncs_after(1);
    let error = store.blobs().put(b"b\n").unwrap_err();
    assert!(
        matches!(&error, Error::Io(io) if io.raw_os_error() == Some(5)),
        "{error:?}"
    );
    store.blobs().put(b"c\n").unwrap();
    assert_eq!(store.blobs().get(one).unwrap(), b"a\n");
}

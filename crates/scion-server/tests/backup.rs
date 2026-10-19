//! A backup taken while the store serves restores whole: the metadata
//! snapshot first, the blob files second, the marker with the metadata
//! (`docs/operations.md`, section 3). The test is a data-directory test:
//! the snapshot is SQLite's `VACUUM INTO`, and the Postgres backup is
//! `pg_dump`, which the runbook documents.

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_server::meta::{Meta as _, MetaTx as _, RepoId};
use scion_server::{Backend, Store, fsck};

/// A backup taken while the store serves: the metadata snapshot names
/// every blob a later blob copy brings, the restored store opens, `fsck`
/// is clean, and every write acknowledged before the snapshot reads back.
#[test]
fn a_backup_taken_while_serving_restores_whole() {
    if Backend::test_is_postgres() {
        // The snapshot of a Postgres-backed store is a pg_dump of the
        // metadata database, which the runbook documents; the test's
        // VACUUM INTO is the SQLite procedure.
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let (repo, blob_ids, ids) = write_chain(&store, "backup", 20);

    // The metadata snapshot: the runbook's `VACUUM INTO`, on its own
    // connection to the live database, while the store keeps serving.
    // The snapshot is a consistent committed state, whatever a write
    // that runs beside it does.
    let backup_db = tempfile::tempdir().unwrap();
    let snapshot = backup_db.path().join("scion.db");
    let live = rusqlite::Connection::open(dir.path().join("scion.db")).unwrap();
    live.execute(&format!("VACUUM INTO '{}'", snapshot.display()), [])
        .expect("the metadata snapshot is taken");

    // The copy of the blob files runs while more writes land: a blob the
    // snapshot names stays reachable, and the copy may or may not hold
    // the newer ones.
    let tip = *ids.last().unwrap();
    let writer = {
        let store = store.clone();
        std::thread::spawn(move || write_raced(&store, repo, "raced", 10, tip))
    };
    let backup = tempfile::tempdir().unwrap();
    copy_tree(&dir.path().join("objects"), &backup.path().join("objects"));
    writer.join().unwrap();

    // The restore: the metadata snapshot and the blob files of the
    // backup, in a fresh directory. A SQLite directory keeps its store
    // UUID in the database itself, so there is no marker file to copy
    // (a Postgres directory has one, and the runbook copies it with the
    // metadata).
    let restored_dir = tempfile::tempdir().unwrap();
    std::fs::copy(&snapshot, restored_dir.path().join("scion.db")).unwrap();
    copy_tree(
        &backup.path().join("objects"),
        &restored_dir.path().join("objects"),
    );

    let restored = Backend::Sqlite(Store::open(restored_dir.path()).unwrap());
    let report = fsck::run(&restored, 1, &mut |_| {}).unwrap();
    assert!(
        report.problems.is_empty(),
        "the restore is whole: {:?}",
        report.problems
    );
    assert_reads_back(&restored, repo, "backup", &ids, &blob_ids);
}

/// Puts the blob named `label i` and returns its ID with its bytes.
fn put_blob(store: &Backend, label: &str, i: u8) -> (BlobId, Vec<u8>) {
    let bytes = format!("{label} {i}\n").into_bytes();
    (store.blobs().put(&bytes).unwrap(), bytes)
}

/// The changeset `i` of a chain: one file backed by `blob`, on `parent`,
/// with a message that names it.
fn chain_changeset(label: &str, i: u8, blob: BlobId, parent: Option<ChangesetId>) -> Changeset {
    Changeset {
        parents: parent.into_iter().collect(),
        author: IdentityTime {
            name: b"b".to_vec(),
            email: b"b@x".to_vec(),
            unix_secs: 0,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: format!("{label} {i}").into_bytes(),
        changes: [(
            RepoPath::new(format!("f{i}").into_bytes()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    }
}

/// The writes the backup must keep: a chain of `n` changesets, each with
/// its own blob, acknowledged before the caller takes its snapshot. The
/// blobs go in first: a blob file is durable before the row that names it.
fn write_chain(
    store: &Backend,
    label: &str,
    n: u8,
) -> (RepoId, Vec<(BlobId, Vec<u8>)>, Vec<ChangesetId>) {
    let blob_ids: Vec<(BlobId, Vec<u8>)> = (0..n).map(|i| put_blob(store, label, i)).collect();
    let (repo, ids) = store
        .write(|c| {
            let repo = c.create_repo("r")?;
            let mut ids = Vec::new();
            let mut parent = None;
            for (i, (blob, _)) in blob_ids.iter().enumerate() {
                c.add_repo_blob(repo, *blob)?;
                parent = Some(
                    c.insert_changeset(repo, &chain_changeset(label, i as u8, *blob, parent))?,
                );
                ids.push(parent.unwrap());
            }
            assert!(c.cas_ref(repo, "main", None, *ids.last().unwrap())?);
            Ok((repo, ids))
        })
        .unwrap();
    (repo, blob_ids, ids)
}

/// The writes that race the blob copy: `n` more changesets on the chain,
/// acknowledged while the caller walks `objects/`.
fn write_raced(store: &Backend, repo: RepoId, label: &str, n: u8, tip: ChangesetId) {
    let mut parent = Some(tip);
    for i in 0..n {
        let (blob, _) = put_blob(store, label, i);
        let id = store
            .write(|c| {
                c.add_repo_blob(repo, blob)?;
                c.insert_changeset(repo, &chain_changeset(label, i, blob, parent))
            })
            .unwrap();
        parent = Some(id);
    }
}

/// Every write acknowledged before the snapshot reads back: the ref at
/// the tip, each changeset of the chain, and the blob bytes.
fn assert_reads_back(
    store: &Backend,
    repo: RepoId,
    label: &str,
    ids: &[ChangesetId],
    blob_ids: &[(BlobId, Vec<u8>)],
) {
    store
        .read(|c| {
            let head = c.get_ref(repo, "main")?.map(|r| r.target);
            assert_eq!(head, Some(*ids.last().unwrap()), "main reads back");
            for (i, id) in ids.iter().enumerate() {
                let cs = c.get_changeset(repo, *id)?;
                assert_eq!(cs.message, format!("{label} {i}").into_bytes());
            }
            Ok(())
        })
        .unwrap();
    for (blob, bytes) in blob_ids {
        assert_eq!(&store.blobs().get(*blob).unwrap(), bytes);
    }
}

/// Copies every file of `from`, in its subdirectories, into `to`.
fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    for entry in walkdir(from) {
        let rel = entry.strip_prefix(from).unwrap();
        let to = to.join(rel);
        if entry.is_dir() {
            std::fs::create_dir_all(&to).unwrap();
        } else {
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::copy(entry, &to).unwrap();
        }
    }
}

/// Every path under `from`, with the parent directories a copy needs
/// made as it goes.
fn walkdir(from: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    let mut stack = vec![from.to_path_buf()];
    while let Some(dir) = stack.pop() {
        paths.push(dir.clone());
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                paths.push(path);
            }
        }
    }
    paths
}

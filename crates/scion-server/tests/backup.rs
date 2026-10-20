//! A backup taken while the store serves restores whole: the metadata
//! snapshot first, the blob files second, the marker with the metadata
//! (`docs/operations.md`, section 3). The test is a server test: writes
//! run against the real server through the whole backup, the operation
//! ledger (`common::ledger`) records every acknowledgment at reply
//! time, and the check runs against the restored store's own server —
//! so what reads back is what a client would read.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::v1::{CreateChangesetRequest, LandRequest, LandState};
use scion_server::Backend;
use scion_server::test_support::backend::from_store;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::store::Store;

use common::ledger::{self, Kind, OpLog};
use common::{
    Ledger, REFS, REPO, Rng, Scion, Server, author, build_files, changeset_id, connect,
    create_repo, resolve_ref,
};

mod common;

/// A backup taken while the store serves restores whole.
///
/// The consistent point is the metadata snapshot itself: `VACUUM INTO`
/// reads one committed state, so a write acknowledged before the call
/// is in it, and one acknowledged while it runs may be in or out. The
/// store's blob-first invariant (a blob file is durable before the row
/// that names it) means every blob the snapshot names is already in
/// `objects/` when the snapshot returns, so the blob copy that follows
/// can only add files, never miss one.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_backup_taken_while_serving_restores_whole() {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let seed = common::env(
        "SCION_BACKUP_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    let mut rng = Rng::new(seed);

    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));

    if Backend::test_is_postgres() {
        // The metadata lives in Postgres: the snapshot is the runbook's
        // `pg_dump`, and the restore loads it into the restored
        // directory's own schema. The writes run in this process against
        // the test backend, the shape the Postgres job runs; the server
        // harness above cannot speak a test schema.
        backup_postgres();
        return;
    }

    create_repo(&data);
    let server = Server::start(&data, &log);
    let signals = Arc::new(common::Signals::default());

    // The writes the backup must keep: a chain of lands, acknowledged
    // before the snapshot, recorded as the ledger sees the replies.
    let mut chain = OpLog::default();
    write_chain(&server.url, &mut chain, 8, &signals).await;

    // The writes that race the backup: a principal writes through the
    // whole backup, until the blob copy is done. Its ledger is
    // separate, so the test can split what was acknowledged before the
    // snapshot from what ran beside it.
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicUsize::new(0));
    let writer = {
        let url = server.url.clone();
        let stop = Arc::clone(&stop);
        let acked = Arc::clone(&acked);
        let signals = Arc::clone(&signals);
        let writer_rng = Rng::new(rng.next());
        tokio::spawn(async move {
            write_raced(&url, "raced", &stop, &acked, writer_rng, &signals).await
        })
    };

    // The writer's first raced acknowledgment: the snapshot starts
    // only once the writer has landed a whole round, so the
    // pre-snapshot window holds raced writes, not just the chain's.
    common::wait_until(|| acked.load(Ordering::SeqCst) > 0).await;

    // The metadata snapshot: the runbook's `VACUUM INTO`, on its own
    // connection to the live database, while the store keeps serving.
    // The snapshot is a consistent committed state, whatever a write
    // that runs beside it does. Everything acknowledged before this
    // call started must read back after the restore; what the writer
    // acknowledges from here on may be in or out. The counter is
    // stored only after a whole round, so the pre-snapshot window
    // never splits a round's records.
    let pre_snapshot = acked.load(Ordering::SeqCst);
    let backup_db = tempfile::tempdir().unwrap();
    let snapshot = backup_db.path().join("scion.db");
    let live = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    live.execute(&format!("VACUUM INTO '{}'", snapshot.display()), [])
        .expect("the metadata snapshot is taken");

    // The copy of the blob files runs while the writer keeps landing: a
    // blob the snapshot names stays reachable, and the copy may or may
    // not hold the newer ones. `gc` must not run during a backup (the
    // runbook's warning); no test runs one here.
    let backup = tempfile::tempdir().unwrap();
    copy_tree(&data.join("objects"), &backup.path().join("objects"));
    stop.store(true, Ordering::SeqCst);
    let raced = writer.await.unwrap();
    let raced_entries = raced.entries().len();

    // The ledger the restore must satisfy: the chain, plus every writer
    // acknowledgment that came before the snapshot call. What came
    // after is checked softer: in or out, never torn.
    let (raced_pre, raced_post) = raced.split_at(pre_snapshot);
    let mut oplog = OpLog::default();
    oplog.merge(chain);
    oplog.merge(raced_pre);

    // The restore: the metadata snapshot and the blob files of the
    // backup, in a fresh directory. A SQLite directory keeps its store
    // UUID in the database itself, so there is no marker file to copy
    // (a Postgres directory has one, and the runbook copies it with the
    // metadata). `fsck` runs before the server serves, the runbook's
    // order.
    let restored_dir = tempfile::tempdir().unwrap();
    std::fs::copy(&snapshot, restored_dir.path().join("scion.db")).unwrap();
    copy_tree(
        &backup.path().join("objects"),
        &restored_dir.path().join("objects"),
    );
    let restored = from_store(Store::open(restored_dir.path()).unwrap());
    ledger::fsck_ok(&restored, "the restored store");
    drop(restored);

    // The restored store serves, and the ledger check runs against it
    // over RPC: every pre-snapshot acknowledgment reads back the way a
    // client would see it.
    let restored_server = Server::start(restored_dir.path(), &log);
    ledger::check(&oplog, &restored_server.url, 0, ledger::SAMPLE, &mut rng).await;

    // What the writer acknowledged beside the backup: in or out, never
    // torn.
    soft_check(&restored_server.url, &raced_post).await;
    println!("the writer acknowledged {raced_entries}, {pre_snapshot} before the snapshot");
}

/// The chain of lands the backup must keep: `n` rounds of put, create
/// and land, acknowledged before the caller takes its snapshot. Every
/// reply is recorded as the ledger sees it.
async fn write_chain(url: &str, oplog: &mut OpLog, n: usize, signals: &Arc<common::Signals>) {
    let (mut scion, mut objects) = connect(url).await.unwrap();
    let mut rng = Rng::new(0xBAC5);
    let mut base = resolve_ref(&mut objects, REFS[0]).await.unwrap();
    for i in 0..n {
        let name = format!("chain{i}");
        let mut files = Ledger::default();
        let changes = build_files(&mut scion, &name, i as u64, &mut rng, &mut files, signals).await;
        let linked = linked_of(&changes);
        oplog.merge(files.oplog);
        let id = create_and_record(&mut scion, &name, base, changes, &linked, oplog).await;
        land_and_record(&mut scion, &name, REFS[0], id, oplog).await;
        // The next round's base: the tip the server rebase-lands onto.
        base = resolve_ref(&mut objects, REFS[0]).await.unwrap();
    }
}

/// The writes that race the backup: rounds of put, create and land,
/// acknowledged while the caller walks `objects/`, until `stop` is set.
/// The count of entries recorded so far is published in `acked` after
/// each whole round, so the caller knows what was acknowledged before
/// it began its snapshot.
async fn write_raced(
    url: &str,
    name: &str,
    stop: &Arc<AtomicBool>,
    acked: &Arc<AtomicUsize>,
    mut rng: Rng,
    signals: &Arc<common::Signals>,
) -> OpLog {
    let (mut scion, mut objects) = connect(url).await.unwrap();
    let mut oplog = OpLog::default();
    let mut bases = std::collections::HashMap::new();
    for op in 0_u64.. {
        if stop.load(Ordering::SeqCst) {
            return oplog;
        }
        let target = REFS[rng.below(REFS.len())];
        let base = match bases.get(target) {
            Some(&base) => base,
            _ => resolve_ref(&mut objects, target).await.unwrap(),
        };
        bases.insert(target, base);
        let round = format!("{name}-{op}");
        let mut files = Ledger::default();
        let changes = build_files(&mut scion, &round, op, &mut rng, &mut files, signals).await;
        let linked = linked_of(&changes);
        let id = create_and_record(&mut scion, &round, base, changes, &linked, &mut oplog).await;
        oplog.merge(files.oplog);
        // The round's file entries join only now, so the acked count
        // never splits a round: a round is visible whole or not at all.
        for (blob, path) in &linked {
            oplog.link_blob(*blob, id, path);
        }
        land_and_record(&mut scion, &round, target, id, &mut oplog).await;
        acked.store(oplog.entries().len(), Ordering::SeqCst);
    }
    unreachable!("the loop returns when stop is set")
}

/// The (blob, path) pairs a changeset takes, for the ledger's links.
fn linked_of(changes: &BTreeMap<RepoPath, Option<FileState>>) -> Vec<(BlobId, Vec<u8>)> {
    changes
        .iter()
        .filter_map(|(path, state)| {
            state
                .as_ref()
                .and_then(FileState::blob)
                .map(|blob| (blob, path.as_bytes().to_vec()))
        })
        .collect()
}

/// Creates the changeset over RPC and records the acknowledgment.
/// Returns the changeset's ID.
async fn create_and_record(
    scion: &mut Scion,
    name: &str,
    base: ChangesetId,
    changes: BTreeMap<RepoPath, Option<FileState>>,
    linked: &[(BlobId, Vec<u8>)],
    oplog: &mut OpLog,
) -> ChangesetId {
    let changeset = Changeset::new(
        vec![base],
        author(),
        None,
        name.as_bytes().to_vec(),
        changes,
    )
    .expect("changeset fits the format");
    let (bytes, id) = changeset.encode_with_id();
    let reply = scion
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: bytes,
        })
        .await
        .unwrap_or_else(|s| panic!("{name}: CreateChangeset failed while serving: {s:?}"));
    assert_eq!(changeset_id(&reply.into_inner().changeset_id), id);
    for (blob, path) in linked {
        oplog.link_blob(*blob, id, path);
    }
    oplog.record(Kind::Changeset { id }, format!("{name}: create"));
    id
}

/// Lands over RPC and records the reply, whatever state it names.
async fn land_and_record(
    scion: &mut Scion,
    key: &str,
    target: &'static str,
    id: ChangesetId,
    oplog: &mut OpLog,
) {
    let request = LandRequest {
        repo: REPO.into(),
        changeset_id: id.as_bytes().to_vec(),
        target_ref: target.into(),
        request_key: key.to_owned(),
    };
    let reply = scion
        .land(request)
        .await
        .unwrap_or_else(|s| panic!("{key}: Land failed while serving: {s:?}"))
        .into_inner();
    let state = reply.state();
    oplog.record(
        Kind::Land {
            key: key.to_owned(),
            land_id: reply.land_id.clone(),
            target,
            state,
            result: (state == LandState::Landed).then(|| changeset_id(&reply.result_changeset_id)),
        },
        format!("{key}: land"),
    );
}

/// What the writer acknowledged beside the backup: in or out, never
/// torn. A blob either reads back whole with its acknowledged digest or
/// is not there; a changeset either decodes to its ID or is not there.
async fn soft_check(url: &str, oplog: &OpLog) {
    let (mut scion, _objects) = connect(url).await.unwrap();
    for entry in oplog.entries() {
        match &entry.kind {
            Kind::Blob {
                id,
                sha256: want,
                len,
                used_in: Some((changeset, path)),
            } => match ledger::try_read_file(&mut scion, *changeset, path).await {
                Ok(Some(served)) => {
                    let got = ledger::sha256(&served);
                    assert_eq!(
                        got, *want,
                        "blob {id} ({}) reads back torn: sha {got:02x?}, expected {want:02x?}",
                        entry.where_
                    );
                    assert_eq!(
                        served.len(),
                        *len,
                        "blob {id} ({}) reads back torn: {} bytes, sent {len}",
                        entry.where_,
                        served.len()
                    );
                }
                Ok(None) => {}
                Err(status) => panic!(
                    "blob {id} ({}) is neither whole nor absent: {status:?}",
                    entry.where_
                ),
            },
            Kind::Changeset { id } => {
                if let Ok(Some((_, got))) = ledger::try_get_changeset(&mut scion, *id).await {
                    assert_eq!(got, *id, "changeset ({}) reads back torn", entry.where_);
                }
            }
            _ => {}
        }
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

/// The backup of a Postgres-backed store: the writes run in this
/// process against the test backend (the shape the Postgres job runs),
/// the snapshot is the runbook's `pg_dump` of the live database, and
/// the restore loads it into the restored directory's own schema. The
/// ledger records every acknowledgment, and the same properties the
/// server test checks over RPC are checked against the restored store.
fn backup_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let store = Backend::open_test(dir.path()).unwrap();
    let (repo, parent) = pg_repo(&store);

    // The writes the backup must keep: a chain of changesets on `main`,
    // acknowledged before the snapshot, recorded as the ledger sees the
    // acks.
    let mut oplog = OpLog::default();
    let parent = pg_chain(&store, &mut oplog, repo, parent);

    // The writes that race the backup: a thread writes more changesets
    // and moves `main` while the caller walks `objects/`, and the
    // ledger counts what it has acknowledged after each round.
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicUsize::new(0));
    let writer = {
        let store = store.clone();
        let stop = Arc::clone(&stop);
        let acked = Arc::clone(&acked);
        std::thread::spawn(move || pg_raced(&store, repo, parent, &stop, &acked))
    };

    // The writer's first raced acknowledgment: the snapshot starts only
    // once the writer has acknowledged a whole round, so the
    // pre-snapshot window holds raced writes, not just the chain's.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while acked.load(Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the raced writer never acknowledged"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // The metadata snapshot: the runbook's `pg_dump --no-owner` of the
    // live database, while the store keeps serving. The snapshot is one
    // consistent committed state; everything acknowledged before the
    // call started must read back after the restore, and what the
    // writer acknowledges from here on may be in or out.
    let pre_snapshot = acked.load(Ordering::SeqCst);
    let dump = tempfile::tempdir().unwrap();
    let dump_file = dump.path().join("metadata.sql");
    let schema = test_schema(dir.path());
    dump_schema(&schema, &dump_file);

    // The copy of the blob files runs while the writer keeps writing: a
    // blob the snapshot names stays reachable, and the copy may or may
    // not hold the newer ones. `gc` must not run during a backup (the
    // runbook's warning); no test runs one here.
    let backup = tempfile::tempdir().unwrap();
    copy_tree(&dir.path().join("objects"), &backup.path().join("objects"));
    stop.store(true, Ordering::SeqCst);
    let raced = writer.join().unwrap();
    let raced_entries = raced.entries().len();

    // The ledger the restore must satisfy: the chain, plus every writer
    // acknowledgment that came before the dump. What came after is
    // checked softer: in or out, never torn.
    let (raced_pre, raced_post) = raced.split_at(pre_snapshot);
    oplog.merge(raced_pre);

    // The restore: the dump loads into the restored directory's own
    // schema, the marker travels with the metadata (the runbook's
    // step: it holds the store UUID the metadata holds too), and the
    // blob files of the backup go beside them. `fsck` runs before the
    // store serves, the runbook's order.
    let restored_dir = tempfile::tempdir().unwrap();
    let restored_schema = test_schema(restored_dir.path());
    rename_in(&dump_file, &schema, &restored_schema);
    load_schema(&dump_file);
    std::fs::copy(
        dir.path().join("backend"),
        restored_dir.path().join("backend"),
    )
    .unwrap();
    copy_tree(
        &backup.path().join("objects"),
        &restored_dir.path().join("objects"),
    );
    let restored = Backend::open_test(restored_dir.path()).unwrap();
    ledger::fsck_ok(&restored, "the restored store");

    // The same properties the server test checks over RPC, read from
    // the restored store: C1, every pre-snapshot blob's bytes; C2, every
    // pre-snapshot changeset decodes; C3, every ref observation sits on
    // the ref's restored history.
    pg_verify(&restored, &oplog, repo);

    // What the writer acknowledged beside the backup: in or out, never
    // torn.
    pg_soft(&restored, &raced_post, repo);
    println!("the writer acknowledged {raced_entries}, {pre_snapshot} before the dump");
}

/// Repo `r` with one root changeset, and refs `main` and `dev` at it:
/// the harness's `create_repo`, against the test backend.
fn pg_repo(store: &Backend) -> (scion_server::test_support::meta::RepoId, ChangesetId) {
    let root_blob = store.blobs().put(b"backup suite\n").unwrap();
    let root = Changeset::new(
        vec![],
        author(),
        None,
        b"root\n".to_vec(),
        [(
            RepoPath::new("README").unwrap(),
            Some(FileState::Regular {
                content: root_blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let id = c.insert_changeset(repo, &root)?;
            for name in REFS {
                assert!(c.cas_ref(repo, name, None, id)?);
            }
            Ok((repo, id))
        })
        .unwrap()
}

/// One changeset of the chain: one file backed by `blob`, on `parent`,
/// with a message that names it.
fn pg_changeset(parent: ChangesetId, name: &str, blob: BlobId, path: &str) -> Changeset {
    Changeset::new(
        vec![parent],
        author(),
        None,
        name.as_bytes().to_vec(),
        [(
            RepoPath::new(path.as_bytes()).unwrap(),
            Some(FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format")
}

/// The chain of changesets the backup must keep: `n` rounds of put and
/// create on `main`, each acknowledged with its ledger entry, then the
/// ref moved to the tip. Returns the tip.
fn pg_chain(
    store: &Backend,
    oplog: &mut OpLog,
    repo: scion_server::test_support::meta::RepoId,
    mut parent: ChangesetId,
) -> ChangesetId {
    for i in 0..8 {
        let bytes = format!("chain {i}\n").into_bytes();
        let blob = store.blobs().put(&bytes).unwrap();
        oplog.record(
            Kind::Blob {
                id: blob,
                sha256: ledger::sha256(&bytes),
                len: bytes.len(),
                used_in: None,
            },
            format!("chain{i}: put"),
        );
        let changeset = pg_changeset(parent, &format!("chain {i}"), blob, &format!("f{i}"));
        let id = changeset.encode_with_id().1;
        store
            .write(|c| {
                c.add_repo_blob(repo, blob)?;
                c.insert_changeset(repo, &changeset)
            })
            .unwrap();
        oplog.link_blob(blob, id, format!("f{i}").as_bytes());
        oplog.record(Kind::Changeset { id }, format!("chain{i}: create"));
        parent = id;
    }
    let version = store
        .write(|c| Ok(c.get_ref(repo, REFS[0])?.expect("main exists").version))
        .unwrap();
    store
        .write(|c| {
            let moved = c.cas_ref(repo, REFS[0], Some(version), parent)?;
            assert!(moved);
            Ok(())
        })
        .unwrap();
    oplog.record(
        Kind::Ref {
            name: REFS[0],
            at: parent,
        },
        "chain: main moved to the chain's tip",
    );
    parent
}

/// The writes that race the backup: rounds of put, create and a ref
/// move, acknowledged while the caller walks `objects/`, until `stop`
/// is set. The count of entries recorded so far is published in
/// `acked` after each whole round.
fn pg_raced(
    store: &Backend,
    repo: scion_server::test_support::meta::RepoId,
    mut parent: ChangesetId,
    stop: &Arc<AtomicBool>,
    acked: &Arc<AtomicUsize>,
) -> OpLog {
    let mut raced = OpLog::default();
    for i in 0_u64.. {
        if stop.load(Ordering::SeqCst) {
            return raced;
        }
        let round = format!("raced-{i}");
        let bytes = format!("{round}\n").into_bytes();
        let blob = store.blobs().put(&bytes).unwrap();
        raced.record(
            Kind::Blob {
                id: blob,
                sha256: ledger::sha256(&bytes),
                len: bytes.len(),
                used_in: None,
            },
            format!("{round}: put"),
        );
        let changeset = pg_changeset(parent, &round, blob, &format!("{round}/f"));
        let id = changeset.encode_with_id().1;
        store
            .write(|c| {
                c.add_repo_blob(repo, blob)?;
                c.insert_changeset(repo, &changeset)
            })
            .unwrap();
        raced.link_blob(blob, id, format!("{round}/f").as_bytes());
        raced.record(Kind::Changeset { id }, format!("{round}: create"));
        // The ref moves too: the observation the ledger records must be
        // one the store made, or the restore check would catch a lie.
        let version = store
            .write(|c| Ok(c.get_ref(repo, REFS[0])?.expect("main exists").version))
            .unwrap();
        store
            .write(|c| {
                let moved = c.cas_ref(repo, REFS[0], Some(version), id)?;
                assert!(moved);
                Ok(())
            })
            .unwrap();
        raced.record(
            Kind::Ref {
                name: REFS[0],
                at: id,
            },
            format!("{round}: main moved"),
        );
        parent = id;
        acked.store(raced.entries().len(), Ordering::SeqCst);
    }
    unreachable!("the loop returns when stop is set")
}

/// The first-parent history of `main`, oldest first, read from
/// `store`: the restored answer to the ledger's RPC walk.
fn pg_history(store: &Backend, repo: scion_server::test_support::meta::RepoId) -> Vec<ChangesetId> {
    let mut head = store
        .read(|c| Ok(c.get_ref(repo, REFS[0])?.expect("main reads back").target))
        .unwrap();
    let mut history = Vec::new();
    loop {
        let (changeset, id) = store
            .read(|c| c.get_changeset(repo, head).map(|cs| (cs, head)))
            .unwrap();
        let parent = changeset.parents().first().copied();
        history.push(id);
        match parent {
            Some(p) => head = p,
            None => break, // the root: a ref may legitimately sit at it
        }
    }
    history
}

/// The restored answers to the ledger check: C1, every pre-snapshot
/// blob's bytes; C2, every pre-snapshot changeset decodes; C3, every
/// ref observation sits on the ref's restored history.
fn pg_verify(restored: &Backend, oplog: &OpLog, repo: scion_server::test_support::meta::RepoId) {
    for entry in oplog.entries() {
        if let Kind::Blob {
            id,
            sha256: want,
            len,
            ..
        } = &entry.kind
        {
            let served = restored.blobs().get(*id).unwrap_or_else(|e| {
                panic!(
                    "acknowledged blob {id} ({}) reads back as: {e}",
                    entry.where_
                )
            });
            let got = ledger::sha256(&served);
            assert_eq!(got, *want, "blob {id} ({}) served torn bytes", entry.where_);
            assert_eq!(
                served.len(),
                *len,
                "blob {id} ({}) served torn bytes",
                entry.where_
            );
        }
        if let Kind::Changeset { id } = &entry.kind {
            restored
                .read(|c| c.get_changeset(repo, *id).map(|_| ()))
                .unwrap_or_else(|e| {
                    panic!(
                        "acknowledged changeset {id} ({}) reads back as: {e}",
                        entry.where_
                    )
                });
        }
        if let Kind::Ref { name: _, at } = &entry.kind {
            let history = pg_history(restored, repo);
            assert!(
                history.contains(at),
                "acknowledged position {at} ({}) is not on main's history after the restore",
                entry.where_
            );
        }
    }
}

/// What the writer acknowledged beside the backup: in or out, never
/// torn.
fn pg_soft(restored: &Backend, raced_post: &OpLog, repo: scion_server::test_support::meta::RepoId) {
    for entry in raced_post.entries() {
        let served = match &entry.kind {
            Kind::Blob { id, .. } => restored.blobs().get(*id),
            Kind::Changeset { id } => {
                // In or out: an absent changeset is fine, a torn one is
                // not. `get_changeset` decodes, so a wrong decode fails
                // here.
                let _ = restored.read(|c| c.get_changeset(repo, *id).map(|_| ()));
                continue;
            }
            _ => continue,
        };
        if let Ok(served) = served {
            let Kind::Blob {
                id,
                sha256: want,
                len,
                ..
            } = &entry.kind
            else {
                unreachable!("served came from the Blob branch");
            };
            assert_eq!(
                ledger::sha256(&served),
                *want,
                "blob {id} ({}) reads back torn",
                entry.where_
            );
            assert_eq!(
                served.len(),
                *len,
                "blob {id} ({}) reads back torn",
                entry.where_
            );
        }
    }
}

/// The schema name the test backend gives `data`: the same hash the
/// server's `test_schema` computes, so the dump loads where the
/// restored store looks.
fn test_schema(data: &std::path::Path) -> String {
    let path = data.canonicalize().unwrap_or_else(|_| data.to_path_buf());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&path, &mut hasher);
    format!("scion_test_{:016x}", std::hash::Hasher::finish(&hasher))
}

/// Dumps `schema` with the runbook's `pg_dump --no-owner`, beside the
/// server that keeps serving. The dump runs in the job's Postgres
/// container, the only one on the runner: its tools match the server's
/// version, where a client from the runner image may not.
fn dump_schema(schema: &str, out: &std::path::Path) {
    let container = std::env::var("SCION_TEST_PG_CONTAINER")
        .expect("SCION_TEST_PG_CONTAINER names the job's Postgres container");
    let db = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database")
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let file = std::fs::File::create(out).unwrap();
    let status = std::process::Command::new("docker")
        .args(["exec", &container, "pg_dump", "-U", "postgres"])
        .args(["--no-owner", &format!("--schema={schema}"), &db])
        .stdout(std::process::Stdio::from(file))
        .status()
        .expect("docker runs");
    assert!(status.success(), "pg_dump of {schema} failed");
    let dumped = std::fs::read_to_string(out).unwrap();
    assert!(
        dumped.contains(schema),
        "the dump does not carry the schema {schema}"
    );
}

/// Renames the schema in the dump, so it loads under the name the
/// restored directory's test backend computes. The names are
/// `scion_test_<16 hex digits>`, which no stored data contains.
fn rename_in(file: &std::path::Path, from: &str, to: &str) {
    let text = std::fs::read_to_string(file).unwrap();
    std::fs::write(file, text.replace(from, to)).unwrap();
}

/// Loads a dump into the database, as the runbook's restore does.
fn load_schema(file: &std::path::Path) {
    let container = std::env::var("SCION_TEST_PG_CONTAINER")
        .expect("SCION_TEST_PG_CONTAINER names the job's Postgres container");
    let db = std::env::var("SCION_TEST_POSTGRES_URL")
        .expect("SCION_TEST_POSTGRES_URL names the test database")
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    let dump = std::fs::File::open(file).unwrap();
    let output = std::process::Command::new("docker")
        .args(["exec", "-i", &container, "psql", "-U", "postgres"])
        .args(["-v", "ON_ERROR_STOP=1", "-d", &db])
        .stdin(std::process::Stdio::from(dump))
        .output()
        .expect("docker runs");
    assert!(
        output.status.success(),
        "the dump failed to load: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

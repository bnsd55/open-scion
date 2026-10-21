//! Crash suite: the real `scion-server` binary, killed with SIGKILL while
//! several clients upload, create changesets and land, then started again on
//! the same data directory. After each restart, [`check`] checks what a crash
//! must never break; `docs/architecture.md` (section 10) lists it. A second
//! test kills the server while it upgrades the schema of a data directory.
//!
//! What this does not prove: SIGKILL stops the process, but the kernel keeps
//! the writes that reached its page cache, synced or not. So the suite checks
//! the order of writes, renames and transactions, not durability after a
//! power loss. A missing `fsync` or full flush passes it; the power-loss
//! suite ([`power`]) covers that on Linux.
//!
//! Each run of the first test picks a seed and prints it.
//! `SCION_CRASH_SEED=<n>` runs the same workload and kill times again;
//! thread timing still differs. `SCION_CRASH_KILLS=<n>` sets the number of
//! kills (default 6).
#![cfg(unix)]

mod common;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};

use common::*;
use scion_proto::v1::GetRootTreeRequest;

/// `Objects.GetRootTree` as one call: the ID of the root tree of `changeset`.
async fn root_tree(objects: &mut Objects, changeset: ChangesetId) -> TreeId {
    let request = GetRootTreeRequest {
        repo: REPO.into(),
        changeset_id: changeset.as_bytes().to_vec(),
    };
    let reply = objects.get_root_tree(request).await.unwrap();
    TreeId::try_from(reply.into_inner().tree_id.as_slice()).unwrap()
}

use scion_core::{Changeset, ChangesetId, Tree, TreeId};

fn seed() -> u64 {
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
    let now = now.unwrap();
    let seed = env(
        "SCION_CRASH_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    println!("seed {seed}; replay with SCION_CRASH_SEED={seed}");
    seed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reply_survives_a_kill_of_the_server() {
    let mut rng = Rng::new(seed());
    // Ten kills: at the measured rate about half the kills meet a land
    // pending in the database, so the chance that all ten miss is about
    // 0.5^10, just under 0.1% - the bar the assert's margin needs, so
    // the assert itself cannot be the next flake.
    let kills = usize::try_from(env("SCION_CRASH_KILLS", 10)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);

    let mut ledger = Ledger::default();
    let mut checked = Checked::default();
    let (mut left, mut cut) = (0, 0);
    let mut pending_kills = 0usize;
    for round in 0..kills {
        let server = Server::start(&data, &log);
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        let signals = Arc::new(Signals::default());
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let name = format!("k{round}c{c}");
                let rng = Rng::new(rng.next());
                tokio::spawn(client(server.url.clone(), name, rng, Arc::clone(&signals)))
            })
            .collect();
        let run = 100 + rng.below(500);
        tokio::time::sleep(Duration::from_millis(u64::try_from(run).unwrap())).await;
        // Two kills in three, on average, wait for a moment that a crash
        // must survive: a blob write, soon after a large upload starts, or a
        // land that is pending in the database.
        match rng.below(3) {
            0 => {}
            1 => {
                wait_until("a large upload is in flight", || {
                    signals.large_uploads.load(Ordering::SeqCst) > 0
                })
                .await;
                let delay = u64::try_from(rng.below(30)).unwrap();
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            _ => {
                let db = rusqlite::Connection::open(data.join("scion.db")).unwrap();
                wait_until_bounded("a land is pending in the database", 5, || {
                    pending_lands(&db) > 0
                })
                .await;
            }
        }
        signals.killed.store(true, Ordering::SeqCst);
        {
            // Whatever arm chose the moment, the kill may still meet a
            // land the clients have in flight: count it, the test's
            // whole point is that a crash meets a pending row.
            let db = rusqlite::Connection::open(data.join("scion.db")).unwrap();
            if pending_lands(&db) > 0 {
                pending_kills += 1;
            }
        }
        drop(server);
        for client in clients {
            let mut got = client.await.unwrap();
            ledger.blobs.append(&mut got.blobs);
            ledger.oplog.merge(got.oplog);
            cut += got.lands.iter().filter(|sent| sent.reply.is_none()).count();
            ledger.lands.append(&mut got.lands);
        }
        // The staging folders of the killed processes are swept by the
        // restart inside `check`; what a kill left is a folder per process.
        left += fs::read_dir(data.join("tmp")).unwrap().count();
    }
    let server = Server::start(&data, &log);
    check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;

    println!(
        "{} lands, {cut} of them cut by a kill; {} blob files; {left} temp files left by the kills",
        ledger.lands.len(),
        checked.files.len(),
    );
    assert!(ledger.lands.len() > kills, "the clients did little work");
    assert!(
        pending_kills > 0,
        "no kill met a land pending in the database: {pending_kills} of {kills} kills; \
         the wide lands must keep pending common"
    );
    println!("{pending_kills} of {kills} kills met a land pending in the database");
}

/// Derived rows in the schema-upgrade test, so that the upgrade of version 2
/// to 3, which copies every derived row, takes a while.
const FILLER_ROWS: i64 = 30_000;

/// A data directory at schema version 2, as a build of that version left it:
/// repo `r` with a root changeset, ref `main` at it, and [`FILLER_ROWS`]
/// derived rows of a kind that no build has. Returns the root.
fn version_2_data(data: &Path) -> ChangesetId {
    fs::create_dir_all(data).unwrap();
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    // As the server leaves it. So a reader never blocks the server's open.
    c.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    c.execute_batch(include_str!("../migrations/0001_init.sql"))
        .unwrap();
    c.execute_batch(include_str!("../migrations/0002_land_retention.sql"))
        .unwrap();
    let root = Changeset::new(vec![], author(), None, b"root\n".to_vec(), BTreeMap::new())
        .expect("changeset fits the format");
    let (body, id) = root.encode_with_id();
    let id_bytes = id.as_bytes().as_slice();
    c.execute("INSERT INTO repos (repo_id, name) VALUES (1, ?1)", [REPO])
        .unwrap();
    let row = "INSERT INTO changesets (id, gen, body) VALUES (?1, 1, ?2)";
    c.execute(row, rusqlite::params![id_bytes, body]).unwrap();
    let row = "INSERT INTO repo_changesets (repo_id, cs) VALUES (1, ?1)";
    c.execute(row, [id_bytes]).unwrap();
    let row = "INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'main', ?1, 1)";
    c.execute(row, [id_bytes]).unwrap();
    c.execute_batch(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {FILLER_ROWS})
             INSERT INTO changesets (id, gen, body) SELECT randomblob(32), 1, x'' FROM n;
         INSERT INTO derived (kind, version, cs, value)
             SELECT 'filler', 1, id, randomblob(32) FROM changesets WHERE body = x'';
         PRAGMA user_version = 2;"
    ))
    .unwrap();
    id
}

/// What a kill can leave of the database in `data`: its schema version, its
/// tables and indexes (name and SQL), and its filler rows.
#[derive(Debug, PartialEq, Eq)]
struct Schema {
    version: u32,
    tables: Vec<(String, Option<String>)>,
    filler: i64,
}

fn schema(data: &Path) -> Schema {
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    let version = c.query_row("PRAGMA user_version", [], |row| row.get(0));
    let mut tables = c
        .prepare("SELECT name, sql FROM sqlite_master ORDER BY name")
        .unwrap();
    let tables = tables.query_map([], |row| Ok((row.get(0)?, row.get(1)?)));
    let filler = "SELECT count(*) FROM derived WHERE kind = 'filler'";
    Schema {
        version: version.unwrap(),
        tables: tables.unwrap().map(Result::unwrap).collect(),
        filler: c.query_row(filler, [], |row| row.get(0)).unwrap(),
    }
}

/// A kill while the server upgrades the schema leaves the database at one
/// whole migration step, never a mix: each migration commits in one
/// transaction. The next start upgrades it and serves.
#[test]
fn a_kill_during_a_schema_upgrade_leaves_the_old_schema_or_the_new_one() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    let root = version_2_data(&data);
    let old = dir.path().join("version-2.db");
    fs::copy(data.join("scion.db"), &old).unwrap();
    let version_2 = schema(&data);
    assert_eq!((version_2.version, version_2.filler), (2, FILLER_ROWS));
    let restore = || {
        for file in ["scion.db-wal", "scion.db-shm"] {
            fs::remove_file(data.join(file)).ok();
        }
        fs::copy(&old, data.join("scion.db")).unwrap();
    };

    // A whole start with the upgrade. Its time sets the step of the kills,
    // so their number does not depend on the speed of the machine. Its
    // schema is the final one, whatever migrations a build ships.
    let start = Instant::now();
    drop(Server::start(&data, &log));
    let step = start.elapsed() / 64;
    let upgraded = schema(&data);
    assert!(upgraded.version > version_2.version, "no migration ran");
    assert_eq!(upgraded.filler, FILLER_ROWS);

    // Kills 0, 1, 2 steps and so on after a start, each from version 2,
    // until one comes after the upgrade committed. The kills after the
    // upgrade began stop it in the middle.
    let mut kills = 0;
    loop {
        restore();
        let server = Server::spawn(&data, &log);
        let after = step * kills;
        std::thread::sleep(after);
        drop(server);
        kills += 1;
        let found = schema(&data);
        if found == upgraded {
            break;
        }
        // Each migration commits in one transaction, so a kill leaves the
        // database at one whole step: the version it started at (its exact
        // tables and rows), the final one, or one between them, which also
        // keeps every filler row.
        let whole = found == version_2
            || (found.version > version_2.version
                && found.version < upgraded.version
                && found.filler == FILLER_ROWS);
        assert!(whole, "a kill after {after:?}: {found:?}");
        assert!(kills < 256, "no upgrade");
    }
    println!("{kills} kills, {step:?} apart; the last one came after the upgrade");

    let server = Server::start(&data, &log);
    assert_eq!(schema(&data), upgraded);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_, mut objects) = connect(&server.url).await.unwrap();
        assert_eq!(resolve_ref(&mut objects, "main").await.unwrap(), root);
        assert_eq!(root_tree(&mut objects, root).await, Tree::default().id());
    });
}

/// The failed-migration runbook (docs/operations.md, "A failed
/// migration"), step by step: an open of a database whose schema is
/// newer than the build refuses, with both versions named; the fix is
/// the old binary, or the restore of a backup per section 3; and the
/// restored store serves. `user_version` is never edited by hand in
/// the runbook - the test edits it only to forge the newer schema the
/// error talks about.
#[test]
fn a_newer_schema_refuses_the_open_and_a_restore_serves() {
    let dir = tempfile::tempdir().unwrap();
    let (data, log) = (dir.path().join("data"), dir.path().join("server.log"));
    create_repo(&data);
    let server = Server::start(&data, &log);
    drop(server);

    // The backup the runbook restores from: the server is stopped, so
    // the copy of the directory is exact (section 3).
    let backup = dir.path().join("backup");
    fs::create_dir_all(&backup).unwrap();
    for entry in fs::read_dir(&data).unwrap() {
        let entry = entry.unwrap();
        let target = backup.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }

    // The database is ahead of the binary: the runbook's error case,
    // forged the only way a test can - the version of a future schema.
    let version = schema(&data).version;
    let c = rusqlite::Connection::open(data.join("scion.db")).unwrap();
    c.execute_batch(&format!("PRAGMA user_version = {}", version + 1))
        .unwrap();
    drop(c);

    // The operator's start: the server refuses to open, and the error
    // names both versions - the data's and the build's.
    let mut refused = Server::spawn(&data, &log);
    let started = Instant::now();
    loop {
        if let Some(status) = refused.try_wait() {
            assert!(!status.success(), "the server served a newer schema");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "the server neither served nor refused"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let text = fs::read_to_string(&log).unwrap();
    // The refusal names both versions, the whole phrase the code writes:
    // the data's (the forged version+1) and the build's.
    assert!(
        text.contains(&format!(
            "database schema version {} is newer than this build ({})",
            version + 1,
            version
        )),
        "the refusal names both versions: {text}"
    );

    // The fix, the restore arm: the backup's database replaces the
    // forged one (the WAL of the refused open goes with it), and the
    // server starts and serves.
    fs::remove_file(data.join("scion.db-wal")).ok();
    fs::remove_file(data.join("scion.db-shm")).ok();
    fs::copy(backup.join("scion.db"), data.join("scion.db")).unwrap();
    let server = Server::start(&data, &log);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (_, mut objects) = connect(&server.url).await.unwrap();
        let _ = resolve_ref(&mut objects, "main").await.unwrap();
    });
}

/// A recursive copy of one directory tree, for the runbook's backup.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

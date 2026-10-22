//! A real disk full on the data directory: a small `tmpfs` holds the
//! store, the writes fill it, and the answers are the ones the fault
//! matrix promises - the client sees out-of-space on the blob path and
//! on the metadata path, reads keep working, and a bigger mount (or a
//! restart, when the space came back) needs no repair.
//!
//! The test needs Linux and the rights to mount a `tmpfs`, so it runs
//! only with `SCION_DISK_REQUIRE=1` set (its CI step does); plain
//! `cargo test` skips it like the power-loss suite skips without its
//! variable. Every mount is one test's own: `sudo` is only used on the
//! `mount`, `remount`, and `umount` commands themselves.
mod common;

use scion_core::{Changeset, FileState, RepoPath};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::tree;
use scion_server::test_support::write;
use scion_server::{Backend, Error};

use crate::common::{REPO, author};

fn required() -> bool {
    if !cfg!(target_os = "linux") {
        println!("skipping: a tmpfs is a Linux mount");
        return false;
    }
    if std::env::var("SCION_DISK_REQUIRE").as_deref() != Ok("1") {
        println!("skipping the disk-full suite: set SCION_DISK_REQUIRE=1 to run it");
        return false;
    }
    true
}

/// Runs `args` as root, and fails the test when the command fails.
fn sudo(args: &[&str]) {
    // A child shares a flock until it execs (see `common::spawn_guard`): the
    // read side keeps this spawn out of a lock-release window of another test.
    let out = common::spawn_guard::across_spawn(|| {
        std::process::Command::new("sudo")
            .args(args)
            .output()
            .expect("sudo runs")
    });
    assert!(
        out.status.success(),
        "sudo {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A `tmpfs` mount of `size` with `inodes`, owned by this user. The
/// `Drop` unmounts it, whatever the test did.
struct Mount(std::path::PathBuf);

impl Mount {
    fn new(name: &str, size: &str, inodes: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("scion-diskfull-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        sudo(&[
            "mount",
            "-t",
            "tmpfs",
            "-o",
            &format!("size={size},nr_inodes={inodes}"),
            "tmpfs",
            dir.to_str().unwrap(),
        ]);
        sudo(&["chown", "-R", &owner(), dir.to_str().unwrap()]);
        Self(dir)
    }

    fn remount_bigger(&self, size: &str, inodes: &str) {
        sudo(&[
            "mount",
            "-o",
            &format!("remount,size={size},nr_inodes={inodes}"),
            self.0.to_str().unwrap(),
        ]);
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        sudo(&["umount", self.0.to_str().unwrap()]);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// This user, as `chown` wants it, from Linux's own `/proc`.
fn owner() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let field = |name: &str| {
        status
            .lines()
            .find(|line| line.starts_with(name))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .to_owned()
    };
    format!("{}:{}", field("Uid:"), field("Gid:"))
}

/// Whether the error is the disk's answer, mapped to out-of-space.
fn is_out_of_space(e: &Error) -> bool {
    match e {
        Error::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
        ),
        Error::Storage(storage) => storage.class() == scion_server::StorageClass::DiskFull,
        _ => false,
    }
}

/// A store on the mount, like a server opens its data directory.
/// `which` names the call site, so a failure says which open failed.
fn open_store(mount: &Mount, which: &str) -> Backend {
    Backend::open_test(mount.0.join("data").as_path())
        .unwrap_or_else(|error| panic!("{which}: {error}"))
}

/// Fills the store with distinct blobs of `len` bytes until the disk
/// refuses one, and returns the error and the blobs that landed. The
/// blobs are distinct: a put of a blob the store already holds writes
/// nothing, so a repeated chunk would fill nothing.
fn fill_with_blobs(store: &Backend, len: usize) -> (Error, Vec<scion_core::BlobId>) {
    let mut chunk = vec![b'x'; len.max(8)];
    let mut landed = Vec::new();
    loop {
        // A backstop: a disk that never refuses means this fill is
        // broken, not that the disk is endless.
        assert!(
            landed.len() < 200_000,
            "the fill never hit the disk's limit"
        );
        chunk[..8].copy_from_slice(&landed.len().to_be_bytes());
        match store.blobs().put(&chunk) {
            Ok(id) => landed.push(id),
            Err(error) => return (error, landed),
        }
    }
}

#[test]
fn a_full_disk_answers_out_of_space_and_needs_no_repair() {
    if !required() {
        return;
    }
    let mount = Mount::new("space", "8m", "4096");
    let store = open_store(&mount, "first open of the full-disk test");
    // The first blob lands, so a read has something to keep serving.
    let kept = store.blobs().put(b"kept\n").unwrap();
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    // The disk fills, at whatever call it fills: create, write, rename,
    // or fsync.
    let (error, landed) = fill_with_blobs(&store, 64 * 1024);
    assert!(is_out_of_space(&error), "{error:?}");
    // The client's view: the real error, one fixed out-of-space reply.
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status:?}");
    assert_eq!(
        status.message(),
        "the server is out of disk space; retry later"
    );
    // Every blob the disk accepted is still there: reads work.
    for id in &landed {
        assert_eq!(store.blobs().get(*id).unwrap().len(), 64 * 1024);
    }
    // The disk comes back, and the next write works with no restart.
    mount.remount_bigger("64m", "4096");
    let after = store.blobs().put(b"after\n").unwrap();
    assert_eq!(store.blobs().get(after).unwrap(), b"after\n");
    drop(store);
    // A reaped helper's inherited lock can outlive the drop by one tick:
    // no spawn starts, and the lock is free, before the reopen (#281).
    common::spawn_guard::across_release(|| {
        common::spawn_guard::wait_for_lock_release(&mount.0.join("data"));
    });
    // A restart needs no repair: the store opens again, and fsck is
    // clean.
    let store = open_store(&mount, "reopen of the full-disk test");
    assert_eq!(store.blobs().get(kept).unwrap(), b"kept\n");
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(report.is_clean(), "{:?}", report.problems);
}

#[test]
fn a_full_metadata_disk_answers_out_of_space_and_needs_no_repair() {
    if !required() {
        return;
    }
    if Backend::test_is_postgres() {
        // The metadata lives in the database then; its disk full is the
        // SQLSTATE 53100 arm of the pg mapping, a unit test of pg.rs,
        // not a data-directory row.
        return;
    }
    let mount = Mount::new("meta", "1m", "4096");
    let store = open_store(&mount, "first open of the metadata test");
    // Repos fill the metadata database until its file cannot grow.
    let mut names = Vec::new();
    let error = loop {
        let name = format!("repo-{}", names.len());
        match store.write(|c| c.create_repo(&name)) {
            Ok(_) => names.push(name),
            Err(error) => break error,
        }
    };
    assert!(is_out_of_space(&error), "{error:?}");
    // The client's view: the real error, one fixed out-of-space reply.
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status:?}");
    assert_eq!(
        status.message(),
        "the server is out of disk space; retry later"
    );
    // Reads work: every repo row that landed is still there.
    assert_eq!(store.read(|c| c.repos()).unwrap().len(), names.len());
    // The disk comes back, and the next row works with no restart.
    mount.remount_bigger("64m", "4096");
    store.write(|c| c.create_repo("after")).unwrap();
    drop(store);
    // As above: the reopen must not race a lingering lock.
    common::spawn_guard::across_release(|| {
        common::spawn_guard::wait_for_lock_release(&mount.0.join("data"));
    });
    // A restart needs no repair, and fsck is clean.
    let store = open_store(&mount, "reopen of the metadata test");
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(report.is_clean(), "{:?}", report.problems);
}

#[test]
fn a_disk_out_of_inodes_answers_out_of_space_and_needs_no_repair() {
    if !required() {
        return;
    }
    let mount = Mount::new("inodes", "64m", "128");
    let store = open_store(&mount, "first open of the inodes test");
    // Tiny blobs eat the inode budget one file at a time, until the
    // create itself is refused.
    let (error, landed) = fill_with_blobs(&store, 16);
    assert!(is_out_of_space(&error), "{error:?}");
    // The client's view: the real error, one fixed out-of-space reply.
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status:?}");
    assert_eq!(
        status.message(),
        "the server is out of disk space; retry later"
    );
    for id in &landed {
        assert_eq!(store.blobs().get(*id).unwrap().len(), 16);
    }
    // More inodes, and the next write works with no restart.
    mount.remount_bigger("64m", "4096");
    store.blobs().put(b"after\n").unwrap();
    drop(store);
    // As above: the reopen must not race a lingering lock.
    common::spawn_guard::across_release(|| {
        common::spawn_guard::wait_for_lock_release(&mount.0.join("data"));
    });
    let store = open_store(&mount, "reopen of the inodes test");
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(report.is_clean(), "{:?}", report.problems);
}

/// The client's blob put, the server's own path: `put_blob` records
/// the `repo_blobs` row that gc's live-set walk reads; a bare
/// `blobs().put` does not.
fn seed_client_blob(
    store: &Backend,
    repo: scion_server::test_support::meta::RepoId,
    content: &[u8],
) -> scion_core::BlobId {
    let upload = store.blobs().upload().unwrap().append(content).unwrap();
    scion_server::test_support::write::put_blob(store, repo, upload)
        .expect("the client's blob put lands")
}

/// The disk-full runbook (docs/operations.md, "Disk full"), step by
/// step: the disk fills and refuses, the gc routine frees what nothing
/// names (a dry run first, then a real one), fsck is clean, and writes
/// resume with no restart and no repair.
/// The output of one `gc` run on `data`, dry or real. On the database
/// backend the subprocess needs what the store's own open had: the URL,
/// whose `options` carry the schema, so the gc's open lands in this
/// directory's database corner and not beside it.
fn gc_args(data: &std::path::Path, dry: bool) -> String {
    let mut args = vec!["--data".to_owned(), data.display().to_string()];
    if Backend::test_is_postgres()
        && let Ok(url) = std::env::var("SCION_TEST_POSTGRES_URL")
    {
        // The URL is a flag of the root command, so it goes before the
        // subcommand; its options carry this directory's schema.
        let schema = Backend::test_schema(data).expect("the test names a schema");
        args.push("--postgres-url".to_owned());
        args.push(format!(
            "{url}{}options=--search_path%3D{schema}",
            if url.contains('?') { "&" } else { "?" }
        ));
    }
    args.extend(["gc".to_owned(), "--grace-secs".to_owned(), "0".to_owned()]);
    if dry {
        args.push("--dry-run".to_owned());
    }
    let out = common::spawn_guard::across_spawn(|| {
        std::process::Command::new(env!("CARGO_BIN_EXE_scion-server"))
            .args(&args)
            .env("NO_COLOR", "1")
            .output()
            .expect("the gc command runs")
    });
    assert!(
        out.status.success(),
        "gc {} failed: {}",
        if dry { "--dry-run" } else { "real" },
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
fn a_disk_full_runbook_gcs_then_writes_resume() {
    if !required() {
        return;
    }
    let mount = Mount::new("runbook", "8m", "4096");
    let store = open_store(&mount, "first open of the runbook test");
    // The data the operator keeps: one repo whose changeset names one
    // blob, so the gc routine must keep it and free the rest. The blob
    // goes up the server's own path: put_blob records the repo_blobs
    // row the gc's live-set walk reads, as a client's PutBlob would.
    let repo = store.write(|c| c.create_repo("main")).unwrap();
    let named = seed_client_blob(&store, repo, b"named\n");
    let cs = scion_core::Changeset::new(
        vec![],
        scion_core::IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"runbook\n".to_vec(),
        [(
            scion_core::RepoPath::new("RUNBOOK.md").unwrap(),
            Some(scion_core::FileState::Regular {
                content: named,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    let cs = store
        .write(|c| c.insert_changeset(repo, &cs))
        .expect("the changeset lands");
    store
        .write(|c| c.cas_ref(repo, "main", None, cs))
        .expect("the ref moves");
    let unnamed = store.blobs().put(b"unnamed\n").unwrap();

    // Step 0, the alert's trigger: the disk fills and refuses.
    let (error, _landed) = fill_with_blobs(&store, 64 * 1024);
    assert!(is_out_of_space(&error), "{error:?}");

    // Step 1, the runbook's gc routine: a dry run reports what it
    // would collect and removes nothing, then a real run collects the
    // unnamed blobs. The blobs are seconds old, so the grace must be
    // zero for the run to take them.
    let data = mount.0.join("data");
    let gc = |dry: bool| gc_args(&data, dry);
    let dry = gc(true);
    assert!(dry.contains("(dry run)"), "a dry run labels itself: {dry}");
    let real = gc(false);
    assert!(
        !real.contains("dry run"),
        "the real run does not label itself: {real}"
    );
    // The real run's count: "gc: N repos, M rows pruned, K files removed".
    let files = real
        .split(" rows pruned, ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    assert!(
        files > 0,
        "the real run removed the unnamed blobs, not {files}: {real}"
    );

    // The gc freed the space the unnamed blobs held: the named one
    // still reads, the unnamed one is gone, and a write lands again
    // with no restart and no grow.
    assert_eq!(store.blobs().get(named).unwrap(), b"named\n");
    assert!(
        store.blobs().get(unnamed).is_err(),
        "gc removed the blob nothing names"
    );
    let freed = store.blobs().put(b"freed\n").unwrap();
    assert_eq!(store.blobs().get(freed).unwrap(), b"freed\n");

    // The audit records the run it could not greet: both runs' intents
    // failed to go in on the full disk, and the real run's line went in
    // after the run freed the space, marked with why it was late. The
    // dry run removed nothing, so its deferred line had nowhere to go.
    let audit = std::fs::read_to_string(data.join("audit.log")).unwrap();
    let lines: Vec<serde_json::Value> = audit
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        lines.len(),
        2,
        "one deferred intent and one result: {lines:?}"
    );
    assert_eq!(lines[0]["event"], "admin");
    assert_eq!(lines[0]["outcome"], "intent");
    assert_eq!(lines[0]["what"], "gc");
    assert_eq!(lines[0]["deferred"], "disk_full");
    assert_eq!(lines[1]["event"], "admin");
    assert_eq!(lines[1]["outcome"], "ok");

    // Step 2 of the runbook: fsck, and it is clean.
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(report.is_clean(), "{:?}", report.problems);

    // Step 3's premise: gc freed too little here (the named data stays
    // and the mount is still nearly full is fine) - the runbook's own
    // step is to grow the volume, which needs no restart.
    mount.remount_bigger("64m", "4096");
    let after = store.blobs().put(b"after\n").unwrap();
    assert_eq!(store.blobs().get(after).unwrap(), b"after\n");
    drop(store);
    common::spawn_guard::across_release(|| {
        common::spawn_guard::wait_for_lock_release(&mount.0.join("data"));
    });
    let store = open_store(&mount, "reopen of the runbook test");
    let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
    assert!(report.is_clean(), "{:?}", report.problems);
}

/// A raw connection of the database, for the fill and the checks.
fn raw(url: &str) -> postgres::Client {
    postgres::Client::connect(url, postgres::NoTls).expect("the test database is up")
}

/// The SQLSTATE a store error carries, when it does.
fn sqlstate(e: &Error) -> Option<String> {
    use std::error::Error as _;
    match e {
        Error::Storage(storage) => {
            let inner = storage.source()?;
            let down = inner.downcast_ref::<postgres::Error>()?;
            down.code().map(|state| state.code().to_owned())
        }
        _ => None,
    }
}

/// The bytes free on the database's data directory: the tmpfs's size
/// minus what the default tablespace holds. The run prints it at each
/// phase, so one run shows the whole picture.
fn free_bytes(client: &mut postgres::Client) -> String {
    let used: i64 = client
        .query_one("SELECT pg_tablespace_size('pg_default')", &[])
        .expect("the tablespace size answers")
        .get(0);
    match std::env::var("SCION_PG_TMPFS_BYTES")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
    {
        Some(total) => format!("{} bytes free", total - used),
        // The dry run has no tmpfs witness: report what is held.
        None => format!("{used} bytes held (no tmpfs witness)"),
    }
}

/// The database's data directory, as the database names it.
fn data_dir(client: &mut postgres::Client) -> String {
    client
        .query_one("SHOW data_directory", &[])
        .expect("the data directory answers")
        .get::<_, String>(0)
}

/// A chunk the database cannot shrink: bytes of a plain pseudorandom
/// sequence, so TOAST's compression keeps none of the room the test
/// means to spend (run 7's evidence: a chunk of one repeated byte
/// compressed 4:1, and the 64 MiB margin held under 1 MiB).
#[allow(clippy::cast_possible_truncation)]
fn incompressible(len: usize) -> Vec<u8> {
    let mut state = 0x5343_494F_4E53_u64;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) as u8
        })
        .collect()
}

/// Fills the margin table to `total` bytes, the room the store's open
/// and first lands will spend later. Its files unlink at the DROP, so
/// freeing it needs no vacuum's luck about where the dead pages sit.
fn fill_margin(client: &mut postgres::Client, chunk_len: usize, total: usize) {
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS diskfull_filler;
             CREATE TABLE IF NOT EXISTS diskfull_filler.margin (chunk bytea)",
        )
        .expect("the margin table is created");
    let chunk = incompressible(chunk_len);
    let mut put = 0usize;
    while put < total {
        client
            .execute("INSERT INTO diskfull_filler.margin VALUES ($1)", &[&chunk])
            .expect("the margin's insert lands");
        put += chunk_len;
    }
    client
        .batch_execute("CHECKPOINT")
        .expect("the margin's checkpoint runs");
}

/// Fills the database with `chunk`-sized rows from its own connection
/// until its on-disk size passes `ceiling` bytes, and returns the raw
/// error of the insert that hit the full disk, when one did - the fill
/// itself can be the write that fails first.
fn fill(client: &mut postgres::Client, chunk_len: usize) -> Option<postgres::Error> {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS diskfull_filler")
        .expect("the filler schema is created");
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS diskfull_filler.filler (chunk bytea)")
        .expect("the filler table is created");
    let chunk = incompressible(chunk_len);
    loop {
        match client.execute("INSERT INTO diskfull_filler.filler VALUES ($1)", &[&chunk]) {
            Ok(_) => {
                // A checkpoint keeps the WAL small, so the tmpfs's space
                // goes to the filler's heap pages and the margin holds.
                client
                    .batch_execute("CHECKPOINT")
                    .expect("the checkpoint runs");
            }
            Err(e) => return Some(e),
        }
    }
}

/// Fills to a byte budget instead of to the wall: the dry run's fill,
/// which stops while the disk still has room.
fn fill_to(client: &mut postgres::Client, chunk_len: usize, budget: usize) {
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS diskfull_filler;
             CREATE TABLE IF NOT EXISTS diskfull_filler.filler (chunk bytea)",
        )
        .expect("the filler table is created");
    let chunk = incompressible(chunk_len);
    let mut put = 0usize;
    while put < budget {
        client
            .execute("INSERT INTO diskfull_filler.filler VALUES ($1)", &[&chunk])
            .expect("the dry fill's insert lands");
        put += chunk_len;
    }
    client
        .batch_execute("CHECKPOINT")
        .expect("the dry fill's checkpoint runs");
}

/// A changeset whose rows are wide: many entries, each naming a path,
/// so one land's metadata write must grow the database.
/// The paths the lands rewrite: the root creates them, and each land
/// either deletes or re-creates the set, so one land's metadata write
/// is wide and the next land applies onto it.
const WIDE_PATHS: usize = 400;

fn wide_changeset(
    store: &Backend,
    name: &str,
    create: bool,
    blob: scion_core::BlobId,
) -> Changeset {
    let base = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main exists").target)
        })
        .unwrap();
    let author = crate::common::author();
    let entries: std::collections::BTreeMap<_, _> = (0..WIDE_PATHS)
        .map(|i| {
            let state = if create {
                Some(FileState::Regular {
                    content: blob,
                    executable: false,
                })
            } else {
                None
            };
            (
                RepoPath::new(format!("d/wide/f{i}.txt").as_str()).unwrap(),
                state,
            )
        })
        .collect();
    Changeset::new(
        vec![base],
        author,
        None,
        format!("{name}\n").into_bytes(),
        entries,
    )
    .expect("changeset fits the format")
}
/// Fills the margin and the filler: to the wall on the wet run (the
/// 53100 must be the filler's own insert's error), to a budget on the
/// dry one. The lands' count starts at the fill's chunk count, so the
/// land names stay distinct.
fn fill_all(client: &mut postgres::Client, dry: bool) -> usize {
    println!("phase: before the fill, {}", free_bytes(client));
    // The margin: room for the open, the repo, and the first lands.
    // The wet run's lands spend about 14 KiB a land, so a small margin
    // closes the wall within a few hundred lands - a big margin would
    // spend itself on thousands of lands first (run 13's evidence: 64
    // MiB was spent past the 5000-land backstop).
    let margin = if dry { 8 } else { 12 } * 1024 * 1024;
    fill_margin(client, 4 * 1024 * 1024, margin);
    // The lands' count starts at the fill's chunk count, so the land
    // names stay distinct from the filler's row names.
    let landed = 0usize;
    let filled_by = if dry {
        // No wall to reach: the fill stops at a budget the dry disk
        // holds with room to spare.
        fill_to(client, 4 * 1024 * 1024, 32 * 1024 * 1024);
        None
    } else {
        fill(client, 4 * 1024 * 1024)
    };
    if !dry {
        match &filled_by {
            Some(e) if e.code() == Some(&postgres::error::SqlState::DISK_FULL) => {
                println!("the filler's insert took the 53100 first");
            }
            Some(e) => panic!("the fill failed with something else: {e:?}"),
            None => {}
        }
    }
    println!("phase: the fill ended, {}", free_bytes(client));
    landed
}

/// Lands wide changesets until the wall stops one (the wet run: the
/// error and the stopped changeset's bytes come back for the retry),
/// or until 40 have landed (the dry run: the last land's id). The
/// count rides in `from` so the land names stay distinct.
/// How the lands ended: the last land's id, and the wall - the error
/// and the stopped changeset's bytes, for the retry - when one stopped
/// them.
type LandStop = (
    Option<scion_core::ChangesetId>,
    Result<(), (Error, Vec<u8>)>,
);

fn land_until(store: &Backend, from: usize, blob: scion_core::BlobId, dry: bool) -> LandStop {
    let mut landed = from;
    let mut last = None;
    loop {
        // A backstop: lands that never stop mean the fill was not full,
        // which is this test's own failure, not the server's.
        assert!(landed < 5000, "the lands never hit the database's limit");
        if dry && landed == 40 {
            return (last, Ok(()));
        }
        landed += 1;
        let cs = wide_changeset(store, &format!("fill{landed}"), landed % 2 == 1, blob);
        // The helper stores the changeset and its tree; the ref move is
        // the land's own act, so main advances here and the next land's
        // base tree holds the last land's files.
        let id = match write::create_changeset(store, REPO, &cs.encode()) {
            Ok(id) => id,
            Err(e) => return (last, Err((e, cs.encode()))),
        };
        move_main(store, id);
        last = Some(id);
    }
}

#[test]
fn a_full_postgres_database_answers_out_of_space_and_needs_no_repair() {
    if !Backend::test_is_postgres() {
        println!("skipping: the database's own disk full is a Postgres row");
        return;
    }
    // The dry run: the plumbing of this row - margin, drop, open, repo,
    // wide lands - against any Postgres, with the fill capped small and
    // the lands stopped by a count. No wall, so no 53100 to assert; the
    // point is that a dispatch is not the first run of a change here.
    let dry = std::env::var("SCION_PG_DISKFULL_DRY").is_ok();
    // The wet run needs the tmpfs's witness: the workflow's job mounts
    // the small disk and says so here. SHORTCUT: the env var is the
    // tmpfs's only witness - a local run against a big disk would fill
    // for hours before 53100.
    let bytes = match std::env::var("SCION_PG_TMPFS_BYTES") {
        Ok(bytes) => bytes,
        Err(_) if dry => String::new(),
        Err(_) => {
            println!(
                "skipping: set SCION_PG_TMPFS_BYTES to the service's tmpfs size, or SCION_PG_DISKFULL_DRY=1 for the dry run"
            );
            return;
        }
    };
    if !dry {
        bytes
            .parse::<u64>()
            .expect("SCION_PG_TMPFS_BYTES must be a number of bytes");
    }
    let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL");
    let mut client = raw(&url);
    if !dry {
        // The wet run's data directory is the service container's own;
        // the job cannot see it, and the env var is the witness instead.
        let dir = data_dir(&mut client);
        if !std::path::Path::new(&dir).exists() {
            println!("the data directory {dir} is the service's own; the env var vouches for it");
        }
    }
    // The WAL is off the tmpfs (the service mounts it elsewhere), so a
    // heap extend is the write that fails, with the clean 53100 - a
    // lost connection is the Database unreachable row, not this one.
    let landed = fill_all(&mut client, dry);
    // Free the margin: the DROP unlinks the margin table's files at
    // commit, so the room returns with nothing left to luck - a VACUUM
    // only truncates a table's empty tail (run 4's evidence). The
    // store's open and lands spend the freed room until the wall
    // closes again.
    client
        .batch_execute("DROP TABLE diskfull_filler.margin;")
        .expect("the margin frees");
    println!("phase: the margin dropped, {}", free_bytes(&mut client));

    // The proving write: lands a client would run, through the store's
    // own write path, until the wall stops one of them.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let store = Backend::open_test(&data).expect("the store opens");
    // The repo, its root changeset, and the ref main, all through the
    // same open the lands below use: a second open of one schema cannot
    // run beside the first (the per-schema one-server lock), and a bare
    // repo has no main for the lands' parent read.
    let blob = repo_with_main(&store);
    let (last_land, stopped) = land_until(&store, landed, blob, dry);
    let last_land = last_land.expect("the lands landed at least one");
    if let Ok(()) = stopped {
        // The dry arm: the plumbing ran; the row's read, drop, and the
        // land after, close it out.
        println!("dry: the lands landed with no wall in the way");
        client
            .batch_execute("DROP SCHEMA diskfull_filler CASCADE")
            .expect("the filler drops");
        client
            .batch_execute("CHECKPOINT")
            .expect("the checkpoint runs");
        store
            .write(|c| c.create_repo("after"))
            .expect("the same write lands");
        return;
    }
    let Err((outcome, failed)) = stopped else {
        unreachable!("the dry arm returned above");
    };
    // The wet run's break carries the error; a land that never fails is
    // the backstop's own panic above.
    assert!(
        sqlstate(&outcome).as_deref() == Some("53100"),
        "the proving write failed another way: {outcome:?}"
    );
    // The clean 53100, mapped to the disk-full storage class - the
    // class the fixed ResourceExhausted reply rides on (the pg_error_of
    // unit test pins the reply's text). The WAL is off the tmpfs, so a
    // heap extend is the write that fails, and a lost connection cannot
    // wear the disk's reply.
    assert!(
        is_out_of_space(&outcome),
        "the full database's write must map to out of space: {outcome:?}"
    );
    // Reads work on both arms: the rows the fill landed are still there.
    let read: i64 = client
        .query_one("SELECT count(*) FROM diskfull_filler.filler", &[])
        .expect("the filler reads")
        .get(0);
    assert!(read > 0, "the filler's rows vanished");

    // Reads work through the store while the disk is full: main names
    // the last land, and that land's blob reads back whole.
    assert_eq!(main_target(&store), last_land, "main names the last land");
    let readback = store.blobs().get(blob).expect("the blob reads");
    assert_eq!(readback, incompressible(64), "the blob reads back whole");

    // The space comes back, and the exact write the wall stopped - the
    // same encoded bytes - lands now, and main moves to it.
    client
        .batch_execute("DROP SCHEMA diskfull_filler CASCADE")
        .expect("the filler drops");
    client
        .batch_execute("CHECKPOINT")
        .expect("the checkpoint runs");
    let retried = write::create_changeset(&store, REPO, &failed)
        .expect("the stopped write lands once the database has room");
    move_main(&store, retried);
    assert_eq!(
        main_target(&store),
        retried,
        "main moves to the retried write"
    );
}

/// Moves the ref main to `id`: the ref move is the land's own act, so
/// every stored changeset needs this to become the head.
fn move_main(store: &Backend, id: scion_core::ChangesetId) {
    let head = store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?
                .map(|r| (repo, r.version))
                .expect("main exists"))
        })
        .unwrap();
    store
        .write(|c| {
            assert!(c.cas_ref(head.0, "main", Some(head.1), id)?, "main moves");
            Ok(())
        })
        .expect("main advances");
}

/// The repo the lands land into: the root changeset, the ref main,
/// and one stored blob the re-creations share - all through the same
/// open the lands use (a second open of one schema cannot run beside
/// the first: the per-schema one-server lock).
fn repo_with_main(store: &Backend) -> scion_core::BlobId {
    // The blob the re-creations share: stored first, then named a
    // member of the repo, so the lands' checks accept it.
    let blob = store.blobs().put(&incompressible(64)).unwrap();
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = Changeset::new(
                vec![],
                author(),
                None,
                b"root\n".to_vec(),
                std::collections::BTreeMap::new(),
            )
            .expect("the root fits the format");
            let (id, _) = tree::store_changeset(c, repo, &root, &mut tree::lens_of(store))?;
            assert!(c.cas_ref(repo, "main", None, id)?);
            c.add_repo_blobs(repo, &[blob])?;
            Ok(())
        })
        .expect("the repo and its main land");
    blob
}

/// The ref main's target, read through the store.
fn main_target(store: &Backend) -> scion_core::ChangesetId {
    store
        .read(|c| {
            let repo = c.repo_id(REPO)?;
            Ok(c.get_ref(repo, "main")?.expect("main exists").target)
        })
        .expect("main reads")
}

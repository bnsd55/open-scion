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

use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, Error};

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
        // SQLSTATE 53100 arm, not a data-directory row.
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
    let gc = |dry: bool| {
        let mut args = vec![
            "--data".to_owned(),
            data.display().to_string(),
            "gc".to_owned(),
            "--grace-secs".to_owned(),
            "0".to_owned(),
        ];
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
    };
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

/// The database's data directory, as the database names it.
fn data_dir(client: &mut postgres::Client) -> String {
    client
        .query_one("SHOW data_directory", &[])
        .expect("the data directory answers")
        .get::<_, String>(0)
}

/// Fills the database with `chunk`-sized rows from its own connection
/// until its on-disk size passes `ceiling` bytes, and returns the raw
/// error of the insert that hit the full disk, when one did - the fill
/// itself can be the write that fails first.
fn fill(client: &mut postgres::Client, chunk_len: usize, ceiling: u64) -> Option<postgres::Error> {
    client
        .batch_execute("CREATE SCHEMA IF NOT EXISTS pg_diskfull_filler")
        .expect("the filler schema is created");
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS pg_diskfull_filler.filler (chunk bytea)")
        .expect("the filler table is created");
    let chunk = vec![b'f'; chunk_len];
    loop {
        let size: i64 = client
            .query_one("SELECT pg_database_size(current_database())", &[])
            .expect("the database size answers")
            .get(0);
        if size.cast_unsigned() >= ceiling {
            return None;
        }
        match client.execute(
            "INSERT INTO pg_diskfull_filler.filler VALUES ($1)",
            &[&chunk],
        ) {
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

#[test]
fn a_full_postgres_database_answers_out_of_space_and_needs_no_repair() {
    if !required() {
        return;
    }
    if !Backend::test_is_postgres() {
        println!("skipping: the database's own disk full is a Postgres row");
        return;
    }
    // The fill only proves the row where the database's data directory
    // sits on a small tmpfs: the workflow's own job mounts it and says
    // so here. SHORTCUT: the env var is the tmpfs's only witness - a
    // local run against a big disk would fill for hours before 53100.
    let Ok(bytes) = std::env::var("SCION_PG_TMPFS_BYTES") else {
        println!("skipping: set SCION_PG_TMPFS_BYTES to the service's tmpfs size");
        return;
    };
    let Ok(bytes) = bytes.parse::<u64>() else {
        panic!("SCION_PG_TMPFS_BYTES must be a number of bytes");
    };
    let url = std::env::var("SCION_TEST_POSTGRES_URL").expect("SCION_TEST_POSTGRES_URL");
    let mut client = raw(&url);
    let dir = data_dir(&mut client);
    if !std::path::Path::new(&dir).exists() {
        // A service container's path: the job cannot see it, and the
        // env var is the witness instead.
        println!("the data directory {dir} is the service's own; the env var vouches for it");
    }
    // The margin keeps the proving write's own extend inside a full
    // disk. The WAL is off the tmpfs (the service mounts it elsewhere),
    // so a heap extend is the write that fails, with the clean 53100 -
    // the WAL-panic arm cannot fire here, and the Database unreachable
    // row covers a lost connection.
    let margin = 8 * 1024 * 1024;
    assert!(bytes > margin * 2, "the tmpfs is too small to fill");
    let ceiling = bytes - margin;

    // The fill: the database's own disk fills to the margin, either by
    // the filler's inserts or by the insert that failed first.
    let filled_by = fill(&mut client, 4 * 1024 * 1024, ceiling);
    match &filled_by {
        Some(e) if e.code() == Some(&postgres::error::SqlState::DISK_FULL) => {
            println!("the filler's insert took the 53100 first");
        }
        Some(e) => panic!("the fill failed with something else: {e:?}"),
        None => {}
    }

    // The proving write: the same land a client's client would run,
    // through the store's own write path.
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let store = Backend::open_test(&data).expect("the store opens");
    let _repo = store
        .read(|c| c.create_repo("filler"))
        .expect("the repo lands");
    let name = "the write that must fail";
    let outcome = store.write(|c| c.create_repo(name));
    match outcome {
        Err(e) if sqlstate(&e).as_deref() == Some("53100") => {
            // The clean 53100, mapped to the disk-full storage class -
            // the class the fixed ResourceExhausted reply rides on (the
            // pg_error_of unit test pins the reply's text). The WAL is
            // off the tmpfs, so a heap extend is the write that fails,
            // and a lost connection cannot wear the disk's reply.
            assert!(
                is_out_of_space(&e),
                "the full database's write must map to out of space: {e:?}"
            );
        }
        Err(e) => panic!("the proving write failed another way: {e:?}"),
        Ok(_) => panic!(
            "the proving write landed on a database filled to its margin; the fill was not full"
        ),
    }

    // Reads work on both arms: the rows the fill landed are still there.
    let read: i64 = client
        .query_one("SELECT count(*) FROM pg_diskfull_filler.filler", &[])
        .expect("the filler reads")
        .get(0);
    assert!(read > 0, "the filler's rows vanished");

    // The space comes back, and the same write lands with no restart.
    client
        .batch_execute("DROP SCHEMA pg_diskfull_filler CASCADE")
        .expect("the filler drops");
    client
        .batch_execute("CHECKPOINT")
        .expect("the checkpoint runs");
    store
        .write(|c| c.create_repo("after"))
        .expect("the same write lands");
}

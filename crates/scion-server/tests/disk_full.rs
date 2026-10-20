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

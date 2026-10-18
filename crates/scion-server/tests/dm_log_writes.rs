//! The durability of an acknowledged blob at the block level: the store
//! runs on an `ext4` over a dm-log-writes target, the story is recorded
//! with a named mark at every put, and each replay stops at the last
//! flush entry before a mark - a power cut that lost everything the
//! drive was never told to flush. What survives each cut is checked by
//! mounting the file system, reopening the store, and running fsck.
//!
//! This is the cut the `LazyFS` suite cannot make: LazyFS persists a
//! rename at rename time, so a lost directory fsync is invisible to it.
//! Here a cut between the file flush and the directory flush takes the
//! name away, and the test goes red.
//!
//! The suite needs Linux, the rights to mount and to run `dmsetup`, and
//! the `replay-log` tool (the CI step builds it and names it with
//! `SCION_DMW_REPLAY`). Plain `cargo test` skips it, like the disk-full
//! suite skips without its variable.
use scion_server::{Backend, Store as _};
use std::cell::RefCell;
use std::path::{Path, PathBuf};

fn required() -> bool {
    if !cfg!(target_os = "linux") {
        println!("skipping: dm-log-writes is a Linux target");
        return false;
    }
    if std::env::var("SCION_DMW_REQUIRE").as_deref() != Ok("1") {
        println!("skipping the dm-log-writes suite: set SCION_DMW_REQUIRE=1 to run it");
        return false;
    }
    true
}

/// Runs `args` as root, and fails the test when the command fails.
fn sudo(args: &[&str]) -> String {
    let out = std::process::Command::new("sudo")
        .args(args)
        .output()
        .expect("sudo runs");
    assert!(
        out.status.success(),
        "sudo {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// Runs `args` as root, tolerating a failure: the drop takes down what
/// the test body may already have taken down.
fn sudo_soft(args: &[&str]) {
    let _ = std::process::Command::new("sudo").args(args).output();
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

/// One recording: the dm-log-writes pair, its mount, and the marks the
/// story wrote. The `Drop` takes it all down, best effort.
struct Recording {
    /// The scratch directory of the images.
    work: PathBuf,
    /// The mount of the `ext4` the story was recorded on.
    mount: PathBuf,
    /// The loop device that holds the log, attached for good.
    log_dev: String,
    /// The loop device that holds the data image, replaced per replay.
    data_loop: RefCell<String>,
    disk: PathBuf,
    /// The mkfs'd data image, restored before every replay.
    pristine: PathBuf,
}

impl Recording {
    fn new(work: &Path) -> Self {
        let disk = work.join("disk.img");
        let log_file = work.join("log.img");
        for name in ["disk.img", "log.img"] {
            let file = std::fs::File::create(work.join(name)).unwrap();
            // One gibibyte each: the dm table length is checked against
            // the data device, and the log only grows with its entries.
            // Sparse: the real space is what the log and the ext4 use.
            file.set_len(1024 * 1024 * 1024).unwrap();
        }
        // log-writes <data dev> <log dev>: drivers/md/dm-log-writes.c
        // shifts the data device first.
        let data_loop = sudo(&["losetup", "--find", "--show", disk.to_str().unwrap()]);
        let log_dev = sudo(&["losetup", "--find", "--show", log_file.to_str().unwrap()]);
        let sectors = sudo(&["blockdev", "--getsize64", &data_loop])
            .parse::<u64>()
            .unwrap()
            / 512;
        sudo(&[
            "dmsetup",
            "create",
            "logdisk",
            "--table",
            &format!("0 {sectors} log-writes {data_loop} {log_dev}"),
        ]);
        sudo(&["mkfs.ext4", "-F", "/dev/mapper/logdisk"]);
        std::fs::create_dir_all(work.join("mnt")).unwrap();
        sudo(&[
            "mount",
            "/dev/mapper/logdisk",
            work.join("mnt").to_str().unwrap(),
        ]);
        sudo(&["chown", "-R", &owner(), work.join("mnt").to_str().unwrap()]);
        std::fs::copy(&disk, work.join("disk.pristine.img")).unwrap();
        Self {
            work: work.to_path_buf(),
            mount: work.join("mnt"),
            log_dev,
            data_loop: RefCell::new(data_loop),
            disk,
            pristine: work.join("disk.pristine.img"),
        }
    }

    /// The data image, restored to its mkfs'd bytes on a fresh loop, so
    /// a replay never sees a previous cut. The log loop stays.
    fn fresh_disk(&self) -> String {
        sudo(&["losetup", "-d", &self.data_loop.borrow()]);
        std::fs::copy(&self.pristine, &self.disk).unwrap();
        let fresh = sudo(&["losetup", "--find", "--show", self.disk.to_str().unwrap()]);
        self.data_loop.borrow_mut().clone_from(&fresh);
        fresh
    }
}

/// A mark in the log: the story reached this point, so every put
/// before it was answered.
fn mark(name: &str) {
    sudo(&["dmsetup", "message", "logdisk", "0", "mark", name]);
}

impl Drop for Recording {
    fn drop(&mut self) {
        sudo_soft(&["umount", self.mount.to_str().unwrap()]);
        sudo_soft(&["dmsetup", "remove", "logdisk"]);
        sudo_soft(&["losetup", "-D"]);
    }
}

/// The zero-based number of the first entry at or after `start` that
/// carries the flag, or of the named mark; `None` when the scan walks
/// past the log's end. replay-log.c prints `cur_entry - 1`, the index
/// of the entry it stopped on.
fn find_entry(
    replay: &str,
    log_dev: &str,
    start: u64,
    next_flush: bool,
    mark: Option<&str>,
) -> Option<u64> {
    let mut args = vec![
        replay.to_owned(),
        "--log".to_owned(),
        log_dev.to_owned(),
        "--find".to_owned(),
        "--start-entry".to_owned(),
        start.to_string(),
    ];
    if next_flush {
        args.push("--next-flush".to_owned());
    }
    if let Some(mark) = mark {
        args.push("--end-mark".to_owned());
        args.push(mark.to_owned());
    }
    let str_args: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = std::process::Command::new("sudo")
        .args(&str_args)
        .output()
        .unwrap();
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

#[test]
fn an_acknowledged_blob_survives_a_cut_at_the_last_flush_before_its_reply() {
    if !required() {
        return;
    }
    let replay =
        std::env::var("SCION_DMW_REPLAY").expect("the CI step builds replay-log and names it");
    let work = tempfile::tempdir().unwrap();
    let recording = Recording::new(work.path());
    let mount = recording.mount.clone();

    // The story: three puts, each acknowledged at its mark. The store
    // synced the file, the rename, the fan-out directory, and the drive
    // before a put returned, so every mark is a promise the cut keeps.
    let store = Backend::open_test(mount.join("data").as_path()).unwrap();
    let mut ids = Vec::new();
    for k in 1..=3u64 {
        let raw = format!("acknowledged {k}\n");
        ids.push(store.blobs().put(raw.as_bytes()).unwrap());
        mark(&format!("reply-{k}"));
    }
    drop(store);
    sudo(&["umount", mount.to_str().unwrap()]);
    sudo(&["dmsetup", "remove", "logdisk"]);

    // The reply marks and the flush entries, by number.
    let marks: Vec<u64> = (1..=3)
        .map(|k| {
            find_entry(
                &replay,
                &recording.log_dev,
                0,
                false,
                Some(&format!("reply-{k}")),
            )
            .unwrap_or_else(|| panic!("the mark reply-{k} is in the log"))
        })
        .collect();
    let flushes: Vec<u64> = std::iter::successors(
        find_entry(&replay, &recording.log_dev, 0, true, None),
        |f| find_entry(&replay, &recording.log_dev, f + 1, true, None),
    )
    .collect();
    assert!(!flushes.is_empty(), "the log carries flush entries");

    for (i, mark) in marks.iter().enumerate() {
        let n = i as u64 + 1;
        // The cut: everything logged up to and with the last flush at
        // or before the mark. The drive lost the entries after it.
        let cut = *flushes
            .iter()
            .rev()
            .find(|f| **f <= *mark)
            .unwrap_or_else(|| panic!("a flush precedes the mark reply-{n}"));
        // A fresh disk, and the log up to the cut, replayed onto it.
        let disk = recording.fresh_disk();
        sudo(&[
            replay.as_str(),
            "--log",
            recording.log_dev.as_str(),
            "--replay",
            disk.as_str(),
            "--limit",
            &(cut + 1).to_string(),
        ]);
        // Mounting replays the journal; the store reopens on the cut.
        sudo(&["mount", disk.as_str(), mount.to_str().unwrap()]);
        let store = Backend::open_test(mount.join("data").as_path()).unwrap();
        for (k, id) in ids.iter().take(n as usize).enumerate() {
            let k = k + 1;
            assert_eq!(
                store.blobs().get(*id).unwrap(),
                format!("acknowledged {k}\n").into_bytes(),
                "blob {k}, acknowledged before the cut at entry {cut}, lost its bytes or its name"
            );
        }
        let report = scion_server::fsck::run(&store, 1, &mut |_| {}).unwrap();
        assert!(report.is_clean(), "{:?}", report.problems);
        drop(store);
        sudo(&["umount", mount.to_str().unwrap()]);
    }
}

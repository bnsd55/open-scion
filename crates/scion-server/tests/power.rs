//! Power-loss suite: the real `scion-server` binary writes into a `LazyFS`
//! mount. `LazyFS` keeps its own page cache and writes a file's cached data
//! to the backing directory only when the application fsyncs it. The cut is
//! one command on its faults fifo, `clear-cache`: it drops every cached
//! write that was not fsynced, file by file, so what a reply acknowledged
//! must have been synced before the reply went out, and what was still only
//! in the cache is lost. After the cut, the server is killed, and the store
//! reopens from the backing directory, where [`check`] (shared with the
//! crash suite) checks the same invariants as after a SIGKILL.
//!
//! The model: the cut loses the writes that the application had not fsynced
//! when the cut came, so the suite tests sync-before-reply. It does not
//! model a drive's volatile cache, which can also lose writes that reached
//! the drive but were never flushed, and it does not reorder writes; the
//! dm-log-writes suite (`dm_log_writes.rs`) replays a block-level log to
//! each flush, which is what models that drive.
//!
//! SHORTCUT: the cut cannot catch a lost fsync of a directory. `LazyFS`
//! persists a rename at rename time, so the directory-entry durability
//! that `fsync_dir` guards (the `fsync` of the blob directory after the
//! rename) is not modelled; the mutation without it passes. The
//! dm-log-writes suite (`dm_log_writes.rs`) is that block-level replay:
//! it cuts at each flush and a lost directory fsync turns it red.
//!
//! SHORTCUT: the cut is sync-before-reply, and the sync path tested is the
//! Linux one; macOS syncs differently (`F_FULLFSYNC`) and its CI job cannot
//! run `LazyFS`, so there the crash suite is what runs. Upgrade to a model
//! that replays writes to each FLUSH point (dm-log-writes) when a report
//! says that acknowledged writes were lost on hardware with a volatile
//! cache.
//!
//! The suite needs a `LazyFS` build, pinned to release 0.3.1, commit
//! `045a0b3a1126725e693934e29d3ba15e08cc39ec` (the CI job clones and builds
//! it, and caches the binary). `SCION_LAZYFS` names the `LazyFS` checkout;
//! without it the suite skips with a message, and with
//! `SCION_POWER_REQUIRE=1` set (its CI job does) a missing build fails, not
//! skips. As in the crash suite, `SCION_POWER_SEED=<n>` replays a run and
//! `SCION_POWER_ROUNDS=<n>` sets the number of cuts (default 4).
#![cfg(all(unix, target_os = "linux"))]

mod common;

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use common::*;

/// The `LazyFS` checkout the suite runs against: it must hold
/// `lazyfs/build/lazyfs` and `libs/libpcache/build`.
fn lazyfs_dir() -> Option<PathBuf> {
    let dir = std::env::var_os("SCION_LAZYFS")?;
    let dir = PathBuf::from(dir);
    let binary = dir.join("lazyfs/build/lazyfs");
    assert!(
        binary.is_file(),
        "SCION_LAZYFS={} holds no lazyfs/build/lazyfs",
        dir.display()
    );
    Some(dir)
}

/// Whether the test may run: only with `SCION_LAZYFS` set, and only when
/// `SCION_POWER_REQUIRE=1` asks for it, so plain `cargo test --workspace`
/// skips it with a message. A run whose `LazyFS` is missing then fails, not
/// skips, so the CI job cannot pass by skipping.
fn may_run() -> bool {
    if std::env::var("SCION_POWER_REQUIRE").as_deref() != Ok("1") {
        println!("skipping the power-loss suite: set SCION_POWER_REQUIRE=1 to run it");
        return false;
    }
    assert!(
        lazyfs_dir().is_some(),
        "SCION_LAZYFS is not set (it must name the pinned `LazyFS` checkout)"
    );
    true
}

/// One `LazyFS` mount over a fresh backing directory, and the server's data
/// directory in it.
struct Lazyfs {
    child: Child,
    fifo: PathBuf,
    completed: fs::File,
    mount: PathBuf,
    backing: PathBuf,
}

impl Lazyfs {
    /// Mounts a fresh `LazyFS` over a fresh backing directory. `lazyfs` is the
    /// pinned checkout; `round` keeps the paths and the mapper names apart.
    fn start(dir: &Path, lazyfs: &Path, round: usize) -> Self {
        let round_dir = dir.join(format!("r{round}"));
        fs::create_dir_all(&round_dir).unwrap();
        let backing = round_dir.join("backing");
        let mount = round_dir.join("mnt");
        fs::create_dir(&backing).unwrap();
        fs::create_dir(&mount).unwrap();
        let fifo = round_dir.join("faults.fifo");
        let completed_path = round_dir.join("faults.fifo.completed");
        let config = round_dir.join("lazyfs.toml");
        // The ops trace lives in the round tempdir, which goes away with
        // the test; a CI job that wants it after a failure names a fixed
        // directory to upload from. `LazyFS` does not create the folder of
        // its logfile, so since #181 every upload found no files at all:
        // the test creates it.
        let trace = match std::env::var_os("SCION_LAZYFS_TRACE_DIR") {
            Some(dir) => {
                let trace = PathBuf::from(dir).join(format!("lazyfs-r{round}.log"));
                if let Some(dir) = trace.parent() {
                    fs::create_dir_all(dir).unwrap();
                }
                trace
            }
            None => round_dir.join("lazyfs.log"),
        };
        fs::write(
            &config,
            format!(
                "[faults]\nfifo_path=\"{}\"\nfifo_path_completed=\"{}\"\n\
                 [cache]\napply_eviction=false\n[cache.simple]\n\
                 custom_size=\"256mb\"\nblocks_per_page=1\n\
                 [filesystem]\nlog_all_operations=true\nlogfile=\"{}\"\n",
                fifo.display(),
                completed_path.display(),
                trace.display(),
            ),
        )
        .unwrap();
        let script = lazyfs.join("lazyfs/scripts/mount-lazyfs.sh");
        let library = lazyfs.join("libs/libpcache/build");
        // The script leaves the daemon running in the background, and it
        // looks for the binary relative to the `LazyFS` directory.
        let child = Command::new(script)
            .current_dir(lazyfs.join("lazyfs"))
            .arg("-c")
            .arg(&config)
            .arg("-m")
            .arg(&mount)
            .arg("-r")
            .arg(&backing)
            .arg("-s")
            .env("LD_LIBRARY_PATH", library)
            .stdout(fs::File::create(format!("/tmp/mount-{round}.log")).unwrap())
            .stderr({
                let mut o = fs::OpenOptions::new();
                o.append(true);
                o.open(format!("/tmp/mount-{round}.log")).unwrap()
            })
            .spawn()
            .unwrap();
        wait_mounted(&mount);
        // The daemon keeps the completed fifo's write end open forever, so
        // the file never sees an EOF: the cut below reads one message, not
        // to the end.
        let completed = fs::File::open(&completed_path).unwrap();
        Self {
            child,
            fifo,
            completed,
            mount,
            backing,
        }
    }

    /// The cut: drops every cached write that was not fsynced. The command
    /// runs through the faults fifo, and the completed fifo answers when the
    /// cache is cleared.
    fn cut(&mut self) {
        let mut fifo = fs::OpenOptions::new().write(true).open(&self.fifo).unwrap();
        fifo.write_all(b"lazyfs::clear-cache\n").unwrap();
        let mut done = [0u8; 64];
        let n = self.completed.read(&mut done).unwrap();
        let done = String::from_utf8_lossy(&done[..n]).into_owned();
        assert!(done.contains("finished::clear-cache"), "{done:?}");
    }
}

impl Drop for Lazyfs {
    fn drop(&mut self) {
        // Best effort: the daemon dies with the mount, and the round's
        // directories go away with the test's temp directory.
        let _ = Command::new("fusermount3")
            .arg("-u")
            .arg(&self.mount)
            .output();
        let _ = Command::new("umount").arg(&self.mount).output();
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// Waits until the mount appears, for at most 60 s: `LazyFS` pre-allocates its
/// cache before it serves, which takes a few seconds.
fn wait_mounted(mount: &Path) {
    let want = mount.to_str().unwrap().to_owned();
    let start = std::time::Instant::now();
    loop {
        if fs::read_to_string("/proc/mounts").unwrap().contains(&want) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "`LazyFS` did not mount {}",
            mount.display()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `SCION_POWER_SEED`, or a new seed. Printed, so a failed run can be run
/// again with it.
fn seed() -> u64 {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let seed = env(
        "SCION_POWER_SEED",
        now.as_secs() ^ u64::from(now.subsec_nanos()),
    );
    println!("seed {seed}; replay with SCION_POWER_SEED={seed}");
    seed
}

/// The real server, several clients, and a cut at three kinds of moment, as
/// in the crash suite: at a random moment, in the middle of a large upload,
/// and while a land is pending. Every reply the clients got must survive:
/// the cut takes what was never fsynced, so a missing sync before a reply
/// loses that reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reply_survives_a_power_cut() {
    if !may_run() {
        return;
    }
    let lazyfs = lazyfs_dir().unwrap();
    let seed = seed();
    let mut rng = Rng::new(seed);
    let rounds = usize::try_from(env("SCION_POWER_ROUNDS", 4)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("server.log");
    let mut cut_lands = 0;
    let mut total = 0;
    for round in 0..rounds {
        // Every round is its own experiment on a fresh repo, so the ledger
        // and the checks start over with it.
        let mut ledger = Ledger::default();
        let mut checked = Checked::default();
        let mut lazy = Lazyfs::start(dir.path(), &lazyfs, round);
        let data = lazy.mount.join("data");
        eprintln!("round {round}: mounted");
        create_repo(&data);
        eprintln!("round {round}: repo created");
        let server = Server::start(&data, &log);
        eprintln!("round {round}: server up");
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        eprintln!("round {round}: check 1 done");
        let signals = Arc::new(Signals::default());
        let clients: Vec<_> = (0..CLIENTS)
            .map(|c| {
                let name = format!("p{round}c{c}");
                let rng = Rng::new(rng.next());
                tokio::spawn(client(server.url.clone(), name, rng, Arc::clone(&signals)))
            })
            .collect();
        match rng.below(3) {
            0 => {
                // The `LazyFS` cache is slower than the crash suite's tmpfs,
                // so the cut waits longer before it takes a quiet moment.
                let run = 500 + rng.below(2000);
                tokio::time::sleep(Duration::from_millis(u64::try_from(run).unwrap())).await;
            }
            1 => {
                wait_until(|| signals.large_uploads.load(Ordering::SeqCst) > 0).await;
                let delay = u64::try_from(rng.below(30)).unwrap();
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            _ => {
                let db = rusqlite::Connection::open(data.join("scion.db")).unwrap();
                wait_until(|| pending_lands(&db) > 0).await;
            }
        }
        eprintln!("round {round}: clients spawning");
        signals.killed.store(true, Ordering::SeqCst);
        eprintln!("round {round}: cutting");
        lazy.cut();
        eprintln!("round {round}: cut done");
        drop(server);
        for client in clients {
            let mut got = client.await.unwrap();
            ledger.blobs.append(&mut got.blobs);
            cut_lands += got.lands.iter().filter(|sent| sent.reply.is_none()).count();
            ledger.lands.append(&mut got.lands);
        }
        total += ledger.lands.len();
        // The store reopens from the backing directory: what `LazyFS`
        // persisted, and nothing else.
        let data = lazy.backing.join("data");
        let server = Server::start(&data, &log);
        eprintln!("round {round}: restart up");
        check(&server.url, &data, &mut ledger, &mut checked, &mut rng).await;
        eprintln!("round {round}: check 2 done");
        drop(server);
        drop(lazy);
    }
    println!("{rounds} cuts; {total} lands, {cut_lands} of them cut mid-call");
    assert!(total > 0, "the clients did little work");
}

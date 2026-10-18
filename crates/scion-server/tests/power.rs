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
//! the drive but were never flushed, and it does not reorder writes.
//!
//! SHORTCUT: the cut cannot catch a lost fsync of a directory. `LazyFS`
//! persists a rename at rename time, so the directory-entry durability
//! that `fsync_dir` guards (the `fsync` of the blob directory after the
//! rename) is not modelled; the mutation without it passes. Upgrade to a
//! block-level replay that drops un-flushed writes in order
//! (dm-log-writes replay to each FLUSH), when a bug report or a change to
//! the rename path says a directory entry was lost.
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
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use common::*;
use scion_core::BlobId;
use scion_server::Store;
use scion_server::store::BlobStatus;

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
        // its logfile, so the test does.
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
    /// cache is cleared. The answer is waited for behind a ceiling, so a cut
    /// that never completes fails in seconds and names its step.
    fn cut(&mut self) {
        let mut fifo = fs::OpenOptions::new().write(true).open(&self.fifo).unwrap();
        fifo.write_all(b"lazyfs::clear-cache\n").unwrap();
        let completed = self.completed.try_clone().unwrap();
        let done = std::thread::spawn(move || {
            let mut done = [0u8; 64];
            let n = completed.read(&mut done).unwrap();
            String::from_utf8_lossy(&done[..n]).into_owned()
        });
        let done = wait_for("clear-cache to finish", 15, move || {
            done.is_finished().then(|| done.join().unwrap())
        });
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
        let mut server = Server::start(&data, &log);
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
        // The freeze: no reply can follow it, so no reply can follow the
        // cut either ([#185]).
        server.freeze();
        eprintln!("round {round}: cutting");
        lazy.cut();
        eprintln!("round {round}: cut done");
        drop(server);
        for client in clients {
            let mut got = tokio::time::timeout(Duration::from_secs(60), client)
                .await
                .unwrap_or_else(|_| panic!("a client never finished after the kill"))
                .unwrap();
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

const RACERS: usize = 16;

/// Waits for one step of the cut sequence to finish, for at most `secs`
/// seconds, and fails naming the step: a hang must say where it hangs.
fn wait_for<T>(step: &str, secs: u64, mut ready: impl FnMut() -> Option<T>) -> T {
    let give_up = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(done) = ready() {
            return done;
        }
        assert!(
            Instant::now() < give_up,
            "the cut sequence hung at {step} for over {secs} s"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// How many waves of racing puts one round of the W1 test runs.
const WAVES: usize = 4;

/// The W1 test's evidence: one line per event, to stderr and to a file in
/// the trace directory (which CI uploads on a failure), flushed per line,
/// so a panic cannot lose what came before it.
#[derive(Clone)]
struct EventLog {
    file: Arc<Mutex<Option<fs::File>>>,
}

impl EventLog {
    /// Opens `name.log` in the trace directory, if the run named one.
    fn new(name: &str) -> Self {
        let file = std::env::var_os("SCION_LAZYFS_TRACE_DIR").map(|dir| {
            let dir = PathBuf::from(dir);
            fs::create_dir_all(&dir).unwrap();
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(format!("{name}.log")))
                .unwrap();
            Mutex::new(Some(file))
        });
        Self {
            file: Arc::new(file.unwrap_or_else(|| Mutex::new(None))),
        }
    }

    /// One event, now to stderr and now to the file.
    fn line(&self, line: &str) {
        eprintln!("{line}");
        if let Ok(mut slot) = self.file.lock()
            && let Some(file) = slot.as_mut()
        {
            let _ = writeln!(file, "{line}");
            let _ = file.flush();
        }
    }
}

/// One wave of the W1 test: `RACERS` clients put the same bytes at once,
/// started together by a barrier, so two of them always race one blob ID.
/// Every reply must name that blob.
async fn race_wave(
    log: &EventLog,
    round: usize,
    wave: usize,
    url: String,
    content: Vec<u8>,
) -> (Vec<BlobId>, usize) {
    let barrier = Arc::new(tokio::sync::Barrier::new(RACERS));
    let racers: Vec<_> = (0..RACERS)
        .map(|i| {
            let (log, url, content, barrier) = (
                log.clone(),
                url.clone(),
                content.clone(),
                Arc::clone(&barrier),
            );
            tokio::spawn(async move {
                let (mut scion, _objects) = connect(&url).await.unwrap();
                barrier.wait().await;
                match put_blob(&mut scion, &content).await {
                    Ok(id) => {
                        log.line(&format!("round {round} wave {wave} racer {i}: acked {id}"));
                        Ok((id, content))
                    }
                    Err(status) => {
                        // The freeze ends the wave, so a racer still in
                        // flight loses its call. No reply, no guarantee.
                        log.line(&format!(
                            "round {round} wave {wave} racer {i}: put failed {status:?}"
                        ));
                        Err(status)
                    }
                }
            })
        })
        .collect();
    let mut ids = Vec::new();
    let mut failed = 0;
    for racer in racers {
        let (id, content) = match racer.await.unwrap() {
            Ok(put) => put,
            Err(status) => {
                failed += 1;
                log.line(&format!(
                    "round {round} wave {wave}: no reply, {failed} racers cut: {status:?}"
                ));
                continue;
            }
        };
        log.line(&format!(
            "round {round} wave {wave}: collected {id}, want {}",
            BlobId::of(&content)
        ));
        assert_eq!(id, BlobId::of(&content));
        ids.push(id);
    }
    (ids, failed)
}

/// Issue #185, window W1. Many clients put the SAME blob at once, wave
/// after wave: both racers of a blob ID see it missing, both write and
/// fsync a temp file, and both rename onto the same path (`Store::replace`).
/// Under `LazyFS` the loser's rename may free the winner's cached item, and
/// a later cut loses the winner's acknowledged write. The test makes the
/// race certain, cuts once after the acked waves and once while a wave is
/// in flight, and then reopens the store from the backing directory: every
/// acknowledged blob must be there and intact, and no blob file may be
/// corrupt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_puts_of_one_blob_survive_a_cut() {
    if !may_run() {
        return;
    }
    let lazyfs = lazyfs_dir().unwrap();
    let seed = seed();
    let mut rng = Rng::new(seed);
    let rounds = usize::try_from(env("SCION_RACE_ROUNDS", 8)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    // The server's log goes where CI can upload it: next to the LazyFS
    // trace, when the run named that directory.
    let server_log = std::env::var_os("SCION_LAZYFS_TRACE_DIR").map_or_else(
        || dir.path().join("race-server.log"),
        |dir| {
            let dir = PathBuf::from(dir);
            fs::create_dir_all(&dir).unwrap();
            dir.join("race-server.log")
        },
    );
    let events = EventLog::new("race-w1");
    let mut total = 0;
    // Rounds from 100 up: the trace file and the mount log of a round are
    // named after it, and must not collide with the other tests' rounds.
    for round in 100..100 + rounds {
        events.line(&format!("round {round}: mounts"));
        let mut lazy = Lazyfs::start(dir.path(), &lazyfs, round);
        let data = lazy.mount.join("data");
        create_repo(&data);
        let mut server = Server::start(&data, &server_log);
        events.line(&format!("round {round}: server up"));
        // Acked waves: every put is answered before the cut, so every one
        // of them must survive it.
        let mut acked = Vec::new();
        for wave in 0..WAVES - 1 {
            let content = rng.bytes(3 << 10);
            let want = BlobId::of(&content);
            events.line(&format!(
                "round {round} wave {wave}: starts, one blob {want}"
            ));
            let (ids, failed) = race_wave(&events, round, wave, server.url.clone(), content).await;
            assert_eq!(failed, 0, "a put failed before the freeze");
            assert!(ids.windows(2).all(|w| w[0] == w[1]), "one blob ID");
            acked.extend(ids);
        }
        events.line(&format!("round {round}: cut after the acked waves"));
        server.freeze();
        lazy.cut();
        server.resume();
        // A wave cut in flight: the freeze ends it, so no racer of the
        // wave gets a reply past the cut. Whatever the acked waves put
        // must be whole, and so must every file of the store.
        let wave = WAVES - 1;
        let content = rng.bytes(3 << 10);
        let want = BlobId::of(&content);
        events.line(&format!(
            "round {round} wave {wave}: starts in flight, one blob {want}"
        ));
        let started = Instant::now();
        let flight = race_wave(&events, round, wave, server.url.clone(), content);
        let delay = u64::try_from(rng.below(40)).unwrap() + 4;
        tokio::time::sleep(Duration::from_millis(delay)).await;
        events.line(&format!(
            "round {round}: cut mid-wave at {} ms",
            started.elapsed().as_millis()
        ));
        server.freeze();
        lazy.cut();
        // The kill comes before the join: the racers of the wave hang on
        // the frozen server, and only the kill lets their calls fail.
        drop(server);
        let (ids, cut) = tokio::time::timeout(Duration::from_secs(30), flight)
            .await
            .unwrap_or_else(|_| panic!("the cut wave's racers never finished"));
        events.line(&format!(
            "round {round} wave {wave}: {} acked before the freeze, {cut} cut",
            ids.len()
        ));
        acked.extend(ids);
        // The store reopens from the backing directory: what `LazyFS`
        // persisted, and nothing else.
        let data = lazy.backing.join("data");
        let store = Store::open_shared(&data).unwrap();
        events.line(&format!("round {round}: reopens, {} acked", acked.len()));
        for id in &acked {
            let status = store.blobs().verify(*id).unwrap();
            events.line(&format!("round {round}: verify acked {id}: {status:?}"));
            assert_eq!(status, BlobStatus::Valid, "acknowledged blob {id}");
        }
        for id in blob_files(&data) {
            let status = store.blobs().verify(id).unwrap();
            events.line(&format!("round {round}: verify file {id}: {status:?}"));
            assert_eq!(status, BlobStatus::Valid, "stored blob file {id}");
        }
        drop(store);
        drop(lazy);
        total += RACERS * WAVES;
    }
    println!("{total} racing puts across {rounds} rounds");
}

/// Every file under `dir`, with its bytes: what the W2 test compares the
/// backing store against.
fn snapshot(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files: Vec<_> = walk(dir)
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    files.sort();
    files
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Issue #185, window W2. The checker writes directly into the backing
/// directory while the `LazyFS` daemon still runs, as `check` does. If the
/// daemon's exit writes its cache back over those direct writes, the
/// checker's own view of the store is not what the drive holds. The test
/// forces the order: the server writes through the mount and is killed,
/// the checker opens the store directly on the backing and snapshots every
/// byte, the mount goes away, and every byte must still be what the
/// checker saw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checker_writes_outlive_the_lazyfs_exit() {
    if !may_run() {
        return;
    }
    let lazyfs = lazyfs_dir().unwrap();
    let seed = seed();
    let mut rng = Rng::new(seed);
    let rounds = usize::try_from(env("SCION_CHECKER_ROUNDS", 4)).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("checker.log");
    for round in 200..200 + rounds {
        let mut lazy = Lazyfs::start(dir.path(), &lazyfs, round);
        let data = lazy.mount.join("data");
        create_repo(&data);
        let mut server = Server::start(&data, &log);
        // The server writes through the mount: blobs, acknowledged.
        let (mut scion, _objects) = connect(&server.url).await.unwrap();
        let mut acked = Vec::new();
        for _ in 0..4 {
            let content = rng.bytes(1 << 10);
            let id = put_blob(&mut scion, &content).await.unwrap();
            acked.push((id, content));
        }
        // The cut, and writes the server fsyncs after it: their pages sit
        // in the daemon's cache, clean, when the server dies.
        server.freeze();
        lazy.cut();
        server.resume();
        for _ in 0..3 {
            let content = rng.bytes(1 << 10);
            let id = put_blob(&mut scion, &content).await.unwrap();
            acked.push((id, content));
        }
        drop(scion);
        drop(server);
        // The checker writes directly into the backing, as `check` does: it
        // opens the store, reads the metadata, and verifies every blob. Its
        // close checkpoints the WAL, so the snapshot of what it wrote comes
        // after the close: from there on, only the daemon's exit can change
        // a byte.
        let backing = lazy.backing.join("data");
        let store = Store::open_shared(&backing).unwrap();
        for (id, _) in &acked {
            let status = wait_for("the checker's verify", 30, || {
                Some(store.blobs().verify(*id).unwrap())
            });
            assert_eq!(status, BlobStatus::Valid, "acknowledged blob {id}");
        }
        drop(store);
        let before = snapshot(&backing);
        // The mount goes away: the daemon's exit. Its cache must not
        // rewrite what the checker wrote and saw.
        drop(lazy);
        let after = snapshot(&backing);
        if before != after {
            // Name the files that changed, with their sizes: the trace
            // says what the daemon wrote and when.
            let changed: Vec<String> = before
                .iter()
                .zip(&after)
                .filter(|(b, a)| b != a)
                .map(|((path, before), (_, after))| {
                    format!(
                        "{}: {} bytes before, {} after",
                        path.display(),
                        before.len(),
                        after.len()
                    )
                })
                .collect();
            panic!("the backing changed while the mount went away: {changed:?}");
        }
        // What the checker verified still verifies.
        let store = Store::open_shared(&backing).unwrap();
        for (id, _) in &acked {
            let status = store.blobs().verify(*id).unwrap();
            assert_eq!(status, BlobStatus::Valid, "blob {id} after the exit");
        }
        drop(store);
    }
}

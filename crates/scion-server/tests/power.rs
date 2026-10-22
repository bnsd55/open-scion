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
//! What the suite proves is sync-before-reply: a reply's data was fsynced
//! before the reply went out, so a cut can lose nothing that a reply
//! promised. It is not a full proof of power-loss survival: it does not
//! model a drive's volatile cache, which can also lose writes that reached
//! the drive but were never flushed, and it does not reorder writes; the
//! dm-log-writes suite (`dm_log_writes.rs`) replays a block-level log to
//! each barrier - a flush or a FUA write - which is what models that
//! drive.
//!
//! SHORTCUT: the cut cannot catch a lost fsync of a directory. `LazyFS`
//! persists a rename at rename time, so the directory-entry durability
//! that `fsync_dir` guards (the `fsync` of the blob directory after the
//! rename) is not modelled; the mutation without it passes. The
//! dm-log-writes suite (`dm_log_writes.rs`) is that block-level replay:
//! it cuts at each barrier, and on ext4 the directory fsync and the full
//! flush back each other up, so there a mutation of both turns it red.
//!
//! SHORTCUT: the cut is sync-before-reply, and the sync path tested is the
//! Linux one; macOS syncs differently (`F_FULLFSYNC`) and its CI job cannot
//! run `LazyFS`, so there the crash suite is what runs. No macOS tool cuts
//! power or drops flushes at a barrier today, and the Linux replay
//! (`dm-log-writes`) cannot run there; a harness that replays a macOS run
//! to each `F_FULLFSYNC`, or a `LazyFS` port to the Mac, would close it.
//! Write one when a report says acknowledged writes were lost on a Mac.
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
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use common::*;
use scion_core::BlobId;
use scion_server::Backend;
use scion_server::test_support::store::BlobStatus;
use tonic::Status;

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
    trace: PathBuf,
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
            trace,
        }
    }

    /// The cut: drops every cached write that was not fsynced. The command
    /// runs through the faults fifo, and the completed fifo answers when the
    /// cache is cleared. The answer is waited for behind a ceiling, so a cut
    /// that never completes fails in seconds and names its step.
    fn cut(&mut self) {
        // The open runs on its own thread behind the ceiling: a write
        // open of a fifo blocks until a reader exists, and if `LazyFS`
        // died there is none.
        let fifo_path = self.fifo.clone();
        let opening = std::thread::spawn(move || {
            let mut fifo = fs::OpenOptions::new().write(true).open(fifo_path)?;
            let _ = fifo.write_all(b"lazyfs::clear-cache\n");
            Ok::<_, std::io::Error>(fifo)
        });
        let mut opening = Some(opening);
        let _fifo = wait_for("the faults fifo to open", 15, || {
            let finished = opening.as_ref().unwrap().is_finished();
            finished.then(|| opening.take().unwrap().join().unwrap())
        });
        let mut completed = self.completed.try_clone().unwrap();
        let answer = std::thread::spawn(move || {
            let mut done = [0u8; 64];
            let n = completed.read(&mut done).unwrap();
            String::from_utf8_lossy(&done[..n]).into_owned()
        });
        let mut answer = Some(answer);
        let done = wait_for("clear-cache to finish", 15, || {
            let finished = answer.as_ref().unwrap().is_finished();
            finished.then(|| answer.take().unwrap().join().unwrap())
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

/// The lands pending in this directory's metadata, whatever backend
/// holds it: a SQLite file beside the blobs, or the schema the test
/// database names for the directory. The read runs on threads of its
/// own, because the sync client blocks on its connection's runtime and
/// the test's async worker already hosts one.
fn pending(data: &std::path::Path) -> i64 {
    std::thread::scope(|s| {
        s.spawn(|| common::LandsReader::open(data).counts().1)
            .join()
            .unwrap()
    })
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
    // Ten cuts, same margin as the crash suite: at the conservative
    // rate of about half the cuts meeting a land pending in the
    // database, the chance all ten miss is about 0.5^10, just under
    // 0.1%.
    let rounds = usize::try_from(env("SCION_POWER_ROUNDS", 10)).unwrap();
    let mut pending_kills = 0usize;
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
        // A reaped child's flock can outlive the waitpid by one tick, and
        // the restart's open raced it (#260): wait for the lock to be free
        // before the new server opens.
        common::spawn_guard::across_release(|| {
            common::spawn_guard::wait_for_lock_release(&data);
        });
        let mut server = Server::start(&data, &log);
        eprintln!("round {round}: server up");
        check(
            &server.url,
            &data,
            &data,
            &mut ledger,
            &mut checked,
            &mut rng,
        )
        .await;
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
                wait_until("a large upload is in flight", || {
                    signals.large_uploads.load(Ordering::SeqCst) > 0
                })
                .await;
                let delay = u64::try_from(rng.below(30)).unwrap();
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            _ => {
                wait_until_bounded("a land is pending in the database", 5, || {
                    pending(&data) > 0
                })
                .await;
            }
        }
        let pending_at_kill = pending(&data) > 0;
        if pending_at_kill {
            pending_kills += 1;
        }
        eprintln!("round {round}: clients spawning (pending at kill: {pending_at_kill})");
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
            ledger.oplog.merge(got.oplog);
            cut_lands += got.lands.iter().filter(|sent| sent.reply.is_none()).count();
            ledger.lands.append(&mut got.lands);
        }
        total += ledger.lands.len();
        // The store reopens from the backing directory: what `LazyFS`
        // persisted, and nothing else. The server serves the schema the
        // first open named: the metadata lives in the database of the
        // original path, and the backing path would name a fresh, empty
        // schema, whose open adopts the marker and serves nothing. The
        // schema follows the store, not the path.
        let data = lazy.backing.join("data");
        let server = Server::start_reopened(&data, &lazy.mount.join("data"), &log);
        eprintln!("round {round}: restart up");
        check(
            &server.url,
            &data,
            &lazy.mount.join("data"),
            &mut ledger,
            &mut checked,
            &mut rng,
        )
        .await;
        eprintln!("round {round}: check 2 done");
        drop(server);
        drop(lazy);
    }
    println!("{rounds} cuts; {total} lands, {cut_lands} of them cut mid-call");
    assert!(total > 0, "the clients did little work");
    assert!(
        pending_kills > 0,
        "no cut met a land pending in the database: {pending_kills} of {rounds} cuts; \
         the wide lands must keep pending common"
    );
    println!("{pending_kills} of {rounds} cuts met a land pending in the database");
}

/// The audit proves sync-before-reply on its own: a write whose reply
/// went out must have its intent and result lines in the backing file
/// after a cut, because the audit fsyncs each line before the handler
/// answers. If an append skipped its fsync, `LazyFS` would drop the
/// line at the cut and this test would find the reply acknowledged
/// with no evidence behind it.
#[tokio::test(flavor = "multi_thread")]
async fn an_acknowledged_write_s_audit_lines_survive_a_power_cut() {
    if !may_run() {
        return;
    }
    let lazyfs = lazyfs_dir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("server.log");
    let lazy = Lazyfs::start(dir.path(), &lazyfs, 0);
    let data = lazy.mount.join("data");
    create_repo(&data);
    common::spawn_guard::across_release(|| {
        common::spawn_guard::wait_for_lock_release(&data);
    });
    let server = Server::start(&data, &log);

    // One upload with a request ID, through the real binary.
    let request_id = "cut-1";
    put_blob_marked(&server.url, request_id, b"audit survives the cut\n").await;

    // The cut drops every write that `LazyFS` had not seen an fsync
    // for. The reply is out, so the audit's lines cannot be among them.
    let mut lazy = lazy;
    lazy.cut();
    drop(server);
    let audit = lazy.backing.join("data").join("audit.log");
    let bytes = fs::read(&audit).unwrap_or_else(|why| {
        panic!("the backing audit log is gone: {why}");
    });
    let text = String::from_utf8(bytes).unwrap();
    drop(lazy);
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let intent = lines
        .iter()
        .find(|line| {
            line["event"] == "write"
                && line["outcome"] == "intent"
                && line["request_id"] == request_id
        })
        .expect("the acknowledged write's intent line survived the cut");
    assert_eq!(intent["rpc"], "PutBlob");
    assert_eq!(intent["bytes"], 23, "the intent names its bytes");
    let result = lines
        .iter()
        .find(|line| {
            line["event"] == "write" && line["outcome"] == "ok" && line["request_id"] == request_id
        })
        .expect("the acknowledged write's result line survived the cut");
    assert_eq!(result["bytes"], 23);
}

/// One upload through the real server, marked with a request ID.
async fn put_blob_marked(url: &str, request_id: &str, data: &[u8]) {
    use scion_proto::v1::PutBlobRequest;
    use scion_proto::v1::scion_client::ScionClient;
    use tonic::Request;
    use tonic::service::interceptor::Interceptor;
    use tonic::transport::Channel;

    /// The bearer token of the default principal, and the request ID
    /// the audit's pair joins by.
    struct Mark(Option<tonic::metadata::AsciiMetadataValue>, String);

    impl Interceptor for Mark {
        fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
            request
                .metadata_mut()
                .insert("authorization", self.0.clone().unwrap());
            let id = self.1.parse().unwrap();
            request.metadata_mut().insert("x-request-id", id);
            Ok(request)
        }
    }

    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let bearer: tonic::metadata::AsciiMetadataValue =
        format!("Bearer {}", common::TOKEN).parse().unwrap();
    let mut scion =
        ScionClient::with_interceptor(channel, Mark(Some(bearer), request_id.to_owned()));
    scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: common::REPO.into(),
            data: data.to_vec(),
        }))
        .await
        .expect("the upload is allowed");
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
/// One racer of a wave: bounded connect, the barrier whatever the
/// connect did, and one put whose counters the wave's test reads.
#[allow(clippy::too_many_arguments)]
async fn racer(
    log: EventLog,
    round: usize,
    wave: usize,
    i: usize,
    url: String,
    content: Vec<u8>,
    barrier: Arc<tokio::sync::Barrier>,
    crossed: Arc<AtomicUsize>,
    replied: Arc<AtomicUsize>,
) -> Result<(BlobId, Vec<u8>), Status> {
    // The connect is bounded, and every racer reaches the barrier
    // whatever it did: one failed connect must not hold the other
    // fifteen at the barrier to the ceiling.
    let connected = tokio::time::timeout(Duration::from_secs(30), connect(&url)).await;
    let put = match connected {
        Ok(Ok((mut scion, _objects))) => {
            barrier.wait().await;
            crossed.fetch_add(1, Ordering::SeqCst);
            let put = put_blob(&mut scion, &content).await;
            if put.is_ok() {
                replied.fetch_add(1, Ordering::SeqCst);
            }
            put
        }
        Ok(Err(error)) => {
            barrier.wait().await;
            Err(Status::unknown(format!("connect failed: {error}")))
        }
        Err(_) => {
            barrier.wait().await;
            Err(Status::deadline_exceeded("the connect hung"))
        }
    };
    match put {
        Ok(id) => {
            log.line(&format!("round {round} wave {wave} racer {i}: acked {id}"));
            Ok((id, content))
        }
        Err(status) => {
            // The freeze ends the wave, so a racer still in flight loses
            // its call. No reply, no guarantee.
            log.line(&format!(
                "round {round} wave {wave} racer {i}: put failed {status:?}"
            ));
            Err(status)
        }
    }
}

async fn race_wave(
    log: EventLog,
    round: usize,
    wave: usize,
    url: String,
    content: Vec<u8>,
    // Counters the test reads while the wave runs: `crossed` rises when a
    // racer passed the barrier, `replied` when a racer's put came back.
    // At the freeze, crossed > replied means a put was really in flight,
    // and the cut interrupts it.
    crossed: Arc<AtomicUsize>,
    replied: Arc<AtomicUsize>,
) -> Vec<BlobId> {
    let barrier = Arc::new(tokio::sync::Barrier::new(RACERS));
    let racers: Vec<_> = (0..RACERS)
        .map(|i| {
            tokio::spawn(racer(
                log.clone(),
                round,
                wave,
                i,
                url.clone(),
                content.clone(),
                Arc::clone(&barrier),
                Arc::clone(&crossed),
                Arc::clone(&replied),
            ))
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
    log.line(&format!(
        "round {round} wave {wave}: {failed} racers cut, {} crossed",
        crossed.load(Ordering::SeqCst)
    ));
    ids
}

/// Runs the waves whose every put must be acknowledged, and returns their
/// ids: each wave races `RACERS` clients on one fresh blob, and every
/// racer gets a reply before the next wave starts.
async fn run_acked_waves(events: &EventLog, round: usize, url: &str, rng: &mut Rng) -> Vec<BlobId> {
    let mut acked = Vec::new();
    for wave in 0..WAVES - 1 {
        let content = rng.bytes(3 << 10);
        let want = BlobId::of(&content);
        events.line(&format!(
            "round {round} wave {wave}: starts, one blob {want}"
        ));
        let ids = tokio::time::timeout(
            Duration::from_secs(60),
            race_wave(
                events.clone(),
                round,
                wave,
                url.to_owned(),
                content,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("the acked wave never finished"));
        assert!(ids.windows(2).all(|w| w[0] == w[1]), "one blob ID");
        acked.extend(ids);
    }
    acked
}

/// Says every acknowledged blob and every blob file of the store verifies.
fn verify_whole(store: &Backend, acked: &[BlobId], events: &EventLog, round: usize, data: &Path) {
    events.line(&format!("round {round}: reopens, {} acked", acked.len()));
    for id in acked {
        let status = store.blobs().verify(*id).unwrap();
        events.line(&format!("round {round}: verify acked {id}: {status:?}"));
        assert_eq!(status, BlobStatus::Valid, "acknowledged blob {id}");
    }
    for id in blob_files(data) {
        let status = store.blobs().verify(id).unwrap();
        events.line(&format!("round {round}: verify file {id}: {status:?}"));
        assert_eq!(status, BlobStatus::Valid, "stored blob file {id}");
    }
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
    if Backend::test_is_postgres() {
        // The park that makes the race certain is a SQLite hook: it parks
        // the store's writer, and the Postgres store has no park. The
        // blob path of the race is the data directory's, and the cut rows
        // above prove it; this row stays SQLite-proven.
        eprintln!("skip on postgres: the write park is a SQLite hook");
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
        // A reaped child's flock can outlive the waitpid by one tick, and
        // the next round's server must not race it (#260).
        common::spawn_guard::across_release(|| {
            common::spawn_guard::wait_for_lock_release(&data);
        });
        let mut server = Server::start(&data, &server_log);
        events.line(&format!("round {round}: server up"));
        // Acked waves: every put is answered before the cut, so every one
        // of them must survive it.
        let mut acked = run_acked_waves(&events, round, &server.url, &mut rng).await;
        // Cut one: after the acked waves. The server freezes, is cut, and
        // dies; a new one opens on the same mount, which is what a restart
        // after a real cut does.
        events.line(&format!("round {round}: cut after the acked waves"));
        server.freeze();
        lazy.cut();
        drop(server);
        let mut server = Server::start(&data, &server_log);
        events.line(&format!("round {round}: server up after the cut"));
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
        // The wave is spawned, not built and awaited later: the racers
        // run while the server lives, and the cut lands in their flight.
        // The counters are shared, so the test reads them at the freeze:
        // a racer past the barrier whose put has not replied yet is work
        // the cut really interrupts.
        let crossed = Arc::new(AtomicUsize::new(0));
        let replied = Arc::new(AtomicUsize::new(0));
        let flight = tokio::spawn(race_wave(
            events.clone(),
            round,
            wave,
            server.url.clone(),
            content,
            Arc::clone(&crossed),
            Arc::clone(&replied),
        ));
        // The wave runs until a put is really in flight: a racer past
        // the barrier whose put has not replied yet is work the cut
        // interrupts. The ceiling only bounds the wait of the good code;
        // if no put ever goes in flight, the test says so instead of
        // cutting nothing.
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let crossed_now = crossed.load(Ordering::SeqCst);
            let replied_now = replied.load(Ordering::SeqCst);
            if crossed_now > replied_now {
                break;
            }
            assert!(
                Instant::now() < until,
                "no put ever went in flight: {crossed_now} crossed, {replied_now} replied"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        events.line(&format!(
            "round {round}: cut mid-wave at {} ms, {} crossed, {} replied",
            started.elapsed().as_millis(),
            crossed.load(Ordering::SeqCst),
            replied.load(Ordering::SeqCst)
        ));
        server.freeze();
        lazy.cut();
        // The kill comes before the join: the racers of the wave hang on
        // the frozen server, and only the kill lets their calls fail.
        drop(server);
        let ids = tokio::time::timeout(Duration::from_secs(30), flight)
            .await
            .unwrap_or_else(|_| panic!("the cut wave's racers never finished"))
            .unwrap();
        events.line(&format!(
            "round {round} wave {wave}: {} acked before the freeze",
            ids.len()
        ));
        acked.extend(ids);
        // The store reopens from the backing directory: what `LazyFS`
        // persisted, and nothing else. The schema follows the store, not
        // the path: the metadata lives in the database the original
        // path's schema names.
        let data = lazy.backing.join("data");
        let store = open_store_shared_reopened(&data, &lazy.mount.join("data"));
        verify_whole(&store, &acked[..], &events, round, &data);
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
        // The server writes through the mount: blobs, acknowledged. The
        // waits run behind ceilings, so a hang names its step.
        let (mut scion, _objects) =
            tokio::time::timeout(Duration::from_secs(30), connect(&server.url))
                .await
                .unwrap_or_else(|_| panic!("the checker test's client never connected"))
                .unwrap();
        let mut acked = Vec::new();
        for _ in 0..4 {
            let content = rng.bytes(1 << 10);
            let id = tokio::time::timeout(Duration::from_secs(30), put_blob(&mut scion, &content))
                .await
                .unwrap_or_else(|_| panic!("a put of the checker test never finished"))
                .unwrap();
            acked.push((id, content));
        }
        // The cut ends the server: on real hardware, nothing of it runs
        // past the cut. The pages of its synced writes sit in the daemon's
        // cache, clean, when the process dies.
        server.freeze();
        lazy.cut();
        drop(scion);
        drop(server);
        // The checker writes directly into the backing, as `check` does: it
        // opens the store, reads the metadata, and verifies every blob. Its
        // close checkpoints the WAL, so the snapshot of what it wrote comes
        // after the close: from there on, only the daemon's exit can change
        // a byte.
        let backing = lazy.backing.join("data");
        // The schema follows the store, not the path: the metadata lives
        // in the database the original path's schema names, and the
        // backing path would name a fresh, empty schema.
        let store = open_store_shared_reopened(&backing, &data);
        for (id, _) in &acked {
            let status = store.blobs().verify(*id).unwrap();
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
        let store = open_store_shared_reopened(&backing, &data);
        for (id, _) in &acked {
            let status = store.blobs().verify(*id).unwrap();
            assert_eq!(status, BlobStatus::Valid, "blob {id} after the exit");
        }
        drop(store);
    }
}

/// Two processes rename onto the same blob path, deterministically: the
/// server's writer parks on the test seam after its keep found the blob
/// missing, the import writes and renames the same bytes, and the
/// release lets the server's rename land after the import's. The ops
/// trace names every rename, so the test asserts the order itself, then
/// freezes, cuts, kills and reopens from the backing: the raced blob
/// verifies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn import_process_races_the_server_on_the_same_blob() {
    if !may_run() {
        return;
    }
    if Backend::test_is_postgres() {
        // The park that holds the server's writer between keep and rename
        // is a SQLite hook, and the import beside it opens the data
        // directory's own database. The row stays SQLite-proven.
        eprintln!("skip on postgres: the write park is a SQLite hook");
        return;
    }
    let lazyfs = lazyfs_dir().unwrap();
    let seed = seed();
    let mut rng = Rng::new(seed);
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("import-race-server.log");
    let mut lazy = Lazyfs::start(dir.path(), &lazyfs, 300);
    let data = lazy.mount.join("data");
    create_repo(&data);
    let mut server = Server::start(&data, &log);
    let (mut scion, _objects) = tokio::time::timeout(Duration::from_secs(30), connect(&server.url))
        .await
        .unwrap_or_else(|_| panic!("the client never connected"))
        .unwrap();
    // One blob both processes write: the import commits it, the client
    // puts it, and the park holds the server's writer between keep and
    // rename. The writer holds the fan-out directory's flock while it
    // sits in the park, so the import's write of the same blob waits
    // for it: no rename of the blob lands while the park holds.
    let content = rng.bytes(2 << 10);
    let want = BlobId::of(&content);
    arm_park(&data, &want);

    // A git repository for the import process: one commit whose file
    // holds the same bytes the client puts.
    let git_dir = dir.path().join("git");
    one_file_repo(&git_dir, &content);
    // The put goes first: keep finds the blob missing, and the server's
    // writer parks between keep and rename. The seam's marker file says
    // the writer is inside: a stat of the park file can come from the
    // kernel's cache, so the trace cannot show the park itself.
    let mut put = tokio::spawn(async move { put_blob(&mut scion, &content).await });
    wait_parked(&data.join("hooks"), &want);
    // Only now the import runs. Its write of the blob waits for the
    // writer's flock, the way any other process's write would.
    let import = common::spawn_guard::across_spawn(|| {
        Command::new(env!("CARGO_BIN_EXE_scion-server"))
            .args([
                "--data",
                data.to_str().unwrap(),
                "import",
                "--repo",
                "race",
                git_dir.to_str().unwrap(),
            ])
            // An exported variable on a dev box must never park both
            // processes: the import never takes the seam.
            .env_remove("SCION_TEST_PARK_WRITES")
            .stderr(Stdio::piped())
            .spawn()
    })
    .unwrap();
    let import_handle = Mutex::new(Some(std::thread::spawn(move || import.wait_with_output())));
    // The dest path of a rename splits the ID: its first two hex digits
    // name the folder, the remaining sixty-two name the file, so the
    // needle is the tail.
    let blob_name = want.to_string()[2..].to_owned();
    // The release ends the park: the server's writer renames the blob,
    // answers the put, and drops the flock; the import's write that
    // waited for it finds the blob stored and keeps it.
    release_park(&lazy.mount.join("data"), &want);
    let id = tokio::time::timeout(Duration::from_secs(30), &mut put)
        .await
        .unwrap_or_else(|_| panic!("the parked put never finished"))
        .unwrap()
        .unwrap();
    assert_eq!(id, want);
    wait_import(&import_handle);
    // The order is the point: the two processes are ordered by the
    // fan-out directory's flock, so the import's write of the blob
    // never renamed it mid-put. The trace names every rename, and the
    // dest path splits the ID: its first two hex digits name the
    // folder, the remaining sixty-two name the file, so the needle is
    // the tail. Exactly one rename of the blob landed - the server's;
    // on a build without the flock the import's rename lands first and
    // the server's follows it, two renames of one blob.
    let trace = fs::read_to_string(&lazy.trace).unwrap_or_default();
    let renames = trace
        .lines()
        .filter(|line| line.contains("lfs_rename") && line.contains(&blob_name))
        .count();
    assert_eq!(renames, 1, "the blob was renamed {renames} times");
    server.freeze();
    lazy.cut();
    // The client moved into the parked put's task: it ends with it.
    drop(server);
    // What both processes acknowledged survives: the blob verifies from
    // the backing, where only synced writes live.
    let backing = lazy.backing.join("data");
    drop(lazy);
    // The schema follows the store, not the path (inert on SQLite, whose
    // metadata is the directory).
    let store = open_store_shared_reopened(&backing, &data);
    let status = store.blobs().verify(id).unwrap();
    assert_eq!(status, BlobStatus::Valid, "raced blob {id}");
    drop(store);
}

/// Waits for the import process to finish, and fails with its stderr if
/// it exited badly.
fn wait_import(
    handle: &Mutex<Option<std::thread::JoinHandle<std::io::Result<std::process::Output>>>>,
) -> std::process::Output {
    let import = wait_for("the import to finish", 60, || {
        let finished = handle
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished);
        finished.then(|| {
            handle
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .join()
                .unwrap()
                .unwrap_or_else(|error| panic!("waiting for the import failed: {error}"))
        })
    });
    let stderr = String::from_utf8_lossy(&import.stderr).into_owned();
    assert!(
        import.status.success(),
        "the import failed: {:?}: {stderr}",
        import.status
    );
    import
}

/// The park file of one blob, under the data directory's hooks folder.
fn park_file(data: &Path, id: &BlobId) -> PathBuf {
    data.join("hooks").join(format!("park-{id}"))
}

/// Arms the park of one blob: the hooks folder with the park file, so
/// the server's writer parks when its keep finds the blob missing.
fn arm_park(data: &Path, id: &BlobId) {
    fs::create_dir_all(data.join("hooks")).unwrap();
    fs::write(park_file(data, id), b"").unwrap();
}

/// Lifts the park of one blob.
fn release_park(data: &Path, id: &BlobId) {
    fs::remove_file(park_file(data, id)).unwrap();
}

/// Waits until the server's writer is inside the park: the seam leaves
/// a marker file next to the park file for exactly that.
fn wait_parked(hooks: &Path, id: &BlobId) {
    let marker = hooks.join(format!("parked-{id}"));
    let until = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        assert!(Instant::now() < until, "the server never reached the park");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Makes a git repository of one commit, whose file holds `content`.
fn one_file_repo(git_dir: &Path, content: &[u8]) {
    fs::create_dir_all(git_dir).unwrap();
    git(git_dir, &["init", "-q"]);
    fs::write(git_dir.join("f.bin"), content).unwrap();
    git(git_dir, &["add", "."]);
    git(git_dir, &["commit", "-q", "-m", "the race"]);
}

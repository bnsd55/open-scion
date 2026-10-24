//! `scn commit` streams its puts from disk: one 1 MiB chunk in flight,
//! never the whole file, so memory no longer caps the file size. The
//! guards: a file bigger than two chunks lands byte-exact, a file whose
//! RSS the ceiling measures stays under it, and a file that changes
//! while it is read is refused instead of committed as a torn mix.

#![cfg(unix)]

use std::fs;
use std::process::{Command, Output};
use std::sync::Arc;

use open_scion::client::set_put_pause;
use scion_proto::runtime::listen::Limits;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::service;
use scion_server::{Backend, Quotas};
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "secret";
const REPO: &str = "r";

/// The pause hook is one per process, and the tests run in parallel:
/// every hook user holds this from `set_put_pause` to its clear. A
/// tokio mutex: the user's await sits inside.
static HOOK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A server on an empty store, with one changeset to hold the ref.
struct Fixture {
    url: String,
    /// The local directory the commit's files live in.
    work: TempDir,
    /// The sync tests' runtime: it owns the server, so the server lives
    /// as long as it does. The async tests run on the test's own
    /// runtime, and hold none.
    _runtime: Option<tokio::runtime::Runtime>,
}

impl Fixture {
    /// For the sync tests: owns the runtime the server runs on.
    fn start() -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let fx = runtime.block_on(Self::start_async());
        Self {
            _runtime: Some(runtime),
            ..fx
        }
    }

    /// For the async tests: the server spawns on the current runtime.
    async fn start_async() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo(REPO)).unwrap();
        // A root that puts one seed file, so `main` exists for the
        // commits to sit on.
        let seed = store.blobs().put(b"seed\n").unwrap();
        store
            .write(|c| {
                c.insert_changeset(
                    repo,
                    &scion_core::Changeset::new(
                        vec![],
                        scion_core::IdentityTime::new(
                            b"Ada".to_vec(),
                            b"ada@example.com".to_vec(),
                            1_700_000_000,
                            0,
                        )
                        .unwrap(),
                        None,
                        b"root\n".to_vec(),
                        std::iter::once((
                            scion_core::RepoPath::new(b"seed.txt").unwrap(),
                            Some(scion_core::FileState::Regular {
                                content: seed,
                                executable: false,
                            }),
                        ))
                        .collect(),
                    )
                    .unwrap(),
                )
            })
            .unwrap();
        store
            .write(|c| {
                let root = scion_core::ChangesetId::try_from(
                    c.repo_changeset_ids(repo).unwrap()[0].as_slice(),
                )
                .unwrap();
                c.cas_ref(repo, "main", None, root).unwrap();
                Ok(())
            })
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(service::serve(
            store,
            listener,
            TOKEN,
            service::ServeOptions {
                data: dir.path().to_path_buf(),
                audit: scion_server::test_support::audit::Audit::off(),
                limits: Limits::default(),
                quotas: Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: Arc::default(),
                reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
            },
            None,
            std::future::pending(),
        ));
        std::mem::forget(dir);
        Self {
            url,
            work: tempfile::tempdir().unwrap(),
            _runtime: None,
        }
    }

    fn scn(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(["--server", &self.url, "--repo", REPO])
            .args(args)
            .env("SCION_TOKEN", TOKEN)
            .output()
            .unwrap()
    }

    /// A local file for `--put`, and its path.
    fn put_file(&self, name: &str, len: usize) -> std::path::PathBuf {
        let file = self.work.path().join(name);
        // Pseudorandom-ish, so a torn mix cannot pass a byte compare by
        // repeating one short pattern at a chunk boundary.
        let bytes: Vec<u8> = (0..len)
            .map(|i| u8::try_from(i * 7 % 251).unwrap())
            .collect();
        fs::write(&file, bytes).unwrap();
        file
    }
}

/// Commits `file` as `name`, and answers whether the commit succeeded.
fn commit(fx: &Fixture, name: &str, file: &std::path::Path) -> Output {
    fx.scn(&[
        "commit",
        "--json",
        "--message",
        "the streamed put",
        "--put",
        &format!("{name}={}", file.display()),
    ])
}

/// The tip's tree path names, through `scn ls --json`.
fn ls_names(fx: &Fixture) -> Vec<String> {
    let out = fx.scn(&["ls", "--json", "main"]);
    assert!(
        out.status.success(),
        "ls failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out.stdout).unwrap()).unwrap();
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_owned())
        .collect()
}

/// A file of two chunks and a half lands byte-exact: the parts reassemble
/// to the bytes on disk, across the chunk boundary.
#[test]
fn a_file_bigger_than_two_chunks_lands_byte_exact() {
    let fx = Fixture::start();
    let file = fx.put_file("big.bin", 2 * 1_048_576 + 512 * 1024);
    let out = commit(&fx, "big.bin", &file);
    assert!(
        out.status.success(),
        "commit failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // A commit moves no ref: land the changeset, then read it back.
    let value: serde_json::Value =
        serde_json::from_str(&String::from_utf8(out.stdout).unwrap()).unwrap();
    let id = value["changeset"].as_str().unwrap().to_owned();
    let out = fx.scn(&["land", "--json", &id]);
    assert!(
        out.status.success(),
        "land failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let names = ls_names(&fx);
    assert!(names.iter().any(|n| n == "big.bin"), "{names:?}");
}

/// An empty file still commits: one empty message.
#[test]
fn an_empty_file_still_commits() {
    let fx = Fixture::start();
    let file = fx.put_file("empty.bin", 0);
    let out = commit(&fx, "empty.bin", &file);
    assert!(
        out.status.success(),
        "commit failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// The ceiling pair: a 24 MiB file must stream in under a 32 MiB
// peak. The lever test below proves the ceiling still bites: raise
// CEILING_KB or shrink FILE_BYTES and it says the ceiling no longer
// catches a whole-file read.
const FILE_BYTES: usize = 24 * 1024 * 1024;
const CEILING_KB: u64 = 32 * 1024;

/// The ceiling: a commit of a 24 MiB file runs as its own process, whose
/// peak RSS the test measures. Streaming holds about one chunk, so the
/// ceiling sits a few MiB over the process's floor; the old whole-file
/// read adds the file twice over and must fail. The file must be at
/// least twice the headroom the ceiling gives, or a whole-file read
/// would fit under it.
#[test]
fn a_commit_of_a_big_file_stays_under_its_rss_ceiling() {
    let fx = Fixture::start();
    let file = fx.put_file("huge.bin", FILE_BYTES);
    let out = commit_measured(&fx, "huge.bin", &file, false);
    assert!(
        out.status.success(),
        "commit failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let peak_kb = out.peak_kb;
    println!("the scn commit's peak RSS was {peak_kb} kB; the ceiling is {CEILING_KB} kB");
    assert!(peak_kb > 0, "the sampler never saw the commit");
    assert!(
        peak_kb <= CEILING_KB,
        "the commit's peak RSS ({peak_kb} kB) passed the ceiling ({CEILING_KB} kB); \
         a whole-file read is back"
    );
}

/// The ceiling test's lever, run for good: the same file committed with
/// the whole-file read forced on must blow the ceiling. If it does not,
/// the ceiling no longer catches a whole-file read and the RSS guard
/// proves nothing.
#[test]
fn the_ceiling_still_catches_a_whole_file_read() {
    let fx = Fixture::start();
    let file = fx.put_file("huge.bin", FILE_BYTES);
    let out = commit_measured(&fx, "huge.bin", &file, true);
    let peak_kb = out.peak_kb;
    println!("the levered commit's peak RSS was {peak_kb} kB; the ceiling is {CEILING_KB} kB");
    assert!(
        peak_kb > CEILING_KB,
        "the levered commit's peak RSS ({peak_kb} kB) stayed under the ceiling \
         ({CEILING_KB} kB): the ceiling no longer catches a whole-file read"
    );
}

/// A commit's exit state plus the child process's peak RSS in kB. The
/// measurement is `getrusage(RUSAGE_CHILDREN)`'s number, read the way
/// each platform reports it: Linux through `/proc/<pid>/status`
/// (`VmHWM`, kB), macOS through `/usr/bin/time -l` (the child's peak, in
/// bytes). The libc call itself is unsafe, which this workspace denies,
/// so the wrappers stand in for it.
struct Measured {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    peak_kb: u64,
}

/// The child's env when the lever is flipped: the whole-file read.
fn lever_env(lever: bool) -> impl Iterator<Item = (&'static str, &'static str)> {
    lever
        .then_some(("SCION_TEST_COMMIT_MUTATION", "whole_file"))
        .into_iter()
}

fn commit_measured(fx: &Fixture, name: &str, file: &std::path::Path, lever: bool) -> Measured {
    let args = [
        "--server",
        &fx.url,
        "--repo",
        REPO,
        "commit",
        "--message",
        "measured",
        "--put",
    ];
    let put = format!("{name}={}", file.display());
    if cfg!(target_os = "macos") {
        let out = Command::new("/usr/bin/time")
            .arg("-l")
            .arg(env!("CARGO_BIN_EXE_scn"))
            .args(args)
            .arg(&put)
            .env("SCION_TOKEN", TOKEN)
            .envs(lever_env(lever))
            .output()
            .expect("/usr/bin/time exists on macOS");
        let text = String::from_utf8_lossy(&out.stderr);
        // macOS prints the size first: `21037056  maximum resident set
        // size`, in bytes.
        let peak = text
            .lines()
            .find(|l| l.to_ascii_lowercase().contains("maximum resident set size"))
            .and_then(|l| l.split_whitespace().next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or_else(|| panic!("the peak line is missing: {text}"));
        Measured {
            status: out.status,
            stdout: out.stdout,
            stderr: out.stderr,
            peak_kb: peak / 1024,
        }
    } else {
        let child = Command::new(env!("CARGO_BIN_EXE_scn"))
            .args(args)
            .arg(&put)
            .env("SCION_TOKEN", TOKEN)
            .envs(lever_env(lever))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id();
        let peak = std::thread::spawn(move || {
            // VmHWM is the process's high-water mark in kB. The sampler
            // reads it every 5 ms until the process is gone, and keeps
            // the last value it saw: the mark never falls.
            let status = format!("/proc/{pid}/status");
            let mut last = 0_u64;
            let mut seen = 0_u64;
            while let Ok(text) = fs::read_to_string(&status) {
                for line in text.lines() {
                    if let Some(kb) = line.strip_prefix("VmHWM:")
                        && let Ok(kb) = kb.trim().trim_end_matches("kB").trim().parse::<u64>()
                    {
                        last = last.max(kb);
                    }
                }
                seen += 1;
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            assert!(seen > 0, "the sampler never saw the commit's VmHWM");
            last
        });
        let out = child.wait_with_output().unwrap();
        Measured {
            status: out.status,
            stdout: out.stdout,
            stderr: out.stderr,
            peak_kb: peak.join().unwrap(),
        }
    }
}

/// The torn reads, on the client seam directly: the pause hook runs in
/// this process, and the commit's put is this call. The refusal names
/// the file: it changed while it was read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_that_shrinks_while_it_is_read_is_refused() {
    let fx = Fixture::start_async().await;
    let file = fx.put_file("torn.bin", 2 * 1_048_576 + 7);
    let torn = file.clone();
    let _hook = HOOK.lock().await;
    set_put_pause(Some(Box::new(move || {
        // The pause sits between the metadata read and the first chunk:
        // the file loses its tail here.
        let short = fs::read(&torn).unwrap();
        fs::write(&torn, &short[..1024]).unwrap();
    })));
    let mut ctx =
        open_scion::client::Ctx::connect(&fx.url, None, TOKEN, None, REPO.to_owned(), false)
            .await
            .unwrap();
    let put = ctx.put_blob_file(&file).await;
    set_put_pause(None);
    let err = put.expect_err("the shrunk file must refuse");
    assert!(
        err.to_string()
            .contains("changed while it was read; commit again"),
        "the refusal must name the change: {err:#}"
    );
}

/// A byte past the length the file had when it was opened: grown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_that_grows_while_it_is_read_is_refused() {
    let fx = Fixture::start_async().await;
    let file = fx.put_file("grown.bin", 2 * 1_048_576 + 7);
    let grown = file.clone();
    let _hook = HOOK.lock().await;
    set_put_pause(Some(Box::new(move || {
        let mut bytes = fs::read(&grown).unwrap();
        bytes.push(0xff);
        fs::write(&grown, bytes).unwrap();
    })));
    let mut ctx =
        open_scion::client::Ctx::connect(&fx.url, None, TOKEN, None, REPO.to_owned(), false)
            .await
            .unwrap();
    let put = ctx.put_blob_file(&file).await;
    set_put_pause(None);
    let err = put.expect_err("the grown file must refuse");
    assert!(
        err.to_string()
            .contains("changed while it was read; commit again"),
        "the refusal must name the change: {err:#}"
    );
}

/// A file whose mtime changed while it was read, at the same length:
/// the metadata check refuses what the byte counts let through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_whose_mtime_changes_while_it_is_read_is_refused() {
    let fx = Fixture::start_async().await;
    let file = fx.put_file("touched.bin", 1_048_576 + 7);
    let touched = file.clone();
    let _hook = HOOK.lock().await;
    set_put_pause(Some(Box::new(move || {
        let new = fs::File::options().append(true).open(&touched).unwrap();
        new.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(2))
            .unwrap();
    })));
    let mut ctx =
        open_scion::client::Ctx::connect(&fx.url, None, TOKEN, None, REPO.to_owned(), false)
            .await
            .unwrap();
    let put = ctx.put_blob_file(&file).await;
    set_put_pause(None);
    let err = put.expect_err("the touched file must refuse");
    assert!(
        err.to_string()
            .contains("changed while it was read; commit again"),
        "the refusal must name the change: {err:#}"
    );
}

/// A client used directly refuses the same way (the seam the binary
/// reaches through `commit`): the library call fails, not only the CLI.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_refuses_a_torn_file() {
    let fx = Fixture::start_async().await;
    let file = fx.put_file("direct.bin", 1_048_576 + 7);
    let torn = file.clone();
    let _hook = HOOK.lock().await;
    set_put_pause(Some(Box::new(move || {
        let short = fs::read(&torn).unwrap();
        fs::write(&torn, &short[..3]).unwrap();
    })));
    let mut ctx =
        open_scion::client::Ctx::connect(&fx.url, None, TOKEN, None, REPO.to_owned(), false)
            .await
            .unwrap();
    let put = ctx.put_blob_file(&file).await;
    set_put_pause(None);
    let err = put.expect_err("the torn read must fail");
    assert!(
        err.to_string().contains("changed while it was read"),
        "the refusal must name the change: {err:#}"
    );
}

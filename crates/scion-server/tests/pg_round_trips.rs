//! The statement budget of one land on Postgres: a land runs serial
//! client-to-database round trips inside the per-ref critical section,
//! and each round trip is part of every land that queues behind it. A
//! design note in the land performance issue holds the plan to shrink
//! the count; this guard pins today's count of the real path - one land
//! through the server's Land RPC - so it can only fall. On SQLite the
//! store's statements are in-process calls, which is why the test skips
//! there.
#![cfg(unix)]

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use scion_core::{Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::LandRequest;
use scion_proto::v1::scion_client::ScionClient;
use scion_server::test_support::audit::Audit;
use scion_server::test_support::meta::Meta as _;
use scion_server::test_support::service;
use scion_server::test_support::{meta::MetaTx as _, write};
use scion_server::{Access, Backend, ImportOptions, Keys, Metrics, import_git};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::Request;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;

const REPO: &str = "r";

/// Today's statement counts of one land through the server's Land RPC,
/// from this test's own run (stable across runs: the statements are
/// the store's fixed SQL paths; the window waits for the startup
/// sweep's first pass, so no background statement can fall inside it).
/// The design lowers both to a budget of 15; each step of that work
/// moves these numbers down before it can claim to.
const BUDGET: u64 = 48;

/// The same for a land whose base is no longer the ref's head, so the
/// land must rebase: the slow case under contention.
const REBASE_BUDGET: u64 = 55;

/// The same for a wide land: a merge of two parents that puts 50 files.
/// The multi-row writes batch what scales with the changeset's width:
/// one statement per table instead of one per row.
const WIDE_BUDGET: u64 = 56;

/// A one-commit git repository, imported on `main`.
fn git_seed() -> (TempDir, ChangesetId) {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        let out = Command::new("git")
            .args(["-c", "gc.auto=0", "-c", "maintenance.auto=false"])
            .args(args)
            .current_dir(dir.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_AUTHOR_DATE", "1700000000 +0530")
            .env("GIT_COMMITTER_NAME", "Bob")
            .env("GIT_COMMITTER_EMAIL", "bob@example.com")
            .env("GIT_COMMITTER_DATE", "1700000100 -0700")
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init", "-q", "-b", "main"]);
    fs::write(dir.path().join("a.txt"), "one\n").unwrap();
    run(&["add", "."]);
    run(&["commit", "-qm", "root"]);

    let store = open_store(dir.path());
    common::spawn_guard::prime_gix_lookup();
    let tip = import_git(
        &store,
        REPO,
        dir.path(),
        &ImportOptions {
            rev: "HEAD".into(),
            target_ref: "main".into(),
            verify_sample: 0,
        },
    )
    .unwrap()
    .tip;
    (dir, tip)
}

/// One backend for the data directory the tests run on.
fn open_store(dir: &Path) -> Backend {
    Backend::open_test(dir).unwrap()
}

/// One created changeset on top of `base`.
fn create(store: &Backend, base: ChangesetId, path: &str) -> ChangesetId {
    create_many(store, &[base], &[(path, "one")])
}

/// One created changeset over `bases`, one file per (path, content).
fn create_many(store: &Backend, bases: &[ChangesetId], files: &[(&str, &str)]) -> ChangesetId {
    let repo = store.read(|c| c.repo_id(REPO)).unwrap();
    let mut changes = BTreeMap::new();
    for (path, content) in files {
        let upload = store.blobs().upload().unwrap();
        let upload = upload.append(content.as_bytes()).unwrap();
        let file = FileState::Regular {
            content: write::put_blob(store, repo, upload).unwrap(),
            executable: false,
        };
        changes.insert(
            RepoPath::new(path.as_bytes()).expect("a plain path"),
            Some(file),
        );
    }
    let cs = Changeset::new(
        bases.to_vec(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_100,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"wide\n".to_vec(),
        changes,
    )
    .expect("changeset fits the format");
    write::create_changeset(
        store,
        REPO,
        &cs.encode(),
        &scion_server::test_support::deadline::Deadline::new(
            std::time::Instant::now() + Duration::from_secs(600),
        ),
    )
    .unwrap()
}

/// The access file one principal and one repo: the land must pass the
/// write check like any client's.
fn access() -> Access {
    let token = blake3::Hash::from(token_digest("ada")).to_hex().to_string();
    Access::parse(&format!(
        "token\tada\t{token}\nread\tada\t{REPO}\nwrite\tada\t{REPO}\n"
    ))
    .unwrap()
}

/// The token as the client carries it.
fn bearer() -> impl Interceptor {
    let token: AsciiMetadataValue = "Bearer ada"
        .parse()
        .expect("a plain token parses as metadata");
    move |req: Request<()>| {
        let mut req = req;
        req.metadata_mut().insert("authorization", token.clone());
        Ok(req)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_land_stays_under_the_round_trip_budget() {
    if !Backend::test_is_postgres() {
        println!("skipping: Postgres only");
        return;
    }
    let (dir, tip) = git_seed();
    let store = open_store(dir.path());
    // Two changesets on the same base: the first lands on the head (the
    // fast path), the second is left behind by it and must rebase (the
    // slow case under contention).
    let cs = create(&store, tip, "landed.txt");
    let cs2 = create(&store, tip, "rebased.txt");
    // The stop sender keeps the server up; hold it through the test.
    let (_stop, mut client) = serve(&dir, &store).await;

    // The fast land.
    let statements = land_through(&mut client, cs).await;
    println!("one land through the server ran {statements} statements");
    for sql in Backend::test_statement_log() {
        println!("  {sql}");
    }
    assert!(
        statements <= BUDGET,
        "one land through the server ran {statements} statements, above the budget {BUDGET}"
    );

    // The rebase land: main moved since cs2's base, so the land must
    // rebase - the slow case under contention.
    let rebased = land_through(&mut client, cs2).await;
    println!("one rebase land through the server ran {rebased} statements");
    assert!(
        rebased <= REBASE_BUDGET,
        "one rebase land through the server ran {rebased} statements, above the budget {REBASE_BUDGET}"
    );

    // The wide land: 50 files in one changeset, so the per-row writes
    // have rows to batch. (A client changeset carries exactly one
    // parent; merges come from git import, whose statements the import
    // path batches separately.)
    let files: Vec<(String, String)> = (0..50)
        .map(|i| (format!("wide/{i}.txt"), format!("file {i}\n")))
        .collect();
    let wide = create_many(
        &store,
        &[cs],
        &files
            .iter()
            .map(|(p, c)| (p.as_str(), c.as_str()))
            .collect::<Vec<_>>(),
    );
    let wide_statements = land_through(&mut client, wide).await;
    println!("one wide land through the server ran {wide_statements} statements");
    assert!(
        wide_statements <= WIDE_BUDGET,
        "one wide land through the server ran {wide_statements} statements, above the budget {WIDE_BUDGET}"
    );
}

/// The real server handler: one `service::serve` on the store, one gRPC
/// client, and every land below goes through the same code a client's
/// land rides. Waits for the startup stale-land sweep, which is the only
/// database-speaking background task the server runs; the next sweep is
/// an hour away, and no other spawned task speaks SQL.
async fn serve(
    dir: &TempDir,
    store: &Backend,
) -> (
    tokio::sync::oneshot::Sender<()>,
    ScionClient<InterceptedService<Channel, impl Interceptor>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        store.clone(),
        listener,
        access(),
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(Keys::generate())),
        async {
            let _ = stopped.await;
        },
    ));

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let client = ScionClient::with_interceptor(channel, bearer());
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        while !Backend::test_first_sweep_done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "the startup sweep never finished");
    (stop_tx, client)
}

/// One land through the server, counted: resets the statement counter,
/// lands `cs` on `main`, and returns the statements the land ran.
async fn land_through(
    client: &mut ScionClient<InterceptedService<Channel, impl Interceptor>>,
    cs: ChangesetId,
) -> u64 {
    Backend::test_reset_statement_count();
    let reply = client
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: cs.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: cs.to_string(),
        })
        .await
        .expect("the land lands")
        .into_inner();
    assert_eq!(reply.state(), scion_proto::v1::LandState::Landed);
    Backend::test_statement_count()
}

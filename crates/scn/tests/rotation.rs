//! Ticket key rotation, following `docs/operations.md`'s steps one restart
//! at a time: the server signs with key A and a tier verifies public A
//! while a checkout reads; the tier then gets B and A (new first), the
//! server moves to B, and tickets of either key read; then A is dropped
//! and an A ticket fails. No step sleeps. The in-flight case of the doc's
//! "rotation while checkouts run" is the ticket a client already holds
//! across the switch: `ticket_a` is minted in step 1 and carried through
//! every later step, which is exactly what the doc promises for it.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use scion_core::{BlobId, FileState};
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};
use scion_server::Keys;
use scion_server::{Backend, test_support::service};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, DAEMON_UPSTREAM_TIMEOUT, DiskStore, MemoryStore, Tier};
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

mod common;
use common::{REPO, TOKEN, changeset, land};

type Client = ObjectsClient<InterceptedService<Channel, Bearer>>;

/// The token client of `url`: the calls that mint tickets.
async fn token_client(url: &str) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .unwrap();
    ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
}

/// A ref of `blob` at `path`.
fn blob_ref(id: BlobId, path: &[u8]) -> ObjectRef {
    ObjectRef {
        kind: ObjectKind::Blob.into(),
        id: id.as_bytes().to_vec(),
        path: path.to_vec(),
    }
}

/// A server that signs with `keys`, a cache that verifies `verify` (new
/// key first, as the doc's step 3 says), and a daemon that checks no
/// ticket and forwards, all in one process. Aborting the handles stops
/// all three, as a restart of each does.
fn start_stack(
    runtime: &Runtime,
    store: Backend,
    keys: Keys,
    verify: Vec<ed25519_dalek::VerifyingKey>,
) -> (
    String,
    String,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<TempDir>,
) {
    let tcp = || {
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (listener, url)
    };
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut dirs: Vec<TempDir> = Vec::new();

    let data_dir = tempfile::tempdir().unwrap();
    let data = data_dir.path().to_path_buf();
    dirs.push(data_dir);
    let (listener, server) = tcp();
    tasks.push(runtime.spawn(async move {
        let _ = service::serve(
            store,
            listener,
            TOKEN,
            service::ServeOptions {
                data,
                audit: scion_server::test_support::audit::Audit::off(),
                limits: Limits::default(),
                quotas: scion_server::Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: Arc::default(),
                reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
            },
            Some(Arc::new(keys)),
            std::future::pending(),
        )
        .await;
    }));

    let (listener, cache_url) = tcp();
    let memory = MemoryStore::new(64 << 20);
    let cache = Tier::new(memory, &server, TOKEN, CACHE_UPSTREAM_TIMEOUT, None, verify).unwrap();
    tasks.push(runtime.spawn(async move {
        let _ = cache
            .serve(
                TOKEN,
                TcpListenerStream::new(listener),
                Limits::default(),
                None,
                std::future::pending(),
            )
            .await;
    }));

    let node = tempfile::tempdir().unwrap();
    let disk = DiskStore::open(&node.path().join("data"), 64 << 20).unwrap();
    let (listener, daemon) = tcp();
    let daemon_tier = Tier::new(
        disk,
        &cache_url,
        TOKEN,
        DAEMON_UPSTREAM_TIMEOUT,
        None,
        Vec::new(),
    )
    .unwrap();
    tasks.push(runtime.spawn(async move {
        let _ = daemon_tier
            .serve(
                TOKEN,
                TcpListenerStream::new(listener),
                Limits::default(),
                None,
                std::future::pending(),
            )
            .await;
    }));
    dirs.push(node);

    (server, daemon, tasks, dirs)
}

/// `scn checkout main --into dest` through `through`.
fn checkout(through: &str, server: &str, dest: &Path) {
    let out = Command::new(env!("CARGO_BIN_EXE_scn"))
        .args([
            "--server",
            server,
            "--objects",
            through,
            "--repo",
            REPO,
            "checkout",
            "main",
            "--into",
        ])
        .arg(dest)
        .env("SCION_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The read of one blob through `url`, carrying `ticket`: the whole reply,
/// streamed to the end.
fn read_blob(runtime: &Runtime, url: &str, ticket: Vec<u8>, blob: BlobId) -> Result<(), Status> {
    runtime.block_on(async {
        let mut objects = token_client(url).await;
        let request = GetObjectsRequest {
            refs: vec![blob_ref(blob, b"README.md")],
            accept: Vec::new(),
            ticket,
        };
        let mut stream = objects.get_objects(request).await?.into_inner();
        while let Some(part) = stream.message().await? {
            drop(part);
        }
        Ok(())
    })
}

/// The read ticket of `cs`'s root tree, minted by the server at `url`.
async fn root_ticket(url: &str, cs: scion_core::ChangesetId) -> Result<Vec<u8>, Status> {
    let reply = token_client(url)
        .await
        .get_root_tree(Request::new(GetRootTreeRequest {
            repo: REPO.to_owned(),
            changeset_id: cs.as_bytes().to_vec(),
        }))
        .await?
        .into_inner();
    assert!(!reply.ticket.is_empty(), "the server minted a ticket");
    Ok(reply.ticket)
}

#[test]
fn ticket_key_rotation_while_a_checkout_reads() {
    let runtime = Runtime::new().unwrap();
    let _entered = runtime.enter();
    let data = tempfile::tempdir().unwrap();
    let store = Backend::open_test(data.path()).unwrap();
    let readme = FileState::Regular {
        content: store.blobs().put(b"hello\n").unwrap(),
        executable: false,
    };
    let first = land(&store, &changeset(None, &[("README.md", readme)]));
    let blob = BlobId::of(b"hello\n");
    let tmp = tempfile::tempdir().unwrap();

    // Step 1: the server signs with key A, the tier verifies public A, and
    // a checkout reads.
    let a = Keys::generate();
    let a_public = a.public_hex();
    assert_eq!(a_public.len(), 64, "the public key is 64 hex characters");
    assert!(
        a_public.chars().all(|c| c.is_ascii_hexdigit()),
        "{a_public}"
    );
    let (server, daemon, tasks, _dirs) =
        start_stack(&runtime, store.clone(), a.clone(), a.for_verify().to_vec());
    checkout(&daemon, &server, &tmp.path().join("on-a"));
    // The ticket of key A, minted by the server of key A, kept across the
    // whole rotation: the in-flight case of the doc's "rotation while
    // checkouts run".
    let ticket_a = runtime
        .block_on(root_ticket(&server, first))
        .expect("the server minted a ticket");
    read_blob(&runtime, &daemon, ticket_a.clone(), blob).expect("a ticket of key A reads on A");
    for task in &tasks {
        task.abort();
    }

    // The doc's step 2 done first — server signs B while the caches still
    // verify A only — is the broken state: the new server mints a B ticket
    // and the cache refuses it, so a checkout through a cache fails. The
    // fixed order is tiers first, and operations.md says why.
    let b = Keys::generate();
    let (server_b, daemon_b, tasks_b, _dirs) =
        start_stack(&runtime, store.clone(), b.clone(), a.for_verify().to_vec());
    let ticket_b = runtime
        .block_on(root_ticket(&server_b, first))
        .expect("the server of key B minted a ticket");
    let error = read_blob(&runtime, &daemon_b, ticket_b.clone(), blob)
        .expect_err("the tier that verifies only A refuses every B ticket");
    assert_eq!(error.code(), Code::PermissionDenied, "{error:?}");
    for task in &tasks_b {
        task.abort();
    }

    // The fixed step 3: the tiers get B then A (new first) while the
    // server still signs with A. A ticket of either key reads.
    let verify_both = {
        let mut verify = b.for_verify().to_vec();
        verify.extend_from_slice(a.for_verify());
        verify.clone()
    };
    let (server, daemon, tasks, _dirs) =
        start_stack(&runtime, store.clone(), a.clone(), verify_both.clone());
    checkout(&daemon, &server, &tmp.path().join("tier-both"));
    read_blob(&runtime, &daemon, ticket_a.clone(), blob)
        .expect("a ticket of key A reads through the tier that holds both keys");
    // Step 2 of the doc: the server moves to B. Tickets of either key
    // verify while both are given.
    let (server_b, daemon_b, tasks_b, _dirs) =
        start_stack(&runtime, store.clone(), b.clone(), verify_both);
    checkout(&daemon_b, &server_b, &tmp.path().join("on-both"));
    read_blob(&runtime, &daemon_b, ticket_a.clone(), blob)
        .expect("a ticket of key A reads while both keys are given");
    let ticket_b = runtime
        .block_on(root_ticket(&server_b, first))
        .expect("the server of key B minted a ticket");
    read_blob(&runtime, &daemon_b, ticket_b.clone(), blob)
        .expect("a ticket of key B reads while both keys are given");
    for task in tasks.iter().chain(tasks_b.iter()) {
        task.abort();
    }

    // Step 4: A is dropped; the server signs B and the tier verifies B
    // only. An A ticket fails at the cache (the daemon carries no keys and
    // only forwards); a B ticket still reads.
    let (server, daemon, tasks, _dirs) =
        start_stack(&runtime, store.clone(), b.clone(), b.for_verify().to_vec());
    let error = read_blob(&runtime, &daemon, ticket_a, blob)
        .expect_err("a ticket of the dropped key verifies no more");
    assert_eq!(error.code(), Code::PermissionDenied, "{error:?}");
    checkout(&daemon, &server, &tmp.path().join("after-drop"));
    for task in &tasks {
        task.abort();
    }
}

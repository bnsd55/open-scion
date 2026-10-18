//! Ticket key rotation while checkouts read, following
//! `docs/operations.md`'s rotation steps exactly: the server signs with
//! key A and a tier verifies public A while a checkout reads; the tier
//! then gets B and A (new first) and the server moves to B, and the reads
//! go on; then A is dropped and an A ticket fails. No step sleeps: every
//! promise the doc makes at a step is asserted at that step.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use scion_core::{BlobId, FileState};
use scion_proto::auth::Bearer;
use scion_proto::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef};
use scion_server::tickets::Keys;
use scion_server::{Backend, service};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, DAEMON_UPSTREAM_TIMEOUT, DiskStore, MemoryStore, Tier};
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

    let (listener, server) = tcp();
    tasks.push(runtime.spawn(async move {
        let _ = service::serve(
            store,
            listener,
            TOKEN,
            service::ServeOptions {
                limits: Limits::default(),
                quotas: scion_server::quotas::Quotas::off(),
                pending_lands: None,
                land_stale_secs: 1 << 40,
                tls: None,
                metrics: Arc::default(),
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

/// `scn checkout main --into dest` through `through`: the client mints the
/// ticket of the server and carries it through the tiers.
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
    // The ticket of key A, minted by the server of key A, kept for step 4.
    let ticket_a = runtime
        .block_on(root_ticket(&server, first))
        .expect("the server minted a ticket");
    read_blob(&runtime, &daemon, ticket_a.clone(), blob).expect("a ticket of key A reads on A");
    drop(tasks);

    // Steps 2 and 3: the tier gets B then A (new first), the server signs
    // with B and still verifies public A; the reads go on.
    let b = Keys::generate();
    let verify_both = {
        let mut verify = b.for_verify().to_vec();
        verify.extend_from_slice(a.for_verify());
        verify
    };
    let (server, daemon, tasks, _dirs) =
        start_stack(&runtime, store.clone(), b.clone(), verify_both);
    checkout(&daemon, &server, &tmp.path().join("on-both"));
    // Tickets of either key verify while both are given.
    read_blob(&runtime, &daemon, ticket_a.clone(), blob)
        .expect("a ticket of key A reads while both keys are given");
    let ticket_b = runtime
        .block_on(root_ticket(&server, first))
        .expect("the server of key B minted a ticket");
    read_blob(&runtime, &daemon, ticket_b.clone(), blob)
        .expect("a ticket of key B reads while both keys are given");
    drop(tasks);

    // Step 4: A is dropped; the server signs B and the tier verifies B
    // only. An A ticket fails as the doc says; a B ticket still reads.
    let (server, daemon, tasks, _dirs) =
        start_stack(&runtime, store.clone(), b.clone(), b.for_verify().to_vec());
    let error = read_blob(&runtime, &daemon, ticket_a, blob)
        .expect_err("a ticket of the dropped key verifies no more");
    assert_eq!(error.code(), Code::PermissionDenied, "{error:?}");
    checkout(&daemon, &server, &tmp.path().join("after-drop"));
    drop(tasks);
}

//! The metrics of the gRPC services: what calls count, read back from the
//! registry's rendered text, over an in-process server.

use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_core::{BlobId, Changeset, ChangesetId, FileState, IdentityTime, RepoPath};
use scion_proto::CHUNK_BYTES;
use scion_proto::runtime::auth::Bearer;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{BlameRequest, CreateChangesetRequest, LandRequest, ReadFileRequest};
use scion_server::Metrics;
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::{backend::from_store, service, store::Store};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "secret";
const CONTENT: &[u8] = b"the file of the metrics tests";

type Client = ScionClient<InterceptedService<Channel, Bearer>>;

/// The rendered registry.
fn text(metrics: &Metrics) -> String {
    metrics.render()
}

/// Waits until `want` is in the rendered registry, or fails after a short
/// while: a reply body is counted when its last frame is polled or when it
/// is dropped, which can lag the call that returned.
async fn eventually(metrics: &Metrics, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if text(metrics).contains(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "the registry never showed {want:?}; it shows:\n{}",
        text(metrics)
    );
}

fn author(secs: u64) -> IdentityTime {
    IdentityTime::new(
        b"Ada".to_vec(),
        b"ada@example.com".to_vec(),
        i64::try_from(secs).unwrap_or(i64::MAX),
        0,
    )
    .expect("identity fits the format")
}

fn changeset(parents: Vec<ChangesetId>, path: &str, state: Option<FileState>) -> Changeset {
    Changeset::new(
        parents,
        author(1_700_000_000),
        None,
        path.as_bytes().to_vec(),
        [(RepoPath::new(path).unwrap(), state)]
            .into_iter()
            .collect(),
    )
    .expect("changeset fits the format")
}

/// A store with one repo, one root changeset that writes a file, and a
/// server that counts into `metrics`. Returns the stop handle, the client,
/// the root, and the address to reach the server again.
#[allow(clippy::type_complexity)]
async fn setup() -> (
    tempfile::TempDir,
    tokio::sync::oneshot::Sender<()>,
    Store,
    Arc<Metrics>,
    Client,
    ChangesetId,
    std::net::SocketAddr,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    store.blobs().put(CONTENT).unwrap();
    let root = changeset(
        vec![],
        "file.txt",
        Some(FileState::Regular {
            content: BlobId::of(CONTENT),
            executable: false,
        }),
    );
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &root)?;
            // The land target: main points at the root.
            c.cas_ref(repo, "main", None, root)?;
            Ok(root)
        })
        .unwrap();

    let metrics = Arc::new(Metrics::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        from_store(store.clone()),
        listener,
        TOKEN,
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: metrics.clone(),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        None,
        async {
            stopped.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let client = ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    (dir, stop, store, metrics, client, root, addr)
}

/// Puts `content` into the store and names it in a changeset on top of
/// `parent`. Returns the changeset's id.
fn on_top(store: &Store, parent: ChangesetId, path: &str, content: &[u8]) -> ChangesetId {
    store.blobs().put(content).unwrap();
    store
        .write(|c| {
            let repo = c.repo_id(REPO)?;
            let cs = Changeset::new(
                vec![parent],
                author(1_700_000_001),
                None,
                path.as_bytes().to_vec(),
                [(
                    RepoPath::new(path).unwrap(),
                    Some(FileState::Regular {
                        content: BlobId::of(content),
                        executable: false,
                    }),
                )]
                .into_iter()
                .collect(),
            )
            .expect("changeset fits the format");
            c.insert_changeset(repo, &cs)
        })
        .unwrap()
}

/// A blame counts its request, its latency and its bytes, and the registry
/// renders them as Prometheus text.
#[tokio::test(flavor = "multi_thread")]
async fn a_blame_counts_a_request_with_its_latency_and_bytes() {
    let (_dir, _stop, _store, metrics, mut client, root, _addr) = setup().await;
    assert!(
        !text(&metrics).contains("scion_requests_total{"),
        "nothing has run yet: {}",
        text(&metrics)
    );

    let mut pages = client
        .blame(BlameRequest {
            repo: REPO.into(),
            changeset_id: root.as_bytes().to_vec(),
            path: b"file.txt".to_vec(),
            cursor: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut ranges = 0usize;
    while let Some(page) = pages.message().await.unwrap() {
        ranges += page.ranges.len();
    }
    assert_eq!(ranges, 1, "{ranges}");

    let text = text(&metrics);
    assert!(
        text.contains("scion_requests_total{rpc=\"Scion/Blame\",code=\"ok\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains("# TYPE scion_request_seconds histogram\n"),
        "{text}"
    );
    assert!(
        text.contains("scion_request_seconds_count{rpc=\"Scion/Blame\"} 1\n"),
        "{text}"
    );
    // The smallest bucket holds the fast call, and so does the infinite one.
    assert!(
        text.contains("scion_request_seconds_bucket{rpc=\"Scion/Blame\",le=\"1\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains("scion_request_seconds_bucket{rpc=\"Scion/Blame\",le=\"+Inf\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains("scion_request_seconds_sum{rpc=\"Scion/Blame\"} "),
        "{text}"
    );
    // The reply moved bytes out. (The request bytes are counted from the
    // content-length header, which tonic's client does not send, so the
    // `in` line appears only for clients that send one.)
    assert!(
        text.matches("scion_rpc_bytes_total{rpc=\"Scion/Blame\",direction=\"out\"} ")
            .count()
            == 1,
        "{text}"
    );
}

/// A call that the access rules refuse counts with the code it was refused
/// with: the layer sits outside the rules.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_call_counts_its_code() {
    let (_dir, _stop, _store, metrics, _client, root, addr) = setup().await;

    // A client with the wrong token is refused before any handler runs.
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    let mut stranger = ScionClient::with_interceptor(channel, Bearer::new("wrong").unwrap());
    let status = stranger
        .create_changeset(CreateChangesetRequest {
            repo: REPO.into(),
            changeset: changeset(vec![root], "x", None).encode(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");

    eventually(
        &metrics,
        "scion_requests_total{rpc=\"Scion/CreateChangeset\",code=\"unauthenticated\"} 1\n",
    )
    .await;
    // The refusal carries no reply body: no bytes moved out, and the
    // histogram shows the call.
    eventually(
        &metrics,
        "scion_request_seconds_count{rpc=\"Scion/CreateChangeset\"} 1\n",
    )
    .await;
    assert!(
        !text(&metrics).contains("direction=\"out\""),
        "{}",
        text(&metrics)
    );
}

/// A land counts its outcome: one landed, one conflicting.
#[tokio::test(flavor = "multi_thread")]
async fn lands_count_their_outcomes() {
    let (_dir, _stop, store, metrics, mut client, root, _addr) = setup().await;

    // The first land rebases onto main and lands.
    let first = on_top(&store, root, "a.txt", b"a");
    client
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: first.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k1".into(),
        })
        .await
        .unwrap();
    eventually(&metrics, "scion_lands_total{outcome=\"landed\"} 1\n").await;

    // A second land from the same base, on the same path, now conflicts
    // with the moved ref.
    let second = on_top(&store, root, "a.txt", b"b");
    client
        .land(LandRequest {
            repo: REPO.into(),
            changeset_id: second.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k2".into(),
        })
        .await
        .unwrap();
    eventually(&metrics, "scion_lands_total{outcome=\"conflict\"} 1\n").await;
}

/// A read of a stored file counts the bytes it moved out, and a call the
/// client stops in the middle counts as cancelled.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_file_counts_its_bytes_and_a_stopped_call_counts_as_cancelled() {
    let (_dir, _stop, store, metrics, mut client, root, _addr) = setup().await;

    // A file far larger than the HTTP/2 flow-control windows and the
    // buffers between the server's reply and the client: the server cannot
    // finish it before the client stops, whatever the transport buffers.
    let data: Vec<u8> = (0..251_u8).cycle().take(CHUNK_BYTES * 64).collect();
    let big = on_top(&store, root, "big.bin", &data);

    // Read it, and take only its first part.
    let mut stream = client
        .read_file(ReadFileRequest {
            repo: REPO.into(),
            changeset_id: big.as_bytes().to_vec(),
            path: b"big.bin".to_vec(),
        })
        .await
        .unwrap()
        .into_inner();
    let part = stream.next().await.unwrap().unwrap();
    assert_ne!(part.data.len(), 0);
    drop(stream);

    // The bytes of the parts that left count as out.
    eventually(
        &metrics,
        "scion_rpc_bytes_total{rpc=\"Scion/ReadFile\",direction=\"out\"} ",
    )
    .await;
    // The reply body without its trailers counts as cancelled.
    eventually(
        &metrics,
        "scion_requests_total{rpc=\"Scion/ReadFile\",code=\"cancelled\"} 1\n",
    )
    .await;
}
// A streamed `Log` the client stops has no test here: the transport counts
// a dropped reply as cancelled before any answer of the server's own is
// read, so the count is the same whatever `check()` answers, and no test
// of it can fail for a real reason (mutation-checked). The locks for a
// cancelled call are the unit tests of `deadline::check` and the graph
// walks, and the land tests that read a pending row and its retry.

#[path = "common/mod.rs"]
mod common;

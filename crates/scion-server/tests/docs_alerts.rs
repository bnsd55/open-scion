//! The first step of each alert runbook of docs/operations.md section 6,
//! run for real: the latency runbook's per-rpc p99 bucket, read off a
//! served server's `/metrics` body; the view-memo runbook's warm
//! derivation (a second read of one changeset walks nothing); and the
//! tier-miss runbook's miss ratio falling when the tier's store serves
//! the second read. The stalled-reply runbook's step is the existing
//! `log_stream` test the guide names.

mod common;

use scion_core::{BlobId, Changeset, FileState, IdentityTime, RepoPath};
use scion_proto::runtime::auth::{Bearer, token_digest};
use scion_proto::runtime::listen::Limits;
use scion_proto::runtime::object::{self, Assembler, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{
    GetObjectsRequest, GetRootTreeRequest, ObjectKind, ObjectRef, PutBlobRequest,
};
use scion_server::test_support::meta::MetaTx as _;
use scion_server::test_support::{backend::from_store, service, store::Store};
use scion_server::{Access, Keys, Metrics};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

const REPO: &str = "main";
const TOKEN: &str = "secret";
const CHILD: &str = "child";

type Objects = ObjectsClient<InterceptedService<Channel, Bearer>>;

/// The access file of the runbooks' server: `ci` reads the repo whole,
/// and `child` is a partial reader of `src` only, whose derivations the
/// view memo serves.
fn access() -> Access {
    let ci = blake3::Hash::from(token_digest(TOKEN)).to_hex().to_string();
    let child = blake3::Hash::from(token_digest(CHILD)).to_hex().to_string();
    Access::parse(&format!(
        "token\tci\t{ci}\n\
         read\tci\t{REPO}\n\
         write\tci\t{REPO}\n\
         token\tchild\t{child}\n\
         read\tchild\t{REPO}\tsrc\n"
    ))
    .expect("the access file parses")
}

/// A server of `main` with two folders (`src/hello.rs`, `docs/links.md`),
/// the metrics registry it renders from, its ticket keys, and the two
/// readers. The data directory must outlive the server.
struct Runbook {
    full: Objects,
    partial: Objects,
    keys: Keys,
    cs: scion_core::ChangesetId,
    hello: BlobId,
    addr: String,
    /// The address the guide's blocks curl: the same registry, over the
    /// HTTP body `/metrics` serves.
    metrics_addr: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    /// Keeps the metrics endpoint's shutdown open for the test's life.
    _stop_metrics: tokio::sync::oneshot::Sender<()>,
    _dir: tempfile::TempDir,
}

async fn runbook() -> Runbook {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let hello = store.blobs().put(b"pub fn main() {}\n").unwrap();
    let links = store.blobs().put(b"# links\n").unwrap();
    let changeset = Changeset::new(
        Vec::new(),
        IdentityTime::new(
            b"Ada".to_vec(),
            b"ada@example.com".to_vec(),
            1_700_000_000,
            0,
        )
        .expect("identity fits the format"),
        None,
        b"two folders\n".to_vec(),
        [
            (
                RepoPath::new("src/hello.rs").unwrap(),
                Some(FileState::Regular {
                    content: hello,
                    executable: false,
                }),
            ),
            (
                RepoPath::new("docs/links.md").unwrap(),
                Some(FileState::Regular {
                    content: links,
                    executable: false,
                }),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .expect("changeset fits the format");
    let (_bytes, cs) = changeset.encode_with_id();
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            c.insert_changeset(repo, &changeset)
        })
        .unwrap();

    let metrics = Arc::new(Metrics::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let keys = Keys::generate();
    tokio::spawn(service::serve(
        from_store(store),
        listener,
        access(),
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::clone(&metrics),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(keys.clone().into()),
        async {
            stopped.await.ok();
        },
    ));
    // The metrics endpoint the guide's blocks curl: the same body
    // `/metrics` serves, on its own loopback port.
    let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_addr = metrics_listener.local_addr().unwrap();
    let (stop_metrics, stopped_metrics) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(scion_proto::runtime::metrics::serve(
        metrics_listener,
        Arc::clone(&metrics),
        async {
            stopped_metrics.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{addr}")).await.unwrap();
    Runbook {
        full: ObjectsClient::with_interceptor(channel.clone(), Bearer::new(TOKEN).unwrap()),
        partial: ObjectsClient::with_interceptor(channel, Bearer::new(CHILD).unwrap()),
        keys,
        cs,
        hello,
        addr: format!("http://{addr}"),
        metrics_addr,
        stop,
        _stop_metrics: stop_metrics,
        _dir: dir,
    }
}

/// Runs the guide's block off the test's runtime thread: the block
/// curls the same runtime's metrics endpoint, so a blocking run would
/// deadlock the single-threaded test runtime against its own server.
/// The block is the shared helper's: the first fenced sh block under
/// `heading`, with the real metrics address in place of the
/// documented one.
async fn run_guide_block_async(
    heading: &'static str,
    metrics_addr: std::net::SocketAddr,
) -> String {
    let addr = metrics_addr.to_string();
    tokio::task::spawn_blocking(move || {
        scion_server::test_support::docs::run_guide_block(
            heading,
            &[("127.0.0.1:9100", addr.as_str())],
        )
    })
    .await
    .expect("the blocking pool runs the guide's block")
}

/// The calls that put two RPCs' histograms on the wire: an upload and a
/// read of the root tree.
async fn warm_the_histograms(rb: &mut Runbook) {
    let channel = common::channel(&rb.addr).await.unwrap();
    let mut scion = ScionClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());
    scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"# latency runbook\n".to_vec(),
        }))
        .await
        .expect("the token may upload");
    let _ = rb
        .full
        .get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: rb.cs.as_bytes().to_vec(),
        })
        .await
        .expect("the token may read");
}

#[tokio::test]
async fn latency_p99_names_the_slow_rpc_from_the_served_histogram() {
    let mut rb = runbook().await;
    warm_the_histograms(&mut rb).await;

    // The guide's own awk, over the real endpoint, with the documented
    // address swapped for the test's: every RPC that served a call gets
    // a p99 line, and the bucket's bound is one of the histogram's own.
    let stdout = run_guide_block_async("Latency p99 high", rb.metrics_addr).await;
    let mut lines = 0;
    for line in stdout.lines() {
        let (rpc, bound) = line.split_once(" p99 in ").unwrap_or_else(|| {
            panic!("the guide's awk printed a line the runbook does not explain: {line}")
        });
        let bounds = [
            "0.001", "0.004", "0.016", "0.064", "0.256", "1", "4", "16", "64", "+Inf",
        ];
        assert!(
            bounds.contains(&bound),
            "{rpc}: the p99 bucket {bound} is not a bucket bound"
        );
        lines += 1;
    }
    assert!(
        lines >= 2,
        "the guide's awk named {lines} RPCs; the upload and the read served two"
    );
    assert!(stdout.contains("Scion/PutBlob"));
    assert!(stdout.contains("Objects/GetRootTree"));
}

#[tokio::test]
async fn view_memo_serves_the_second_read_of_one_changeset() {
    let mut rb = runbook().await;

    // The first derivation of the partial reader's view walks the real
    // tree: the memo has no row for it yet.
    let _ = rb
        .partial
        .get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: rb.cs.as_bytes().to_vec(),
        })
        .await
        .expect("the partial reader may read its view");
    let first_text = run_guide_block_async("View memo bypassed", rb.metrics_addr).await;
    let after_first: u64 = first_text
        .lines()
        .find_map(|line| line.split_whitespace().last()?.parse().ok())
        .expect("the guide's grep found the walked counter");
    assert!(
        after_first > 0,
        "the first derivation walked nothing: the counter is not on the wire"
    );

    // The second read of the SAME changeset finds the memo's rows and
    // walks nothing — the runbook's warmup case: a steady rate, not a
    // falling one, is the fault.
    let _ = rb
        .partial
        .get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: rb.cs.as_bytes().to_vec(),
        })
        .await
        .expect("the partial reader may read its view again");
    let stdout = run_guide_block_async("View memo bypassed", rb.metrics_addr).await;
    let after_second: u64 = stdout
        .lines()
        .find_map(|line| line.split_whitespace().last()?.parse().ok())
        .expect("the guide's grep found the walked counter");
    assert_eq!(
        after_second, after_first,
        "the second read walked again: the memo did not serve it"
    );
}

/// One `GetObjects` of the runbook's blob through the tier, assembled to
/// its bytes: the call each read of the tier-miss runbook makes.
async fn read_through_tier(tier: &mut Objects, ticket: &[u8], id: BlobId) -> Vec<u8> {
    let refs = vec![ObjectRef {
        kind: i32::from(ObjectKind::Blob),
        id: id.as_bytes().to_vec(),
        path: b"src/hello.rs".to_vec(),
    }];
    let mut stream = common::ceiling(
        "the tier's GetObjects",
        tier.get_objects(GetObjectsRequest {
            refs,
            accept: Vec::new(),
            ticket: ticket.to_vec(),
        }),
    )
    .await
    .expect("the ticket buys the read")
    .into_inner();
    let first = common::ceiling("the first object part", stream.message())
        .await
        .unwrap()
        .unwrap();
    let mut got = Assembler::new(Header::of(&first, false).unwrap(), first);
    while !got.is_done() {
        let next = common::ceiling("the next object part", stream.message())
            .await
            .unwrap()
            .unwrap();
        got.push(&next).unwrap();
    }
    let (header, data) = got.finish();
    object::decode(&header, data).unwrap()
}

#[tokio::test]
async fn tier_miss_ratio_falls_when_the_store_serves_the_second_read() {
    use scion_proto::v1::objects_client::ObjectsClient as TierClient;
    use scion_tier::{MemoryStore, Tier};

    let mut rb = runbook().await;

    // The tier in front of the server: no token of the server's, only
    // its public keys — the ticket buys the read, as section 1's
    // regional cache runs.
    let cache = Tier::new(
        MemoryStore::new(16 << 20),
        &rb.addr,
        "",
        std::time::Duration::from_secs(5),
        None,
        rb.keys.for_verify().to_vec(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cache_addr = listener.local_addr().unwrap();
    let (stop_cache, stopped_cache) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(cache.clone().serve(
        TOKEN,
        tokio_stream::wrappers::TcpListenerStream::new(listener),
        Limits::default(),
        None,
        async {
            stopped_cache.await.ok();
        },
    ));
    // The tier's own metrics endpoint: the block under the tier-miss
    // heading curls the TIER's /metrics, the alert's own subject.
    let tier_metrics = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tier_metrics_addr = tier_metrics.local_addr().unwrap();
    let (stop_tier_metrics, stopped_tier_metrics) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(scion_proto::runtime::metrics::serve(
        tier_metrics,
        cache.metrics(),
        async {
            stopped_tier_metrics.await.ok();
        },
    ));
    let channel = common::channel(&format!("http://{cache_addr}"))
        .await
        .unwrap();
    let mut tier = TierClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap());

    // The ticket from the server; the objects ride the tier.
    let reply = rb
        .full
        .get_root_tree(GetRootTreeRequest {
            repo: REPO.into(),
            changeset_id: rb.cs.as_bytes().to_vec(),
        })
        .await
        .expect("the token may read")
        .into_inner();

    // The first read through an empty tier is all misses.
    let bytes = read_through_tier(&mut tier, &reply.ticket, rb.hello).await;
    assert_eq!(bytes, b"pub fn main() {}\n");

    // The guide's own awk, over the tier's real endpoint: the ratio is
    // 1 — misses against no hits.
    let stdout = run_guide_block_async("Tier miss rate", tier_metrics_addr).await;
    let ratio: f64 = stdout
        .trim()
        .parse()
        .expect("the guide's awk printed the ratio");
    assert!(
        (ratio - 1.0).abs() < f64::EPSILON,
        "the first read's ratio is {ratio}, not 1"
    );

    // The second read of the same object is the store's: the ratio the
    // guide's awk computes falls, which is the warmup case its first
    // branch names.
    let bytes = read_through_tier(&mut tier, &reply.ticket, rb.hello).await;
    assert_eq!(bytes, b"pub fn main() {}\n");
    let stdout = run_guide_block_async("Tier miss rate", tier_metrics_addr).await;
    let ratio: f64 = stdout
        .trim()
        .parse()
        .expect("the guide's awk printed the ratio");
    assert!(
        ratio < 1.0,
        "the ratio never fell: {ratio} after a served read"
    );
    let _ = (rb.stop, stop_cache, stop_tier_metrics);
}

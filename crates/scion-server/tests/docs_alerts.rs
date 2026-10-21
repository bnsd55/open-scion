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
use std::collections::BTreeMap;
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
    metrics: Arc<Metrics>,
    addr: String,
    stop: tokio::sync::oneshot::Sender<()>,
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
        },
        Some(keys.clone().into()),
        async {
            stopped.await.ok();
        },
    ));
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    Runbook {
        full: ObjectsClient::with_interceptor(channel.clone(), Bearer::new(TOKEN).unwrap()),
        partial: ObjectsClient::with_interceptor(channel, Bearer::new(CHILD).unwrap()),
        keys,
        cs,
        hello,
        metrics,
        addr: format!("http://{addr}"),
        stop,
        _dir: dir,
    }
}

/// The `value` of every `name` line of a `/metrics` body, by label set.
/// A bare counter's line has no labels, so its label set is empty.
fn scrape(text: &str, name: &str) -> Vec<(String, u64)> {
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(name)?;
            let rest = rest.strip_suffix('}').unwrap_or(rest);
            let (labels, value) = match rest.trim().split_once(' ') {
                Some((labels, value)) => (labels.trim_end_matches('}').to_owned(), value),
                None => (String::new(), rest.trim()),
            };
            Some((labels, value.parse().ok()?))
        })
        .collect()
}

/// The runbook's per-RPC p99 table: for every RPC that served a call,
/// the first `le` whose cumulative count reaches 99% of its calls — the
/// awk of the guide, in Rust, over the same `/metrics` body.
fn p99_buckets(text: &str) -> BTreeMap<String, String> {
    let mut buckets: BTreeMap<String, Vec<(String, u64)>> = BTreeMap::new();
    for (labels, value) in scrape(text, "scion_request_seconds_bucket{") {
        let rpc = labels
            .split(',')
            .find_map(|kv| kv.strip_prefix("rpc=\"")?.strip_suffix('"'))
            .unwrap()
            .to_owned();
        let le = labels
            .split(',')
            .find_map(|kv| kv.strip_prefix("le=\"")?.strip_suffix('"'))
            .unwrap_or_else(|| panic!("no le in labels: {labels}"))
            .to_owned();
        buckets.entry(rpc).or_default().push((le, value));
    }
    let mut p99 = BTreeMap::new();
    for (rpc, series) in buckets {
        let total = series.last().unwrap().1;
        if total == 0 {
            continue;
        }
        let (le, _) = series
            .iter()
            .find(|(_, count)| count * 100 >= 99 * total)
            .unwrap();
        p99.insert(rpc, le.clone());
    }
    p99
}

/// The calls that put two RPCs' histograms on the wire: an upload and a
/// read of the root tree.
async fn warm_the_histograms(rb: &mut Runbook) {
    let channel = Channel::from_shared(rb.addr.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
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

    // The endpoint serves render() verbatim, so the runbook's awk sees
    // the same body. Every RPC that served a call gets a p99 bucket, and
    // the bucket's bound is one of the histogram's own.
    let text = rb.metrics.render();
    let p99 = p99_buckets(&text);
    let bounds = [
        "0.001", "0.004", "0.016", "0.064", "0.256", "1", "4", "16", "64", "+Inf",
    ];
    for (rpc, le) in &p99 {
        assert!(
            bounds.contains(&le.as_str()),
            "{rpc}: the p99 bucket {le} is not a bucket bound"
        );
    }
    for rpc in ["Scion/PutBlob", "Objects/GetRootTree"] {
        assert!(
            p99.contains_key(rpc),
            "{rpc} served a call but has no p99 bucket"
        );
    }
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
    let after_first = scrape(&rb.metrics.render(), "scion_view_trees_walked_total")
        .into_iter()
        .map(|(_, value)| value)
        .sum::<u64>();
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
    let after_second = scrape(&rb.metrics.render(), "scion_view_trees_walked_total")
        .into_iter()
        .map(|(_, value)| value)
        .sum::<u64>();
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
    let channel = Channel::from_shared(format!("http://{cache_addr}"))
        .unwrap()
        .connect()
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

    // The first read through an empty tier is all misses; the body the
    // runbook's awk reads holds the misses line and no hits line yet.
    let bytes = read_through_tier(&mut tier, &reply.ticket, rb.hello).await;
    assert_eq!(bytes, b"pub fn main() {}\n");
    let text = cache.metrics().render();
    let misses: u64 = scrape(&text, "scion_cache_misses_total")
        .into_iter()
        .map(|(_, value)| value)
        .sum();
    let hits: u64 = scrape(&text, "scion_cache_hits_total")
        .into_iter()
        .map(|(_, value)| value)
        .sum();
    assert!(misses > 0, "the first read fetched nothing upstream");
    assert_eq!(hits, 0, "the first read was served from the empty store");

    // The second read of the same object is the store's: the miss ratio
    // the runbook computes falls, which is the warmup case its first
    // branch names.
    let _ = read_through_tier(&mut tier, &reply.ticket, rb.hello).await;
    let text = cache.metrics().render();
    let misses: u64 = scrape(&text, "scion_cache_misses_total")
        .into_iter()
        .map(|(_, value)| value)
        .sum();
    let hits: u64 = scrape(&text, "scion_cache_hits_total")
        .into_iter()
        .map(|(_, value)| value)
        .sum();
    assert!(hits > 0, "the second read was not served from the store");
    // The ratio the runbook computes is misses over all reads: one hit
    // among them is what makes it fall.
    assert!(
        misses > 0 && hits < misses + hits,
        "the ratio never falls: {misses} misses against {hits} hits"
    );
    let _ = (rb.stop, stop_cache);
}

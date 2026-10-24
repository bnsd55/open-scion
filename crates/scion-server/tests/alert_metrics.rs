//! Every `scion_*` metric the alert rules name must be served by the
//! binary that exports it, so a rename cannot silently kill an alert.
//! The rules file is read from the repository; the names are checked
//! against a real server's `/metrics` (the same render the endpoint
//! returns) after the calls that make each metric exist, and against a
//! real tier's registry for the tier's cache counters. The gc
//! collector's metrics are the node exporter's textfile, not a
//! `/metrics` of either binary, and the Postgres metrics render only
//! on a Postgres backend, where the postgres CI job runs this test.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, LandRequest, PutBlobRequest};

use scion_server::Access;
use std::collections::BTreeMap;

use scion_server::Keys;
use scion_server::Metrics;
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::{Backend, test_support::service};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::{Request, Status};

const REPO: &str = "main";

#[derive(Clone)]
struct Bearer(tonic::metadata::AsciiMetadataValue);

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

fn access() -> Access {
    let digest = token_digest("ci");
    let digest = blake3::Hash::from(digest).to_hex().to_string();
    let text = format!("token\tci\t{digest}\nread\tci\t{REPO}\nwrite\tci\t{REPO}\n");
    Access::parse(&text).expect("the access file parses")
}

async fn served() -> (
    tempfile::TempDir,
    tokio::sync::oneshot::Sender<()>,
    Arc<Metrics>,
    ScionClient<InterceptedService<tonic::transport::Channel, Bearer>>,
    scion_core::ChangesetId,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = {
        let path = dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || Backend::open_test(&path).unwrap())
            .await
            .unwrap()
    };
    let root = store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(
                repo,
                &scion_core::Changeset::new(
                    vec![],
                    scion_core::IdentityTime::new(
                        b"Ada".to_vec(),
                        b"ada@example.com".to_vec(),
                        1_700_000_000,
                        0,
                    )
                    .expect("identity fits the format"),
                    None,
                    b"root\n".to_vec(),
                    BTreeMap::new(),
                )
                .expect("changeset fits the format"),
            )?;
            c.cas_ref(repo, "main", None, root)?;
            Ok(root)
        })
        .expect("the test store opens a write");
    let metrics = Arc::new(Metrics::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(service::serve(
        store,
        listener,
        access(),
        service::ServeOptions {
            data: dir.path().to_path_buf(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            audit: scion_server::test_support::audit::Audit::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::clone(&metrics),
            reply_memory_budget: scion_server::REPLY_MEMORY_BUDGET,
        },
        Some(Arc::new(Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let bearer = Bearer(
        "Bearer ci"
            .parse()
            .expect("the header value is visible ASCII"),
    );
    let scion = ScionClient::with_interceptor(channel, bearer);
    (dir, stop, metrics, scion, root)
}

/// Every `scion_*` metric name the alert rules use, with a histogram's
/// `_bucket`/`_sum`/`_count` parts folded into the metric's own name.
fn rules_metric_names() -> Vec<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/prometheus/openscion-alerts.yml");
    let text = std::fs::read_to_string(path).expect("the alert rules are in the repo");
    let mut names: Vec<String> = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(found) = text[i..].find("scion_") {
        let start = i + found;
        let mut end = start + "scion_".len();
        while end < bytes.len()
            && (bytes[end].is_ascii_lowercase()
                || bytes[end].is_ascii_digit()
                || bytes[end] == b'_')
        {
            end += 1;
        }
        let name = text[start..end].to_owned();
        let name = name
            .strip_suffix("_bucket")
            .or_else(|| name.strip_suffix("_sum"))
            .or_else(|| name.strip_suffix("_count"))
            .unwrap_or(&name)
            .to_owned();
        if !names.contains(&name) {
            names.push(name);
        }
        i = end;
    }
    names.sort();
    names
}

#[tokio::test]
async fn every_metric_the_alert_rules_name_is_served() {
    let (_dir, _stop, metrics, mut scion, root) = served().await;

    // A real upload puts the blob-sync histogram and the byte counters
    // on the wire, and the free-space poll runs before its first sleep.
    scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"# alert metrics\n".to_vec(),
        }))
        .await
        .expect("the token may upload");

    // Two changesets planned on the same base, landed in order: the
    // second's ref moved under it before it planned, which replans it.
    let mut lands: Vec<LandRequest> = Vec::new();
    for (n, content) in ["one", "two"].iter().enumerate() {
        let _ = scion
            .put_blob(tokio_stream::once(PutBlobRequest {
                repo: REPO.into(),
                data: content.as_bytes().to_vec(),
            }))
            .await
            .expect("the token may upload");
        let cs = scion_core::Changeset::new(
            vec![root],
            scion_core::IdentityTime::new(
                b"Ada".to_vec(),
                b"ada@example.com".to_vec(),
                1_700_000_000,
                0,
            )
            .expect("identity fits the format"),
            None,
            format!("{content}\n").into_bytes(),
            [(
                scion_core::RepoPath::new(format!("ALERTS{n}.md")).unwrap(),
                Some(scion_core::FileState::Regular {
                    content: scion_core::BlobId::of(content.as_bytes()),
                    executable: false,
                }),
            )]
            .into(),
        )
        .expect("changeset fits the format");
        let (bytes, id) = cs.encode_with_id();
        scion
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await
            .expect("the token may create changesets")
            .into_inner();
        lands.push(LandRequest {
            repo: REPO.into(),
            changeset_id: id.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: format!("alert-metrics-{content}"),
        });
    }
    // The lands race: the land whose ref moved under it between its
    // plan and its apply replans, which is the metric's whole event.
    let mut scion2 = scion.clone();
    let (a, b) = tokio::join!(scion.land(lands[0].clone()), scion2.land(lands[1].clone()));
    let _ = a.expect("the token may land").into_inner();
    let _ = b.expect("the token may land").into_inner();

    // The endpoint serves render() verbatim, so the render is the
    // /metrics body a scraper sees.
    // The rest of the names belong to surfaces this test does not
    // scrape: the cache counters are a tier's registry, the Postgres
    // metrics a Postgres backend's render (served_metrics.rs covers
    // the collector key there), and a replan needs an import to move
    // the ref between a land's plan and its settle, which only a hook
    // can time, and a stalled reply needs a client that stops reading.
    // A rename in either the rules or the exporter must fail
    // loudly, so each of those names is pinned to the exporter's
    // source: the shared renderer for the tier and Postgres names, the
    // gc collector's script in main.rs for the gc names.
    let gc = |name: &str| name.starts_with("scion_gc_");
    let elsewhere = |name: &str| {
        name.starts_with("scion_cache_")
            || name.starts_with("scion_pg_")
            || name == "scion_land_replans_total"
            || name == "scion_stalled_reply_bytes"
    };
    // A pin holds when the source writes the name as a Help line's
    // label, a quoted render call's argument, or a textfile record's
    // key.
    let pins = |name: &str, source: &str| {
        source.contains(&format!("{name} "))
            || source.contains(&format!("{name}\""))
            || source.contains(&format!("{name}{{"))
    };
    let text = metrics.render();
    for name in rules_metric_names() {
        if gc(&name) {
            let script =
                std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"))
                    .expect("the gc collector is in the repo");
            assert!(
                pins(&name, &script),
                "the alert rules name {name}, but the gc collector does not write it"
            );
            continue;
        }
        if elsewhere(&name) {
            let wired = std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../scion-proto/src/runtime/metrics.rs"
            ))
            .expect("the metrics renderer is in the repo");
            assert!(
                pins(&name, &wired),
                "the alert rules name {name}, but the metrics renderer does not know it"
            );
            continue;
        }
        assert!(
            text.contains(&format!("{name} ")),
            "the alert rules name {name}, but the served server's /metrics does not render it:\n{text}"
        );
    }
}

#[path = "common/mod.rs"]
mod common;

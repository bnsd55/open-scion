//! A served server records the operating metrics: a real `PutBlob` shows
//! in the blob-sync histogram, the free-space gauge is fresh, and a
//! Postgres write shows its wait for the collector's key. This is the
//! end-to-end check that the wiring reached the backend, which the unit
//! tests of each set-point cannot say.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest};

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
    String,
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
            audit: scion_server::test_support::audit::Audit::off(),
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
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
    (dir, stop, format!("http://{addr}"), metrics, scion, root)
}

#[tokio::test]
async fn a_served_server_renders_the_operating_metrics() {
    let (_dir, _stop, _url, metrics, mut scion, root) = served().await;
    // A real upload ends in a full flush of the drive cache.
    scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"# served metrics\n".to_vec(),
        }))
        .await
        .expect("the token may upload");
    // The upload fsynced its temp file and the blob directories, on
    // every platform, and each fsync is one observation.
    let mut spins = 0;
    loop {
        let count = metrics
            .render()
            .lines()
            .find(|line| line.starts_with("scion_blob_sync_seconds_count "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        if count >= 1 {
            break;
        }
        spins += 1;
        assert!(spins < 1_000_000, "the blob sync never rendered a fsync");
        tokio::task::yield_now().await;
    }
    // The free-space poll runs before its first sleep.
    let text = metrics.render();
    assert!(text.contains("scion_data_free_bytes "), "{text}");
    // On Postgres, the changeset's write transaction waited for the
    // collector's key, and the wait is in the histogram.
    // On Postgres, a write transaction waits for the collector's key,
    // and the wait is in the histogram. The changeset names the blob the
    // upload above stored.
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
        b"metrics\n".to_vec(),
        [(
            scion_core::RepoPath::new("METRICS.md").unwrap(),
            Some(scion_core::FileState::Regular {
                content: scion_core::BlobId::of(b"# served metrics\n"),
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    if Backend::test_is_postgres() {
        let (bytes, _id) = cs.encode_with_id();
        let _ = scion
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await
            .expect("the token may create a changeset");
        // The seed's writes waited too, so the count is at least the
        // changeset's own one wait.
        let text = metrics.render();
        let count = text
            .lines()
            .find(|line| line.starts_with("scion_pg_collector_key_wait_seconds_count "))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|n| n.parse::<u64>().ok())
            .unwrap_or(0);
        assert!(count >= 1, "{text}");
    }
}

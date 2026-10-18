//! A served server records the operating metrics: a real `PutBlob` shows
//! in the blob-sync histogram, the free-space gauge is fresh, and a
//! Postgres write shows its wait for the collector's key. This is the
//! end-to-end check that the wiring reached the backend, which the unit
//! tests of each set-point cannot say.

use scion_proto::auth::token_digest;
use scion_proto::listen::Limits;
use scion_proto::v1::scion_client::ScionClient;
use scion_proto::v1::{CreateChangesetRequest, PutBlobRequest};

use scion_server::access::Access;
use scion_server::meta::Meta as _;
use scion_server::metrics::Metrics;
use scion_server::tickets::Keys;
use scion_server::{Backend, service};
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
) {
    let dir = tempfile::tempdir().unwrap();
    let store = {
        let path = dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || Backend::open_test(&path).unwrap())
            .await
            .unwrap()
    };
    store
        .write(|c| c.create_repo(REPO).map(|_| ()))
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
            quotas: scion_server::quotas::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::clone(&metrics),
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
    (dir, stop, format!("http://{addr}"), metrics, scion)
}

#[tokio::test]
async fn a_served_server_renders_the_operating_metrics() {
    let (_dir, _stop, _url, metrics, mut scion) = served().await;
    // A real upload ends in a full flush of the drive cache.
    scion
        .put_blob(tokio_stream::once(PutBlobRequest {
            repo: REPO.into(),
            data: b"# served metrics\n".to_vec(),
        }))
        .await
        .expect("the token may upload");
    // The free-space poll runs before its first sleep.
    let mut spins = 0;
    while !metrics
        .render()
        .contains("scion_blob_sync_seconds_count 1\n")
    {
        spins += 1;
        assert!(spins < 1_000_000, "the blob sync never rendered the flush");
        tokio::task::yield_now().await;
    }
    let text = metrics.render();
    assert!(text.contains("scion_data_free_bytes "), "{text}");
    // On Postgres, the changeset's write transaction waited for the
    // collector's key, and the wait is in the histogram.
    // On Postgres, a write transaction waits for the collector's key,
    // and the wait is in the histogram. The changeset names the blob the
    // upload above stored.
    let cs = scion_core::Changeset {
        parents: vec![],
        author: scion_core::IdentityTime {
            name: b"Ada".to_vec(),
            email: b"ada@example.com".to_vec(),
            unix_secs: 1_700_000_000,
            tz_offset_minutes: 0,
        },
        committer: None,
        message: b"metrics\n".to_vec(),
        changes: [(
            scion_core::RepoPath::new("METRICS.md").unwrap(),
            Some(scion_core::FileState::Regular {
                content: scion_core::BlobId::of(b"# served metrics\n"),
                executable: false,
            }),
        )]
        .into(),
    };
    if Backend::test_is_postgres() {
        let (bytes, _id) = cs.encode_with_id();
        let _ = scion
            .create_changeset(CreateChangesetRequest {
                repo: REPO.into(),
                changeset: bytes,
            })
            .await
            .expect("the token may create a changeset");
        let text = metrics.render();
        assert!(
            text.contains("scion_pg_collector_key_wait_seconds_count 1\n"),
            "{text}"
        );
    }
}

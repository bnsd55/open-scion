//! The audit log of a served server: refused authentication makes one
//! counted line per peer, never a line per call, and no line ever
//! carries the refused token or its digest.

use scion_proto::runtime::auth::token_digest;
use scion_proto::runtime::listen::Limits;
use scion_proto::v1::ResolveRefRequest;
use scion_proto::v1::scion_client::ScionClient;

use scion_server::test_support::audit::{Audit, Event};
use scion_server::test_support::meta::{Meta as _, MetaTx as _};
use scion_server::test_support::service;
use scion_server::{Access, Backend, Keys, Metrics, serve};
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::service::interceptor::{InterceptedService, Interceptor};
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

const REPO: &str = "main";

/// Adds `authorization: Bearer <token>` to every request.
#[derive(Clone)]
struct Bearer(Option<tonic::metadata::AsciiMetadataValue>);

impl Interceptor for Bearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = self.0.clone() {
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

type Scion = ScionClient<InterceptedService<Channel, Bearer>>;

fn access() -> Access {
    let digest = blake3::Hash::from(token_digest("ci")).to_hex().to_string();
    let text = format!("token\tci\t{digest}\nread\tci\t{REPO}\n");
    Access::parse(&text).expect("the access file parses")
}

/// A store whose repo `main` has one root changeset that puts `README`,
/// and its blob.
fn store(dir: &std::path::Path) -> Backend {
    let store = Backend::open_test(dir).unwrap();
    let blob = store.blobs().put(b"# hello\n").unwrap();
    let cs = scion_core::Changeset::new(
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
        [(
            scion_core::RepoPath::new("README").unwrap(),
            Some(scion_core::FileState::Regular {
                content: blob,
                executable: false,
            }),
        )]
        .into(),
    )
    .expect("changeset fits the format");
    store
        .write(|c| {
            let repo = c.create_repo(REPO)?;
            let root = c.insert_changeset(repo, &cs)?;
            c.cas_ref(repo, "main", None, root)?;
            c.add_repo_blob(repo, blob)?;
            Ok(root)
        })
        .unwrap();
    store
}

/// The server of `store` with a real audit file, as a `http://` URL.
/// Hold the stop signal, or the server shuts down when it drops.
async fn serve_with_audit(
    store: &Backend,
    dir: &std::path::Path,
    access: Access,
    audit: Audit,
) -> (tokio::sync::oneshot::Sender<()>, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(serve(
        store.clone(),
        listener,
        access,
        service::ServeOptions {
            data: dir.to_path_buf(),
            audit,
            limits: Limits::default(),
            quotas: scion_server::Quotas::off(),
            pending_lands: None,
            land_stale_secs: 1 << 40,
            tls: None,
            metrics: Arc::new(Metrics::default()),
        },
        Some(Arc::new(Keys::generate())),
        async {
            stopped.await.ok();
        },
    ));
    (stop, format!("http://{addr}"))
}

async fn client(url: &str, token: &str) -> Scion {
    let channel = Channel::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let value = format!("Bearer {token}")
        .parse()
        .expect("a bearer header parses");
    ScionClient::with_interceptor(channel, Bearer(Some(value)))
}

fn read_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
    let bytes = std::fs::read(path).unwrap();
    String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[tokio::test]
async fn refused_authentication_is_one_counted_line_per_peer_and_names_no_token() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(dir.path());
    let audit_path = dir.path().join("audit.log");
    let audit = Audit::open(&audit_path).unwrap();
    audit
        .record(Event::new(
            "admin",
            "ok",
            serde_json::json!({ "what": "start" }),
        ))
        .await
        .unwrap();
    let (stop, url) = serve_with_audit(&store, dir.path(), access(), audit).await;

    // Two peers, each with a stream of refused calls. The wrong token
    // is a secret: no line may ever carry it.
    let wrong = "wrong-token-9f2c7a";
    let (a, b) = tokio::join!(refused_stream(&url, wrong), refused_stream(&url, wrong));
    assert_eq!(a, Code::Unauthenticated);
    assert_eq!(b, Code::Unauthenticated);

    // The stop flushes the meter's counts before the server ends.
    stop.send(()).unwrap();
    wait_for_lines(&audit_path, 2).await;

    let lines = read_lines(&audit_path);
    assert_eq!(lines[0]["event"], "admin");
    let refusals: Vec<_> = lines[1..]
        .iter()
        .filter(|line| line["event"] == "auth" && line["outcome"] == "refused")
        .collect();
    // Both connections come from this test process, so one address:
    // one counted line, with every refusal on it. One line per peer,
    // never per call, and a port rotation cannot bury anyone.
    assert_eq!(refusals.len(), 1, "one line per address, not per call");
    assert_eq!(refusals[0]["count"], 40, "every refusal is counted");
    assert_eq!(refusals[0]["peer"], "127.0.0.1");
    let bytes = std::fs::read(&audit_path).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains(wrong), "the refused token never appears");
    let digest = blake3::Hash::from(token_digest(wrong)).to_hex().to_string();
    assert!(
        !text.contains(&digest),
        "the refused token's digest never appears"
    );
}

/// Twenty refused calls from one connection.
async fn refused_stream(url: &str, token: &str) -> Code {
    let mut client = client(url, token).await;
    let mut code = None;
    for _ in 0..20 {
        let err = client
            .resolve_ref(ResolveRefRequest {
                repo: REPO.into(),
                name: "main".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
        code = Some(err.code());
    }
    code.expect("twenty calls made")
}

/// Waits until the file holds `n` lines: the stop's flush runs on its
/// own task, so the lines land a moment after the stop.
async fn wait_for_lines(path: &std::path::Path, n: usize) {
    for _ in 0..100 {
        if std::fs::read(path)
            .is_ok_and(|bytes| String::from_utf8_lossy(&bytes).lines().count() >= n)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let bytes = std::fs::read(path).unwrap_or_default();
    panic!(
        "the audit file never reached {n} lines: {:?}",
        String::from_utf8_lossy(&bytes)
    );
}

//! One request ID rides from the caller through a daemon and a cache to
//! the upstream, and both tiers log it on their `GetObjects` line, so one
//! command is followable through every hop's log.

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use hyper_util::rt::TokioIo;
use scion_core::{BlobId, PathElem, Tree};
use scion_proto::Bytes;
use scion_proto::runtime::request_id;
use scion_proto::ticket;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use tempfile::TempDir;
use tonic::transport::{Endpoint, Server, Uri};
use tonic::{Request, Response, Status};
use tower::service_fn;

/// The request ID this test sends, and looks for in both tier logs.
const ID: &str = "0123456789abcdef";

/// A fake upstream of one blob at `f`, whose tickets its key signs. It
/// keeps the request IDs of its `GetObjects` calls.
#[derive(Clone)]
struct Fake {
    parts: Arc<Mutex<HashMap<Vec<u8>, Bytes>>>,
    ids: Arc<Mutex<Vec<String>>>,
    key: SigningKey,
    root: scion_core::TreeId,
    blob: Vec<u8>,
}

impl Fake {
    fn new() -> Self {
        let data = b"ride one id\n";
        let blob = BlobId::of(data);
        let (tree_bytes, root) = Tree::new(
            [(
                PathElem::new(b"f".to_vec()).unwrap(),
                scion_core::Entry::File(scion_core::FileEntry::new(
                    scion_core::FileState::Regular {
                        content: blob,
                        executable: false,
                    },
                    data.len() as u64,
                )),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap()
        .encode_with_id();
        let parts = HashMap::from([
            (root.as_bytes().to_vec(), Bytes::from(tree_bytes)),
            (blob.as_bytes().to_vec(), Bytes::from(data.to_vec())),
        ]);
        Self {
            parts: Arc::new(Mutex::new(parts)),
            ids: Arc::new(Mutex::new(Vec::new())),
            key: SigningKey::from_bytes(&[7u8; 32]),
            root,
            blob: blob.as_bytes().to_vec(),
        }
    }

    /// The signed ticket for the root tree, in repo `r`.
    fn ticket(&self) -> Vec<u8> {
        ticket::sign(&self.key, "r", "test", *self.root.as_bytes(), ticket::now()).unwrap()
    }

    /// The hex of the public key that verifies its tickets.
    fn public_key_hex(&self) -> String {
        hex_upper(self.key.verifying_key().as_bytes())
    }
}

fn hex_upper(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[tonic::async_trait]
impl Objects for Fake {
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented("the fake has no refs"))
    }

    async fn get_root_tree(
        &self,
        _: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        Err(Status::unimplemented("the fake has no refs"))
    }

    type GetObjectsStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<ObjectPart, Status>> + Send>>;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        if let Some(id) = request.metadata().get(request_id::REQUEST_ID) {
            self.ids.lock().unwrap().push(id.to_str().unwrap().into());
        }
        let refs = request.into_inner().refs;
        let parts = self.parts.lock().unwrap().clone();
        let mut out = Vec::new();
        for r in refs {
            let data = parts.get(&r.id).cloned().unwrap_or_default();
            let len = data.len() as u64;
            out.push(Ok(ObjectPart {
                kind: r.kind,
                id: r.id,
                data,
                encoding: i32::from(Encoding::Raw),
                encoded_size: len,
                size: len,
            }));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(out))))
    }
}

/// A started real binary, killed when the handle drops.
struct Bin {
    child: Child,
}

impl Drop for Bin {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// Starts `bin` with its output in `log`, uncolored.
fn spawn(bin: &str, args: &[&str], log: &PathBuf) -> Bin {
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let child = Command::new(bin)
        .args(args)
        .env("NO_COLOR", "1")
        // GetObjects logs at DEBUG (its per-call line at INFO was
        // 7.7 GB in the soak's 66 h), and the id rides that line.
        .env("RUST_LOG", "debug")
        .stdout(file.try_clone().unwrap())
        .stderr(file)
        .spawn()
        .unwrap();
    Bin { child }
}

fn wait_for(what: &str, mut check: impl FnMut() -> Option<String>) -> String {
    let started = std::time::Instant::now();
    loop {
        if let Some(found) = check() {
            return found;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The fake upstream, the real cache in front of it, and the real daemon
/// in front of the cache, with a client on the daemon's socket. The logs
/// are `logs/cache.log` and `logs/daemon.log`; the returned handles keep
/// the two processes alive.
async fn chain(fake: &Fake) -> (ObjectsClient<tonic::transport::Channel>, TempDir, Bin, Bin) {
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let served = fake.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ObjectsServer::new(served))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(upstream))
            .await
            .unwrap();
    });

    let logs = TempDir::new().unwrap();
    let cache_log = logs.path().join("cache.log");
    let cache_bin = spawn(
        env!("CARGO_BIN_EXE_scion-cache"),
        &[
            "--listen",
            "127.0.0.1:0",
            "--token",
            "t",
            "--upstream-token",
            "t",
            "--upstream",
            &format!("http://{upstream_addr}"),
            "--ticket-public-key",
            &fake.public_key_hex(),
        ],
        &cache_log,
    );
    let cache_addr = wait_for("the cache's serving line", || {
        let text = fs::read_to_string(&cache_log).ok()?;
        let line = text.lines().find(|line| line.contains("serving"))?;
        let rest = line.split("listen=").nth(1)?;
        rest.split_whitespace().next()?.parse().ok()
    });

    let daemon_dir = TempDir::new().unwrap();
    fs::set_permissions(daemon_dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let daemon_log = logs.path().join("daemon.log");
    let daemon_bin = spawn(
        env!("CARGO_BIN_EXE_scion-daemon"),
        &[
            "--data",
            &daemon_dir.path().join("data").display().to_string(),
            "--upstream",
            &format!("http://{cache_addr}"),
            "--token",
            "t",
            "--upstream-token",
            "t",
        ],
        &daemon_log,
    );
    let socket = wait_for("the daemon's serving line", || {
        let text = fs::read_to_string(&daemon_log).ok()?;
        let line = text.lines().find(|line| line.contains("serving"))?;
        let rest = line.split("socket=").nth(1)?;
        Some(rest.split_whitespace().next()?.to_owned())
    });

    let channel = Endpoint::try_from("http://daemon")
        .unwrap()
        // A daemon that died must not leave the test dialing forever.
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(socket).await?;
                Ok::<_, std::io::Error>(TokioIo::new(stream))
            }
        }))
        .await
        .unwrap();
    (ObjectsClient::new(channel), logs, cache_bin, daemon_bin)
}

#[tokio::test]
async fn one_request_id_shows_in_both_tier_logs() {
    let fake = Fake::new();
    let (mut client, logs, _cache, _daemon) = chain(&fake).await;

    let mut request = Request::new(GetObjectsRequest {
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: fake.blob.clone(),
            path: b"f".to_vec(),
        }],
        accept: Vec::new(),
        ticket: fake.ticket(),
    });
    request
        .metadata_mut()
        .insert(request_id::REQUEST_ID, ID.parse().unwrap());
    request
        .metadata_mut()
        .insert("authorization", "Bearer t".parse().unwrap());
    let mut stream = client.get_objects(request).await.unwrap().into_inner();
    let mut bytes = 0;
    while let Some(part) = tokio_stream::StreamExt::next(&mut stream).await {
        bytes += part.unwrap().data.len();
    }
    assert!(bytes > 0, "the reply carried the blob");

    // The same ID in both tier logs, and on the fetch the cache sent
    // upstream.
    let needle = format!("request_id={ID}");
    wait_for("the id in both tier logs", || {
        let in_daemon = fs::read_to_string(logs.path().join("daemon.log"))
            .unwrap_or_default()
            .contains(&needle);
        let in_cache = fs::read_to_string(logs.path().join("cache.log"))
            .unwrap_or_default()
            .contains(&needle);
        (in_daemon && in_cache).then(|| "both".to_owned())
    });
    assert!(
        fake.ids.lock().unwrap().contains(&ID.to_string()),
        "the cache forwarded the request ID upstream"
    );
}

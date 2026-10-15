//! A tier against a fake upstream: it checks what upstream sends, fetches a
//! shared miss once, and refuses callers without the token.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use scion_core::{BlobId, Entry, FileState, PathElem, Tree};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, Object, ObjectKind, ObjectRef,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_tier::{DiskStore, MemoryStore, Tier, bind_unix};
use tokio::net::TcpListener;
use tokio_stream::Stream;
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status};

const TOKEN: &str = "secret";

/// An upstream that sends fixed objects, each as given (so possibly with the
/// wrong bytes), after `delay`, and counts the `GetObjects` calls.
#[derive(Clone, Default)]
struct Fake {
    objects: Arc<HashMap<Vec<u8>, Object>>,
    delay: Duration,
    calls: Arc<AtomicUsize>,
}

impl Fake {
    fn new(objects: &[&Object], delay: Duration) -> Self {
        let objects = objects.iter().map(|o| (o.id.clone(), (*o).clone()));
        Self {
            objects: Arc::new(objects.collect()),
            delay,
            calls: Arc::default(),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

type Replies = Pin<Box<dyn Stream<Item = Result<Object, Status>> + Send>>;

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
        Err(Status::unimplemented("the fake has no changesets"))
    }

    type GetObjectsStream = Replies;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Replies>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        let replies: Vec<_> = request
            .into_inner()
            .refs
            .iter()
            .map(|r| Ok(self.objects[&r.id].clone()))
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
    }
}

async fn serve_fake(fake: Fake) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        Server::builder()
            .add_service(ObjectsServer::new(fake))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    url
}

/// A cache in front of `upstream`, and its address.
async fn serve_cache(upstream: &str) -> (Tier<MemoryStore>, String) {
    let cache = Tier::new(MemoryStore::new(1 << 20), upstream, TOKEN).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(cache.clone().serve(TOKEN, incoming, std::future::pending()));
    (cache, url)
}

type Client = ObjectsClient<InterceptedService<Channel, Box<dyn Fn(Request<()>) -> Bearer + Send>>>;
type Bearer = Result<Request<()>, Status>;

/// A client that sends `token`, or no token.
async fn connect(url: &str, token: Option<&str>) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let header: Option<MetadataValue<Ascii>> =
        token.map(|token| format!("Bearer {token}").parse().unwrap());
    let auth: Box<dyn Fn(Request<()>) -> Bearer + Send> = Box::new(move |mut request| {
        if let Some(header) = &header {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
        }
        Ok(request)
    });
    ObjectsClient::with_interceptor(channel, auth)
}

/// The objects a `GetObjects` call returns, or the error it ends with.
async fn get(client: &mut Client, objects: &[&Object]) -> Result<Vec<Object>, Status> {
    let refs = objects
        .iter()
        .map(|o| ObjectRef {
            kind: o.kind,
            id: o.id.clone(),
        })
        .collect();
    let request = GetObjectsRequest {
        repo: "r".into(),
        refs,
    };
    let mut stream = client.get_objects(request).await?.into_inner();
    let mut got = Vec::new();
    while let Some(object) = stream.message().await? {
        got.push(object);
    }
    Ok(got)
}

fn blob(data: &[u8]) -> Object {
    Object {
        kind: ObjectKind::Blob.into(),
        id: BlobId::of(data).as_bytes().to_vec(),
        data: data.to_vec(),
    }
}

#[tokio::test]
async fn a_tier_refuses_an_object_whose_bytes_do_not_match_its_id() {
    let good = blob(b"good\n");
    let bad_blob = Object {
        data: b"evil\n".to_vec(),
        ..blob(b"bad\n")
    };
    // A valid tree, sent for the ID of another one.
    let tree = Tree {
        entries: [(
            PathElem::new("f").unwrap(),
            Entry::File(FileState::Regular {
                content: BlobId::of(b"good\n"),
                executable: false,
            }),
        )]
        .into(),
    };
    let bad_tree = Object {
        kind: ObjectKind::Tree.into(),
        id: tree.id().as_bytes().to_vec(),
        data: Tree::default().encode(),
    };
    let fake = Fake::new(&[&good, &bad_blob, &bad_tree], Duration::ZERO);
    let upstream = serve_fake(fake.clone()).await;
    let (_cache, url) = serve_cache(&upstream).await;
    let mut client = connect(&url, Some(TOKEN)).await;

    // A good object passes and is kept, so the second call stays in the cache.
    for _ in 0..2 {
        assert_eq!(
            get(&mut client, &[&good]).await.unwrap(),
            vec![good.clone()]
        );
    }
    assert_eq!(fake.calls(), 1);

    for wrong in [&bad_blob, &bad_tree] {
        let before = fake.calls();
        for _ in 0..2 {
            let status = get(&mut client, &[wrong]).await.unwrap_err();
            assert_eq!(status.code(), Code::DataLoss, "{status:?}");
        }
        // Not kept: each call asked upstream again.
        assert_eq!(fake.calls(), before + 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_for_one_object_share_one_fetch() {
    let object = blob(b"wanted by every job\n");
    // Slow, so every client misses while the first fetch runs.
    let fake = Fake::new(&[&object], Duration::from_millis(300));
    let upstream = serve_fake(fake.clone()).await;
    let (cache, url) = serve_cache(&upstream).await;

    let mut clients = Vec::new();
    for _ in 0..10 {
        let mut client = connect(&url, Some(TOKEN)).await;
        let object = object.clone();
        clients.push(tokio::spawn(
            async move { get(&mut client, &[&object]).await },
        ));
    }
    for client in clients {
        assert_eq!(client.await.unwrap().unwrap(), vec![object.clone()]);
    }
    assert_eq!(fake.calls(), 1);
    assert_eq!(cache.upstream_calls(), 1);
}

#[tokio::test]
async fn each_tier_refuses_a_call_without_the_token() {
    let fake = Fake::default();
    let upstream = serve_fake(fake.clone()).await;
    let (cache, cache_url) = serve_cache(&upstream).await;
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("daemon.sock");
    let store = DiskStore::open(&dir.path().join("data"), 1 << 20).unwrap();
    let daemon = Tier::new(store, &cache_url, TOKEN).unwrap();
    let incoming = UnixListenerStream::new(bind_unix(&socket).await.unwrap());
    tokio::spawn(
        daemon
            .clone()
            .serve(TOKEN, incoming, std::future::pending()),
    );

    // Only this user can connect to the daemon, and only one daemon listens.
    let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    let taken = bind_unix(&socket).await.unwrap_err();
    assert_eq!(taken.kind(), std::io::ErrorKind::AddrInUse);

    let daemon_url = format!("unix://{}", socket.display());
    for url in [&cache_url, &daemon_url] {
        for token in [None, Some("wrong")] {
            let mut client = connect(url, token).await;
            let resolve = ResolveRefRequest {
                repo: "r".into(),
                name: "main".into(),
            };
            let status = client.resolve_ref(resolve).await.unwrap_err();
            assert_eq!(status.code(), Code::Unauthenticated, "{url} {token:?}");
            let status = get(&mut client, &[&blob(b"x")]).await.unwrap_err();
            assert_eq!(status.code(), Code::Unauthenticated, "{url} {token:?}");
        }
    }
    assert_eq!(daemon.upstream_calls(), 0);
    assert_eq!(cache.upstream_calls(), 0);

    // With the token, the call gets through both tiers to the fake, which
    // has no refs.
    let mut client = connect(&daemon_url, Some(TOKEN)).await;
    let resolve = ResolveRefRequest {
        repo: "r".into(),
        name: "main".into(),
    };
    let status = client.resolve_ref(resolve).await.unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
    assert_eq!((daemon.upstream_calls(), cache.upstream_calls()), (1, 1));
}

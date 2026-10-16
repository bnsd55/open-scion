//! A caller's deadline through the tiers: each tier waits no longer than
//! what is left of it, and passes the rest upstream, so the caller gets the
//! status of the deepest tier, never a timeout of a tier in the middle.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use scion_core::BlobId;
use scion_proto::auth::Bearer;
use scion_proto::deadline::Until;
use scion_proto::listen::Limits;
use scion_proto::object::{self, Header};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{Bytes, MAX_PART_MESSAGE_BYTES};
use scion_tier::{MemoryStore, Tier};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_stream::Stream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status};

const TOKEN: &str = "secret";

/// The deepest tier, as the server behaves when it is busy: it finds room
/// for an object only after `ready`, and waits for room at most `busy`, and
/// not past its caller's deadline, less its margin. Then it fails with
/// `RESOURCE_EXHAUSTED`, as the server does when it has no memory for an
/// object.
#[derive(Clone)]
struct Deepest {
    data: Bytes,
    ready: Duration,
    busy: Duration,
    calls: Arc<AtomicUsize>,
    called: Arc<Notify>,
}

impl Deepest {
    fn new(ready: Duration, busy: Duration) -> Self {
        Self {
            data: Bytes::from(vec![7; 1000]),
            ready,
            busy,
            calls: Arc::default(),
            called: Arc::default(),
        }
    }

    fn id(&self) -> BlobId {
        BlobId::of(&self.data)
    }
}

type Parts = Pin<Box<dyn Stream<Item = Result<ObjectPart, Status>> + Send>>;

#[tonic::async_trait]
impl Objects for Deepest {
    async fn resolve_ref(
        &self,
        _: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        Err(Status::unimplemented("not here"))
    }

    async fn get_root_tree(
        &self,
        _: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        Err(Status::unimplemented("not here"))
    }

    type GetObjectsStream = Parts;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Parts>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.called.notify_one();
        let wait = Until::of(&request).wait(self.busy);
        if wait < self.ready {
            tokio::time::sleep(wait).await;
            return Err(Status::resource_exhausted("no room for the object"));
        }
        tokio::time::sleep(self.ready).await;
        let header = Header {
            kind: ObjectKind::Blob,
            id: *self.id().as_bytes(),
            encoding: Encoding::Raw,
            encoded_size: self.data.len(),
            size: self.data.len(),
        };
        let parts = header.parts(object::read_chunks(&self.data[..], self.data.len()).unwrap());
        Ok(Response::new(Box::pin(tokio_stream::iter(
            parts.into_iter().map(Ok),
        ))))
    }
}

/// `deepest` on a port of its own, and its address.
async fn serve_deepest(deepest: Deepest) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(
        Server::builder()
            .add_service(ObjectsServer::new(deepest))
            .serve_with_incoming(incoming),
    );
    url
}

/// A tier in front of `upstream`, which waits at most `timeout` for each
/// step of it, and its address.
async fn serve_tier(upstream: &str, timeout: Duration) -> String {
    let tier = Tier::new(MemoryStore::new(16 << 20), upstream, TOKEN, timeout, None).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(tier.serve(
        TOKEN,
        incoming,
        Limits::default(),
        None,
        std::future::pending(),
    ));
    url
}

type Client = ObjectsClient<InterceptedService<Channel, Bearer>>;

async fn connect(url: &str) -> Client {
    let channel = Endpoint::from_shared(url.to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
        .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
}

/// The status of a `GetObjects` call for `id` with `deadline`, or `OK` once
/// the whole reply has come.
async fn get(client: &mut Client, id: BlobId, deadline: Option<Duration>) -> Code {
    let mut request = Request::new(GetObjectsRequest {
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: id.as_bytes().to_vec(),
            path: Vec::new(),
        }],
        accept: Vec::new(),
        ticket: Vec::new(),
    });
    if let Some(deadline) = deadline {
        request.set_timeout(deadline);
    }
    let mut stream = match client.get_objects(request).await {
        Ok(reply) => reply.into_inner(),
        Err(status) => return status.code(),
    };
    loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => return Code::Ok,
            Err(status) => return status.code(),
        }
    }
}

/// A call through a daemon and a cache to a busy deepest tier fails with the
/// deepest tier's `RESOURCE_EXHAUSTED`, never with a timeout of the daemon or
/// the cache: for deadlines shorter than every fixed limit, between them,
/// and for no deadline, where the fixed limits nest.
#[tokio::test(flavor = "multi_thread")]
async fn the_caller_gets_the_status_of_the_deepest_tier_for_any_deadline() {
    // It never finds room. Its own limit, 2 s, is below the cache's limit
    // per step, 3 s, which is below the daemon's, 6 s.
    let deepest = Deepest::new(Duration::MAX, Duration::from_secs(2));
    let id = deepest.id();
    let cache = serve_tier(&serve_deepest(deepest).await, Duration::from_secs(3)).await;
    let daemon = serve_tier(&cache, Duration::from_secs(6)).await;
    let mut client = connect(&daemon).await;
    for deadline in [
        Some(Duration::from_millis(200)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(4)),
        None,
    ] {
        let code = get(&mut client, id, deadline).await;
        assert_eq!(code, Code::ResourceExhausted, "deadline {deadline:?}");
    }
}

/// A call with a short deadline starts a fetch, and a call with a long one
/// joins it. The fetch fails at the short deadline. The long call asks again
/// with its own deadline, and gets the object.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_call_asks_again_when_a_shorter_deadline_failed_its_fetch() {
    let deepest = Deepest::new(Duration::from_secs(1), Duration::from_secs(30));
    let (id, calls, called) = (
        deepest.id(),
        Arc::clone(&deepest.calls),
        Arc::clone(&deepest.called),
    );
    let cache = serve_tier(&serve_deepest(deepest).await, Duration::from_secs(30)).await;
    let (mut short, mut long) = (connect(&cache).await, connect(&cache).await);

    let short =
        tokio::spawn(async move { get(&mut short, id, Some(Duration::from_millis(300))).await });
    called.notified().await;
    let long = get(&mut long, id, Some(Duration::from_secs(10))).await;
    assert_eq!(long, Code::Ok, "the long call gets the object");
    assert_ne!(short.await.unwrap(), Code::Ok, "the short one does not");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the long call asked again once"
    );
}

/// A call with a long deadline starts a fetch, and a call with a short one
/// joins it. The short call stops at its own deadline. The fetch goes on,
/// and the long call gets the object from it.
#[tokio::test(flavor = "multi_thread")]
async fn a_short_call_stops_waiting_and_the_fetch_goes_on() {
    let deepest = Deepest::new(Duration::from_secs(1), Duration::from_secs(30));
    let (id, calls, called) = (
        deepest.id(),
        Arc::clone(&deepest.calls),
        Arc::clone(&deepest.called),
    );
    let cache = serve_tier(&serve_deepest(deepest).await, Duration::from_secs(30)).await;
    let (mut short, mut long) = (connect(&cache).await, connect(&cache).await);

    let long = tokio::spawn(async move { get(&mut long, id, Some(Duration::from_secs(10))).await });
    called.notified().await;
    let started = tokio::time::Instant::now();
    let short = get(&mut short, id, Some(Duration::from_millis(300))).await;
    assert_eq!(
        short,
        Code::DeadlineExceeded,
        "the short call stops at its deadline"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "before the fetch ends"
    );
    assert_eq!(
        long.await.unwrap(),
        Code::Ok,
        "the fetch goes on for the long call"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one fetch");
}

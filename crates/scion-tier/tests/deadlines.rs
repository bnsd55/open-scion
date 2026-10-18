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
    /// The key that signs the tickets, and the root tree of its tickets,
    /// which reaches the blob at `x`.
    key: ed25519_dalek::SigningKey,
    root: scion_core::TreeId,
    tree: Bytes,
    ready: Duration,
    busy: Duration,
    calls: Arc<AtomicUsize>,
    called: Arc<Notify>,
}

impl Deepest {
    fn new(ready: Duration, busy: Duration) -> Self {
        let data = Bytes::from(vec![7; 1000]);
        let entries = [(
            scion_core::PathElem::new(b"x".to_vec()).unwrap(),
            scion_core::Entry::File(scion_core::FileEntry::new(
                scion_core::FileState::Regular {
                    content: BlobId::of(&data),
                    executable: false,
                },
                0,
            )),
        )]
        .into_iter()
        .collect();
        let (tree_bytes, root) = scion_core::Tree { entries }.encode_with_id();
        Self {
            data,
            key: ed25519_dalek::SigningKey::from_bytes(&[4; 32]),
            root,
            tree: Bytes::from(tree_bytes.clone()),
            ready,
            busy,
            calls: Arc::default(),
            called: Arc::default(),
        }
    }

    /// The signed ticket for the root tree.
    fn ticket(&self) -> Vec<u8> {
        scion_proto::ticket::sign(
            &self.key,
            "r",
            "test",
            *self.root.as_bytes(),
            scion_proto::ticket::now(),
        )
        .unwrap()
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
        // The walk of the ticket asks for the root tree by its ID first.
        let asked = request.get_ref().refs.first().cloned();
        let is_tree = asked
            .as_ref()
            .is_some_and(|r| r.id == self.root.as_bytes().to_vec());
        let (kind, bytes) = if is_tree {
            (ObjectKind::Tree, self.tree.clone())
        } else {
            (ObjectKind::Blob, self.data.clone())
        };
        let mut id = [0u8; 32];
        if let Some(r) = &asked {
            id[..32.min(r.id.len())].copy_from_slice(&r.id[..32.min(r.id.len())]);
        } else {
            id = *self.id().as_bytes();
        }
        let header = Header {
            kind,
            id,
            encoding: Encoding::Raw,
            encoded_size: bytes.len(),
            size: bytes.len(),
        };
        let parts = header.parts(object::read_chunks(&bytes[..], bytes.len()).unwrap());
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
async fn serve_tier(
    upstream: &str,
    timeout: Duration,
    keys: &[ed25519_dalek::VerifyingKey],
) -> String {
    let tier = Tier::new(
        MemoryStore::new(16 << 20),
        upstream,
        TOKEN,
        timeout,
        None,
        keys.to_vec(),
    )
    .unwrap();
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
async fn get(client: &mut Client, id: BlobId, deadline: Option<Duration>, ticket: &[u8]) -> Code {
    let mut request = Request::new(GetObjectsRequest {
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: id.as_bytes().to_vec(),
            path: b"x".to_vec(),
        }],
        accept: Vec::new(),
        ticket: ticket.to_vec(),
    });
    if let Some(deadline) = deadline {
        request.set_timeout(deadline);
    }
    let mut stream = match client.get_objects(request).await {
        Ok(reply) => reply.into_inner(),
        Err(status) => return status.code(),
    };
    // A ceiling per message: a server that stalls mid-drain fails the
    // test in seconds, not at the CI cancel.
    loop {
        let message = tokio::time::timeout(Duration::from_secs(30), stream.message())
            .await
            .unwrap_or_else(|_| panic!("the drain never finished within 30 s"));
        match message {
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
    // per step, 60 s, which is below the daemon's, 120 s. Only their order
    // matters: each outer limit must be longer than the inner's own, so
    // the inner's `RESOURCE_EXHAUSTED` wins. They are far longer than the
    // 2 s the deepest waits, so a loaded machine never flips the outcome
    // into an outer tier's `UNAVAILABLE`.
    let deepest = Deepest::new(Duration::MAX, Duration::from_secs(2));
    let id = deepest.id();
    let (ticket, keys) = (deepest.ticket(), vec![deepest.key.verifying_key()]);
    let cache = serve_tier(
        &serve_deepest(deepest).await,
        Duration::from_secs(60),
        &keys,
    )
    .await;
    let daemon = serve_tier(&cache, Duration::from_secs(120), &keys).await;
    let mut client = connect(&daemon).await;
    for deadline in [
        Some(Duration::from_millis(200)),
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(4)),
        None,
    ] {
        let code = get(&mut client, id, deadline, &ticket).await;
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
    let (ticket, keys) = (deepest.ticket(), vec![deepest.key.verifying_key()]);
    let cache = serve_tier(
        &serve_deepest(deepest).await,
        Duration::from_secs(30),
        &keys,
    )
    .await;
    let (mut short, mut long) = (connect(&cache).await, connect(&cache).await);

    let spawned = ticket.clone();
    let short = tokio::spawn(async move {
        get(&mut short, id, Some(Duration::from_millis(300)), &spawned).await
    });
    called.notified().await;
    let long = get(&mut long, id, Some(Duration::from_secs(10)), &ticket).await;
    assert_eq!(long, Code::Ok, "the long call gets the object");
    assert_ne!(short.await.unwrap(), Code::Ok, "the short one does not");
    // The short call fetched the root tree and the object; the long call
    // fetched the object again from the kept root tree.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
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
    let (ticket, keys) = (deepest.ticket(), vec![deepest.key.verifying_key()]);
    let cache = serve_tier(
        &serve_deepest(deepest).await,
        Duration::from_secs(30),
        &keys,
    )
    .await;
    let (mut short, mut long) = (connect(&cache).await, connect(&cache).await);

    let spawned = ticket.clone();
    let long =
        tokio::spawn(
            async move { get(&mut long, id, Some(Duration::from_secs(10)), &spawned).await },
        );
    called.notified().await;
    let started = tokio::time::Instant::now();
    let short = get(&mut short, id, Some(Duration::from_millis(300)), &ticket).await;
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
    // One fetch of the object, plus the root tree of the walk.
    assert_eq!(calls.load(Ordering::SeqCst), 2, "one fetch");
}

//! A tier's caps on connections and on streams per connection, as on the
//! server. A client that stops reading keeps what its replies hold in
//! HTTP/2, so the caps bound how many such replies there are.

use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::pin;
use std::time::Duration;

use scion_core::BlobId;
use scion_proto::auth::Bearer;
use scion_proto::listen::Limits;
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::{GetObjectsRequest, ObjectKind, ObjectPart, ObjectRef};
use scion_proto::{Bytes, MAX_PART_MESSAGE_BYTES};
use scion_tier::{CACHE_UPSTREAM_TIMEOUT, Key, MemoryStore, Store as _, Tier};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Streaming};

const TOKEN: &str = "secret";

/// A wait that ends at once when nothing holds the call back.
const HELD: Duration = Duration::from_millis(500);

type Client = ObjectsClient<InterceptedService<Channel, Bearer>>;

fn client(channel: Channel) -> Client {
    ObjectsClient::with_interceptor(channel, Bearer::new(TOKEN).unwrap())
        .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
}

/// A cache within `limits` that holds one blob of 128 KiB, more than a
/// client's whole connection window of 64 KiB. It never calls its upstream.
/// Its address and the blob.
async fn serve(limits: Limits) -> (String, BlobId) {
    let data = vec![0; 128 << 10];
    let blob = BlobId::of(&data);
    let store = MemoryStore::new(1 << 20);
    let frame = zstd::bulk::compress(&data, 3).unwrap();
    store.put(Key::Blob(blob), &Bytes::from(frame)).unwrap();
    let cache = Tier::new(store, "http://127.0.0.1:1", TOKEN, CACHE_UPSTREAM_TIMEOUT).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(cache.serve(TOKEN, incoming, limits, std::future::pending()));
    (url, blob)
}

/// The blob, RAW, from a call whose reply the caller never reads.
async fn stall(client: &mut Client, blob: BlobId) -> Streaming<ObjectPart> {
    let request = GetObjectsRequest {
        repo: "main".into(),
        refs: vec![ObjectRef {
            kind: ObjectKind::Blob.into(),
            id: blob.as_bytes().to_vec(),
        }],
        accept: Vec::new(),
    };
    client.get_objects(request).await.unwrap().into_inner()
}

/// A call that the tier answers at once, with no data: `INVALID_ARGUMENT`
/// for no objects, with no flow-controlled bytes, so a full connection
/// window does not hold it.
fn no_objects(client: &mut Client) -> impl Future<Output = Code> + '_ {
    let request = GetObjectsRequest {
        repo: "main".into(),
        refs: Vec::new(),
        accept: Vec::new(),
    };
    async move { client.get_objects(request).await.unwrap_err().code() }
}

/// A client that stops reading keeps its streams open. With the streams of
/// its connection all open, its next call waits, and goes on once one of
/// them ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_opens_at_most_its_streams() {
    let streams = 4;
    let limits = Limits {
        streams: NonZeroU32::new(streams).unwrap(),
        ..Limits::default()
    };
    let (url, blob) = serve(limits).await;
    // The smallest windows, so no reply of the blob ends while it is unread.
    let channel = Endpoint::from_shared(url)
        .unwrap()
        .initial_stream_window_size(65_535)
        .initial_connection_window_size(65_535)
        .connect()
        .await
        .unwrap();
    let mut client = client(channel);
    let mut stalled = Vec::new();
    for _ in 0..streams {
        stalled.push(stall(&mut client, blob).await);
    }

    let mut next = client.clone();
    let mut next = pin!(no_objects(&mut next));
    assert!(
        tokio::time::timeout(HELD, &mut next).await.is_err(),
        "a stream beyond the cap was opened"
    );
    drop(stalled.pop());
    let code = tokio::time::timeout(Duration::from_secs(10), next)
        .await
        .expect("the call goes on once a stream ends");
    assert_eq!(code, Code::InvalidArgument);
}

/// With all its connections open, the tier leaves a new one unread, and
/// serves it once another closes.
#[tokio::test(flavor = "multi_thread")]
async fn a_process_serves_at_most_its_connections() {
    let connections = 2;
    let limits = Limits {
        connections: NonZeroUsize::new(connections).unwrap(),
        ..Limits::default()
    };
    let (url, _) = serve(limits).await;
    let endpoint = Endpoint::from_shared(url).unwrap();
    let mut open = Vec::new();
    for _ in 0..connections {
        let mut client = client(endpoint.connect().await.unwrap());
        // Served, so the tier holds a place for it.
        assert_eq!(no_objects(&mut client).await, Code::InvalidArgument);
        open.push(client);
    }

    let mut extra = client(endpoint.connect_lazy());
    let mut call = pin!(no_objects(&mut extra));
    assert!(
        tokio::time::timeout(HELD, &mut call).await.is_err(),
        "a connection beyond the cap was served"
    );
    // Its connection closes once no handle is left.
    drop(open.pop());
    let code = tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect("the connection is served once another closes");
    assert_eq!(code, Code::InvalidArgument);
}

//! Cache tiers for the `Objects` service: `scion-cache` and `scion-daemon`.
//!
//! A [`Tier`] answers the same `Objects` calls as the server. It serves trees
//! and blobs from its own [`Store`] and sends each miss to its one upstream:
//! the next tier, or the server. It never trusts that upstream:
//!
//! - Every tree and blob from upstream is unpacked within the object limit
//!   and checked against its ID before the tier stores it or sends it on.
//! - `GetRootTree` answers are kept, but only answers that upstream gave. A
//!   changeset never changes, so its root tree never does.
//! - `ResolveRef` is always forwarded, never kept. A ref moves.
//!
//! A tier keeps a tree as its `TreeV1` bytes and a blob as one zstd frame,
//! the form in which the server stores it. It sends a blob on as that frame
//! to a client that accepts zstd, so a blob is packed once, on the server,
//! and unpacked once, by the client that writes it.
//!
//! A `GetObjects` reply follows the server's rules ([`scion_proto::reply`]):
//! 1 to 1,000 objects per call, a reply stops after 64 MiB, each object takes
//! its share of a byte budget before its bytes are read, and a reply ends when
//! its client stops reading. Misses are fetched in batches, and concurrent
//! misses for the same object share one fetch.

mod store;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::Read as _;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fmt, fs, io, iter};

use anyhow::Context as _;
use hashlink::LruCache;
use scion_core::{BlobId, ChangesetId, Tree, TreeId};
use scion_proto::auth::{Auth, Bearer};
use scion_proto::check;
use scion_proto::deadline::Until;
use scion_proto::listen::Limits;
use scion_proto::object::{self, Assembler, Header, MAX_ENCODED_BYTES, UNPACK_BYTES, zstd_bound};
use scion_proto::reply::{
    self, BUSY_TIMEOUT, Body, IN_FLIGHT_BYTES, ObjectStream, Outgoing, STALL_TIMEOUT,
};
use scion_proto::tls::{ClientTls, ServerTls};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{Bytes, MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES, MAX_PART_MESSAGE_BYTES};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_stream::Stream;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status, Streaming};

use store::lock;
pub use store::{DiskStore, MemoryStore, Store};

/// Most `GetRootTree` answers a tier keeps.
const MAX_ROOT_TREES: usize = 10_000;

/// Most objects that one fetch asks upstream for. A reply sends at most
/// 64 MiB, so fetching far ahead of it only fills the store.
const MAX_BATCH: usize = 128;

/// Most fetches that read from upstream at once. Each holds at most one
/// message that the fetch budget has not counted yet: the first part of an
/// object, before its header is read.
const FETCH_SLOTS: usize = 64;

/// The fetch budget: bytes of objects from upstream, from the header of each
/// object until it is stored. With one message per fetch slot, all fetches
/// together hold at most [`IN_FLIGHT_BYTES`].
const FETCH_BYTES: usize = IN_FLIGHT_BYTES - FETCH_SLOTS * MAX_PART_MESSAGE_BYTES;

// The largest object fits in the fetch budget in both its forms, with a
// codec at work, so a fetch that waits for room gets it once others finish.
const _: () = assert!(MAX_ENCODED_BYTES + MAX_OBJECT_BYTES + UNPACK_BYTES <= FETCH_BYTES);

/// How long `scion-cache` waits for each step of the server: an answer, or
/// the next part of an object.
pub const CACHE_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(60);

/// How long `scion-daemon` waits for each step of its cache. Before the cache
/// sends the first part of an object, it may wait [`BUSY_TIMEOUT`] for a
/// fetch slot, one step of the server, [`BUSY_TIMEOUT`] for room in its
/// fetch budget, and [`BUSY_TIMEOUT`] for room in its reply budget.
pub const DAEMON_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(180);

// The limits nest: a busy server answers the cache before the cache gives
// up, and a cache that waits on the server answers the daemon before the
// daemon gives up. A caller's deadline also ends each wait, and each tier
// passes what is left of it upstream, less its margin (`Until`), so the
// deadlines nest as well.
const _: () = {
    // Seconds for the work around the waits: reading, checking and moving one
    // object.
    let margin = 30;
    let (busy, cache) = (BUSY_TIMEOUT.as_secs(), CACHE_UPSTREAM_TIMEOUT.as_secs());
    assert!(busy + margin <= cache);
    assert!(cache + 3 * busy + margin <= DAEMON_UPSTREAM_TIMEOUT.as_secs());
};

/// Most memory that decoding a tree holds, per byte of its `TreeV1` bytes.
/// An entry takes at least 38 bytes encoded; decoded, it is a name and an
/// entry in a list and again in a map, plus the name's own buffer (see the
/// test `decoding_a_tree_fits_its_factor`).
const TREE_DECODE_FACTOR: usize = 6;

/// One object, by kind and ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Key {
    /// A tree, stored as its `TreeV1` bytes.
    Tree(TreeId),
    /// A blob, stored as one zstd frame of its content.
    Blob(BlobId),
}

impl Key {
    fn parse(object: &ObjectRef) -> Result<Self, Status> {
        match ObjectKind::try_from(object.kind) {
            Ok(ObjectKind::Tree) => Ok(Self::Tree(check::id(&object.id, "tree id")?)),
            Ok(ObjectKind::Blob) => Ok(Self::Blob(check::id(&object.id, "blob id")?)),
            Ok(ObjectKind::Unspecified) | Err(_) => {
                Err(Status::invalid_argument("object kind must be TREE or BLOB"))
            }
        }
    }

    fn kind(self) -> ObjectKind {
        match self {
            Self::Tree(_) => ObjectKind::Tree,
            Self::Blob(_) => ObjectKind::Blob,
        }
    }

    fn id(self) -> [u8; 32] {
        match self {
            Self::Tree(id) => *id.as_bytes(),
            Self::Blob(id) => *id.as_bytes(),
        }
    }

    fn to_ref(self) -> ObjectRef {
        ObjectRef {
            kind: self.kind().into(),
            id: self.id().to_vec(),
        }
    }

    /// The encoding of the form in which a tier stores this object.
    fn stored_encoding(self) -> Encoding {
        match self {
            Self::Tree(_) => Encoding::Raw,
            Self::Blob(_) => Encoding::Zstd,
        }
    }

    /// Whether the first part of an object names this object. Says nothing
    /// about its bytes.
    fn names(self, first: &ObjectPart) -> bool {
        first.kind == i32::from(self.kind()) && first.id == self.id()
    }

    /// The header of this object in its stored form `stored`, which was
    /// checked when it was stored.
    fn stored_header(self, stored: &[u8]) -> Result<Header, String> {
        let encoding = self.stored_encoding();
        let size = match encoding {
            Encoding::Raw => stored.len(),
            Encoding::Zstd => object::frame_size(stored)?,
        };
        Ok(Header {
            kind: self.kind(),
            id: self.id(),
            encoding,
            encoded_size: stored.len(),
            size,
        })
    }

    /// Checks that `stored` is this object in the form a tier stores it: a
    /// tree's `TreeV1` bytes that decode and hash to its ID, or one zstd frame
    /// of a blob that unpacks within the object limit and hashes to its ID.
    /// Unpacks as a stream, so it holds at most [`UNPACK_BYTES`], not the
    /// content.
    ///
    /// # Errors
    ///
    /// Why `stored` is not this object.
    pub fn check_stored(self, stored: &[u8]) -> Result<(), String> {
        let matches = match self {
            Self::Tree(id) => Tree::decode_with_id(stored).is_ok_and(|(_, actual)| actual == id),
            Self::Blob(id) => {
                let size = object::frame_size(stored)?;
                let unpacked =
                    object::unpacker(stored).map_err(|e| format!("cannot unpack: {e}"))?;
                // One byte over the size is enough to see that it is larger.
                let mut unpacked = unpacked.take(size as u64 + 1);
                let actual =
                    BlobId::of_reader(&mut unpacked).map_err(|e| format!("cannot unpack: {e}"))?;
                if unpacked.limit() != 1 {
                    return Err("unpacks to another size than its header gives".into());
                }
                actual == id
            }
        };
        if !matches {
            return Err(format!("the bytes are not {self}"));
        }
        Ok(())
    }

    /// The memory that reading this object from a store holds besides its
    /// stored form of `size` bytes: the disk store decodes a tree, and
    /// unpacks a blob, to check it.
    fn read_bytes(self, size: usize) -> usize {
        match self {
            Self::Tree(_) => TREE_DECODE_FACTOR * size,
            Self::Blob(_) => UNPACK_BYTES,
        }
    }

    /// Checks an object from upstream, whose header and encoded bytes are
    /// `header` and `data`, against this key. Returns it in the form a tier
    /// stores: a tree unpacked, a blob packed (as it came, or packed here).
    fn to_stored(self, header: &Header, data: Vec<u8>) -> Result<Vec<u8>, String> {
        let stored = match (self, header.encoding) {
            (Self::Tree(_), _) => object::decode(header, data)?,
            (Self::Blob(_), Encoding::Zstd) => data,
            (Self::Blob(id), Encoding::Raw) => {
                if BlobId::of(&data) != id {
                    return Err(format!("the bytes are not {self}"));
                }
                let packed = object::pack(&data).map_err(|e| format!("cannot pack: {e}"))?;
                // Only the packed form is held from here.
                drop(data);
                packed
            }
        };
        self.check_stored(&stored)?;
        if self.stored_header(&stored)?.size != header.size {
            return Err(format!("{self} is not {} bytes", header.size));
        }
        Ok(stored)
    }

    /// The most memory that [`Key::to_stored`] holds for an object with
    /// `header`: its encoded bytes, its other form, and a codec at work.
    fn check_bytes(self, header: &Header) -> usize {
        let other = match (self, header.encoding) {
            // Decoded, to check it.
            (Self::Tree(_), Encoding::Raw) => TREE_DECODE_FACTOR * header.size,
            // Unpacked whole, then decoded.
            (Self::Tree(_), Encoding::Zstd) => (1 + TREE_DECODE_FACTOR) * header.size,
            // Unpacked as a stream to check it.
            (Self::Blob(_), Encoding::Zstd) => UNPACK_BYTES,
            // Packed here, then checked as a stream.
            (Self::Blob(_), Encoding::Raw) => zstd_bound(header.size) + UNPACK_BYTES,
        };
        header.encoded_size + other
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tree(id) => write!(f, "tree {id}"),
            Self::Blob(id) => write!(f, "blob {id}"),
        }
    }
}

/// The client for the next tier up.
type Upstream = ObjectsClient<InterceptedService<Channel, UpstreamAuth>>;

/// What the replies that wait for one object get from its fetch: nothing yet,
/// then the object in its stored form, or why the fetch failed.
type Outcome = Option<Result<Bytes, Failed>>;

/// Why a tier could not get an object.
#[derive(Clone, Debug)]
struct Failed {
    status: Status,
    /// The whole upstream call failed (its own status, a transport error, a
    /// timeout), not this object alone.
    whole_call: bool,
    /// The deadline of the fetch that failed. A caller with a later one asks
    /// again: the failure may have come from that deadline.
    until: Until,
}

impl From<Status> for Failed {
    fn from(status: Status) -> Self {
        Self {
            status,
            whole_call: false,
            until: Until::NONE,
        }
    }
}

/// An object for a reply, or why the tier could not get it, with its share
/// of the reply budget.
type Found = (Result<Outgoing, Failed>, OwnedSemaphorePermit);

/// Splits what a reply found: `Err` with the call's status if the whole
/// upstream call failed, else the object or why this one object failed.
fn split(found: Found) -> Result<reply::Found, Status> {
    match found {
        (Err(failed), _) if failed.whole_call => Err(failed.status),
        (found, permit) => Ok((found.map_err(|failed| failed.status), permit)),
    }
}

/// The fetches in progress, by repo and object.
type Flights = HashMap<(String, Key), watch::Receiver<Outcome>>;

/// Adds the tier's own token to every upstream call, and counts the calls.
#[derive(Clone)]
struct UpstreamAuth {
    bearer: Bearer,
    calls: Arc<AtomicU64>,
}

impl Interceptor for UpstreamAuth {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bearer.call(request)
    }
}

/// One cache tier. Clones share the same store and upstream.
pub struct Tier<S> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for Tier<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct Inner<S> {
    store: S,
    upstream: Upstream,
    /// How long to wait for upstream.
    timeout: Duration,
    /// How long to wait for room in a budget: [`BUSY_TIMEOUT`].
    busy: Duration,
    /// Calls sent upstream, of every kind.
    upstream_calls: Arc<AtomicU64>,
    /// The reply budget, as on the server: object bytes read or copied for a
    /// reply and not yet taken by its client.
    in_flight: Arc<Semaphore>,
    /// [`FETCH_SLOTS`]: the fetches that read from upstream.
    fetch_slots: Arc<Semaphore>,
    /// [`FETCH_BYTES`]: object bytes received from upstream and not yet
    /// stored.
    fetching: Arc<Semaphore>,
    flights: Mutex<Flights>,
    /// `GetRootTree` answers from upstream, by repo and changeset.
    root_trees: Mutex<LruCache<(String, ChangesetId), TreeId>>,
}

/// What one reply has done: hits and misses for the log, and how far into
/// its keys the fetches it started reach.
#[derive(Default)]
struct Progress {
    hits: AtomicUsize,
    misses: AtomicUsize,
    /// Keys below this index were in a fetch that this reply started.
    fetched_to: AtomicUsize,
}

/// What one reply asks for.
struct Ask {
    repo: String,
    keys: Vec<Key>,
    /// The client accepts zstd.
    zstd: bool,
    /// When the reply ends its waits: the caller's deadline less a margin.
    until: Until,
}

impl<S: Store> Tier<S> {
    /// A tier that keeps objects in `store` and sends misses to `upstream`
    /// (`http://HOST:PORT` or `unix:///PATH`) with `upstream_token`. It waits
    /// at most `timeout` for each step of upstream, and connects on the first
    /// call. With `client_tls`, the upstream is trusted with that CA, and a
    /// client certificate is presented for mTLS.
    ///
    /// # Errors
    ///
    /// A bad upstream address, a token that is not ASCII, or an https
    /// upstream without a CA to trust.
    pub fn new(
        store: S,
        upstream: &str,
        upstream_token: &str,
        timeout: Duration,
        client_tls: Option<&ClientTls>,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::from_shared(upstream.to_owned())
            .with_context(|| format!("bad upstream address {upstream:?}"))?;
        let endpoint = if let Some(client_tls) = client_tls {
            client_tls.apply_to(upstream, endpoint)?
        } else {
            anyhow::ensure!(
                !upstream.starts_with("https://"),
                "the upstream {upstream} is https, but no CA is given to trust its \
                 certificate; pass --tls-ca"
            );
            endpoint
        };
        let channel = endpoint.connect_lazy();
        let bearer = Bearer::new(upstream_token).context("upstream token is not valid ASCII")?;
        let upstream_calls = Arc::default();
        let auth = UpstreamAuth {
            bearer,
            calls: Arc::clone(&upstream_calls),
        };
        let upstream = ObjectsClient::with_interceptor(channel, auth)
            .max_decoding_message_size(MAX_PART_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                upstream,
                timeout,
                busy: BUSY_TIMEOUT,
                upstream_calls,
                in_flight: Arc::new(Semaphore::new(IN_FLIGHT_BYTES)),
                fetch_slots: Arc::new(Semaphore::new(FETCH_SLOTS)),
                fetching: Arc::new(Semaphore::new(FETCH_BYTES)),
                flights: Mutex::default(),
                root_trees: Mutex::new(LruCache::new(MAX_ROOT_TREES)),
            }),
        })
    }

    /// How many calls this tier has sent upstream.
    #[must_use]
    pub fn upstream_calls(&self) -> u64 {
        self.inner.upstream_calls.load(Ordering::Relaxed)
    }

    /// Serves the `Objects` calls on `incoming` connections, to callers that
    /// send `token`, within `limits`, until `shutdown` resolves. With `tls`,
    /// every connection is a TLS connection, whose handshake `listen` ends
    /// within its timeout.
    ///
    /// # Errors
    ///
    /// Transport errors.
    pub async fn serve<I, IO>(
        self,
        token: &str,
        incoming: I,
        limits: Limits,
        tls: Option<ServerTls>,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
    {
        let objects = ObjectsServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_PART_MESSAGE_BYTES);
        let objects = InterceptedService::new(objects, Auth::new(token));
        let router = |mut server: Server| server.add_service(objects);
        limits
            .serve(router, incoming, tls.map(|tls| tls.config()), shutdown)
            .await
    }

    /// Runs store work on a blocking thread.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&S) -> io::Result<T> + Send + 'static,
    ) -> Result<T, Status> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || f(&inner.store))
            .await
            .unwrap_or_else(|e| Err(io::Error::other(e)))
            .map_err(|e| {
                // The detail stays in the log, as on the server.
                tracing::error!(error = %e, "store failed");
                Status::internal("internal error")
            })
    }

    /// Waits for an upstream call at most the tier's timeout, and not past
    /// `until`.
    async fn within<T>(
        &self,
        until: Until,
        call: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let wait = until.wait(self.inner.timeout);
        tokio::time::timeout(wait, call).await.unwrap_or_else(|_| {
            Err(Status::unavailable(format!(
                "upstream did not answer within {wait:?}"
            )))
        })
    }

    /// Streams the objects of `ask` into `tx`, starting with `first`, which
    /// [`Objects::get_objects`] found before the reply began. If the whole
    /// upstream call fails for a later object, the reply ends there without
    /// an error: the client asks again for the rest, and gets the failure as
    /// that call's status. So an error in the stream is always about one
    /// object.
    async fn reply(
        self,
        ask: Ask,
        progress: Progress,
        first: reply::Found,
        tx: mpsc::Sender<reply::Sent>,
    ) {
        let (tier, ask, progress) = (&self, &ask, &progress);
        let produce = |i| async move { split(tier.produce(ask, i, progress).await?).ok() };
        reply::send_objects(first, 1..ask.keys.len(), tx, STALL_TIMEOUT, produce).await;
        tracing::info!(
            objects = ask.keys.len(),
            hits = progress.hits.load(Ordering::Relaxed),
            misses = progress.misses.load(Ordering::Relaxed),
            "GetObjects"
        );
    }

    /// Finds `ask.keys[i]` for a reply: in the store, or else from upstream.
    /// Takes the object's share of the reply budget before its bytes are read
    /// from the store or copied into the reply.
    // SHORTCUT: two blocking hops per hit (size, then read) and no read-ahead,
    // as on the server. Upgrade (issue #6): read a batch capped by bytes per
    // hop, or keep a few reads in flight.
    async fn produce(&self, ask: &Ask, i: usize, progress: &Progress) -> Option<Found> {
        let key = *ask.keys.get(i)?;
        // A share taken before the object was in hand.
        let mut held = None;
        let mut missed = false;
        loop {
            let size = match self.blocking(move |store| store.size(key)).await {
                Ok(size) => size,
                Err(status) => return Some((Err(status.into()), self.share(held, 1, ask).await?)),
            };
            if let Some(size) = size {
                let mut permit = self
                    .share(held.take(), size + key.read_bytes(size), ask)
                    .await?;
                let stored = self.stored(key, size).await;
                reply::trim(&mut permit, size);
                match stored {
                    Ok(Some(stored)) => {
                        if !missed {
                            progress.hits.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(%key, "hit");
                        }
                        return self.outgoing(key, stored, permit, ask).await;
                    }
                    // The stored copy went (dropped, or bad) after its size
                    // was read: fetch it.
                    Ok(None) => held = Some(permit),
                    Err(status) => return Some((Err(status.into()), permit)),
                }
            }
            if !missed {
                missed = true;
                progress.misses.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(%key, "miss");
            }
            let data = match self.fetch(ask, i, progress).await {
                Ok(Some(data)) => data,
                // A fetch stored it a moment ago: read it from the store,
                // with its share, as a hit is.
                Ok(None) => continue,
                Err(failed) => return Some((Err(failed), self.share(held, 1, ask).await?)),
            };
            // The bytes were counted in the fetch budget until stored. Take
            // them into the reply now if the reply budget has room. Else let
            // them go, wait for room, and read them again.
            let permit = match held.take() {
                Some(permit) => self.share(Some(permit), data.len(), ask).await,
                None => reply::try_take(&self.inner.in_flight, data.len()),
            };
            if let Some(permit) = permit {
                return self.outgoing(key, data, permit, ask).await;
            }
            let size = data.len();
            drop(data);
            held = Some(self.share(None, size + key.read_bytes(size), ask).await?);
        }
    }

    /// `held` grown to `bytes` of the reply budget, or a new share of
    /// `bytes`. Waits at most [`BUSY_TIMEOUT`], and not past `ask.until`;
    /// `None` if the room does not come.
    async fn share(
        &self,
        held: Option<OwnedSemaphorePermit>,
        bytes: usize,
        ask: &Ask,
    ) -> Option<OwnedSemaphorePermit> {
        match held {
            Some(mut permit) => {
                self.cover(&mut permit, bytes, ask.until).await?;
                Some(permit)
            }
            None => {
                reply::take(
                    &self.inner.in_flight,
                    bytes,
                    ask.until.wait(self.inner.busy),
                )
                .await
            }
        }
    }

    /// `key` for a reply, from its stored form, in an encoding that the
    /// client accepts. Grows `permit`, the object's share of the reply
    /// budget, to cover what the reply holds. For a client that does not
    /// accept `zstd`, a blob is unpacked as a stream into chunks of its own,
    /// and then only the chunks are held.
    async fn outgoing(
        &self,
        key: Key,
        stored: Bytes,
        mut permit: OwnedSemaphorePermit,
        ask: &Ask,
    ) -> Option<Found> {
        let header = match key.stored_header(&stored) {
            Ok(header) => header,
            Err(why) => {
                tracing::error!(%key, %why, "a stored object has a bad header");
                return Some((Err(Status::internal("internal error").into()), permit));
            }
        };
        if header.encoding == Encoding::Raw || ask.zstd {
            self.cover(&mut permit, stored.len(), ask.until).await?;
            reply::trim(&mut permit, stored.len());
            let body = Body::Whole(stored);
            return Some((Ok(Outgoing { header, body }), permit));
        }
        let size = header.size;
        self.cover(&mut permit, stored.len() + size + UNPACK_BYTES, ask.until)
            .await?;
        let chunks = self
            .blocking(move |_| object::read_chunks(object::unpacker(&stored)?, size))
            .await;
        reply::trim(&mut permit, size);
        let header = Header {
            encoding: Encoding::Raw,
            encoded_size: size,
            ..header
        };
        let object = chunks.map(|chunks| Outgoing {
            header,
            body: Body::Chunks(chunks),
        });
        Some((object.map_err(Failed::from), permit))
    }

    /// Grows `permit` to `bytes` of the reply budget, if it holds less.
    /// Waits at most [`BUSY_TIMEOUT`], and not past `until`; `None` if the
    /// room does not come.
    async fn cover(
        &self,
        permit: &mut OwnedSemaphorePermit,
        bytes: usize,
        until: Until,
    ) -> Option<()> {
        let held = permit.num_permits();
        if bytes > held {
            let wait = until.wait(self.inner.busy);
            let more = reply::take(&self.inner.in_flight, bytes - held, wait).await?;
            permit.merge(more);
        }
        Some(())
    }

    /// Reads `key` in its stored form from the store, at most `size` bytes.
    /// `None` if it is not there.
    async fn stored(&self, key: Key, size: usize) -> Result<Option<Bytes>, Status> {
        self.blocking(move |store| store.get(key, size)).await
    }

    /// Gets `ask.keys[i]` in its stored form from upstream. Joins the fetch
    /// in progress for it, or starts one. A fetch that a reply starts also
    /// takes the later keys, up to [`MAX_BATCH`] in all, that the store
    /// lacks. A key that an earlier fetch of the same reply covered, but the
    /// store did not keep, is fetched alone, so such a store costs one fetch
    /// per object, not one per remaining call. A fetch has the deadline of
    /// the caller that started it.
    ///
    /// The wait is bounded: each step of a fetch has the tier's timeout and
    /// the fetch's deadline, and a fetch always settles every object it
    /// holds. A caller waits no longer than its own deadline. Then it stops,
    /// and the fetch goes on for the others. If the fetch failed as a whole
    /// and its deadline came first, the caller asks again with its own: the
    /// failure may have come from the shorter deadline.
    ///
    /// `None` if a fetch that ended after the caller found no stored copy has
    /// stored the object: the caller reads it from the store, with its
    /// share, and does not fetch it again.
    async fn fetch(
        &self,
        ask: &Ask,
        i: usize,
        progress: &Progress,
    ) -> Result<Option<Bytes>, Failed> {
        let (repo, keys, until) = (&ask.repo, &ask.keys, ask.until);
        let key = keys[i];
        loop {
            let joined = lock(&self.inner.flights).get(&(repo.clone(), key)).cloned();
            let flight = if let Some(flight) = joined {
                Some(flight)
            } else if i < progress.fetched_to.load(Ordering::Relaxed) {
                self.start(repo, key, Vec::new(), until)
            } else {
                let end = keys.len().min(i + MAX_BATCH);
                progress.fetched_to.store(end, Ordering::Relaxed);
                self.start(repo, key, keys[i + 1..end].to_vec(), until)
            };
            let Some(mut flight) = flight else {
                return Ok(None);
            };
            let settled = flight.wait_for(Option::is_some);
            let Ok(settled) = tokio::time::timeout(until.wait(Duration::MAX), settled).await else {
                return Err(Failed {
                    status: Status::deadline_exceeded(
                        "the deadline passed while the call waited for a fetch from upstream",
                    ),
                    whole_call: true,
                    until,
                });
            };
            match settled.ok().and_then(|outcome| outcome.clone()) {
                None => {
                    return Err(
                        Status::internal("a fetch from upstream ended without a result").into(),
                    );
                }
                Some(Err(failed))
                    if failed.whole_call && failed.until.before(until) && !until.passed() =>
                {
                    tracing::debug!(%key, status = %failed.status, "a fetch with an earlier deadline failed; asking again");
                }
                Some(outcome) => return outcome.map(Some),
            }
        }
    }

    /// Starts one fetch for `key` and `later`, minus the objects that another
    /// fetch already has or the store holds. Returns the receiver for `key`,
    /// or `None` if the store holds it.
    ///
    /// The store is asked under the fetch table's lock. A fetch stores each
    /// object before it takes the object out of the table, so an object in
    /// neither has not been fetched yet, and only one fetch starts for it.
    fn start(
        &self,
        repo: &str,
        key: Key,
        later: Vec<Key>,
        until: Until,
    ) -> Option<watch::Receiver<Outcome>> {
        let stored = |k| matches!(self.inner.store.size(k), Ok(Some(_)));
        let mut flights = lock(&self.inner.flights);
        if let Some(rx) = flights.get(&(repo.to_owned(), key)) {
            return Some(rx.clone());
        }
        if stored(key) {
            return None;
        }
        let mut waiting = VecDeque::new();
        for k in iter::once(key).chain(later) {
            if let Entry::Vacant(slot) = flights.entry((repo.to_owned(), k))
                && (k == key || !stored(k))
            {
                let (tx, rx) = watch::channel(None);
                slot.insert(rx);
                waiting.push_back((k, tx));
            }
        }
        let rx = flights[&(repo.to_owned(), key)].clone();
        drop(flights);
        let flight = Flight {
            tier: self.clone(),
            repo: repo.to_owned(),
            waiting,
            until,
        };
        tokio::spawn(flight.run());
        Some(rx)
    }
}

/// One fetch from upstream: a few objects of one repo, which other replies
/// can wait for. It runs on its own task, so it never waits for a reply, and
/// it ends when every object is fetched or failed. Objects still waiting when
/// it ends leave the fetch table, so a later call starts anew.
struct Flight<S: Store> {
    tier: Tier<S>,
    repo: String,
    /// The objects not fetched yet, in request order, each with the sender
    /// that its waiters listen to.
    waiting: VecDeque<(Key, watch::Sender<Outcome>)>,
    /// When the fetch ends its waits: the deadline of the caller that
    /// started it, less the margin.
    until: Until,
}

/// The next object of an upstream reply.
enum Next {
    /// The reply ended between objects.
    End,
    /// The object at the front of the fetch, still encoded, with its share
    /// of the fetch budget.
    Object(Key, Header, Vec<u8>, OwnedSemaphorePermit),
    /// The object at the front of the fetch failed: an error about it, or
    /// parts that cannot be it. The rest of the reply is not read.
    Failed(Status),
}

impl<S: Store> Flight<S> {
    async fn run(mut self) {
        tracing::debug!(objects = self.waiting.len(), "fetching from upstream");
        if let Err(status) = self.fetch_all().await {
            tracing::warn!(%status, objects = self.waiting.len(), "fetch from upstream failed");
            let failed = Failed {
                status,
                whole_call: true,
                until: self.until,
            };
            for (_, tx) in &self.waiting {
                tx.send_replace(Some(Err(failed.clone())));
            }
        }
    }

    /// Hands the first waiting object, or why upstream could not give that
    /// one object, to its waiters. The other objects keep waiting.
    fn settle_front(&mut self, outcome: Result<Bytes, Status>) {
        if let Some((key, tx)) = self.waiting.pop_front() {
            if let Err(status) = &outcome {
                tracing::warn!(%key, %status, "upstream could not give an object");
            }
            tx.send_replace(Some(outcome.map_err(Failed::from)));
            lock(&self.tier.inner.flights).remove(&(self.repo.clone(), key));
        }
    }

    /// Fetches every waiting object. Checks each against its ID, stores it,
    /// and hands it to its waiters. An error inside upstream's reply about
    /// one object, or bytes that cannot be that object, fail only that
    /// object. An upstream reply may stop early; the rest are asked for
    /// again. An error status of the call itself, a transport error, a
    /// timeout or an object out of order fails every object still waiting.
    async fn fetch_all(&mut self) -> Result<(), Status> {
        // Bounded as a reply's wait for memory is. Then every object of the
        // fetch fails with `UNAVAILABLE`, and the callers ask again.
        let Inner {
            fetch_slots, busy, ..
        } = &*self.tier.inner;
        let slot = Arc::clone(fetch_slots).acquire_owned();
        let wait = self.until.wait(*busy);
        let _slot = match tokio::time::timeout(wait, slot).await {
            Ok(Ok(slot)) => slot,
            Ok(Err(_)) => return Err(Status::unavailable("the tier is shutting down")),
            Err(_) => {
                return Err(Status::unavailable(format!(
                    "no fetch slot within {wait:?}; ask again"
                )));
            }
        };
        while !self.waiting.is_empty() {
            if self.until.passed() {
                return Err(Status::deadline_exceeded(
                    "the deadline passed before the fetch could ask upstream",
                ));
            }
            let refs = self.waiting.iter().map(|(key, _)| key.to_ref()).collect();
            // Upstream gets what is left of the deadline.
            let request = self.until.request(GetObjectsRequest {
                repo: self.repo.clone(),
                refs,
                accept: vec![Encoding::Zstd.into()],
            });
            let mut upstream = self.tier.inner.upstream.clone();
            let mut stream = self
                .tier
                .within(self.until, upstream.get_objects(request))
                .await?
                .into_inner();
            let before = self.waiting.len();
            loop {
                let (key, header, data, permit) = match self.next(&mut stream).await? {
                    Next::End => break,
                    Next::Failed(status) => {
                        self.settle_front(Err(status));
                        break;
                    }
                    Next::Object(key, header, data, permit) => (key, header, data, permit),
                };
                // Unpacking and hashing up to 100 MiB takes a while: check
                // and store on a blocking thread.
                let checked = self
                    .tier
                    .blocking(move |store| {
                        let stored = match key.to_stored(&header, data) {
                            Ok(stored) => Bytes::from(stored),
                            Err(why) => return Ok(Err(why)),
                        };
                        // A failed put is logged; the object still goes to
                        // its waiters.
                        if let Err(e) = store.put(key, &stored) {
                            tracing::warn!(%key, error = %e, "cannot store an object");
                        }
                        Ok(Ok(stored))
                    })
                    .await?;
                self.settle_front(checked.map_err(|why| {
                    Status::data_loss(format!("upstream sent bytes that are not {key}: {why}"))
                }));
                drop(permit);
            }
            if self.waiting.len() == before {
                return Err(Status::unavailable(
                    "upstream sent none of the requested objects",
                ));
            }
        }
        Ok(())
    }

    /// Reads the next object of an upstream reply, part by part. It must be
    /// the object at the front of the fetch. Takes its share of the fetch
    /// budget from its header, before the rest of its parts arrive.
    async fn next(&self, stream: &mut Streaming<ObjectPart>) -> Result<Next, Status> {
        let first = match self.tier.within(self.until, stream.message()).await {
            Ok(Some(part)) => part,
            Ok(None) => return Ok(Next::End),
            // Upstream ends its reply after an error about one object.
            Err(status) if about_one_object(&status) => return Ok(Next::Failed(status)),
            Err(status) => return Err(status),
        };
        let Some(&(key, _)) = self.waiting.front().filter(|(key, _)| key.names(&first)) else {
            return Err(Status::internal(
                "upstream sent an object that was not asked for",
            ));
        };
        let bad = |why: String| {
            Ok(Next::Failed(Status::data_loss(format!(
                "upstream sent bytes that are not {key}: {why}"
            ))))
        };
        let header = match Header::of(&first, true) {
            Ok(header) => header,
            Err(why) => return bad(why),
        };
        // The upstream reply waits meanwhile, so the wait is bounded as a
        // reply's is. Then the fetch ends, and every object it holds fails
        // with `UNAVAILABLE`: the callers ask again.
        let Inner { fetching, busy, .. } = &*self.tier.inner;
        let wait = self.until.wait(*busy);
        let permit = reply::take(fetching, key.check_bytes(&header), wait)
            .await
            .ok_or_else(|| {
                Status::unavailable(format!(
                    "no memory within {wait:?} for an object from upstream; ask again"
                ))
            })?;
        let mut object = Assembler::new(header, first);
        while !object.is_done() {
            let part = match self.tier.within(self.until, stream.message()).await {
                Ok(Some(part)) => part,
                Ok(None) => return bad("the reply ended in the middle of the object".into()),
                Err(status) if about_one_object(&status) => return Ok(Next::Failed(status)),
                Err(status) => return Err(status),
            };
            if let Err(why) = object.push(&part) {
                return bad(why);
            }
        }
        let (header, data) = object.finish();
        Ok(Next::Object(key, header, data, permit))
    }
}

impl<S: Store> Drop for Flight<S> {
    fn drop(&mut self) {
        let mut flights = lock(&self.tier.inner.flights);
        for (key, _) in &self.waiting {
            flights.remove(&(self.repo.clone(), *key));
        }
    }
}

/// Whether an upstream error in the stream is about one object, not the
/// whole call. A transport error also ends a stream, with other codes, so
/// only these codes count as one object.
fn about_one_object(status: &Status) -> bool {
    matches!(status.code(), Code::NotFound | Code::DataLoss)
}

/// Checks a repo name by the server's rule, so a tier refuses the calls that
/// the server refuses.
// SHORTCUT: a tier does not ask whether the repo exists, and serves a stored
// object to any caller with the token, for any repo name. Safe with one
// principal; the server also shares trees and blobs across repos. Upgrade
// (issue #13): read tickets, checked by every tier.
fn check_repo(repo: &str) -> Result<(), Status> {
    check::name("repo", repo).map_err(Status::invalid_argument)
}

#[tonic::async_trait]
impl<S: Store> Objects for Tier<S> {
    async fn resolve_ref(
        &self,
        request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        // A ref moves, so its answer is always forwarded and never kept.
        let until = Until::of(&request);
        let mut upstream = self.inner.upstream.clone();
        let reply = self
            .within(
                until,
                upstream.resolve_ref(until.request(request.into_inner())),
            )
            .await?;
        Ok(Response::new(reply.into_inner()))
    }

    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        let until = Until::of(&request);
        let request = request.into_inner();
        check_repo(&request.repo)?;
        let changeset: ChangesetId = check::id(&request.changeset_id, "changeset_id")?;
        let slot = (request.repo.clone(), changeset);
        let kept = lock(&self.inner.root_trees).get(&slot).copied();
        let tree = if let Some(tree) = kept {
            tracing::debug!(%changeset, "root tree hit");
            tree
        } else {
            tracing::debug!(%changeset, "root tree miss");
            let mut upstream = self.inner.upstream.clone();
            let reply = self
                .within(until, upstream.get_root_tree(until.request(request)))
                .await?
                .into_inner();
            let tree: TreeId = TreeId::try_from(reply.tree_id.as_slice())
                .map_err(|_| Status::internal("upstream sent a bad tree id"))?;
            // Only an answer from upstream is kept, never one made here.
            lock(&self.inner.root_trees).insert(slot, tree);
            tree
        };
        Ok(Response::new(GetRootTreeResponse {
            tree_id: tree.as_bytes().to_vec(),
        }))
    }

    type GetObjectsStream = ObjectStream;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<ObjectStream>, Status> {
        let until = Until::of(&request);
        let GetObjectsRequest { repo, refs, accept } = request.into_inner();
        check_repo(&repo)?;
        check::object_count(refs.len())?;
        let keys = refs.iter().map(Key::parse).collect::<Result<Vec<_>, _>>()?;
        let ask = Ask {
            repo,
            keys,
            zstd: accept.contains(&Encoding::Zstd.into()),
            until,
        };
        // The first object is found before the reply starts, as on the
        // server. So when the whole upstream call fails (for example,
        // upstream has no such repo), that is this call's status too, and a
        // tier below fails its fetch once instead of asking again per object.
        let progress = Progress::default();
        let first = self
            .produce(&ask, 0, &progress)
            .await
            .ok_or_else(reply::busy)?;
        let first = split(first)?;
        let (tx, stream) = reply::channel();
        tokio::spawn(self.clone().reply(ask, progress, first, tx));
        Ok(Response::new(stream))
    }
}

/// Checks that only this user can reach `dir`: a real directory (not a
/// symlink), owned by the effective user, with no access for group or
/// others, under directories that no other user can change
/// ([`check_ancestors`](scion_proto::auth::check_ancestors)). Then nobody
/// else can read what is inside, or put a file there. Returns `dir` with its
/// parent resolved: use it, not `dir`, so no symlink is followed again.
/// Fails with `PermissionDenied`, naming the rule that `dir` breaks.
pub(crate) fn check_private_dir(dir: &Path) -> io::Result<std::path::PathBuf> {
    let me = rustix::process::geteuid().as_raw();
    let dir = scion_proto::auth::check_ancestors(dir, me)?;
    let meta = fs::symlink_metadata(&dir)?;
    let refuse = |why: &str| {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} {why}", dir.display()),
        ))
    };
    if !meta.is_dir() {
        return refuse("is not a directory (a symlink is refused too)");
    }
    if meta.uid() != me {
        return refuse("is owned by another user");
    }
    if meta.mode() & 0o077 != 0 {
        return refuse("is open to other users; make it mode 0700");
    }
    Ok(dir)
}

/// Listens on a Unix socket at `path`, in a directory that only this user
/// can reach: a real directory, owned by this user, closed to others. So
/// nobody else can connect, even in the
/// moment before the socket gets mode 0600, or put another socket in its
/// place. Replaces a socket file that nothing listens on, such as one left
/// by a crash.
///
/// # Errors
///
/// [`io::ErrorKind::PermissionDenied`] if the directory is not private,
/// [`io::ErrorKind::AddrInUse`] if a process listens on `path`, or I/O errors.
pub async fn bind_unix(path: &Path) -> io::Result<UnixListener> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no socket file", path.display()),
        )
    })?;
    // The checked directory, so no symlink in `path` is followed again.
    let path = &check_private_dir(dir)?.join(name);
    if fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_socket()) {
        if UnixStream::connect(path).await.is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("a process listens on {}", path.display()),
            ));
        }
        fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use tokio::net::TcpListener;
    use tokio_stream::StreamExt as _;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;

    /// An upstream that serves fixed blobs RAW, and counts the objects asked
    /// for and the most in one call.
    #[derive(Clone)]
    struct Fake {
        blobs: Arc<HashMap<Vec<u8>, Vec<u8>>>,
        asked: Arc<AtomicUsize>,
        largest: Arc<AtomicUsize>,
    }

    type Replies = Pin<Box<dyn Stream<Item = Result<ObjectPart, Status>> + Send>>;

    #[tonic::async_trait]
    impl Objects for Fake {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let refs = request.into_inner().refs;
            self.asked.fetch_add(refs.len(), Ordering::SeqCst);
            self.largest.fetch_max(refs.len(), Ordering::SeqCst);
            let replies: Vec<_> = refs
                .into_iter()
                .flat_map(|r| {
                    let data = self.blobs[&r.id].clone();
                    let header = Header {
                        kind: ObjectKind::Blob,
                        id: r.id.try_into().unwrap(),
                        encoding: Encoding::Raw,
                        encoded_size: data.len(),
                        size: data.len(),
                    };
                    header.parts(object::read_chunks(&data[..], data.len()).unwrap())
                })
                .map(Ok)
                .collect();
            Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
        }
    }

    /// `count` small blobs, their keys, an upstream that serves them, and a
    /// tier in front of it with a store of `limit` bytes.
    async fn setup(count: u32, limit: usize) -> (Vec<Vec<u8>>, Vec<Key>, Fake, Tier<MemoryStore>) {
        let blobs: Vec<Vec<u8>> = (0..count).map(|i| i.to_le_bytes().to_vec()).collect();
        let keys: Vec<Key> = blobs.iter().map(|b| Key::Blob(BlobId::of(b))).collect();
        let ids = keys.iter().map(|k| k.id().to_vec());
        let fake = Fake {
            blobs: Arc::new(ids.zip(blobs.clone()).collect()),
            asked: Arc::default(),
            largest: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake.clone()))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let store = MemoryStore::new(limit);
        let tier = Tier::new(store, &url, "token", CACHE_UPSTREAM_TIMEOUT, None).unwrap();
        (blobs, keys, fake, tier)
    }

    /// What one reply asks for, RAW only.
    fn ask(keys: &[Key]) -> Ask {
        Ask {
            repo: "r".into(),
            keys: keys.to_vec(),
            zstd: false,
            until: Until::NONE,
        }
    }

    #[tokio::test]
    async fn a_store_that_keeps_nothing_costs_one_fetch_per_later_miss() {
        // A limit of 0 bytes: the store keeps nothing.
        let (blobs, keys, fake, tier) = setup(4, 0).await;

        // The objects of one call, each reached after the fetch before it
        // has ended, so none can join a fetch in progress.
        let progress = Progress::default();
        for (i, blob) in blobs.iter().enumerate() {
            let stored = tier
                .fetch(&ask(&keys), i, &progress)
                .await
                .unwrap()
                .unwrap();
            // A RAW blob from upstream is kept packed.
            assert_eq!(object::unpack(&stored, blob.len()).unwrap(), *blob);
            while !lock(&tier.inner.flights).is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        // The first fetch asked for all four; each later one only for itself.
        assert_eq!(fake.asked.load(Ordering::SeqCst), 4 + 3);
    }

    #[tokio::test]
    async fn an_object_that_a_fetch_has_stored_is_never_fetched_again() {
        let (blobs, keys, fake, tier) = setup(3, 1 << 20).await;
        let asked = || fake.asked.load(Ordering::SeqCst);
        let settled = || async {
            while !lock(&tier.inner.flights).is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };

        // A fetch of the last two objects ends, and stores them.
        tier.fetch(&ask(&keys), 1, &Progress::default())
            .await
            .unwrap();
        settled().await;
        assert_eq!(asked(), 2);
        // A batch that starts at the first object leaves out the two stored.
        tier.fetch(&ask(&keys), 0, &Progress::default())
            .await
            .unwrap();
        settled().await;
        assert_eq!(asked(), 3);
        // A reply that missed an object just before its fetch stored it, and
        // asks after the fetch has ended, is sent to the store. This is the
        // window between a reply's store check and its fetch.
        for i in 0..blobs.len() {
            let fetched = tier.fetch(&ask(&keys), i, &Progress::default()).await;
            assert!(fetched.unwrap().is_none(), "read it from the store");
        }
        assert_eq!(asked(), 3, "nothing was fetched twice");
        // Through a reply, the object comes from the store, with its share.
        for (i, blob) in blobs.iter().enumerate() {
            let (object, _permit) = tier
                .produce(&ask(&keys), i, &Progress::default())
                .await
                .unwrap();
            // Unpacked for a client that takes only RAW.
            let Body::Chunks(chunks) = object.unwrap().body else {
                panic!("unpacked into chunks")
            };
            assert_eq!(chunks, std::slice::from_ref(blob));
        }
        assert_eq!(asked(), 3);
    }

    #[tokio::test]
    async fn a_reply_that_waits_for_budget_reads_the_object_again() {
        // A store that keeps the object, then one that keeps nothing.
        for (limit, asked) in [(1 << 20, 1), (0, 2)] {
            let (blobs, keys, fake, tier) = setup(1, limit).await;
            let full = reply::try_take(&tier.inner.in_flight, IN_FLIGHT_BYTES).unwrap();
            let waiting = tier.clone();
            let producing = tokio::spawn(async move {
                let found = waiting.produce(&ask(&keys), 0, &Progress::default()).await;
                found.map(|(parts, _permit)| parts)
            });
            // The fetch ends; the reply finds no room, lets the bytes go and
            // waits.
            while fake.asked.load(Ordering::SeqCst) == 0 || !lock(&tier.inner.flights).is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!producing.is_finished());
            drop(full);

            let object = producing.await.unwrap().unwrap().unwrap();
            // A client that does not accept zstd gets the content.
            assert_eq!(object.header.encoding, Encoding::Raw);
            let Body::Chunks(chunks) = object.body else {
                panic!("unpacked into chunks")
            };
            assert_eq!(chunks, [blobs[0].clone()]);
            // Read again from the store, or fetched again if it was not kept.
            assert_eq!(fake.asked.load(Ordering::SeqCst), asked, "limit {limit}");
            // Every share went back.
            assert_eq!(tier.inner.in_flight.available_permits(), IN_FLIGHT_BYTES);
            assert_eq!(tier.inner.fetching.available_permits(), FETCH_BYTES);
        }
    }

    #[tokio::test]
    async fn a_cold_call_fetches_in_batches_of_at_most_max_batch() {
        let (_, keys, fake, tier) = setup(300, 1 << 20).await;
        let request = GetObjectsRequest {
            repo: "r".into(),
            refs: keys.iter().map(|key| key.to_ref()).collect(),
            accept: vec![Encoding::Zstd.into()],
        };
        let reply = tier.get_objects(Request::new(request)).await.unwrap();
        let mut stream = reply.into_inner();
        let mut got = 0;
        while let Some(part) = stream.next().await {
            // Each blob packed, in one part.
            let part = part.unwrap();
            assert_eq!(part.encoding, i32::from(Encoding::Zstd));
            got += 1;
        }
        assert_eq!(got, 300);
        assert_eq!(fake.asked.load(Ordering::SeqCst), 300);
        assert_eq!(fake.largest.load(Ordering::SeqCst), MAX_BATCH);
        assert_eq!(tier.upstream_calls(), 3);
    }

    #[test]
    fn a_stored_form_is_checked_against_its_key() {
        let content = b"some content\n";
        let key = Key::Blob(BlobId::of(content));
        let frame = object::pack(content).unwrap();
        key.check_stored(&frame).unwrap();
        // The content itself is not the stored form of a blob.
        assert!(key.check_stored(content).is_err());
        // A frame of other content.
        let other = object::pack(b"other content\n").unwrap();
        assert!(key.check_stored(&other).is_err());
        // A RAW blob from upstream is packed; a ZSTD one is kept as it came.
        let raw = Header {
            kind: ObjectKind::Blob,
            id: key.id(),
            encoding: Encoding::Raw,
            encoded_size: content.len(),
            size: content.len(),
        };
        assert_eq!(key.to_stored(&raw, content.to_vec()).unwrap(), frame);
        let packed = Header {
            encoding: Encoding::Zstd,
            encoded_size: frame.len(),
            ..raw.clone()
        };
        assert_eq!(key.to_stored(&packed, frame.clone()).unwrap(), frame);
        // A frame whose header and the object's header disagree on the size.
        let lying = Header {
            size: content.len() + 1,
            ..packed
        };
        assert!(key.to_stored(&lying, frame).is_err());
    }

    /// An upstream that sends the first part of one large RAW blob, then
    /// nothing more.
    struct Stalls(Vec<u8>);

    #[tonic::async_trait]
    impl Objects for Stalls {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::unimplemented("no refs"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("no changesets"))
        }

        type GetObjectsStream = Replies;

        async fn get_objects(
            &self,
            _: Request<GetObjectsRequest>,
        ) -> Result<Response<Replies>, Status> {
            let data = self.0.clone();
            let header = Header {
                kind: ObjectKind::Blob,
                id: *BlobId::of(&data).as_bytes(),
                encoding: Encoding::Raw,
                encoded_size: data.len(),
                size: data.len(),
            };
            let first = header
                .parts(object::read_chunks(&data[..], data.len()).unwrap())
                .remove(0);
            let parts = tokio_stream::iter([Ok(first)]).chain(tokio_stream::pending());
            Ok(Response::new(Box::pin(parts)))
        }
    }

    /// A tier in front of [`Stalls`] that sends the first part of `data`.
    async fn stalled_upstream(data: &[u8]) -> Tier<MemoryStore> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(Stalls(data.to_vec())))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Tier::new(
            MemoryStore::new(0),
            &url,
            "token",
            CACHE_UPSTREAM_TIMEOUT,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_fetch_that_finds_no_slot_ends_with_unavailable() {
        let data = b"an object".to_vec();
        let key = Key::Blob(BlobId::of(&data));
        let mut tier = stalled_upstream(&data).await;
        Arc::get_mut(&mut tier.inner).unwrap().busy = Duration::from_millis(50);
        // Other fetches hold every slot.
        let slots = Arc::clone(&tier.inner.fetch_slots);
        let others = slots
            .acquire_many_owned(u32::try_from(FETCH_SLOTS).unwrap())
            .await
            .unwrap();

        let (ask, progress) = (ask(&[key]), Progress::default());
        let fetch = tier.fetch(&ask, 0, &progress);
        let failed = tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("the wait for a slot is bounded")
            .unwrap_err();
        assert_eq!(failed.status.code(), Code::Unavailable, "{failed:?}");
        assert!(failed.status.message().contains("slot"), "{failed:?}");
        assert!(failed.whole_call);
        drop(others);
    }

    #[test]
    fn a_blob_packed_here_keeps_no_room_for_the_worst_case() {
        // Content that packs small.
        let data = vec![0; 1 << 20];
        let key = Key::Blob(BlobId::of(&data));
        let header = Header {
            kind: ObjectKind::Blob,
            id: key.id(),
            encoding: Encoding::Raw,
            encoded_size: data.len(),
            size: data.len(),
        };
        let stored = key.to_stored(&header, data).unwrap();
        assert!(stored.len() < 1024, "{}", stored.len());
        assert!(
            stored.capacity() <= stored.len() + 64,
            "{}",
            stored.capacity()
        );
    }

    #[test]
    fn decoding_a_tree_fits_its_factor() {
        use scion_core::{Entry, PathElem};
        // The smallest entry: a one-byte name of a directory.
        let tree = |names: &[&str]| Tree {
            entries: names
                .iter()
                .map(|name| {
                    (
                        PathElem::new(*name).unwrap(),
                        Entry::Dir(Tree::default().id()),
                    )
                })
                .collect(),
        };
        let entry = tree(&["a", "b"]).encode().len() - tree(&["a"]).encode().len();
        assert_eq!(entry, 38);
        // Decoded: the pair in a list and in a map node, and the name's
        // buffer (a small allocation takes 16 bytes or more).
        let pair = std::mem::size_of::<(PathElem, Entry)>();
        let node = 2 * pair;
        let decoded = pair + node + 32;
        assert!(
            decoded <= TREE_DECODE_FACTOR * entry,
            "{decoded} > {}",
            TREE_DECODE_FACTOR * entry
        );
    }

    #[tokio::test]
    async fn a_fetch_that_finds_no_memory_ends_with_unavailable() {
        let data = vec![7; 3 * scion_proto::CHUNK_BYTES];
        let key = Key::Blob(BlobId::of(&data));
        let mut tier = stalled_upstream(&data).await;
        Arc::get_mut(&mut tier.inner).unwrap().busy = Duration::from_millis(50);
        // Other fetches hold the whole fetch budget.
        let others = reply::try_take(&tier.inner.fetching, FETCH_BYTES).unwrap();

        // The upstream reply stays open meanwhile, so the wait is bounded:
        // the fetch fails, and so does every object it holds.
        let (ask, progress) = (ask(&[key]), Progress::default());
        let fetch = tier.fetch(&ask, 0, &progress);
        let failed = tokio::time::timeout(Duration::from_secs(10), fetch)
            .await
            .expect("the wait for memory is bounded")
            .unwrap_err();
        assert_eq!(failed.status.code(), Code::Unavailable, "{failed:?}");
        assert!(failed.whole_call);
        assert!(lock(&tier.inner.flights).is_empty());
        drop(others);
    }

    #[tokio::test]
    async fn a_fetch_takes_what_the_first_part_announces_before_the_rest_arrives() {
        let data = vec![7; 3 * scion_proto::CHUNK_BYTES];
        let key = Key::Blob(BlobId::of(&data));
        let tier = stalled_upstream(&data).await;
        let fetching = tier.clone();
        let fetch =
            tokio::spawn(
                async move { fetching.fetch(&ask(&[key]), 0, &Progress::default()).await },
            );

        // The content, the frame packed from it and the codec, no more: a
        // small object takes no room for the largest one.
        let held = || FETCH_BYTES - tier.inner.fetching.available_permits();
        let expected = data.len() + zstd_bound(data.len()) + UNPACK_BYTES;
        let started = tokio::time::Instant::now();
        while held() != expected {
            assert!(held() <= expected, "{} > {expected}", held());
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "held {}",
                held()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(tier.inner.fetch_slots.available_permits(), FETCH_SLOTS - 1);
        fetch.abort();
    }
}

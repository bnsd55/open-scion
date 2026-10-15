//! Cache tiers for the `Objects` service: `scion-cache` and `scion-daemon`.
//!
//! A [`Tier`] answers the same `Objects` calls as the server. It serves trees
//! and blobs from its own [`Store`] and sends each miss to its one upstream:
//! the next tier, or the server. It never trusts that upstream:
//!
//! - Every tree and blob from upstream is checked against its ID before the
//!   tier stores it or sends it on.
//! - `GetRootTree` answers are kept, but only answers that upstream gave. A
//!   changeset never changes, so its root tree never does.
//! - `ResolveRef` is always forwarded, never kept. A ref moves.
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
use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fmt, fs, io, iter};

use anyhow::Context as _;
use hashlink::LruCache;
use scion_core::{BlobId, ChangesetId, Tree, TreeId};
use scion_proto::auth::{Auth, Bearer};
use scion_proto::check;
use scion_proto::reply::{self, IN_FLIGHT_BYTES, ObjectStream, STALL_TIMEOUT, Sent};
use scion_proto::v1::objects_client::ObjectsClient;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, Object, ObjectKind, ObjectRef,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES, MAX_OBJECT_MESSAGE_BYTES};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_stream::Stream;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

use store::lock;
pub use store::{DiskStore, MemoryStore, Store};

/// Most `GetRootTree` answers a tier keeps.
const MAX_ROOT_TREES: usize = 10_000;

/// A tier gives up on its upstream when a call, or the next object of a
/// reply, takes longer than this.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(120);

/// One object, by kind and ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Key {
    /// A tree, stored as `TreeV1` bytes.
    Tree(TreeId),
    /// A blob, stored as its raw content.
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

    fn object(self, data: Vec<u8>) -> Object {
        Object {
            kind: self.kind().into(),
            id: self.id().to_vec(),
            data,
        }
    }

    /// Whether `object` names this object. Says nothing about its bytes.
    fn names(self, object: &Object) -> bool {
        object.kind == i32::from(self.kind()) && object.id == self.id()
    }

    /// Whether `data` is this object: a blob's content hashes to its ID, and
    /// a tree's bytes decode and hash to its ID.
    #[must_use]
    pub fn matches(self, data: &[u8]) -> bool {
        match self {
            Self::Tree(id) => Tree::decode_with_id(data).is_ok_and(|(_, actual)| actual == id),
            Self::Blob(id) => BlobId::of(data) == id,
        }
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
/// then the object, or why the fetch failed.
type Outcome = Option<Result<Arc<Vec<u8>>, Status>>;

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
    /// Calls sent upstream, of every kind.
    upstream_calls: Arc<AtomicU64>,
    /// The reply budget, as on the server: object bytes read or copied for a
    /// reply and not yet taken by its client.
    in_flight: Arc<Semaphore>,
    /// Object bytes received from upstream and not yet stored.
    fetching: Arc<Semaphore>,
    flights: Mutex<Flights>,
    /// `GetRootTree` answers from upstream, by repo and changeset.
    root_trees: Mutex<LruCache<(String, ChangesetId), TreeId>>,
}

/// What one reply has done: hits and misses for the log, and whether it has
/// started a batched fetch.
#[derive(Default)]
struct Progress {
    hits: AtomicUsize,
    misses: AtomicUsize,
    batched: AtomicBool,
}

impl<S: Store> Tier<S> {
    /// A tier that keeps objects in `store` and sends misses to `upstream`
    /// (`http://HOST:PORT` or `unix:///PATH`) with `upstream_token`. It
    /// connects on the first call.
    ///
    /// # Errors
    ///
    /// A bad upstream address, or a token that is not ASCII.
    pub fn new(store: S, upstream: &str, upstream_token: &str) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(upstream.to_owned())
            .with_context(|| format!("bad upstream address {upstream:?}"))?
            .connect_lazy();
        let bearer = Bearer::new(upstream_token).context("upstream token is not valid ASCII")?;
        let upstream_calls = Arc::default();
        let auth = UpstreamAuth {
            bearer,
            calls: Arc::clone(&upstream_calls),
        };
        let upstream = ObjectsClient::with_interceptor(channel, auth)
            .max_decoding_message_size(MAX_OBJECT_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                upstream,
                upstream_calls,
                in_flight: Arc::new(Semaphore::new(IN_FLIGHT_BYTES)),
                fetching: Arc::new(Semaphore::new(IN_FLIGHT_BYTES)),
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
    /// send `token`, until `shutdown` resolves.
    ///
    /// # Errors
    ///
    /// Transport errors.
    pub async fn serve<I, IO, IE>(
        self,
        token: &str,
        incoming: I,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = Result<IO, IE>>,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IE: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let objects = ObjectsServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_OBJECT_MESSAGE_BYTES);
        Server::builder()
            .add_service(InterceptedService::new(objects, Auth::new(token)))
            .serve_with_incoming_shutdown(incoming, shutdown)
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

    /// Streams the objects named by `keys` into `tx`.
    async fn reply(self, repo: String, keys: Vec<Key>, tx: mpsc::Sender<Sent>) {
        let progress = Progress::default();
        let produce = |i| self.produce(&repo, &keys[i..], &progress);
        reply::send_objects(0..keys.len(), tx, STALL_TIMEOUT, produce).await;
        tracing::info!(
            objects = keys.len(),
            hits = progress.hits.into_inner(),
            misses = progress.misses.into_inner(),
            "GetObjects"
        );
    }

    /// Finds the first object of `rest` for a reply: in the store, or else
    /// from upstream. Takes the object's share of the reply budget before its
    /// bytes are read from the store or copied into the reply.
    // SHORTCUT: two blocking hops per hit (size, then read) and no read-ahead,
    // as on the server. Upgrade (issue #6): read a batch capped by bytes per
    // hop, or keep a few reads in flight.
    async fn produce(&self, repo: &str, rest: &[Key], progress: &Progress) -> Option<Sent> {
        let (&key, later) = rest.split_first()?;
        let size = match self.blocking(move |store| store.size(key)).await {
            Ok(size) => size,
            Err(status) => {
                return Some((Err(status), reply::take(&self.inner.in_flight, 1).await?));
            }
        };
        if let Some(size) = size {
            let permit = reply::take(&self.inner.in_flight, size).await?;
            match self.blocking(move |store| store.get(key)).await {
                Ok(Some(data)) if data.len() <= size => {
                    progress.hits.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(%key, "hit");
                    return Some((Ok(key.object(data)), permit));
                }
                // The stored copy went (dropped, or bad) after its size was
                // read: fetch it.
                Ok(_) => {}
                Err(status) => return Some((Err(status), permit)),
            }
        }
        progress.misses.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(%key, "miss");
        // The fetched bytes were counted in the fetch budget until stored.
        // The share taken here counts the reply's copy of them.
        let fetched = self.fetch(repo, key, later, progress).await;
        let size = fetched.as_ref().map_or(1, |data| data.len());
        let permit = reply::take(&self.inner.in_flight, size).await?;
        Some((fetched.map(|data| key.object(data.to_vec())), permit))
    }

    /// Gets `key` from upstream. Joins the fetch in progress for it, or
    /// starts one. A reply's first fetch also takes every object in `later`
    /// that the store lacks, so one upstream call serves many misses. Its
    /// later fetches take only their own object, so a store that does not
    /// keep objects costs one fetch per object, not one per remaining call.
    async fn fetch(
        &self,
        repo: &str,
        key: Key,
        later: &[Key],
        progress: &Progress,
    ) -> Result<Arc<Vec<u8>>, Status> {
        let joined = lock(&self.inner.flights)
            .get(&(repo.to_owned(), key))
            .cloned();
        let mut flight = if let Some(flight) = joined {
            flight
        } else if progress.batched.swap(true, Ordering::Relaxed) {
            self.start(repo, key, Vec::new())
        } else {
            let later = later.to_vec();
            let missing = self
                .blocking(move |store| {
                    Ok(later
                        .into_iter()
                        .filter(|&k| !matches!(store.size(k), Ok(Some(_))))
                        .collect())
                })
                .await?;
            self.start(repo, key, missing)
        };
        let outcome = flight
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|outcome| outcome.clone());
        outcome.unwrap_or_else(|| {
            Err(Status::internal(
                "a fetch from upstream ended without a result",
            ))
        })
    }

    /// Starts one fetch for `key` and `later`, minus the objects that another
    /// fetch already has. Returns the receiver for `key`.
    fn start(&self, repo: &str, key: Key, later: Vec<Key>) -> watch::Receiver<Outcome> {
        let mut flights = lock(&self.inner.flights);
        let mut waiting = VecDeque::new();
        for k in iter::once(key).chain(later) {
            if let Entry::Vacant(slot) = flights.entry((repo.to_owned(), k)) {
                let (tx, rx) = watch::channel(None);
                slot.insert(rx);
                waiting.push_back((k, tx));
            }
        }
        let rx = flights[&(repo.to_owned(), key)].clone();
        drop(flights);
        if !waiting.is_empty() {
            let flight = Flight {
                tier: self.clone(),
                repo: repo.to_owned(),
                waiting,
            };
            tokio::spawn(flight.run());
        }
        rx
    }
}

/// One fetch from upstream: a few objects of one repo, which other replies
/// can wait for. It runs on its own task, so it never waits for a reply, and
/// it ends when every object is fetched or upstream fails. Objects still
/// waiting when it ends leave the fetch table, so a later call starts anew.
struct Flight<S: Store> {
    tier: Tier<S>,
    repo: String,
    /// The objects not fetched yet, in request order, each with the sender
    /// that its waiters listen to.
    waiting: VecDeque<(Key, watch::Sender<Outcome>)>,
}

impl<S: Store> Flight<S> {
    async fn run(mut self) {
        tracing::debug!(objects = self.waiting.len(), "fetching from upstream");
        if let Err(status) = self.fetch_all().await {
            tracing::warn!(%status, objects = self.waiting.len(), "fetch from upstream failed");
            for (_, tx) in &self.waiting {
                tx.send_replace(Some(Err(status.clone())));
            }
        }
    }

    /// Fetches every waiting object. Checks each against its ID, stores it,
    /// and hands it to its waiters. An upstream reply may stop early; the
    /// rest are asked for again.
    async fn fetch_all(&mut self) -> Result<(), Status> {
        while !self.waiting.is_empty() {
            let refs = self.waiting.iter().map(|(key, _)| key.to_ref()).collect();
            let request = GetObjectsRequest {
                repo: self.repo.clone(),
                refs,
            };
            let mut upstream = self.tier.inner.upstream.clone();
            let mut stream = within(upstream.get_objects(request)).await?.into_inner();
            let before = self.waiting.len();
            loop {
                // Budget for the largest object, taken before the next object
                // arrives, then cut to its size.
                // SHORTCUT: so at most four objects come from upstream at
                // once, for the whole tier, however small they are. Upgrade
                // (issue #6): learn an object's size before its bytes, and
                // take only that.
                let mut permit = reply::take(&self.tier.inner.fetching, MAX_OBJECT_BYTES)
                    .await
                    .ok_or_else(|| Status::unavailable("shutting down"))?;
                let Some(object) = within(stream.message()).await? else {
                    break;
                };
                let Some(&(key, _)) = self.waiting.front().filter(|(key, _)| key.names(&object))
                else {
                    return Err(Status::internal(
                        "upstream sent an object that was not asked for",
                    ));
                };
                drop(permit.split(MAX_OBJECT_BYTES.saturating_sub(object.data.len())));
                let data = Arc::new(object.data);
                let checked = Arc::clone(&data);
                // Hashing up to 100 MiB takes a while: check and store on a
                // blocking thread.
                let matches = self
                    .tier
                    .blocking(move |store| {
                        if checked.len() > MAX_OBJECT_BYTES || !key.matches(&checked) {
                            return Ok(false);
                        }
                        // A failed put is logged; the object still goes to
                        // its waiters.
                        if let Err(e) = store.put(key, &checked) {
                            tracing::warn!(%key, error = %e, "cannot store an object");
                        }
                        Ok(true)
                    })
                    .await?;
                if !matches {
                    return Err(Status::data_loss(format!(
                        "upstream sent bytes that are not {key}"
                    )));
                }
                if let Some((key, tx)) = self.waiting.pop_front() {
                    tx.send_replace(Some(Ok(data)));
                    lock(&self.tier.inner.flights).remove(&(self.repo.clone(), key));
                }
            }
            if self.waiting.len() == before {
                return Err(Status::unavailable(
                    "upstream sent none of the requested objects",
                ));
            }
        }
        Ok(())
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

/// Waits at most [`UPSTREAM_TIMEOUT`] for an upstream call.
async fn within<T>(call: impl Future<Output = Result<T, Status>>) -> Result<T, Status> {
    tokio::time::timeout(UPSTREAM_TIMEOUT, call)
        .await
        .unwrap_or_else(|_| Err(Status::unavailable("upstream did not answer in time")))
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
        let mut upstream = self.inner.upstream.clone();
        let reply = within(upstream.resolve_ref(request.into_inner())).await?;
        Ok(Response::new(reply.into_inner()))
    }

    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
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
            let reply = within(upstream.get_root_tree(request)).await?.into_inner();
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
        let GetObjectsRequest { repo, refs } = request.into_inner();
        check_repo(&repo)?;
        check::object_count(refs.len())?;
        let keys = refs.iter().map(Key::parse).collect::<Result<Vec<_>, _>>()?;
        let (tx, stream) = reply::channel();
        tokio::spawn(self.clone().reply(repo, keys, tx));
        Ok(Response::new(stream))
    }
}

/// Listens on a Unix socket at `path` with mode 0600, so only this user can
/// connect. Replaces a socket file that nothing listens on, such as one left
/// by a crash. Put the socket in a directory that only this user can enter,
/// so nobody connects before the mode is set.
///
/// # Errors
///
/// [`io::ErrorKind::AddrInUse`] if a process listens on `path`, or I/O errors.
pub async fn bind_unix(path: &Path) -> io::Result<UnixListener> {
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
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;

    /// An upstream that serves fixed blobs and counts the objects asked for.
    #[derive(Clone)]
    struct Fake {
        blobs: Arc<HashMap<Vec<u8>, Vec<u8>>>,
        asked: Arc<AtomicUsize>,
    }

    type Replies = Pin<Box<dyn Stream<Item = Result<Object, Status>> + Send>>;

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
            let replies: Vec<_> = refs
                .into_iter()
                .map(|r| {
                    Ok(Object {
                        data: self.blobs[&r.id].clone(),
                        kind: r.kind,
                        id: r.id,
                    })
                })
                .collect();
            Ok(Response::new(Box::pin(tokio_stream::iter(replies))))
        }
    }

    #[tokio::test]
    async fn a_store_that_keeps_nothing_costs_one_fetch_per_later_miss() {
        let blobs: Vec<Vec<u8>> = (0..4).map(|i| vec![i; 3]).collect();
        let keys: Vec<Key> = blobs.iter().map(|b| Key::Blob(BlobId::of(b))).collect();
        let fake = Fake {
            blobs: Arc::new(
                keys.iter()
                    .map(|k| k.id().to_vec())
                    .zip(blobs.clone())
                    .collect(),
            ),
            asked: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(
            Server::builder()
                .add_service(ObjectsServer::new(fake.clone()))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        // A limit of 0 bytes: the store keeps nothing.
        let tier = Tier::new(MemoryStore::new(0), &url, "token").unwrap();

        // The objects of one call, each reached after the fetch before it
        // has ended, so none can join a fetch in progress.
        let progress = Progress::default();
        for (i, (&key, blob)) in keys.iter().zip(&blobs).enumerate() {
            let data = tier.fetch("r", key, &keys[i + 1..], &progress).await;
            assert_eq!(*data.unwrap(), *blob);
            while !lock(&tier.inner.flights).is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        // The first fetch asked for all four; each later one only for itself.
        assert_eq!(fake.asked.load(Ordering::SeqCst), 4 + 3);
    }
}

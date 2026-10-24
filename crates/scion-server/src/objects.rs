//! The `Objects` service: the reads that any cache tier can answer.
//!
//! A client resolves a ref, gets the root tree of the changeset, then asks for
//! trees and blobs by ID. A tree goes out as its stored `TreeV1` bytes. A blob
//! goes out as its stored zstd file, as is, if the client accepts zstd, or
//! else unpacked and checked. The client checks each object against its ID.

use std::collections::HashSet;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use scion_core::{BlobId, TreeId};
use scion_proto::runtime::auth::{Auth, Caller};
use scion_proto::runtime::deadline::Until;
use scion_proto::runtime::object::{self, Header, zstd_bound};
use scion_proto::runtime::reply::{
    self, BUSY_TIMEOUT, Body, Found, IN_FLIGHT_BYTES, ObjectStream, Outgoing, STALL_TIMEOUT,
};
use scion_proto::ticket::Walk;
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectRef,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{Bytes, CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_PART_MESSAGE_BYTES};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

use crate::access::Access;
use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::error::{Error, Result};
use crate::meta::Meta as _;
use crate::meta::MetaTx as _;
use crate::metrics::Metrics;
use crate::quotas::{Call, Quotas};
use crate::read;
use crate::service::{Worker, check_reader, parse_id, resolve_ref};
use crate::store::{self};
use crate::tickets::Keys;
use crate::views;

// The store keeps exactly the objects that the API promises to send.
const _: () = assert!(store::MAX_OBJECT_BYTES == scion_proto::MAX_OBJECT_BYTES);

/// The bytes of trees the last `prove` handed to its walk. Tests read it to
/// check that the walk stops before it decodes past the reply budget.
#[cfg(test)]
static WALK_DECODED: AtomicUsize = AtomicUsize::new(0);

/// One object to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Key {
    Tree(TreeId),
    Blob(BlobId),
}

/// How an object goes out, known before its bytes are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Plan {
    encoding: Encoding,
    /// The bytes that go out, which the reply holds.
    encoded: usize,
    /// The size of the object's bytes.
    size: usize,
}

impl Plan {
    /// The most memory that reading the object holds: its bytes, and the
    /// decoder of a blob that goes out unpacked.
    fn read_bytes(&self, key: Key) -> usize {
        match (key, self.encoding) {
            (Key::Blob(_), Encoding::Raw) => self.encoded + store::READER_BYTES,
            _ => self.encoded,
        }
    }
}

/// Implements the `Objects` RPCs.
pub(crate) struct ObjectsService {
    worker: Worker,
    access: Arc<Access>,
    /// The registry that counts the view derivations.
    metrics: Arc<Metrics>,
    /// What one principal may hold at once.
    quotas: Quotas,
    /// The tokens of the server: every call of this service but
    /// `GetObjects` needs one. An object read's permission is its ticket.
    auth: Auth,
    /// The keys that mint the tickets of `GetRootTree` replies. `None`
    /// keeps every object read closed: a reply then carries no ticket.
    tickets: Option<Arc<Keys>>,
    /// `IN_FLIGHT_BYTES` permits, one per object byte, shared by all replies.
    in_flight: Arc<Semaphore>,
}

impl ObjectsService {
    pub(crate) fn new(
        worker: Worker,
        access: Arc<Access>,
        quotas: Quotas,
        tickets: Option<Arc<Keys>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            worker,
            auth: access.auth(),
            access,
            quotas,
            tickets,
            metrics,
            in_flight: Arc::new(Semaphore::new(IN_FLIGHT_BYTES)),
        }
    }

    pub(crate) fn into_server(self) -> ObjectsServer<Self> {
        ObjectsServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_PART_MESSAGE_BYTES)
    }
}

fn parse_ref(object: &ObjectRef) -> Result<Key, Status> {
    match ObjectKind::try_from(object.kind) {
        Ok(ObjectKind::Tree) => Ok(Key::Tree(parse_id(&object.id, "tree id")?)),
        Ok(ObjectKind::Blob) => Ok(Key::Blob(parse_id(&object.id, "blob id")?)),
        Ok(ObjectKind::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("object kind must be TREE or BLOB"))
        }
    }
}

/// Reads an object, as planned. [`read_object`], except in tests that count
/// reads.
type Read = fn(&Backend, Key, Plan) -> Result<Outgoing>;

/// How `key` goes out, without reading its bytes. A blob goes out as its
/// stored zstd file if the client accepts `zstd`. Only an object that the
/// ticket's walk proved goes out: everything else is not found.
fn plan(store: &Backend, key: Key, allowed: &HashSet<Key>, zstd: bool) -> Result<Plan> {
    if !allowed.contains(&key) {
        // The walk did not reach this object from the ticket's root tree.
        return Err(Error::NotFound(format!("{key:?} is not in the ticket")));
    }
    let corrupt = |why: &str| Err(Error::Corrupt(format!("{key:?}: {why}")));
    let raw = |size| Plan {
        encoding: Encoding::Raw,
        encoded: size,
        size,
    };
    let id = match key {
        Key::Tree(id) => {
            let size = store.read(|c| c.tree_size(id))?;
            if size > store::MAX_OBJECT_BYTES {
                return corrupt("larger than the object limit");
            }
            return Ok(raw(size));
        }
        Key::Blob(id) => id,
    };
    // From the frame header, so at most the object limit.
    let size = store.blobs().size(id)?;
    if !zstd {
        return Ok(raw(size));
    }
    let packed = store.blobs().open_file(id)?.metadata()?.len();
    match usize::try_from(packed) {
        // An empty file holds no frame (see `Blobs::verify`): it goes out RAW.
        Ok(0) => Ok(raw(size)),
        Ok(packed) if packed <= zstd_bound(size) => Ok(Plan {
            encoding: Encoding::Zstd,
            encoded: packed,
            size,
        }),
        _ => corrupt("its file is larger than a zstd frame of its size"),
    }
}

/// Reads one object, as planned. A tree and a RAW blob are checked against
/// their IDs on the way out of the store. A ZSTD blob is its stored file,
/// read in chunks and not unpacked: the receiver checks it. A RAW blob is
/// unpacked in chunks, so it is never in memory twice.
fn read_object(store: &Backend, key: Key, plan: Plan) -> Result<Outgoing> {
    let (kind, id, body) = match key {
        Key::Tree(id) => {
            let data = store.read(|c| c.get_tree_body(id))?;
            (
                ObjectKind::Tree,
                *id.as_bytes(),
                Body::Whole(Bytes::from(data)),
            )
        }
        Key::Blob(id) if plan.encoding == Encoding::Zstd => {
            let file = store.blobs().open_file(id)?;
            if file.metadata()?.len() != plan.encoded as u64 {
                return Err(Error::Corrupt(format!("blob {id}: its file changed size")));
            }
            let chunks = object::read_chunks(file, plan.encoded)?;
            (ObjectKind::Blob, *id.as_bytes(), Body::Chunks(chunks))
        }
        Key::Blob(id) => {
            // The last chunk comes only once the whole content matched the
            // ID.
            let mut reader = store.blobs().reader(id)?;
            let mut chunks = Vec::new();
            while let Some(chunk) = reader.read_part(CHUNK_BYTES)? {
                chunks.push(Bytes::from(chunk));
            }
            (ObjectKind::Blob, *id.as_bytes(), Body::Chunks(chunks))
        }
    };
    let read = match &body {
        Body::Whole(data) => data.len(),
        Body::Chunks(chunks) => chunks.iter().map(Bytes::len).sum(),
    };
    if read != plan.encoded {
        return Err(Error::Corrupt(format!(
            "{key:?}: {read} bytes, but its stored size is {}",
            plan.encoded
        )));
    }
    let header = Header {
        kind,
        id,
        encoding: plan.encoding,
        encoded_size: plan.encoded,
        size: plan.size,
    };
    Ok(Outgoing { header, body })
}

/// How long a reply waits: for room in the budget, and for its client. A
/// wait for room also ends at the caller's deadline.
#[derive(Clone, Copy)]
struct Waits {
    busy: Duration,
    stall: Duration,
    until: Until,
}

const WAITS: Waits = Waits {
    busy: BUSY_TIMEOUT,
    stall: STALL_TIMEOUT,
    until: Until::NONE,
};

/// What one `GetObjects` reply runs with, fixed when it starts. A wait for
/// room ends before the caller's deadline, so the caller gets that status,
/// not a timeout of its own.
struct Reply {
    /// The share of `IN_FLIGHT_BYTES` that all replies share.
    in_flight: Arc<Semaphore>,
    waits: Waits,
    /// The handle of the caller's connection: the reply counts what it
    /// leaves in HTTP/2 on it.
    stalled: Option<scion_proto::runtime::listen::Stalled>,
    /// The quota slot of the ticket's principal, held to the stream's end.
    call: Call,
}

/// Streams `keys`: the first object here, the rest from the producer task.
/// `decoded` is the memory that the trees of the ticket's walk decoded; the
/// reply takes its share of the budget for it, held to the stream's end.
async fn start_reply(
    worker: Worker,
    keys: Vec<Key>,
    allowed: HashSet<Key>,
    reply: Reply,
    read: Read,
    zstd: bool,
    decoded: usize,
) -> Result<ObjectStream, Status> {
    let Reply {
        in_flight,
        waits,
        stalled,
        call,
    } = reply;
    let mut rest = keys.into_iter();
    let Some(key) = rest.next() else {
        return Err(Status::invalid_argument("a call names at least one object"));
    };
    let busy = waits.until.wait(waits.busy);
    // The share of the walk's trees: held to the end of the call, so the
    // walks of all calls together cannot outrun the reply budget. No room
    // within the busy wait: the call ends `RESOURCE_EXHAUSTED`. A walk that
    // decoded nothing takes no share.
    let walk_share = if decoded > 0 {
        Some(reply::take(&in_flight, decoded, busy).await)
    } else {
        None
    };
    if decoded > 0 && walk_share.is_none() {
        return Err(reply::busy());
    }
    let first = find(&worker, &in_flight, busy, read, &allowed, zstd, key)
        .await
        .ok_or_else(reply::busy)?;
    let (tx, stream, slot) = reply::channel();
    tokio::spawn(
        async move {
            let _walk_share = walk_share;
            // The principal's quota slot is held to the end of the stream,
            // so past the end of the call.
            let _keep = call;
            let (worker, in_flight) = (&worker, &in_flight);
            let produce = |key| {
                find(
                    worker,
                    in_flight,
                    waits.until.wait(waits.busy),
                    read,
                    &allowed,
                    zstd,
                    key,
                )
            };
            reply::send_objects(first, rest, tx, waits.stall, stalled, slot, produce).await;
        }
        // The reply runs outside the call, but stays in its span.
        .instrument(tracing::Span::current()),
    );
    Ok(stream)
}

/// Finds one object for a reply: plans how it goes out, takes the share of
/// `in_flight` for the memory that reading it holds, and only then reads it.
/// Then keeps only the share of the bytes it sends. `None` if the share is
/// not free within `busy`.
// SHORTCUT: two blocking hops per object and no read-ahead, so many small
// objects are read one after another. Upgrade: read a batch capped by bytes
// per hop, or keep a few reads in flight (issue #6).
async fn find(
    worker: &Worker,
    in_flight: &Arc<Semaphore>,
    busy: Duration,
    read: Read,
    allowed: &HashSet<Key>,
    zstd: bool,
    key: Key,
) -> Option<Found> {
    let allowed = allowed.clone();
    let plan = worker
        .run(move |store| plan(&store, key, &allowed, zstd))
        .await;
    let bytes = plan.as_ref().map_or(1, |plan| plan.read_bytes(key));
    let mut permit = reply::take(in_flight, bytes, busy).await?;
    let object = match plan {
        Ok(plan) => {
            let object = worker.run(move |store| read(&store, key, plan)).await;
            reply::trim(&mut permit, plan.encoded);
            object
        }
        Err(status) => Err(status),
    };
    Some((object, permit))
}

#[tonic::async_trait]
impl Objects for ObjectsService {
    async fn resolve_ref(
        &self,
        mut request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        // No interceptor on this service: `GetObjects` has none, and the
        // service's token-checked calls name their caller themselves.
        let caller = self.auth.caller_of(&request)?;
        request.extensions_mut().insert(caller);
        resolve_ref(&self.worker, &self.access, &self.quotas, request)
            .await
            .map(|r| crate::service::count_reply(Response::new(r)))
    }

    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        let caller = self.auth.caller_of(&request)?;
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The derivation of a first view walks the real tree, under the
        // call's deadline.
        let deadline = Deadline::of_call(&request, crate::deadline::CALL_LIMIT);
        let GetRootTreeRequest { repo, changeset_id } = request.into_inner();
        check_reader(&self.access, &caller, &repo)?;
        let cs = parse_id(&changeset_id, "changeset_id")?;
        // A partial reader gets the root of its view: the real trees with
        // everything its read rules do not cover removed ([#121]). The view
        // is derived on the first read of its rule set, then cached.
        let rule_set = self.access.read_digest(&caller, &repo).map(|digest| {
            (
                digest,
                self.access
                    .read_rule_set(&caller, &repo)
                    .unwrap_or_default(),
                self.access.view_digests(&repo),
                Arc::clone(&self.metrics),
            )
        });
        let (tree, repo) = self
            .worker
            .run(move |store| {
                let tree = match &rule_set {
                    Some((digest, prefixes, live, metrics)) => {
                        let repo_id = store.read(|c| c.repo_id(&repo))?;
                        views::view_root(
                            &store, repo_id, digest, cs, prefixes, live, metrics, &deadline,
                        )?
                    }
                    None => read::root_tree(&store, &repo, cs, &deadline)?,
                };
                Ok((tree, repo))
            })
            .await?;
        let ticket = match &self.tickets {
            Some(keys) => keys
                .sign(&repo, &caller.0, *tree.as_bytes())
                .map_err(Status::internal)?,
            // A server without ticket keys serves no objects; the empty
            // ticket fails every read that carries it.
            None => Vec::new(),
        };
        Ok(crate::service::count_reply(Response::new(
            GetRootTreeResponse {
                tree_id: tree.as_bytes().to_vec(),
                ticket,
            },
        )))
    }

    type GetObjectsStream = ObjectStream;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        // The ticket is the only permission here, but it names a signed
        // principal: its per-principal request quota covers this stream
        // too. The slot lives to the stream's end, as the reply does.
        let waits = Waits {
            until: Until::of(&request),
            ..WAITS
        };
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
        let GetObjectsRequest {
            refs,
            accept,
            ticket,
        } = request.into_inner();
        scion_proto::runtime::check::object_count(refs.len())?;
        // The ticket is the only permission an object read has: it names
        // the repo and the root tree, and every object must be reachable
        // from that tree over the path that its ref names. An object ID
        // alone is not a permission.
        let no_keys: &[ed25519_dalek::VerifyingKey] = &[];
        let keys_for_verify = self
            .tickets
            .as_ref()
            .map_or(no_keys, |keys| keys.for_verify());
        let ticket =
            scion_proto::ticket::parse(&ticket, keys_for_verify, scion_proto::ticket::now())
                .map_err(|why| {
                    // The reason only: no ticket bytes, and no signature.
                    tracing::warn!(reason = %why, "a GetObjects ticket was refused");
                    Status::permission_denied(format!("bad ticket: {why}"))
                })?;
        let call = self
            .quotas
            .enter(&Caller(ticket.principal.clone()))
            .map_err(Status::from)?;
        let refs = refs
            .into_iter()
            .map(|r| Ok((parse_ref(&r)?, r.path)))
            .collect::<Result<Vec<_>, Status>>()?;
        // Encodings that this server does not know are ignored.
        let zstd = accept.contains(&Encoding::Zstd.into());
        // The walk pays as it goes, against the reply budget's whole size
        // (a static cap), and stops at the caller's deadline. The reply
        // itself still waits for its share.
        let in_flight = Arc::clone(&self.in_flight);
        let (allowed, decoded, keys) = self
            .worker
            .run(move |store| {
                let (allowed, decoded) = prove(
                    &store,
                    ticket.root_tree,
                    &refs,
                    IN_FLIGHT_BYTES,
                    waits.until.at(),
                )?;
                let keys = refs.into_iter().map(|(key, _)| key).collect();
                Ok((allowed, decoded, keys))
            })
            .await?;
        // The reply runs to the stream's end, not to this handler's return:
        // its work goes on in the producer task.
        let stream = start_reply(
            self.worker.clone(),
            keys,
            allowed,
            Reply {
                in_flight,
                waits,
                stalled,
                call,
            },
            read_object,
            zstd,
            decoded,
        );
        Ok(crate::service::mark_stream(Response::new(stream.await?)))
    }
}

/// Marks every ref that the walk from `root` reaches, over the trees of
/// `store`: the caller keeps the keys, and `plan` serves only the proven
/// ones. A tree that `store` does not hold, or one whose bytes do not
/// match its ID, refuses the ref.
///
/// The walk pays as it goes, against the reply budget's whole size: a
/// tree is handed to the walk only while the bytes it holds stay within
/// `cap`, the budget's static total, and only while `until` is ahead.
/// Past either, the walk stops before it decodes one more tree, with the
/// same answer the reply would give. The cap never changes with other
/// readers' shares, so a walk that would exceed it fails for a reason
/// that no other reader changes; the reply itself still waits for its
/// share.
fn prove(
    store: &Backend,
    root: [u8; 32],
    refs: &[(Key, Vec<u8>)],
    cap: usize,
    until: Option<Instant>,
) -> Result<(HashSet<Key>, usize)> {
    let mut walk = scion_proto::ticket::Walker::new(root);
    let mut allowed = HashSet::new();
    // The bytes of the trees the walk holds, as the Walker counts them.
    // The reader needs its own count: it decides before the walk decodes.
    let mut held = 0usize;
    store.read(|c| {
        let mut reader = |id: [u8; 32]| {
            // The walk only hands back the 32 bytes it got from a tree.
            let id = TreeId::try_from(id.as_slice()).expect("a walk names a tree id");
            if until.is_some_and(|at| Instant::now() >= at) {
                return Err(Error::DeadlineExceeded(
                    "the ticket walk ran past its deadline".into(),
                ));
            }
            match c.get_tree_body(id) {
                Ok(bytes) => {
                    if held + bytes.len() > cap {
                        return Err(Error::ResourceExhausted(format!(
                            "the ticket walk would hold {} bytes of trees, \
                             more than the {cap} bytes a reply may hold",
                            held + bytes.len()
                        )));
                    }
                    held += bytes.len();
                    #[cfg(test)]
                    WALK_DECODED.store(held, Ordering::SeqCst);
                    Ok(Some(bytes))
                }
                Err(Error::NotFound(_)) => Ok(None),
                Err(why) => Err(why),
            }
        };
        for (key, path) in refs {
            let (kind, id) = of(key);
            if let Walk::Reached = walk.reach(&mut reader, kind, &id, path)? {
                allowed.insert(*key);
            }
        }
        Ok(())
    })?;
    // The trees the walks decoded hold memory: they count against the
    // call's reply budget, as the objects do.
    Ok((allowed, walk.decoded_bytes()))
}

/// The kind and the bytes of the ID that a key names.
fn of(key: &Key) -> (ObjectKind, [u8; 32]) {
    match key {
        Key::Tree(id) => (ObjectKind::Tree, *id.as_bytes()),
        Key::Blob(id) => (ObjectKind::Blob, *id.as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::Backend;
    use crate::store::Store;
    use std::io::Read as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use scion_proto::CHUNK_BYTES;
    use scion_proto::runtime::object::Assembler;
    use tokio_stream::StreamExt as _;
    use tonic::Code;

    use super::*;

    /// Waits until `done` holds, at most 10 s.
    async fn until(done: impl Fn() -> bool) {
        let started = tokio::time::Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(10), "timed out");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Both waits the same.
    fn waits(wait: Duration) -> Waits {
        Waits {
            busy: wait,
            stall: wait,
            until: Until::NONE,
        }
    }

    /// The length of a blob's stored file.
    fn packed_len(store: &Store, key: Key) -> usize {
        let Key::Blob(id) = key else {
            panic!("not a blob")
        };
        let len = store
            .blobs()
            .open_file(id)
            .unwrap()
            .metadata()
            .unwrap()
            .len();
        usize::try_from(len).unwrap()
    }

    // A paused clock moves only when every task waits, and not while a store
    // operation runs (`spawn_blocking`). So the stall cannot end before the
    // test sees the second object hold its share, however busy the machine.
    #[tokio::test(start_paused = true)]
    async fn a_client_that_stops_reading_is_cut_off_and_frees_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let raw = store.as_sqlite().expect("the client tests run on SQLite");
        let keys: Vec<Key> = [b"one".as_slice(), b"second", b"three"]
            .iter()
            .map(|data| Key::Blob(store.blobs().put(data).unwrap()))
            .collect();
        let second = packed_len(raw, keys[1]);
        // Files of two sizes, so the free budget shows which object holds a
        // share. With one size, the first object, before it is sent, looks
        // like the second.
        assert_ne!(packed_len(raw, keys[0]), second);
        let budget = 1000;
        let in_flight = Arc::new(Semaphore::new(budget));
        let stall = Duration::from_millis(50);
        let reply = start_reply(
            Worker::new(store),
            keys.clone(),
            keys.iter().copied().collect(),
            Reply {
                in_flight: Arc::clone(&in_flight),
                waits: waits(stall),
                stalled: None,
                call: Call::default(),
            },
            read_object,
            true,
            0,
        );
        let mut stream = reply.await.unwrap();

        // Nobody reads: the first object waits in the channel, and its share
        // is back. The second holds its share while its send stalls, then
        // the reply ends and gives it back.
        until(|| in_flight.available_permits() == budget - second).await;
        until(|| in_flight.available_permits() == budget).await;
        let first = stream.next().await.unwrap().unwrap();
        let (header, data) = Assembler::new(Header::of(&first, true).unwrap(), first).finish();
        assert_eq!(object::decode(&header, data).unwrap(), b"one");
        assert!(stream.next().await.is_none(), "the reply ended");
    }

    #[tokio::test]
    async fn a_call_with_no_budget_for_its_first_object_fails_and_reads_nothing() {
        static READS: AtomicUsize = AtomicUsize::new(0);
        fn counted(store: &Backend, key: Key, plan: Plan) -> Result<Outgoing> {
            READS.fetch_add(1, Ordering::SeqCst);
            read_object(store, key, plan)
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let a = Key::Blob(store.blobs().put(b"a blob").unwrap());
        let budget = 1000;
        let in_flight = Arc::new(Semaphore::new(budget));
        let worker = Worker::new(store);
        let wait = waits(Duration::from_millis(100));
        let reply = || {
            let reply = start_reply(
                worker.clone(),
                vec![a],
                HashSet::from([a]),
                Reply {
                    in_flight: Arc::clone(&in_flight),
                    waits: wait,
                    stalled: None,
                    call: Call::default(),
                },
                counted,
                true,
                0,
            );
            tokio::time::timeout(Duration::from_secs(10), reply)
        };

        // Other replies hold the whole budget. The call gets no room within
        // the busy limit: it fails, reads nothing, and never ends `OK` with
        // no object.
        let others = reply::try_take(&in_flight, budget).unwrap();
        let status = reply().await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
        assert_eq!(READS.load(Ordering::SeqCst), 0, "nothing was read");
        drop(others);
        reply().await.unwrap().unwrap();
        assert_eq!(READS.load(Ordering::SeqCst), 1);
    }

    // The trees that a ticket's walk decoded hold their share of the reply
    // budget to the end of the call: a walk too big for the free budget
    // fails the call before any object is read.
    #[tokio::test]
    async fn a_walk_bigger_than_the_free_budget_ends_resource_exhausted() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let a = Key::Blob(store.blobs().put(b"a blob").unwrap());
        let budget = 1000;
        let in_flight = Arc::new(Semaphore::new(budget));
        let worker = Worker::new(store);
        let reply = |decoded| {
            let reply = start_reply(
                worker.clone(),
                vec![a],
                HashSet::from([a]),
                Reply {
                    in_flight: Arc::clone(&in_flight),
                    waits: waits(Duration::from_millis(100)),
                    stalled: None,
                    call: Call::default(),
                },
                read_object,
                true,
                decoded,
            );
            tokio::time::timeout(Duration::from_secs(10), reply)
        };
        // A walk that takes every byte of the budget: nothing is read, and
        // the call ends `RESOURCE_EXHAUSTED`.
        let status = reply(budget).await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
        // Within the budget, the walk's share is held while the reply goes,
        // and freed after it: a second call with a walk of the same size
        // gets room again.
        reply(budget / 2).await.unwrap().unwrap();
        reply(budget / 2).await.unwrap().unwrap();
    }

    /// A caller's deadline shorter than the busy limit ends the wait for
    /// room, so the call fails `RESOURCE_EXHAUSTED` before the caller's
    /// deadline, not after the 30 s limit.
    #[tokio::test]
    async fn a_wait_for_room_ends_before_the_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let a = Key::Blob(store.blobs().put(b"a blob").unwrap());
        let in_flight = Arc::new(Semaphore::new(1000));
        let _others = reply::try_take(&in_flight, 1000).unwrap();
        let waits = Waits {
            until: Until::after(Duration::from_millis(300)),
            ..WAITS
        };
        let reply = start_reply(
            Worker::new(store),
            vec![a],
            HashSet::from([a]),
            Reply {
                in_flight,
                waits,
                stalled: None,
                call: Call::default(),
            },
            read_object,
            true,
            0,
        );
        let status = tokio::time::timeout(Duration::from_secs(5), reply)
            .await
            .expect("the wait ends at the deadline")
            .unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    }

    #[tokio::test]
    async fn reading_a_blob_to_unpack_it_takes_the_decoder_too() {
        static HELD: AtomicUsize = AtomicUsize::new(0);
        static BUDGET: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
        fn watched(store: &Backend, key: Key, plan: Plan) -> Result<Outgoing> {
            let free = BUDGET.get().unwrap().available_permits();
            HELD.store(IN_FLIGHT_BYTES - free, Ordering::SeqCst);
            read_object(store, key, plan)
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let content = vec![b'x'; 5000];
        let key = Key::Blob(store.blobs().put(&content).unwrap());
        let in_flight =
            Arc::clone(BUDGET.get_or_init(|| Arc::new(Semaphore::new(IN_FLIGHT_BYTES))));
        let reply = start_reply(
            Worker::new(store),
            vec![key],
            HashSet::from([key]),
            Reply {
                in_flight: Arc::clone(&in_flight),
                waits: WAITS,
                stalled: None,
                call: Call::default(),
            },
            watched,
            false,
            0,
        );
        let objects = collect(reply.await.unwrap(), false).await;
        assert_eq!(objects[0].1, content);
        // While it read, the reply held the content and the decoder.
        assert_eq!(
            HELD.load(Ordering::SeqCst),
            content.len() + store::READER_BYTES
        );
        assert_eq!(in_flight.available_permits(), IN_FLIGHT_BYTES);
    }

    /// Every part of a reply, as a whole object: its header and bytes.
    async fn collect(mut stream: ObjectStream, zstd: bool) -> Vec<(Header, Vec<u8>, usize)> {
        let mut objects = Vec::new();
        while let Some(first) = stream.next().await {
            let first = first.unwrap();
            let mut object = Assembler::new(Header::of(&first, zstd).unwrap(), first);
            let mut parts = 1;
            while !object.is_done() {
                object.push(&stream.next().await.unwrap().unwrap()).unwrap();
                parts += 1;
            }
            let (header, data) = object.finish();
            objects.push((header, data, parts));
        }
        objects
    }

    #[tokio::test]
    async fn a_blob_goes_out_as_its_stored_file_in_parts_or_unpacked() {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        // Over three parts once packed: bytes that do not compress.
        let mut state = 1_u64;
        let content: Vec<u8> = (0..3 * CHUNK_BYTES + 100)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[3]
            })
            .collect();
        let id = store.blobs().put(&content).unwrap();
        let mut stored = Vec::new();
        store
            .blobs()
            .open_file(id)
            .unwrap()
            .read_to_end(&mut stored)
            .unwrap();
        assert!(stored.len() > 3 * CHUNK_BYTES, "{}", stored.len());
        let worker = Worker::new(store);
        let in_flight = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
        let reply = |zstd| {
            let keys = vec![Key::Blob(id)];
            let reply = start_reply(
                worker.clone(),
                keys,
                HashSet::from([Key::Blob(id)]),
                Reply {
                    in_flight: Arc::clone(&in_flight),
                    waits: WAITS,
                    stalled: None,
                    call: Call::default(),
                },
                read_object,
                zstd,
                0,
            );
            async move { collect(reply.await.unwrap(), zstd).await }
        };

        // A client that accepts zstd gets the stored file, byte for byte.
        let [(header, data, parts)] = reply(true).await.try_into().unwrap();
        assert_eq!(header.encoding, Encoding::Zstd);
        assert_eq!(
            (header.encoded_size, header.size),
            (stored.len(), content.len())
        );
        assert_eq!(parts, 4);
        assert_eq!(data, stored);
        assert_eq!(object::decode(&header, data).unwrap(), content);

        // Another client gets the content.
        let [(header, data, parts)] = reply(false).await.try_into().unwrap();
        assert_eq!(header.encoding, Encoding::Raw);
        assert_eq!(
            (header.encoded_size, header.size),
            (content.len(), content.len())
        );
        assert_eq!(parts, 4);
        assert_eq!(data, content);
        assert_eq!(in_flight.available_permits(), IN_FLIGHT_BYTES);
    }

    /// The ticket walk pays as it goes: a tree is handed to the walk only
    /// while the bytes it holds fit the reply budget, and an expired
    /// deadline stops the walk before the first tree. The walk reaches a
    /// ref's own tree by its ID without decoding it, so the wide tree
    /// here is one the path crosses, not one it ends at.
    #[test]
    fn a_ticket_walk_stops_before_it_decodes_past_the_budget() {
        use scion_core::{Entry, FileEntry, FileState, PathElem, Tree};
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        // A middle tree whose one file entry has a wide name, so its body
        // is hundreds of bytes, under a root of one small entry, over a
        // leaf the ref ends at.
        let wide = PathElem::new(vec![b'x'; 300]).unwrap();
        let (root, leaf) = store
            .write(|c| {
                let blob = BlobId::of(b"blob");
                let leaf = c.put_tree(
                    &Tree::new(BTreeMap::from([(
                        PathElem::new("f").unwrap(),
                        Entry::File(FileEntry::new(
                            FileState::Regular {
                                content: blob,
                                executable: false,
                            },
                            0,
                        )),
                    )]))
                    .expect("tree fits the format"),
                )?;
                let mid = c.put_tree(
                    &Tree::new(BTreeMap::from([
                        (
                            wide,
                            Entry::File(FileEntry::new(
                                FileState::Regular {
                                    content: blob,
                                    executable: false,
                                },
                                0,
                            )),
                        ),
                        (PathElem::new("s").unwrap(), Entry::Dir(leaf)),
                    ]))
                    .expect("tree fits the format"),
                )?;
                let root = c.put_tree(
                    &Tree::new(BTreeMap::from([(
                        PathElem::new("d").unwrap(),
                        Entry::Dir(mid),
                    )]))
                    .expect("tree fits the format"),
                )?;
                Ok((root, leaf))
            })
            .unwrap();
        let refs = [(Key::Tree(leaf), b"d/s".to_vec())];

        // Other readers hold the budget's free share, but the walk fits
        // the capacity: it goes ahead, as a reply would. The free share
        // here is 50 bytes, less than the ~410 the walk decodes.
        let shared = Arc::new(Semaphore::new(1 << 20));
        shared.try_acquire_many((1 << 20) - 50).unwrap().forget();
        WALK_DECODED.store(0, Ordering::SeqCst);
        let (_, decoded) = prove(&store, *root.as_bytes(), &refs, 1 << 20, None).unwrap();
        let held = WALK_DECODED.load(Ordering::SeqCst);
        assert!(held > 50, "{held}");
        assert_eq!(decoded, held, "the reply takes what the walk decoded");

        // The budget fits the root but not the middle tree: the walk
        // hands the root over, and stops before it decodes the middle.
        WALK_DECODED.store(0, Ordering::SeqCst);
        let err = prove(&store, *root.as_bytes(), &refs, 200, None).unwrap_err();
        assert!(matches!(err, Error::ResourceExhausted(_)), "{err:?}");
        let decoded = WALK_DECODED.load(Ordering::SeqCst);
        assert!(decoded > 0 && decoded <= 200, "{decoded}");

        // An expired deadline stops the walk before the first tree.
        let past = Some(Instant::now().checked_sub(Duration::from_secs(1)).unwrap());
        let err = prove(&store, *root.as_bytes(), &refs, 1 << 20, past).unwrap_err();
        assert!(matches!(err, Error::DeadlineExceeded(_)), "{err:?}");
        assert_eq!(WALK_DECODED.load(Ordering::SeqCst), decoded);
    }

    /// `tree_size` reads the byte length of a stored tree on every
    /// backend. On Postgres the length expression is `octet_length(body)`,
    /// whose type is int4, so the read must cast it to the int8 it reads
    /// into: without the cast this panics with 'error deserializing
    /// column 0', which the demo's `GetObjects` died of.
    #[test]
    fn a_tree_size_reads_the_octets_of_a_stored_tree() {
        use scion_core::Tree;
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        let tree = Tree::new(BTreeMap::new()).expect("tree fits the format");
        let id = store.write(|c| c.put_tree(&tree)).unwrap();
        let size = store.read(|c| c.tree_size(id)).unwrap();
        let (body, _) = tree.encode_with_id();
        assert_eq!(size, body.len(), "the length of the tree's bytes");
    }

    /// `fsck`'s derived-row walk reads each row's version (BIGINT on
    /// Postgres) into a u32: the read converts the int8 first. A row
    /// whose version overflows u32 is corruption, not a panic.
    #[test]
    fn a_fsck_derived_row_reads_its_version() {
        use crate::meta::MetaTx as _;
        use scion_core::{Changeset, IdentityTime};
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::open_test(dir.path()).unwrap();
        let repo = store.write(|c| c.create_repo("fsck-rows")).unwrap();
        // The derived table's foreign key names a stored changeset.
        let cs = store
            .write(|c| {
                c.insert_changeset(
                    repo,
                    &Changeset::new(
                        Vec::new(),
                        IdentityTime::new(b"A".to_vec(), b"a@example.com".to_vec(), 1, 0)
                            .expect("identity fits the format"),
                        None,
                        b"probe\n".to_vec(),
                        BTreeMap::new(),
                    )
                    .expect("changeset fits the format"),
                )
            })
            .unwrap();
        store
            .write(|c| c.put_derived("probe", 3, cs, &[1, 2, 3]))
            .unwrap();
        let rows = store
            .read(|c| c.derived_rows_after("probe", 0, cs.as_bytes(), 10))
            .unwrap();
        assert_eq!(rows.len(), 1, "the probe row");
        assert_eq!(rows[0].1, 3, "the row's version");
    }
}

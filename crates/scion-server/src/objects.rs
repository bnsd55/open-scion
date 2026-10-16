//! The `Objects` service: the reads that any cache tier can answer.
//!
//! A client resolves a ref, gets the root tree of the changeset, then asks for
//! trees and blobs by ID. A tree goes out as its stored `TreeV1` bytes. A blob
//! goes out as its stored zstd file, as is, if the client accepts zstd, or
//! else unpacked and checked. The client checks each object against its ID.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use scion_core::{BlobId, TreeId};
use scion_proto::deadline::Until;
use scion_proto::object::{self, Header, zstd_bound};
use scion_proto::reply::{
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

use crate::access::Access;
use crate::error::{Error, Result};
use crate::service::{Worker, caller, check_reader, parse_id, resolve_ref};
use crate::store::{self, Store};
use crate::tickets::Keys;
use crate::{db, read};

// The store keeps exactly the objects that the API promises to send.
const _: () = assert!(store::MAX_OBJECT_BYTES == scion_proto::MAX_OBJECT_BYTES);

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
pub struct ObjectsService {
    worker: Worker,
    access: Arc<Access>,
    /// The keys that mint the tickets of `GetRootTree` replies. `None`
    /// keeps every object read closed: a reply then carries no ticket.
    tickets: Option<Arc<Keys>>,
    /// `IN_FLIGHT_BYTES` permits, one per object byte, shared by all replies.
    in_flight: Arc<Semaphore>,
}

impl ObjectsService {
    pub(crate) fn new(worker: Worker, access: Arc<Access>, tickets: Option<Arc<Keys>>) -> Self {
        Self {
            worker,
            access,
            tickets,
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
type Read = fn(&Store, Key, Plan) -> Result<Outgoing>;

/// How `key` goes out, without reading its bytes. A blob goes out as its
/// stored zstd file if the client accepts `zstd`. Only an object that the
/// ticket's walk proved goes out: everything else is not found.
fn plan(store: &Store, key: Key, allowed: &HashSet<Key>, zstd: bool) -> Result<Plan> {
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
            let size = store.read(|c| db::tree_size(c, id))?;
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
fn read_object(store: &Store, key: Key, plan: Plan) -> Result<Outgoing> {
    let (kind, id, body) = match key {
        Key::Tree(id) => {
            let data = store.read(|c| db::get_tree_body(c, id))?;
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

/// object. That comes before the caller's deadline, so the caller gets this
/// status, not a timeout of its own. `stalled` is the handle of the
/// connection, which counts what a stalled reply leaves in HTTP/2.
/// The waits are the caller's budget and the deadlines of its waits; the
/// reply needs every part of it, hence the argument count.
#[allow(clippy::too_many_arguments)]
async fn start_reply(
    worker: Worker,
    keys: Vec<Key>,
    allowed: HashSet<Key>,
    in_flight: Arc<Semaphore>,
    waits: Waits,
    read: Read,
    zstd: bool,
    stalled: Option<scion_proto::listen::Stalled>,
) -> Result<ObjectStream, Status> {
    let mut rest = keys.into_iter();
    let Some(key) = rest.next() else {
        return Err(Status::invalid_argument("a call names at least one object"));
    };
    let busy = waits.until.wait(waits.busy);
    let first = find(&worker, &in_flight, busy, read, &allowed, zstd, key)
        .await
        .ok_or_else(reply::busy)?;
    let (tx, stream) = reply::channel();
    tokio::spawn(async move {
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
        reply::send_objects(first, rest, tx, waits.stall, stalled, produce).await;
    });
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
        request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        resolve_ref(&self.worker, &self.access, request)
            .await
            .map(Response::new)
    }

    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        let caller = caller(&request)?;
        let GetRootTreeRequest { repo, changeset_id } = request.into_inner();
        check_reader(&self.access, &caller, &repo)?;
        let cs = parse_id(&changeset_id, "changeset_id")?;
        let (tree, repo) = self
            .worker
            .run(move |store| {
                let tree = read::root_tree(&store, &repo, cs)?;
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
        Ok(Response::new(GetRootTreeResponse {
            tree_id: tree.as_bytes().to_vec(),
            ticket,
        }))
    }

    type GetObjectsStream = ObjectStream;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        // No caller here: the ticket is the only permission of an object
        // read, and the token interceptor has already authenticated the
        // connection.
        let waits = Waits {
            until: Until::of(&request),
            ..WAITS
        };
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        let GetObjectsRequest {
            refs,
            accept,
            ticket,
        } = request.into_inner();
        scion_proto::check::object_count(refs.len())?;
        // The ticket is the only permission an object read has: it names
        // the repo and the root tree, and every object must be reachable
        // from that tree over the path that its ref names. An object ID
        // alone is not a permission.
        let no_keys: &[ed25519_dalek::VerifyingKey] = &[];
        let keys_for_verify = self
            .tickets
            .as_ref()
            .map_or(no_keys, |keys| keys.for_verify());
        let ticket = scion_proto::ticket::parse(&ticket, keys_for_verify, now())
            .map_err(|why| Status::permission_denied(format!("bad ticket: {why}")))?;
        let refs = refs
            .into_iter()
            .map(|r| Ok((parse_ref(&r)?, r.path)))
            .collect::<Result<Vec<_>, Status>>()?;
        // Encodings that this server does not know are ignored.
        let zstd = accept.contains(&Encoding::Zstd.into());
        let (allowed, keys) = self
            .worker
            .run(move |store| {
                let allowed = prove(&store, ticket.root_tree, &refs)?;
                let keys = refs.into_iter().map(|(key, _)| key).collect();
                Ok((allowed, keys))
            })
            .await?;
        let in_flight = Arc::clone(&self.in_flight);
        let stream = start_reply(
            self.worker.clone(),
            keys,
            allowed,
            in_flight,
            waits,
            read_object,
            zstd,
            stalled,
        );
        Ok(Response::new(stream.await?))
    }
}

/// The moment that ticket checks use.
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Marks every ref that the walk from `root` reaches, over the trees of
/// `store`: the caller keeps the keys, and `plan` serves only the proven
/// ones. A tree that `store` does not hold, or one whose bytes do not
/// match its ID, refuses the ref.
fn prove(store: &Store, root: [u8; 32], refs: &[(Key, Vec<u8>)]) -> Result<HashSet<Key>> {
    let mut walk = scion_proto::ticket::Walker::new(root);
    let mut allowed = HashSet::new();
    store.read(|c| {
        let mut reader = |id: [u8; 32]| {
            // The walk only hands back the 32 bytes it got from a tree.
            let id = TreeId::try_from(id.as_slice()).expect("a walk names a tree id");
            match db::get_tree_body(c, id) {
                Ok(bytes) => Ok(Some(bytes)),
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
    Ok(allowed)
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
    use std::io::Read as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use scion_proto::CHUNK_BYTES;
    use scion_proto::object::Assembler;
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
        let store = Store::open(dir.path()).unwrap();
        let keys: Vec<Key> = [b"one".as_slice(), b"second", b"three"]
            .iter()
            .map(|data| Key::Blob(store.blobs().put(data).unwrap()))
            .collect();
        let second = packed_len(&store, keys[1]);
        // Files of two sizes, so the free budget shows which object holds a
        // share. With one size, the first object, before it is sent, looks
        // like the second.
        assert_ne!(packed_len(&store, keys[0]), second);
        let budget = 1000;
        let in_flight = Arc::new(Semaphore::new(budget));
        let stall = Duration::from_millis(50);
        let reply = start_reply(
            Worker::new(store),
            keys.clone(),
            keys.iter().copied().collect(),
            Arc::clone(&in_flight),
            waits(stall),
            read_object,
            true,
            None,
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
        fn counted(store: &Store, key: Key, plan: Plan) -> Result<Outgoing> {
            READS.fetch_add(1, Ordering::SeqCst);
            read_object(store, key, plan)
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
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
                Arc::clone(&in_flight),
                wait,
                counted,
                true,
                None,
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

    /// A caller's deadline shorter than the busy limit ends the wait for
    /// room, so the call fails `RESOURCE_EXHAUSTED` before the caller's
    /// deadline, not after the 30 s limit.
    #[tokio::test]
    async fn a_wait_for_room_ends_before_the_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
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
            in_flight,
            waits,
            read_object,
            true,
            None,
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
        fn watched(store: &Store, key: Key, plan: Plan) -> Result<Outgoing> {
            let free = BUDGET.get().unwrap().available_permits();
            HELD.store(IN_FLIGHT_BYTES - free, Ordering::SeqCst);
            read_object(store, key, plan)
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let content = vec![b'x'; 5000];
        let key = Key::Blob(store.blobs().put(&content).unwrap());
        let in_flight =
            Arc::clone(BUDGET.get_or_init(|| Arc::new(Semaphore::new(IN_FLIGHT_BYTES))));
        let reply = start_reply(
            Worker::new(store),
            vec![key],
            HashSet::from([key]),
            Arc::clone(&in_flight),
            WAITS,
            watched,
            false,
            None,
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
        let store = Store::open(dir.path()).unwrap();
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
                Arc::clone(&in_flight),
                WAITS,
                read_object,
                zstd,
                None,
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
}

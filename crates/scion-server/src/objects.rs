//! The `Objects` service: the reads that any cache tier can answer.
//!
//! A client resolves a ref, gets the root tree of the changeset, then asks for
//! trees and blobs by ID. A tree goes out as its stored `TreeV1` bytes. A blob
//! goes out as its stored zstd file, as is, if the client accepts zstd, or
//! else unpacked and checked. The client checks each object against its ID.

use std::sync::Arc;
use std::time::Duration;

use scion_core::{BlobId, TreeId};
use scion_proto::object::{self, Header, zstd_bound};
use scion_proto::reply::{self, BUSY_TIMEOUT, Found, IN_FLIGHT_BYTES, ObjectStream, STALL_TIMEOUT};
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind, ObjectPart,
    ObjectRef, ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{MAX_MESSAGE_BYTES, MAX_PART_MESSAGE_BYTES};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use crate::error::{Error, Result};
use crate::service::{Worker, check_name, parse_id, resolve_ref};
use crate::store::{self, Store};
use crate::{db, read};

// The store keeps exactly the objects that the API promises to send.
const _: () = assert!(store::MAX_OBJECT_BYTES == scion_proto::MAX_OBJECT_BYTES);

/// One object to read.
#[derive(Clone, Copy, Debug)]
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

/// Implements the `Objects` RPCs.
pub struct ObjectsService {
    worker: Worker,
    /// `IN_FLIGHT_BYTES` permits, one per object byte, shared by all replies.
    in_flight: Arc<Semaphore>,
}

impl ObjectsService {
    pub(crate) fn new(worker: Worker) -> Self {
        Self {
            worker,
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

/// Reads an object's parts, as planned. [`read_object`], except in tests
/// that count reads.
type Read = fn(&Store, Key, Plan) -> Result<Vec<ObjectPart>>;

/// How `key` goes out, without reading its bytes. A blob goes out as its
/// stored zstd file if the client accepts `zstd`.
fn plan(store: &Store, key: Key, zstd: bool) -> Result<Plan> {
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

/// Reads one object's parts, as planned. A tree and a RAW blob are checked
/// against their IDs on the way out of the store. A ZSTD blob is its stored
/// file, read in parts and not unpacked: the receiver checks it.
fn read_object(store: &Store, key: Key, plan: Plan) -> Result<Vec<ObjectPart>> {
    // SHORTCUT: an object ID is treated as permission to read the object, and
    // trees and blobs are shared by all repos. Safe with one principal.
    // Upgrade (issue #13): a read ticket bound to a principal, a repo and a
    // root tree, checked here and by every cache tier.
    let (kind, id, chunks) = match key {
        Key::Tree(id) => {
            let data = store.read(|c| db::get_tree_body(c, id))?;
            (ObjectKind::Tree, *id.as_bytes(), object::chunks(data))
        }
        Key::Blob(id) if plan.encoding == Encoding::Zstd => {
            let file = store.blobs().open_file(id)?;
            if file.metadata()?.len() != plan.encoded as u64 {
                return Err(Error::Corrupt(format!("blob {id}: its file changed size")));
            }
            let chunks = object::read_chunks(file, plan.encoded)?;
            (ObjectKind::Blob, *id.as_bytes(), chunks)
        }
        Key::Blob(id) => {
            let data = store.blobs().get(id)?;
            (ObjectKind::Blob, *id.as_bytes(), object::chunks(data))
        }
    };
    let read: usize = chunks.iter().map(Vec::len).sum();
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
    Ok(header.parts(chunks))
}

/// How long a reply waits: for room in the budget, and for its client.
#[derive(Clone, Copy)]
struct Waits {
    busy: Duration,
    stall: Duration,
}

const WAITS: Waits = Waits {
    busy: BUSY_TIMEOUT,
    stall: STALL_TIMEOUT,
};

/// Starts the reply to `keys`: finds the first object before the call
/// answers, then sends the rest on a task of its own, part by part, as the
/// client takes them (see [`reply::send_objects`]). If the first object gets
/// no share of `in_flight` within `waits.busy`, the call fails with
/// [`reply::busy`], so a reply never ends `OK` with no object.
async fn start_reply(
    worker: Worker,
    keys: Vec<Key>,
    in_flight: Arc<Semaphore>,
    waits: Waits,
    read: Read,
    zstd: bool,
) -> Result<ObjectStream, Status> {
    let mut rest = keys.into_iter();
    let Some(key) = rest.next() else {
        return Err(Status::invalid_argument("a call names at least one object"));
    };
    let first = find(&worker, &in_flight, waits.busy, read, zstd, key)
        .await
        .ok_or_else(reply::busy)?;
    let (tx, stream) = reply::channel();
    tokio::spawn(async move {
        let (worker, in_flight) = (&worker, &in_flight);
        let produce = |key| find(worker, in_flight, waits.busy, read, zstd, key);
        reply::send_objects(first, rest, tx, waits.stall, produce).await;
    });
    Ok(stream)
}

/// Finds one object for a reply: plans how it goes out, takes the share of
/// `in_flight` for the bytes it sends, and only then reads them. `None` if
/// the share is not free within `busy`.
// SHORTCUT: two blocking hops per object and no read-ahead, so many small
// objects are read one after another. Upgrade: read a batch capped by bytes
// per hop, or keep a few reads in flight (issue #6).
async fn find(
    worker: &Worker,
    in_flight: &Arc<Semaphore>,
    busy: Duration,
    read: Read,
    zstd: bool,
    key: Key,
) -> Option<Found> {
    let plan = worker.run(move |store| plan(&store, key, zstd)).await;
    let bytes = plan.as_ref().map_or(1, |plan| plan.encoded);
    let permit = reply::take(in_flight, bytes, busy).await?;
    let parts = match plan {
        Ok(plan) => worker.run(move |store| read(&store, key, plan)).await,
        Err(status) => Err(status),
    };
    Some((parts, permit))
}

#[tonic::async_trait]
impl Objects for ObjectsService {
    async fn resolve_ref(
        &self,
        request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        resolve_ref(&self.worker, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_root_tree(
        &self,
        request: Request<GetRootTreeRequest>,
    ) -> Result<Response<GetRootTreeResponse>, Status> {
        let GetRootTreeRequest { repo, changeset_id } = request.into_inner();
        check_name("repo", &repo)?;
        let cs = parse_id(&changeset_id, "changeset_id")?;
        let tree = self
            .worker
            .run(move |store| read::root_tree(&store, &repo, cs))
            .await?;
        Ok(Response::new(GetRootTreeResponse {
            tree_id: tree.as_bytes().to_vec(),
        }))
    }

    type GetObjectsStream = ObjectStream;

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<Self::GetObjectsStream>, Status> {
        let GetObjectsRequest { repo, refs, accept } = request.into_inner();
        check_name("repo", &repo)?;
        scion_proto::check::object_count(refs.len())?;
        let keys = refs.iter().map(parse_ref).collect::<Result<Vec<_>, _>>()?;
        // Encodings that this server does not know are ignored.
        let zstd = accept.contains(&Encoding::Zstd.into());
        self.worker
            .run(move |store| store.read(|c| db::repo_id(c, &repo)))
            .await?;
        let in_flight = Arc::clone(&self.in_flight);
        let stream = start_reply(
            self.worker.clone(),
            keys,
            in_flight,
            WAITS,
            read_object,
            zstd,
        );
        Ok(Response::new(stream.await?))
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
        }
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_is_cut_off_and_frees_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let keys: Vec<Key> = [b"one".as_slice(), b"two", b"three"]
            .iter()
            .map(|data| Key::Blob(store.blobs().put(data).unwrap()))
            .collect();
        let budget = 1000;
        let in_flight = Arc::new(Semaphore::new(budget));
        let stall = Duration::from_millis(50);
        let reply = start_reply(
            Worker::new(store),
            keys,
            Arc::clone(&in_flight),
            waits(stall),
            read_object,
            false,
        );
        let mut stream = reply.await.unwrap();

        // Nobody reads: the first object waits in the channel. The second
        // takes its share, its send stalls, and the reply ends.
        until(|| in_flight.available_permits() == budget - 6).await;
        until(|| in_flight.available_permits() == budget - 3).await;
        assert_eq!(stream.next().await.unwrap().unwrap().data, b"one");
        assert!(stream.next().await.is_none(), "the reply ended");
        assert_eq!(in_flight.available_permits(), budget);
    }

    #[tokio::test]
    async fn a_call_with_no_budget_for_its_first_object_fails_and_reads_nothing() {
        static READS: AtomicUsize = AtomicUsize::new(0);
        fn counted(store: &Store, key: Key, plan: Plan) -> Result<Vec<ObjectPart>> {
            READS.fetch_add(1, Ordering::SeqCst);
            read_object(store, key, plan)
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let size = 1000;
        let a = Key::Blob(store.blobs().put(&vec![b'a'; size]).unwrap());
        let b = Key::Blob(store.blobs().put(&vec![b'b'; size]).unwrap());
        // Room for one object, not two.
        let budget = size + size / 2;
        let in_flight = Arc::new(Semaphore::new(budget));
        let worker = Worker::new(store);
        let wait = waits(Duration::from_millis(100));
        let reply = |keys| {
            let reply = start_reply(
                worker.clone(),
                keys,
                Arc::clone(&in_flight),
                wait,
                counted,
                false,
            );
            tokio::time::timeout(Duration::from_secs(10), reply)
        };

        // A call whose client never reads keeps `a` and its share. The next
        // call gets no budget within the busy limit: it fails, and never
        // ends `OK` with no object.
        let held = reply(vec![a]).await.unwrap().unwrap();
        let status = reply(vec![b]).await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
        assert_eq!(READS.load(Ordering::SeqCst), 1, "`b` was never read");
        drop(held);
        until(|| in_flight.available_permits() == budget).await;
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
                Arc::clone(&in_flight),
                WAITS,
                read_object,
                zstd,
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

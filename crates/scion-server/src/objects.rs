//! The `Objects` service: the reads that any cache tier can answer.
//!
//! A client resolves a ref, gets the root tree of the changeset, then asks for
//! trees and blobs by ID. Objects go out in their stored form, a tree as
//! `TreeV1` bytes and a blob as its raw content, so the client checks each one
//! against its ID.

use std::sync::Arc;
use std::time::Duration;

use scion_core::{BlobId, TreeId};
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, Object, ObjectKind, ObjectRef,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{MAX_MESSAGE_BYTES, MAX_OBJECT_MESSAGE_BYTES, MAX_OBJECT_REFS};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::StreamExt as _;
use tokio_stream::adapters::Map;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::error::{Error, Result};
use crate::service::{Worker, check_name, parse_id, resolve_ref};
use crate::store::{self, Store};
use crate::{db, read};

/// A `GetObjects` reply stops before the next object once it has sent this
/// many bytes. The client asks again for the rest, so one call stays short.
const REPLY_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES;

/// Most object bytes that all `GetObjects` replies together hold after a read
/// and before their stream hands the object on.
const IN_FLIGHT_BYTES: usize = 4 * scion_proto::MAX_OBJECT_BYTES;

/// A reply ends when its client takes no object for this long, so a client
/// that stops reading does not hold memory and budget. (The server's request
/// timeout covers only the start of a streamed reply.)
// SHORTCUT: a stalled reply ends, but HTTP/2 keeps the one encoded object it
// was sending until the connection closes. Upgrade (issue #42): a response
// body timeout that resets the stream.
const STALL_TIMEOUT: Duration = Duration::from_secs(120);

// The store keeps exactly the objects that the API promises to send.
const _: () = assert!(store::MAX_OBJECT_BYTES == scion_proto::MAX_OBJECT_BYTES);
// Any one object fits in the budget, and the budget counts in u32 permits.
const _: () = assert!(scion_proto::MAX_OBJECT_BYTES <= IN_FLIGHT_BYTES);
const _: () = assert!(IN_FLIGHT_BYTES <= u32::MAX as usize);

/// An object on its way to the client, with the share of the byte budget
/// that it holds until the stream hands it on.
type Sent = (Result<Object, Status>, OwnedSemaphorePermit);

/// The `GetObjects` reply: the channel, minus the budget each object held.
type ObjectStream = Map<ReceiverStream<Sent>, fn(Sent) -> Result<Object, Status>>;

/// One object to read.
#[derive(Clone, Copy, Debug)]
enum Key {
    Tree(TreeId),
    Blob(BlobId),
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
            .max_encoding_message_size(MAX_OBJECT_MESSAGE_BYTES)
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

/// Reads an object, given the size taken from the budget for it.
/// [`read_object`], except in tests that count reads.
type Read = fn(&Store, Key, usize) -> Result<Object>;

/// An object's size in its stored form, without reading it.
fn object_size(store: &Store, key: Key) -> Result<usize> {
    match key {
        Key::Tree(id) => store.read(|c| db::tree_size(c, id)),
        Key::Blob(id) => store.blobs().size(id),
    }
}

/// Reads one object in its stored form. Trees and blobs are checked against
/// their IDs on the way out of the store, and the bytes must not exceed
/// `size`, the share of the budget taken for them.
fn read_object(store: &Store, key: Key, size: usize) -> Result<Object> {
    // SHORTCUT: an object ID is treated as permission to read the object, and
    // trees and blobs are shared by all repos. Safe with one principal.
    // Upgrade (issue #13): a read ticket bound to a principal, a repo and a
    // root tree, checked here and by every cache tier.
    let (kind, id, data) = match key {
        Key::Tree(id) => (
            ObjectKind::Tree,
            *id.as_bytes(),
            store.read(|c| db::get_tree_body(c, id))?,
        ),
        Key::Blob(id) => (ObjectKind::Blob, *id.as_bytes(), store.blobs().get(id)?),
    };
    if data.len() > size {
        return Err(Error::Corrupt(format!(
            "{key:?}: {} bytes, but its stored size is {size}",
            data.len()
        )));
    }
    Ok(Object {
        kind: kind.into(),
        id: id.to_vec(),
        data,
    })
}

/// Sends `keys` one at a time, as the client takes them. For each object it
/// looks up the size, takes that share of `in_flight`, and only then reads
/// the bytes, so a reply that waits for budget holds no object. Stops after
/// an error, once the reply has sent `REPLY_BUDGET_BYTES`, when the client
/// has gone, or when an object is not handed on within `stall`.
// SHORTCUT: two blocking hops per object and no read-ahead, so many small
// objects are read one after another. Upgrade: read a batch capped by bytes
// per hop, or keep a few reads in flight.
async fn send_objects(
    worker: Worker,
    keys: Vec<Key>,
    tx: mpsc::Sender<Sent>,
    in_flight: Arc<Semaphore>,
    stall: Duration,
    read: Read,
) {
    let mut sent = 0;
    for key in keys {
        if sent >= REPLY_BUDGET_BYTES {
            break;
        }
        let handed_on = async {
            let size = worker.run(move |store| object_size(&store, key)).await;
            // Clamped to the budget, which the asserts above keep within u32.
            let share = size
                .as_ref()
                .map_or(1, |&size| size.clamp(1, IN_FLIGHT_BYTES));
            let share = u32::try_from(share).unwrap_or(u32::MAX);
            let permit = Arc::clone(&in_flight)
                .acquire_many_owned(share)
                .await
                .ok()?;
            let object = match size {
                Ok(size) => worker.run(move |store| read(&store, key, size)).await,
                Err(status) => Err(status),
            };
            let len = object.as_ref().map_or(0, |o| o.data.len());
            let failed = object.is_err();
            tx.send((object, permit)).await.ok()?;
            Some((len, failed))
        };
        match tokio::time::timeout(stall, handed_on).await {
            Ok(Some((len, false))) => sent += len,
            _ => break,
        }
    }
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
        let GetObjectsRequest { repo, refs } = request.into_inner();
        check_name("repo", &repo)?;
        if refs.is_empty() || refs.len() > MAX_OBJECT_REFS {
            return Err(Status::invalid_argument(format!(
                "a call names 1 to {MAX_OBJECT_REFS} objects"
            )));
        }
        let keys = refs.iter().map(parse_ref).collect::<Result<Vec<_>, _>>()?;
        self.worker
            .run(move |store| store.read(|c| db::repo_id(c, &repo)))
            .await?;
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(send_objects(
            self.worker.clone(),
            keys,
            tx,
            Arc::clone(&self.in_flight),
            STALL_TIMEOUT,
            read_object,
        ));
        let release: fn(Sent) -> Result<Object, Status> = |(object, _permit)| object;
        Ok(Response::new(ReceiverStream::new(rx).map(release)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

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
        let (tx, rx) = mpsc::channel(1);
        let task = tokio::spawn(send_objects(
            Worker::new(store),
            keys,
            tx,
            Arc::clone(&in_flight),
            Duration::from_millis(50),
            read_object,
        ));

        // Nobody reads: the first object waits in the channel, the second
        // send stalls and ends the reply.
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("a stalled reply ends")
            .unwrap();
        assert_eq!(in_flight.available_permits(), budget - b"one".len());
        drop(rx);
        assert_eq!(in_flight.available_permits(), budget);
    }

    #[tokio::test]
    async fn a_reply_that_waits_for_budget_has_not_read_its_object() {
        static READS: AtomicUsize = AtomicUsize::new(0);
        fn counted(store: &Store, key: Key, size: usize) -> Result<Object> {
            READS.fetch_add(1, Ordering::SeqCst);
            read_object(store, key, size)
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
        let stall = Duration::from_millis(100);
        let reply = |keys, tx| {
            tokio::spawn(send_objects(
                worker.clone(),
                keys,
                tx,
                Arc::clone(&in_flight),
                stall,
                counted,
            ))
        };

        // Two replies whose clients never read. The first sends `a` into its
        // channel, where it keeps its share; then neither reply can get
        // budget for its next object.
        let (tx_first, rx_first) = mpsc::channel(1);
        let first = reply(vec![a, a], tx_first);
        let started = tokio::time::Instant::now();
        while in_flight.available_permits() == budget {
            assert!(started.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (tx_second, rx_second) = mpsc::channel(1);
        let second = reply(vec![b], tx_second);
        for task in [first, second] {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("a reply that cannot get budget ends after the stall")
                .unwrap();
        }

        assert_eq!(
            READS.load(Ordering::SeqCst),
            1,
            "only the first `a` was read"
        );
        assert_eq!(in_flight.available_permits(), budget - size);
        drop((rx_first, rx_second));
        assert_eq!(in_flight.available_permits(), budget);
    }
}

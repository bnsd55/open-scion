//! The `Objects` service: the reads that any cache tier can answer.
//!
//! A client resolves a ref, gets the root tree of the changeset, then asks for
//! trees and blobs by ID. Objects go out in their stored form, a tree as
//! `TreeV1` bytes and a blob as its raw content, so the client checks each one
//! against its ID.

use std::sync::Arc;
use std::time::Duration;

use scion_core::{BlobId, TreeId};
use scion_proto::reply::{self, IN_FLIGHT_BYTES, ObjectStream, STALL_TIMEOUT, Sent};
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, Object, ObjectKind, ObjectRef,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{MAX_MESSAGE_BYTES, MAX_OBJECT_MESSAGE_BYTES};
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

/// Starts the reply to `keys`: finds the first object before the call
/// answers, then sends the rest on a task of its own, one at a time, as the
/// client takes them (see [`reply::send_objects`]). If the first object gets
/// no share of `in_flight` within `stall`, the call fails with
/// [`reply::busy`], so a reply never ends `OK` with no object.
async fn start_reply(
    worker: Worker,
    keys: Vec<Key>,
    in_flight: Arc<Semaphore>,
    stall: Duration,
    read: Read,
) -> Result<ObjectStream, Status> {
    let mut rest = keys.into_iter();
    let Some(key) = rest.next() else {
        return Err(Status::invalid_argument("a call names at least one object"));
    };
    let first = find(&worker, &in_flight, stall, read, key)
        .await
        .ok_or_else(reply::busy)?;
    let (tx, stream) = reply::channel();
    tokio::spawn(async move {
        let (worker, in_flight) = (&worker, &in_flight);
        let produce = |key| find(worker, in_flight, stall, read, key);
        reply::send_objects(first, rest, tx, stall, produce).await;
    });
    Ok(stream)
}

/// Finds one object for a reply: looks up its size, takes that share of
/// `in_flight`, and only then reads the bytes. `None` if the share is not
/// free within `stall`.
// SHORTCUT: two blocking hops per object and no read-ahead, so many small
// objects are read one after another. Upgrade: read a batch capped by bytes
// per hop, or keep a few reads in flight (issue #6).
async fn find(
    worker: &Worker,
    in_flight: &Arc<Semaphore>,
    stall: Duration,
    read: Read,
    key: Key,
) -> Option<Sent> {
    let size = worker.run(move |store| object_size(&store, key)).await;
    let permit = reply::take(in_flight, *size.as_ref().unwrap_or(&1), stall).await?;
    let object = match size {
        Ok(size) => worker.run(move |store| read(&store, key, size)).await,
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
        scion_proto::check::object_count(refs.len())?;
        let keys = refs.iter().map(parse_ref).collect::<Result<Vec<_>, _>>()?;
        self.worker
            .run(move |store| store.read(|c| db::repo_id(c, &repo)))
            .await?;
        let in_flight = Arc::clone(&self.in_flight);
        let stream = start_reply(
            self.worker.clone(),
            keys,
            in_flight,
            STALL_TIMEOUT,
            read_object,
        );
        Ok(Response::new(stream.await?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            stall,
            read_object,
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
        let reply = |keys| {
            let reply = start_reply(worker.clone(), keys, Arc::clone(&in_flight), stall, counted);
            tokio::time::timeout(Duration::from_secs(10), reply)
        };

        // A call whose client never reads keeps `a` and its share. The next
        // call gets no budget within the stall limit: it fails, and never
        // ends `OK` with no object.
        let held = reply(vec![a]).await.unwrap().unwrap();
        let status = reply(vec![b]).await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
        assert_eq!(READS.load(Ordering::SeqCst), 1, "`b` was never read");
        drop(held);
        until(|| in_flight.available_permits() == budget).await;
    }
}

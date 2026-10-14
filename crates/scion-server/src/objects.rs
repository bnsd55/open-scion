//! The `Objects` service: the reads that any cache tier can answer.
//!
//! A client resolves a ref, gets the root tree of the changeset, then asks for
//! trees and blobs by ID. Objects go out in their stored form, a tree as
//! `TreeV1` bytes and a blob as its raw content, so the client checks each one
//! against its ID.

use std::pin::Pin;

use scion_core::{BlobId, TreeId};
use scion_proto::v1::objects_server::{Objects, ObjectsServer};
use scion_proto::v1::{
    GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, Object, ObjectKind, ObjectRef,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{MAX_MESSAGE_BYTES, MAX_OBJECT_MESSAGE_BYTES, MAX_OBJECT_REFS};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db;
use crate::error::Result;
use crate::service::{Worker, check_name, parse_id, resolve_ref};
use crate::store::{MAX_OBJECT_BYTES, Store};
use crate::tree;

/// A `GetObjects` reply stops before the next object once it has sent this
/// many bytes. The client asks again for the rest, so one call stays short.
const REPLY_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES;

// Every stored object fits in one reply message, with room for its kind and ID.
const _: () = assert!(MAX_OBJECT_BYTES + 64 <= MAX_OBJECT_MESSAGE_BYTES);

/// One object to read.
#[derive(Clone, Copy)]
enum Key {
    Tree(TreeId),
    Blob(BlobId),
}

type ObjectStream = Pin<Box<dyn Stream<Item = Result<Object, Status>> + Send>>;

/// Implements the `Objects` RPCs.
pub struct ObjectsService {
    worker: Worker,
}

impl ObjectsService {
    pub(crate) fn new(worker: Worker) -> Self {
        Self { worker }
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

/// Reads one object in its stored form. Trees and blobs are checked against
/// their IDs on the way out of the store.
fn read_object(store: &Store, key: Key) -> Result<Object> {
    // SHORTCUT: an object ID is treated as permission to read the object, and
    // trees and blobs are shared by all repos. Safe with one principal.
    // Upgrade (issue #13): a read ticket bound to a principal, a repo and a
    // root tree, checked here and by every cache tier.
    let (kind, id, data) = match key {
        Key::Tree(id) => {
            let tree = store.read(|c| db::get_tree(c, id))?;
            (ObjectKind::Tree, *id.as_bytes(), tree.encode())
        }
        Key::Blob(id) => (ObjectKind::Blob, *id.as_bytes(), store.blobs().get(id)?),
    };
    Ok(Object {
        kind: kind.into(),
        id: id.to_vec(),
        data,
    })
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
            .run(move |store| {
                let repo = store.read(|c| db::repo_id(c, &repo))?;
                tree::root_tree(&store, repo, cs)
            })
            .await?;
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
        // Reads one object at a time, as the client takes them, so a call
        // holds at most two objects in memory.
        let (tx, rx) = mpsc::channel(1);
        let worker = self.worker.clone();
        tokio::spawn(async move {
            let mut sent = 0;
            for key in keys {
                if sent >= REPLY_BUDGET_BYTES {
                    break;
                }
                let object = worker.run(move |store| read_object(&store, key)).await;
                let failed = object.is_err();
                sent += object.as_ref().map_or(0, |o| o.data.len());
                // A send fails once the client has gone; an error ends the reply.
                if tx.send(object).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

//! The gRPC front end. Parses requests, runs the work on a blocking thread
//! (`SQLite` and the object folder are synchronous), and maps errors to status
//! codes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use scion_core::{ChangesetId, FileState, RepoPath};
use scion_proto::v1::scion_server::{Scion, ScionServer};
use scion_proto::v1::{
    self, BlameChangeset, BlameRange, BlameRequest, BlameResponse, CreateChangesetRequest,
    CreateChangesetResponse, DiffEntry, DiffRequest, DiffResponse, FileKind, GetChangesetRequest,
    GetChangesetResponse, GetLandRequest, IsAncestorRequest, IsAncestorResponse, LandResponse,
    ListDirRequest, ListDirResponse, LogEntry, LogRequest, LogResponse, MergeBaseRequest,
    MergeBaseResponse, PutBlobRequest, PutBlobResponse, ReadFileRequest, ReadFileResponse,
    ResolveRefRequest, ResolveRefResponse,
};
use scion_proto::{CHUNK_BYTES, MAX_MESSAGE_BYTES};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{Auth, Caller};
use crate::error::Result;
use crate::land::{self, LandOutcome, LandState, Lands};
use crate::objects::ObjectsService;
use crate::store::Store;
use crate::{blame, db, names, read, write};

/// Most changeset bytes in one `Log` reply; the client pages for more.
const LOG_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES / 2;
/// Most store operations running at once.
const MAX_BLOCKING: usize = 256;
/// Most uploads read at once. Each holds one request message in memory, of up
/// to `MAX_MESSAGE_BYTES`, and a spool file of up to `MAX_OBJECT_BYTES`.
const MAX_UPLOADS: usize = 16;
/// Most `Blame` calls running at once. One call can diff for seconds, so
/// blames never take more than this many of the `MAX_BLOCKING` permits that
/// writes and lands share.
const MAX_BLAMES: usize = 4;
/// Longest a request may take before the server gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// Serves the API on `listener`, requiring `token`, until `shutdown` resolves.
///
/// # Errors
///
/// Transport errors.
pub async fn serve(
    store: Store,
    listener: TcpListener,
    token: &str,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let auth = Auth::new(token);
    let worker = Worker::new(store);
    let scion = InterceptedService::new(
        ScionService::new(worker.clone()).into_server(),
        auth.clone(),
    );
    let objects = InterceptedService::new(ObjectsService::new(worker).into_server(), auth);
    tonic::transport::Server::builder()
        .timeout(REQUEST_TIMEOUT)
        .add_service(scion)
        .add_service(objects)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await
}

/// Implements the Scion RPCs.
pub struct ScionService {
    worker: Worker,
    lands: Arc<Lands>,
    /// Bounds uploads read at once (`MAX_UPLOADS`): their messages in memory
    /// and their spool files in `tmp/`.
    uploads: Arc<Semaphore>,
    /// Bounds blames running at once (`MAX_BLAMES`).
    blames: Arc<Semaphore>,
}

impl ScionService {
    fn new(worker: Worker) -> Self {
        Self {
            worker,
            lands: Arc::default(),
            uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
            blames: Arc::new(Semaphore::new(MAX_BLAMES)),
        }
    }

    fn into_server(self) -> ScionServer<Self> {
        ScionServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }
}

/// Runs synchronous store work off the async runtime. Clones share one bound
/// on the work in flight, so both services queue behind the same limit.
#[derive(Clone)]
pub(crate) struct Worker {
    store: Store,
    /// Bounds blocking work, so a flood of requests queues instead of
    /// spawning unbounded threads.
    permits: Arc<Semaphore>,
}

impl Worker {
    pub(crate) fn new(store: Store) -> Self {
        Self {
            store,
            permits: Arc::new(Semaphore::new(MAX_BLOCKING)),
        }
    }

    /// Runs `f` on a blocking thread once a permit is free.
    pub(crate) async fn run<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(Store) -> Result<T> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(store)
        })
        .await
        .map_err(|e| {
            // A panic's message stays in the server log, not in the reply.
            tracing::error!(error = %e, "worker failed");
            Status::internal("internal error")
        })?
        .map_err(Status::from)
    }
}

pub(crate) fn parse_id<T: for<'a> TryFrom<&'a [u8]>>(
    bytes: &[u8],
    what: &str,
) -> Result<T, Status> {
    T::try_from(bytes).map_err(|_| Status::invalid_argument(format!("{what} must be 32 bytes")))
}

pub(crate) fn check_name(what: &str, value: &str) -> Result<(), Status> {
    names::check(what, value).map_err(Status::from)
}

/// `ResolveRef`, served by both services.
pub(crate) async fn resolve_ref(
    worker: &Worker,
    request: ResolveRefRequest,
) -> Result<ResolveRefResponse, Status> {
    let ResolveRefRequest { repo, name } = request;
    check_name("repo", &repo)?;
    check_name("ref", &name)?;
    let id = worker
        .run(move |store| read::resolve_ref(&store, &repo, &name))
        .await?;
    Ok(ResolveRefResponse {
        changeset_id: id.as_bytes().to_vec(),
    })
}

fn parse_path(bytes: Vec<u8>) -> Result<RepoPath, Status> {
    RepoPath::new(bytes).map_err(|e| Status::invalid_argument(format!("bad path: {e}")))
}

/// An optional path on the wire: empty for `None`.
fn parse_optional_path(bytes: Vec<u8>) -> Result<Option<RepoPath>, Status> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        parse_path(bytes).map(Some)
    }
}

fn caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("no caller"))
}

fn land_response(outcome: LandOutcome) -> LandResponse {
    use scion_proto::v1::LandState as Wire;
    let state = match outcome.state {
        LandState::Pending => Wire::Pending,
        LandState::Landed => Wire::Landed,
        LandState::Conflict => Wire::Conflict,
        LandState::Rejected => Wire::Rejected,
    };
    LandResponse {
        land_id: outcome.land_id.to_string(),
        state: state.into(),
        result_changeset_id: id_bytes(outcome.result),
        detail: outcome.detail,
        conflict_paths: outcome
            .conflict_paths
            .into_iter()
            .map(|p| p.as_bytes().to_vec())
            .collect(),
    }
}

type ReadFileStream = Pin<Box<dyn Stream<Item = Result<ReadFileResponse, Status>> + Send>>;

#[tonic::async_trait]
impl Scion for ScionService {
    async fn put_blob(
        &self,
        request: Request<Streaming<PutBlobRequest>>,
    ) -> Result<Response<PutBlobResponse>, Status> {
        let _upload = Arc::clone(&self.uploads)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty upload"))??;
        check_name("repo", &first.repo)?;
        // Check the repo before reading any more data. Each message goes to
        // the spool file before the next one is read, so an upload holds one
        // message in memory, not the whole blob.
        let mut upload = self
            .worker
            .run(move |store| {
                store.read(|c| db::repo_id(c, &first.repo))?;
                store.blobs().upload()?.append(&first.data)
            })
            .await?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if !chunk.repo.is_empty() {
                return Err(Status::invalid_argument(
                    "only the first PutBlob message names the repo",
                ));
            }
            upload = self.worker.run(move |_| upload.append(&chunk.data)).await?;
        }
        let id = self
            .worker
            .run(move |store| store.blobs().put_upload(upload))
            .await?;
        Ok(Response::new(PutBlobResponse {
            blob_id: id.as_bytes().to_vec(),
        }))
    }

    async fn create_changeset(
        &self,
        request: Request<CreateChangesetRequest>,
    ) -> Result<Response<CreateChangesetResponse>, Status> {
        let CreateChangesetRequest { repo, changeset } = request.into_inner();
        check_name("repo", &repo)?;
        let id = self
            .worker
            .run(move |store| write::create_changeset(&store, &repo, &changeset))
            .await?;
        Ok(Response::new(CreateChangesetResponse {
            changeset_id: id.as_bytes().to_vec(),
        }))
    }

    async fn land(
        &self,
        request: Request<scion_proto::v1::LandRequest>,
    ) -> Result<Response<LandResponse>, Status> {
        let caller = caller(&request)?;
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        check_name("target_ref", &req.target_ref)?;
        check_name("request_key", &req.request_key)?;
        let req = land::LandRequest {
            changeset: parse_id(&req.changeset_id, "changeset_id")?,
            repo: req.repo,
            target_ref: req.target_ref,
            request_key: req.request_key,
        };
        let lands = Arc::clone(&self.lands);
        let outcome = self
            .worker
            .run(move |store| lands.land(&store, &caller, &req))
            .await?;
        Ok(Response::new(land_response(outcome)))
    }

    async fn get_land(
        &self,
        request: Request<GetLandRequest>,
    ) -> Result<Response<LandResponse>, Status> {
        let caller = caller(&request)?;
        let GetLandRequest { repo, land_id } = request.into_inner();
        check_name("repo", &repo)?;
        let land_id = land_id
            .parse()
            .map_err(|_| Status::invalid_argument("land_id must be a number"))?;
        let outcome = self
            .worker
            .run(move |store| land::get_land(&store, &caller, &repo, land_id))
            .await?;
        Ok(Response::new(land_response(outcome)))
    }

    async fn resolve_ref(
        &self,
        request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        resolve_ref(&self.worker, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_changeset(
        &self,
        request: Request<GetChangesetRequest>,
    ) -> Result<Response<GetChangesetResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let changeset = self
            .worker
            .run(move |store| read::get_changeset(&store, &req.repo, id))
            .await?;
        Ok(Response::new(GetChangesetResponse {
            changeset: changeset.encode(),
        }))
    }

    type ReadFileStream = ReadFileStream;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileStream>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_path(req.path)?;
        let data = self
            .worker
            .run(move |store| read::read_file(&store, &req.repo, id, &path))
            .await?;
        // Copies one chunk at a time, as the stream is read.
        let chunks = (0..data.len().div_ceil(CHUNK_BYTES)).map(move |i| {
            let end = data.len().min((i + 1) * CHUNK_BYTES);
            Ok(ReadFileResponse {
                data: data[i * CHUNK_BYTES..end].to_vec(),
            })
        });
        Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_optional_path(req.path)?;
        let tree = self
            .worker
            .run(move |store| read::list_dir(&store, &req.repo, id, path.as_ref()))
            .await?;
        Ok(Response::new(ListDirResponse {
            tree: tree.encode(),
        }))
    }

    async fn log(&self, request: Request<LogRequest>) -> Result<Response<LogResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        // `read::MAX_SCAN` also bounds the entries.
        let limit = (req.limit as usize).max(1);
        let path = parse_optional_path(req.path)?;
        let page = self
            .worker
            .run(move |store| read::log_page(&store, &req.repo, id, path.as_ref(), limit))
            .await?;
        // Always send the first entry; stop before the reply gets too big.
        let mut next = page.next;
        let mut budget = LOG_BUDGET_BYTES;
        let mut entries = Vec::new();
        for (id, changeset) in page.entries {
            let body = changeset.encode();
            if !entries.is_empty() && body.len() > budget {
                // Not sent, so the next call starts here.
                next = Some(id);
                break;
            }
            budget = budget.saturating_sub(body.len());
            entries.push(LogEntry {
                changeset_id: id.as_bytes().to_vec(),
                changeset: body,
            });
        }
        Ok(Response::new(LogResponse {
            entries,
            next_changeset_id: id_bytes(next),
        }))
    }

    async fn blame(
        &self,
        request: Request<BlameRequest>,
    ) -> Result<Response<BlameResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_path(req.path)?;
        // Taken before the worker permit, and held until the job ends, also
        // when the client leaves first.
        let slot = Arc::clone(&self.blames)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let blame = self
            .worker
            .run(move |store| {
                let _slot = slot;
                blame::blame(&store, &req.repo, id, &path)
            })
            .await?;
        let ranges = blame
            .ranges
            .into_iter()
            .map(|range| BlameRange {
                start_line: range.start_line,
                line_count: range.line_count,
                changeset_id: id_bytes(range.changeset),
                origin_line: range.origin_line,
            })
            .collect();
        let changesets = blame
            .authors
            .into_iter()
            .map(|(id, author)| BlameChangeset {
                changeset_id: id.as_bytes().to_vec(),
                author_name: author.name,
                author_email: author.email,
                author_unix_secs: author.unix_secs,
                author_tz_offset_minutes: author.tz_offset_minutes,
            })
            .collect();
        Ok(Response::new(BlameResponse {
            ranges,
            changesets,
            next_changeset_id: id_bytes(blame.next),
        }))
    }

    async fn merge_base(
        &self,
        request: Request<MergeBaseRequest>,
    ) -> Result<Response<MergeBaseResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let a = parse_id(&req.a_changeset_id, "a_changeset_id")?;
        let b = parse_id(&req.b_changeset_id, "b_changeset_id")?;
        let bases = self
            .worker
            .run(move |store| read::merge_base(&store, &req.repo, a, b))
            .await?;
        Ok(Response::new(MergeBaseResponse {
            changeset_ids: bases.iter().map(|id| id.as_bytes().to_vec()).collect(),
        }))
    }

    async fn is_ancestor(
        &self,
        request: Request<IsAncestorRequest>,
    ) -> Result<Response<IsAncestorResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let ancestor = parse_id(&req.ancestor_id, "ancestor_id")?;
        let descendant = parse_id(&req.descendant_id, "descendant_id")?;
        let is_ancestor = self
            .worker
            .run(move |store| read::is_ancestor(&store, &req.repo, ancestor, descendant))
            .await?;
        Ok(Response::new(IsAncestorResponse { is_ancestor }))
    }

    async fn diff(&self, request: Request<DiffRequest>) -> Result<Response<DiffResponse>, Status> {
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
        let old = parse_id(&req.old_changeset_id, "old_changeset_id")?;
        let new = parse_id(&req.new_changeset_id, "new_changeset_id")?;
        let path = parse_optional_path(req.path)?;
        let after = parse_optional_path(req.after)?;
        let page = self
            .worker
            .run(move |store| {
                read::diff(&store, &req.repo, old, new, path.as_ref(), after.as_ref())
            })
            .await?;
        let entries = page
            .entries
            .into_iter()
            .map(|entry| DiffEntry {
                path: entry.path.as_bytes().to_vec(),
                old: entry.old.map(file_state),
                new: entry.new.map(file_state),
            })
            .collect();
        Ok(Response::new(DiffResponse {
            entries,
            next_after: page
                .next_after
                .map(|path| path.as_bytes().to_vec())
                .unwrap_or_default(),
        }))
    }
}

/// An optional changeset ID on the wire: empty for `None`.
fn id_bytes(id: Option<ChangesetId>) -> Vec<u8> {
    id.map(|id| id.as_bytes().to_vec()).unwrap_or_default()
}

fn file_state(state: FileState) -> v1::FileState {
    let (kind, id, executable) = match state {
        FileState::Regular {
            content,
            executable,
        } => (FileKind::Regular, content.as_bytes().to_vec(), executable),
        FileState::Symlink { target } => (FileKind::Symlink, target.as_bytes().to_vec(), false),
        FileState::Gitlink { commit } => (FileKind::Gitlink, commit.as_bytes().to_vec(), false),
    };
    v1::FileState {
        kind: kind.into(),
        id,
        executable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_worker_panic_does_not_reach_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let worker = Worker::new(Store::open(dir.path()).unwrap());
        let status = worker
            .run(|_| -> Result<()> { panic!("secret detail") })
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
    }
}

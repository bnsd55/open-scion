//! The gRPC front end. Parses requests, runs the work on a blocking thread
//! (`SQLite` and the object folder are synchronous), and maps errors to status
//! codes.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use scion_core::{BlobId, ChangesetId, FileState, RepoPath};
use scion_proto::listen::{self, Limits};
use scion_proto::tls::ServerTls;
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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::access::Access;
use crate::auth::Caller;
use crate::blame::RenameSearch;
use crate::deadline::Deadline;
use crate::error::Result;
use crate::land::{self, LandOutcome, LandState, Lands};
use crate::meta::MetaTx as _;
use crate::objects::ObjectsService;
use crate::store::{MAX_OBJECT_BYTES, READER_BYTES, Store};
use crate::{blame, names, read, write};

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
/// Most `MergeBase`, `IsAncestor` and `Log` calls with a path running at
/// once. A call has no bound on its reads, only its deadline: a walk, or the
/// first derivation of path history on a database from before it. So these
/// queries never take more than this many of the `MAX_BLOCKING` permits.
const MAX_GRAPH_QUERIES: usize = 4;
// A graph query holds one read connection at a time, through a walk with no
// bound on its reads. So the walks never take every one.
const _: () = assert!(MAX_GRAPH_QUERIES < crate::store::READERS);
/// Most `Diff` calls running at once. One call can read and compare files
/// for about a second to find renames (`read::MAX_RENAME_CHUNKS`), so diffs
/// never take more than this many of the `MAX_BLOCKING` permits either.
const MAX_DIFFS: usize = 4;
/// Longest a request may take before the server gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
/// Most bytes that all `ReadFile` replies together hold: the parts read and
/// not yet sent, and the readers of files of more than one part.
const READ_BUDGET_BYTES: usize = 256 << 20;
/// A `ReadFile` reply ends when its client takes no part for this long, and
/// frees its share, so a client that stops reading does not hold budget.
/// The parts that it leaves in its channel and in HTTP/2 stay outside the
/// budget (`scion_proto::listen`).
const READ_STALL_TIMEOUT: Duration = Duration::from_secs(120);

// A connection that is asked to close keeps its calls for at least as long
// as the longest call takes.
const _: () = assert!(REQUEST_TIMEOUT.as_secs() <= listen::CONNECTION_GRACE.as_secs());

// Any one reply fits in the budget, and the budget counts in u32 permits.
const _: () = assert!(read_share(MAX_OBJECT_BYTES) <= READ_BUDGET_BYTES);
const _: () = assert!(READ_BUDGET_BYTES <= u32::MAX as usize);

/// Serves the API on `listener` within `limits`, until `shutdown` resolves,
/// to the tokens and with the rules of `access`: an [`Access`], or one token
/// (`&str`) that may do everything. With `tls`, every connection is a TLS
/// connection, whose handshake `listen` ends within its timeout.
///
/// # Errors
///
/// Transport errors.
pub async fn serve(
    store: Store,
    listener: TcpListener,
    access: impl Into<Access>,
    limits: Limits,
    tls: Option<ServerTls>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let access = Arc::new(access.into());
    let auth = access.auth();
    let worker = Worker::new(store);
    let scion = InterceptedService::new(
        ScionService::new(worker.clone(), Arc::clone(&access)).into_server(),
        auth.clone(),
    );
    let objects = InterceptedService::new(ObjectsService::new(worker, access).into_server(), auth);
    let router = |server: Server| {
        server
            .timeout(REQUEST_TIMEOUT)
            .add_service(scion)
            .add_service(objects)
    };
    limits
        .serve(
            router,
            listen::tcp(listener),
            tls.map(|tls| tls.config()),
            shutdown,
        )
        .await
}

/// Implements the Scion RPCs.
pub struct ScionService {
    worker: Worker,
    access: Arc<Access>,
    lands: Arc<Lands>,
    /// Bounds uploads read at once (`MAX_UPLOADS`): their messages in memory
    /// and their spool files in `tmp/`.
    uploads: Arc<Semaphore>,
    /// Bounds blames running at once (`MAX_BLAMES`).
    blames: Arc<Semaphore>,
    /// Bounds graph queries running at once (`MAX_GRAPH_QUERIES`).
    graph_queries: Arc<Semaphore>,
    /// Bounds diffs running at once (`MAX_DIFFS`).
    diffs: Arc<Semaphore>,
    /// `READ_BUDGET_BYTES` permits, one per byte, shared by all `ReadFile`
    /// replies.
    reads: Arc<Semaphore>,
}

impl ScionService {
    fn new(worker: Worker, access: Arc<Access>) -> Self {
        Self {
            worker,
            access,
            lands: Arc::default(),
            uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
            blames: Arc::new(Semaphore::new(MAX_BLAMES)),
            graph_queries: Arc::new(Semaphore::new(MAX_GRAPH_QUERIES)),
            diffs: Arc::new(Semaphore::new(MAX_DIFFS)),
            reads: Arc::new(Semaphore::new(READ_BUDGET_BYTES)),
        }
    }

    fn into_server(self) -> ScionServer<Self> {
        ScionServer::new(self)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    /// Runs a `MergeBase`, `IsAncestor` or `Log` with a path: at most
    /// `MAX_GRAPH_QUERIES` at once, each until its call ends or its
    /// `deadline` passes.
    async fn graph_query<T, F>(&self, deadline: Deadline, query: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Store, &Deadline) -> Result<T> + Send + 'static,
    {
        // The query checks it at each read: a request timeout, or a client
        // that cancels, drops this call but not a job that is running. The
        // guard ends the deadline when this call ends or is dropped.
        let _call = deadline.guard();
        // Taken before the worker permit, and held until the job ends, also
        // when the client leaves first.
        let slot = Arc::clone(&self.graph_queries)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        self.worker
            .run(move |store| {
                let _slot = slot;
                query(&store, &deadline)
            })
            .await
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

pub(crate) use scion_proto::check::id as parse_id;

pub(crate) fn check_name(what: &str, value: &str) -> Result<(), Status> {
    names::check(what, value).map_err(Status::from)
}

/// Checks the name of `repo`, then that `caller` may read it. A repo that it
/// may not read gets the answer of a repo that does not exist.
pub(crate) fn check_reader(access: &Access, caller: &Caller, repo: &str) -> Result<(), Status> {
    check_name("repo", repo)?;
    access.check_read(caller, repo).map_err(Status::from)
}

/// `ResolveRef`, served by both services.
pub(crate) async fn resolve_ref(
    worker: &Worker,
    access: &Access,
    request: Request<ResolveRefRequest>,
) -> Result<ResolveRefResponse, Status> {
    let caller = caller(&request)?;
    let ResolveRefRequest { repo, name } = request.into_inner();
    check_reader(access, &caller, &repo)?;
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

pub(crate) fn caller<T>(request: &Request<T>) -> Result<Caller, Status> {
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

/// A part of a `ReadFile` reply, or the error that ends it early.
type Part = Result<ReadFileResponse, Status>;

type ReadFileStream = ReceiverStream<Part>;

/// The share of `READ_BUDGET_BYTES` that a `ReadFile` reply of a `size`-byte
/// file holds. A file of one part is read in one job, so the reply holds only
/// that part. A longer file keeps its reader between parts, and reads the
/// next part while the client takes the one before.
const fn read_share(size: usize) -> usize {
    if size <= CHUNK_BYTES {
        size
    } else {
        READER_BYTES + 2 * CHUNK_BYTES
    }
}

/// Sends a blob to `tx` in parts of up to `CHUNK_BYTES`, as the client takes
/// them. Holds `share` until the client has taken the last part, or until no
/// part is taken within `stall`. A part goes into the channel only when the
/// channel is empty, so its second slot is always free for the error that
/// ends a reply early: a store error, or the stall. A client never takes a
/// short file for a whole one.
async fn send_file(
    worker: Worker,
    blob: BlobId,
    _share: OwnedSemaphorePermit,
    tx: mpsc::Sender<Part>,
    stall: Duration,
    stalled: Option<scion_proto::listen::Stalled>,
) {
    let fail = |status: Status| {
        // Fails only if the client has gone.
        let _ = tx.try_send(Err(status));
    };
    let mut open = None;
    loop {
        let read = worker
            .run(move |store| {
                let mut reader = match open {
                    Some(reader) => reader,
                    None => store.blobs().reader(blob)?,
                };
                let part = reader.read_part(CHUNK_BYTES)?;
                // A reader that has ended has freed its decoder.
                Ok((part, (!reader.ended()).then_some(reader)))
            })
            .await;
        let (part, reader) = match read {
            Ok(read) => read,
            Err(status) => return fail(status),
        };
        open = reader;
        if let Some(data) = part {
            // Both slots free: the client has taken the part before.
            match tokio::time::timeout(stall, tx.reserve_many(2)).await {
                Ok(Ok(mut slots)) => {
                    if let Some(slot) = slots.next() {
                        slot.send(Ok(ReadFileResponse { data }));
                    }
                }
                Ok(Err(_)) => return,
                Err(_) => {
                    // The parts of the file stay in HTTP/2 until the
                    // connection ends; the connection counts them.
                    if let Some(stalled) = &stalled {
                        stalled.add(scion_proto::reply::STALLED_REPLY_BYTES);
                    }
                    let detail = format!("the client took no data for {stall:?}");
                    return fail(Status::deadline_exceeded(detail));
                }
            }
        }
        if open.is_none() {
            // Keeps the share until the client has taken the last part.
            match tokio::time::timeout(stall, tx.reserve_many(2)).await {
                // Sent, or the client has gone with it.
                Ok(_) => {}
                // It stopped reading: what was sent stays in HTTP/2.
                Err(_) => {
                    if let Some(stalled) = &stalled {
                        stalled.add(scion_proto::reply::STALLED_REPLY_BYTES);
                    }
                }
            }
            return;
        }
    }
}

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
        let caller = caller(&request)?;
        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty upload"))??;
        check_name("repo", &first.repo)?;
        // An upload names no path, so a caller that may change some path of
        // the repo may upload.
        self.access.check_writer(&caller, &first.repo)?;
        // Check the repo before reading any more data. Each message goes to
        // the spool file before the next one is read, so an upload holds one
        // message in memory, not the whole blob.
        let (repo, mut upload) = self
            .worker
            .run(move |store| {
                let repo = store.read(|c| c.repo_id(&first.repo))?;
                Ok((repo, store.blobs().upload()?.append(&first.data)?))
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
            .run(move |store| write::put_blob(&store, repo, upload))
            .await?;
        Ok(Response::new(PutBlobResponse {
            blob_id: id.as_bytes().to_vec(),
        }))
    }

    async fn create_changeset(
        &self,
        request: Request<CreateChangesetRequest>,
    ) -> Result<Response<CreateChangesetResponse>, Status> {
        let caller = caller(&request)?;
        let CreateChangesetRequest { repo, changeset } = request.into_inner();
        check_reader(&self.access, &caller, &repo)?;
        let access = Arc::clone(&self.access);
        let id = self
            .worker
            .run(move |store| {
                // The rules first, so a change that the caller may not make
                // reads no blob. Decoding twice costs little next to reading
                // every blob back.
                let (cs, _) = write::decode(&changeset)?;
                access.check_write(&caller, &repo, cs.changes.keys())?;
                write::create_changeset(&store, &repo, &changeset)
            })
            .await?;
        Ok(Response::new(CreateChangesetResponse {
            changeset_id: id.as_bytes().to_vec(),
        }))
    }

    async fn land(
        &self,
        request: Request<scion_proto::v1::LandRequest>,
    ) -> Result<Response<LandResponse>, Status> {
        // The job checks it in its walks: a request timeout, or a client
        // that cancels, drops this call but not a job that is running. The
        // guard ends the deadline when this call ends or is dropped.
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let _call = deadline.guard();
        let caller = caller(&request)?;
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        check_name("target_ref", &req.target_ref)?;
        check_name("request_key", &req.request_key)?;
        let req = land::LandRequest {
            changeset: parse_id(&req.changeset_id, "changeset_id")?,
            repo: req.repo,
            target_ref: req.target_ref,
            request_key: req.request_key,
            deadline,
        };
        let (lands, access) = (Arc::clone(&self.lands), Arc::clone(&self.access));
        let outcome = self
            .worker
            .run(move |store| {
                // A rebase keeps the changes, so these are the paths that the
                // land changes, on any ref of the repo. Checked on every call,
                // before the land records anything, also on a retry.
                let cs = read::get_changeset(&store, &req.repo, req.changeset)?;
                access.check_write(&caller, &req.repo, cs.changes.keys())?;
                lands.land(&store, &caller, &req)
            })
            .await?;
        Ok(Response::new(land_response(outcome)))
    }

    async fn get_land(
        &self,
        request: Request<GetLandRequest>,
    ) -> Result<Response<LandResponse>, Status> {
        let caller = caller(&request)?;
        let GetLandRequest { repo, land_id } = request.into_inner();
        check_reader(&self.access, &caller, &repo)?;
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
        resolve_ref(&self.worker, &self.access, request)
            .await
            .map(Response::new)
    }

    async fn get_changeset(
        &self,
        request: Request<GetChangesetRequest>,
    ) -> Result<Response<GetChangesetResponse>, Status> {
        let caller = caller(&request)?;
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
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
        let caller = caller(&request)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_path(req.path)?;
        let (blob, size) = self
            .worker
            .run(move |store| {
                let blob = read::file_blob(&store, &req.repo, id, &path)?;
                Ok((blob, store.blobs().size(blob)?))
            })
            .await?;
        // Taken before any content is read, so a call that waits holds none.
        let share = u32::try_from(read_share(size)).expect("asserted to fit in u32");
        let share = Arc::clone(&self.reads)
            .acquire_many_owned(share)
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(send_file(
            self.worker.clone(),
            blob,
            share,
            tx,
            READ_STALL_TIMEOUT,
            stalled,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let caller = caller(&request)?;
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
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
        let caller = caller(&request)?;
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        // `read::MAX_SCAN` also bounds the entries.
        let limit = (req.limit as usize).max(1);
        let path = parse_optional_path(req.path)?;
        let page = if let Some(path) = path {
            // It may derive the path history of a long history first, so it
            // runs as a graph query, with its slot and its deadline.
            self.graph_query(deadline, move |store, deadline| {
                read::log_page(store, &req.repo, id, Some(&path), limit, deadline)
            })
            .await?
        } else {
            self.worker
                .run(move |store| read::log_page(&store, &req.repo, id, None, limit, &deadline))
                .await?
        };
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
        let caller = caller(&request)?;
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_path(req.path)?;
        let search = RenameSearch {
            searched: usize::try_from(req.rename_searched)
                .map_err(|_| Status::invalid_argument("rename_searched is too large"))?,
            best: parse_optional_path(req.rename_best)?,
        };
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
                blame::blame(&store, &req.repo, id, &path, &search)
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
            .changesets
            .into_iter()
            .map(|(id, author, path)| BlameChangeset {
                changeset_id: id.as_bytes().to_vec(),
                author_name: author.name,
                author_email: author.email,
                author_unix_secs: author.unix_secs,
                author_tz_offset_minutes: author.tz_offset_minutes,
                path: path.as_bytes().to_vec(),
            })
            .collect();
        let (next, next_path) = blame.next.unzip();
        Ok(Response::new(BlameResponse {
            ranges,
            changesets,
            next_changeset_id: id_bytes(next),
            next_path: path_bytes(next_path),
            next_rename_searched: blame.next_search.searched as u64,
            next_rename_best: path_bytes(blame.next_search.best),
        }))
    }

    async fn merge_base(
        &self,
        request: Request<MergeBaseRequest>,
    ) -> Result<Response<MergeBaseResponse>, Status> {
        let caller = caller(&request)?;
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let a = parse_id(&req.a_changeset_id, "a_changeset_id")?;
        let b = parse_id(&req.b_changeset_id, "b_changeset_id")?;
        let bases = self
            .graph_query(deadline, move |store, deadline| {
                read::merge_base(store, &req.repo, a, b, deadline)
            })
            .await?;
        Ok(Response::new(MergeBaseResponse {
            changeset_ids: bases.iter().map(|id| id.as_bytes().to_vec()).collect(),
        }))
    }

    async fn is_ancestor(
        &self,
        request: Request<IsAncestorRequest>,
    ) -> Result<Response<IsAncestorResponse>, Status> {
        let caller = caller(&request)?;
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let ancestor = parse_id(&req.ancestor_id, "ancestor_id")?;
        let descendant = parse_id(&req.descendant_id, "descendant_id")?;
        let is_ancestor = self
            .graph_query(deadline, move |store, deadline| {
                read::is_ancestor(store, &req.repo, ancestor, descendant, deadline)
            })
            .await?;
        Ok(Response::new(IsAncestorResponse { is_ancestor }))
    }

    async fn diff(&self, request: Request<DiffRequest>) -> Result<Response<DiffResponse>, Status> {
        let caller = caller(&request)?;
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let old = parse_id(&req.old_changeset_id, "old_changeset_id")?;
        let new = parse_id(&req.new_changeset_id, "new_changeset_id")?;
        let path = parse_optional_path(req.path)?;
        let after = parse_optional_path(req.after)?;
        // Held until the job ends, as for `Blame`.
        let slot = Arc::clone(&self.diffs)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let page = self
            .worker
            .run(move |store| {
                let _slot = slot;
                read::diff(&store, &req.repo, old, new, path.as_ref(), after.as_ref())
            })
            .await?;
        let entries = page
            .entries
            .into_iter()
            .map(|entry| {
                // The other path goes in `rename_from` on the added file,
                // and in `rename_to` on the deleted one.
                let (rename_from, rename_to, similarity) = match entry.rename {
                    Some(r) if entry.old.is_none() => {
                        (r.path.as_bytes().to_vec(), vec![], r.similarity)
                    }
                    Some(r) => (vec![], r.path.as_bytes().to_vec(), r.similarity),
                    None => Default::default(),
                };
                DiffEntry {
                    path: entry.path.as_bytes().to_vec(),
                    old: entry.old.map(file_state),
                    new: entry.new.map(file_state),
                    rename_from,
                    rename_to,
                    similarity,
                }
            })
            .collect();
        Ok(Response::new(DiffResponse {
            entries,
            next_after: path_bytes(page.next_after),
            renames_incomplete: page.renames_incomplete,
        }))
    }
}

/// An optional changeset ID on the wire: empty for `None`.
fn id_bytes(id: Option<ChangesetId>) -> Vec<u8> {
    id.map(|id| id.as_bytes().to_vec()).unwrap_or_default()
}

/// An optional path on the wire: empty for `None`.
fn path_bytes(path: Option<RepoPath>) -> Vec<u8> {
    path.map(|path| path.as_bytes().to_vec())
        .unwrap_or_default()
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
    use crate::db;
    use std::time::Instant;

    use scion_core::{Changeset, FileState, IdentityTime};
    use tonic::Code;

    use super::*;

    /// A service whose one principal may do everything.
    fn test_service(store: Store) -> ScionService {
        ScionService::new(Worker::new(store), Arc::new(Access::single("t")))
    }

    /// `message` as a request of that principal.
    fn as_default<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        let caller = Caller(scion_proto::auth::DEFAULT_PRINCIPAL.into());
        request.extensions_mut().insert(caller);
        request
    }

    /// A store whose repo `r` has one changeset, which puts `f` with
    /// `content`. Also returns the changeset and the blob.
    fn file_store(content: &[u8]) -> (tempfile::TempDir, Store, ChangesetId, BlobId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let blob = store.blobs().put(content).unwrap();
        let state = FileState::Regular {
            content: blob,
            executable: false,
        };
        let changeset = Changeset {
            parents: vec![],
            author: IdentityTime {
                name: b"t".to_vec(),
                email: b"t@x".to_vec(),
                unix_secs: 0,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: vec![],
            changes: [(RepoPath::new("f").unwrap(), Some(state))].into(),
        };
        let cs = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                c.insert_changeset(repo, &changeset)
            })
            .unwrap();
        (dir, store, cs, blob)
    }

    /// `parts` parts of `CHUNK_BYTES`, each filled with its own byte.
    fn content(parts: u8) -> Vec<u8> {
        (0..parts).flat_map(|i| vec![i; CHUNK_BYTES]).collect()
    }

    /// Starts [`send_file`] for `blob` with all of a budget of `budget`.
    /// Returns the task, the reply and the budget.
    async fn reply(
        store: Store,
        blob: BlobId,
        budget: usize,
        stall: Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<Part>,
        Arc<Semaphore>,
    ) {
        let reads = Arc::new(Semaphore::new(budget));
        let share = u32::try_from(budget).unwrap();
        let share = Arc::clone(&reads).acquire_many_owned(share).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        let task = tokio::spawn(send_file(Worker::new(store), blob, share, tx, stall, None));
        (task, rx, reads)
    }

    #[tokio::test]
    async fn a_read_file_reply_whose_client_stops_reading_ends_with_an_error() {
        let data = content(3);
        let (_dir, store, _, blob) = file_store(&data);
        let budget = read_share(data.len());
        let (task, mut rx, reads) = reply(store, blob, budget, Duration::from_millis(50)).await;

        // Nobody reads: the first part waits in the channel, and the second
        // stalls and ends the reply, which frees its share.
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("a stalled reply ends")
            .unwrap();
        assert_eq!(reads.available_permits(), budget, "the share is free");
        // A client that comes back gets the part and then the error, so it
        // never takes the file for a whole one.
        let first = rx.recv().await.unwrap().unwrap();
        assert!(first.data == data[..CHUNK_BYTES], "the first part");
        let status = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn the_last_part_holds_the_share_until_the_client_takes_it() {
        let data = content(1);
        let (_dir, store, _, blob) = file_store(&data);
        let budget = read_share(data.len());

        // A client that takes the part: the share comes back then.
        let stall = Duration::from_secs(60);
        let (task, mut rx, reads) = reply(store.clone(), blob, budget, stall).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(reads.available_permits(), 0, "the part is not taken yet");
        assert!(rx.recv().await.unwrap().unwrap().data == data, "the file");
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the reply ends once the client has the last part")
            .unwrap();
        assert_eq!(reads.available_permits(), budget);
        assert!(rx.recv().await.is_none());

        // A client that never takes it: the share comes back after the
        // stall, and the reply still ends whole.
        let stall = Duration::from_millis(50);
        let (task, mut rx, reads) = reply(store, blob, budget, stall).await;
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the reply ends after the stall")
            .unwrap();
        assert_eq!(reads.available_permits(), budget);
        assert!(rx.recv().await.unwrap().unwrap().data == data, "the file");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_damaged_blob_ends_the_reply_with_internal_before_its_last_part() {
        let data = content(3);
        for flip in [10, data.len() / 2, data.len() - 10] {
            let (_dir, store, _, blob) = file_store(&data);
            // A valid zstd frame of other content, under the blob's ID.
            let mut damaged = data.clone();
            damaged[flip] ^= 1;
            let packed = zstd::bulk::compress(&damaged, 3).unwrap();
            std::fs::write(store.blobs().location(blob).1, packed).unwrap();

            let budget = read_share(data.len());
            let stall = Duration::from_secs(60);
            let (task, mut rx, reads) = reply(store, blob, budget, stall).await;
            let mut parts = 0;
            let status = loop {
                match rx.recv().await.expect("the reply ends with an error") {
                    Ok(_) => parts += 1,
                    Err(status) => break status,
                }
            };
            assert_eq!(status.code(), Code::Internal, "bit {flip}: {status:?}");
            assert_eq!(parts, 2, "bit {flip}: every part but the last");
            assert!(rx.recv().await.is_none());
            task.await.unwrap();
            assert_eq!(reads.available_permits(), budget, "bit {flip}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_whose_call_is_dropped_stops_and_stays_pending() {
        // main goes root ─ B. The land's base is root, below the floor of a
        // new path index at B, so its plan walks back.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let blob = store.blobs().put(b"x").unwrap();
        let put = |parents, path: &str| Changeset {
            parents,
            author: IdentityTime {
                name: b"t".to_vec(),
                email: b"t@x".to_vec(),
                unix_secs: 0,
                tz_offset_minutes: 0,
            },
            committer: None,
            message: path.as_bytes().to_vec(),
            changes: [(
                RepoPath::new(path).unwrap(),
                Some(FileState::Regular {
                    content: blob,
                    executable: false,
                }),
            )]
            .into(),
        };
        let (b, cs) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                let root = c.insert_changeset(repo, &put(vec![], "a"))?;
                let b = c.insert_changeset(repo, &put(vec![root], "b"))?;
                assert!(c.cas_ref(repo, "main", None, b)?);
                Ok((b, c.insert_changeset(repo, &put(vec![root], "c"))?))
            })
            .unwrap();
        let service = Arc::new(test_service(store.clone()));
        let idle = || service.worker.permits.available_permits() == MAX_BLOCKING;

        // Hold the write connection, so the job waits at its first step.
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let blocker = std::thread::spawn({
            let store = store.clone();
            move || {
                store.read_unflushed(|_| {
                    held_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                    Ok(())
                })
            }
        });
        held_rx.recv().unwrap();
        let request = as_default(scion_proto::v1::LandRequest {
            repo: "r".into(),
            changeset_id: cs.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k".into(),
        });
        let call = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.land(request).await }
        });
        let started = Instant::now();
        while idle() {
            assert!(started.elapsed() < Duration::from_secs(10), "the job runs");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // The client goes away while the job runs. The job stops at its next
        // check and leaves the land pending.
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        go_tx.send(()).unwrap();
        blocker.join().unwrap().unwrap();
        while !idle() {
            assert!(started.elapsed() < Duration::from_secs(10), "the job ends");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let sql = "SELECT state FROM lands WHERE request_key = 'k'";
        let state: String = store
            .read(|c| Ok(c.query_row(sql, [], |row| row.get(0))?))
            .unwrap();
        assert_eq!(state, "pending");
        let main = store.read(|c| {
            let repo = c.repo_id("r")?;
            c.get_ref(repo, "main")
        });
        assert_eq!(main.unwrap().unwrap().target, b, "main did not move");
    }

    #[tokio::test]
    async fn a_read_file_call_waits_for_its_share_before_it_reads() {
        let data = content(2);
        let (_dir, store, cs, _) = file_store(&data);
        // Room for one reply of this file, not two.
        let service = Arc::new(ScionService {
            reads: Arc::new(Semaphore::new(read_share(data.len()))),
            ..test_service(store)
        });
        let request = move || {
            as_default(ReadFileRequest {
                repo: "r".into(),
                changeset_id: cs.as_bytes().to_vec(),
                path: b"f".to_vec(),
            })
        };
        let read_all = |mut reply: ReadFileStream| async move {
            let mut read = Vec::new();
            while let Some(part) = reply.next().await {
                read.extend(part.unwrap().data);
            }
            read
        };

        let first = service.read_file(request()).await.unwrap().into_inner();
        let second = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.read_file(request()).await.unwrap().into_inner() }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!second.is_finished(), "the second call waits for budget");
        assert!(read_all(first).await == data, "the first reply");
        let second = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .expect("the first reply freed its share")
            .unwrap();
        assert!(read_all(second).await == data, "the second reply");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn at_most_max_graph_queries_run_at_once() {
        // root ─ a, and root ─ b.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let ids = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                db::testing::insert_graph(c, repo, &[vec![], vec![0], vec![0]])
            })
            .unwrap();
        let bytes = |i: usize| ids[i].as_bytes().to_vec();
        let service = Arc::new(test_service(store.clone()));
        let busy = || MAX_BLOCKING - service.worker.permits.available_permits();

        // Hold every read connection, so each job waits at its first read.
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let go = Arc::new(std::sync::Barrier::new(crate::store::READERS + 1));
        let blockers: Vec<_> = (0..crate::store::READERS)
            .map(|_| {
                let (store, held_tx, go) = (store.clone(), held_tx.clone(), Arc::clone(&go));
                std::thread::spawn(move || {
                    store.read(|_| {
                        held_tx.send(()).unwrap();
                        go.wait();
                        Ok(())
                    })
                })
            })
            .collect();
        for _ in &blockers {
            held_rx.recv().unwrap();
        }
        // One call more than the limit, of each kind.
        let calls: Vec<_> = (0..=MAX_GRAPH_QUERIES)
            .map(|i| {
                let service = Arc::clone(&service);
                let (root, a, b) = (bytes(0), bytes(1), bytes(2));
                tokio::spawn(async move {
                    if i % 3 == 2 {
                        // `a` puts `f1`.
                        let request = as_default(LogRequest {
                            repo: "r".into(),
                            changeset_id: a.clone(),
                            limit: 10,
                            path: "f1".into(),
                        });
                        let reply = service.log(request).await.unwrap().into_inner();
                        let ids: Vec<Vec<u8>> =
                            reply.entries.into_iter().map(|e| e.changeset_id).collect();
                        ids == [a]
                    } else if i % 3 == 0 {
                        let request = as_default(MergeBaseRequest {
                            repo: "r".into(),
                            a_changeset_id: a,
                            b_changeset_id: b,
                        });
                        let reply = service.merge_base(request).await.unwrap();
                        reply.into_inner().changeset_ids == [root]
                    } else {
                        let request = as_default(IsAncestorRequest {
                            repo: "r".into(),
                            ancestor_id: root,
                            descendant_id: a,
                        });
                        let reply = service.is_ancestor(request).await.unwrap();
                        reply.into_inner().is_ancestor
                    }
                })
            })
            .collect();
        let started = Instant::now();
        while busy() < MAX_GRAPH_QUERIES {
            assert!(started.elapsed() < Duration::from_secs(10), "the jobs run");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(busy(), MAX_GRAPH_QUERIES, "the last call waits for a slot");

        go.wait();
        for blocker in blockers {
            blocker.join().unwrap().unwrap();
        }
        for call in calls {
            let answer = tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("every call ends")
                .unwrap();
            assert!(answer, "every call answers");
        }
    }

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

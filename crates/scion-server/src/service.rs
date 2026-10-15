//! The gRPC front end. Parses requests, runs the work on a blocking thread
//! (`SQLite` and the object folder are synchronous), and maps errors to status
//! codes.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scion_core::{BlobId, ChangesetId, RepoPath};
use scion_proto::v1::scion_server::{Scion, ScionServer};
use scion_proto::v1::{
    BlameChangeset, BlameRange, BlameRequest, BlameResponse, CreateChangesetRequest,
    CreateChangesetResponse, GetChangesetRequest, GetChangesetResponse, GetLandRequest,
    LandResponse, ListDirRequest, ListDirResponse, LogEntry, LogRequest, LogResponse,
    PutBlobRequest, PutBlobResponse, ReadFileRequest, ReadFileResponse, ResolveRefRequest,
    ResolveRefResponse,
};
use scion_proto::{CHUNK_BYTES, MAX_MESSAGE_BYTES};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status, Streaming};

use crate::auth::{Auth, Caller};
use crate::error::Result;
use crate::land::{self, LandOutcome, LandState, Lands};
use crate::objects::ObjectsService;
use crate::store::{MAX_OBJECT_BYTES, READER_BYTES, Store};
use crate::{blame, db, names, read, write};

/// Most changeset bytes in one `Log` reply; the client pages for more.
const LOG_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES / 2;
/// Most store operations running at once.
const MAX_BLOCKING: usize = 256;
/// Most uploads in flight at once. Each buffers up to `MAX_OBJECT_BYTES`.
const MAX_UPLOADS: usize = 16;
/// Most `Blame` calls running at once. One call can diff for seconds, so
/// blames never take more than this many of the `MAX_BLOCKING` permits that
/// writes and lands share.
const MAX_BLAMES: usize = 4;
/// Longest a request may take before the server gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
/// Most bytes that all `ReadFile` replies together hold: the parts read and
/// not yet sent, and the readers of files of more than one part.
const READ_BUDGET_BYTES: usize = 256 << 20;
/// A `ReadFile` reply ends when its client takes no part for this long, so a
/// client that stops reading does not hold memory and budget.
// SHORTCUT: the reply ends, but HTTP/2 keeps the part it was sending until
// the connection closes. Upgrade (issue #42): a response body timeout that
// resets the stream.
const READ_STALL_TIMEOUT: Duration = Duration::from_secs(120);

// Any one reply fits in the budget, and the budget counts in u32 permits.
const _: () = assert!(read_share(MAX_OBJECT_BYTES) <= READ_BUDGET_BYTES);
const _: () = assert!(READ_BUDGET_BYTES <= u32::MAX as usize);

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
    /// Bounds buffered uploads, so their memory stays under
    /// `MAX_UPLOADS * MAX_OBJECT_BYTES`.
    uploads: Arc<Semaphore>,
    /// Bounds blames running at once (`MAX_BLAMES`).
    blames: Arc<Semaphore>,
    /// `READ_BUDGET_BYTES` permits, one per byte, shared by all `ReadFile`
    /// replies.
    reads: Arc<Semaphore>,
}

impl ScionService {
    fn new(worker: Worker) -> Self {
        Self {
            worker,
            lands: Arc::default(),
            uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
            blames: Arc::new(Semaphore::new(MAX_BLAMES)),
            reads: Arc::new(Semaphore::new(READ_BUDGET_BYTES)),
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
/// them, and holds `share` until it stops. A part goes into the channel only
/// when the channel is empty, so its second slot is always free for the
/// error that ends a reply early: a store error, no part taken within
/// `stall`, or the `deadline`. A client never takes a short file for a whole
/// one.
async fn send_file(
    worker: Worker,
    blob: BlobId,
    share: OwnedSemaphorePermit,
    tx: mpsc::Sender<Part>,
    stall: Duration,
    deadline: Instant,
) {
    let _share = share;
    let fail = |status: Status| {
        // Fails only if the client has gone.
        let _ = tx.try_send(Err(status));
    };
    let late = || Status::deadline_exceeded("the reply ran past its deadline");
    let mut open = None;
    loop {
        if Instant::now() >= deadline {
            return fail(late());
        }
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
            let wait = deadline.min(Instant::now() + stall);
            match tokio::time::timeout_at(wait.into(), tx.reserve_many(2)).await {
                Ok(Ok(mut slots)) => {
                    if let Some(slot) = slots.next() {
                        slot.send(Ok(ReadFileResponse { data }));
                    }
                }
                Ok(Err(_)) => return,
                Err(_) if Instant::now() >= deadline => return fail(late()),
                Err(_) => {
                    let detail = format!("the client took no data for {stall:?}");
                    return fail(Status::deadline_exceeded(detail));
                }
            }
        }
        if open.is_none() {
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
        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty upload"))??;
        check_name("repo", &first.repo)?;
        let repo = first.repo;
        // Check the repo before buffering any more data.
        let lookup = repo.clone();
        self.worker
            .run(move |store| store.read(|c| db::repo_id(c, &lookup)))
            .await?;
        let mut data = first.data;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if !chunk.repo.is_empty() {
                return Err(Status::invalid_argument(
                    "only the first PutBlob message names the repo",
                ));
            }
            if data.len() + chunk.data.len() > MAX_OBJECT_BYTES {
                return Err(Status::invalid_argument(format!(
                    "blob is larger than {MAX_OBJECT_BYTES} bytes"
                )));
            }
            data.extend_from_slice(&chunk.data);
        }
        let id = self
            .worker
            .run(move |store| write::put_blob(&store, &repo, &data))
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
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let req = request.into_inner();
        check_name("repo", &req.repo)?;
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
        // The asserts at the top keep the share within u32.
        let share = u32::try_from(read_share(size)).unwrap_or(u32::MAX);
        let share = Arc::clone(&self.reads)
            .acquire_many_owned(share)
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(send_file(
            self.worker.clone(),
            blob,
            share,
            tx,
            READ_STALL_TIMEOUT,
            deadline,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
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
}

/// An optional changeset ID on the wire: empty for `None`.
fn id_bytes(id: Option<ChangesetId>) -> Vec<u8> {
    id.map(|id| id.as_bytes().to_vec()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use scion_core::{Changeset, FileState, IdentityTime};
    use tonic::Code;

    use super::*;

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
            .write(|c| db::insert_changeset(c, db::create_repo(c, "r")?, &changeset))
            .unwrap();
        (dir, store, cs, blob)
    }

    /// `parts` parts of `CHUNK_BYTES`, each filled with its own byte.
    fn content(parts: u8) -> Vec<u8> {
        (0..parts).flat_map(|i| vec![i; CHUNK_BYTES]).collect()
    }

    #[tokio::test]
    async fn a_read_file_reply_whose_client_stops_reading_ends_with_an_error() {
        let data = content(3);
        let (_dir, store, _, blob) = file_store(&data);
        let budget = read_share(data.len());
        let reads = Arc::new(Semaphore::new(budget));
        let share = u32::try_from(budget).unwrap();
        let share = Arc::clone(&reads).acquire_many_owned(share).await.unwrap();
        let (tx, mut rx) = mpsc::channel(2);
        let stall = Duration::from_millis(50);
        let deadline = Instant::now() + Duration::from_secs(60);
        let task = tokio::spawn(send_file(
            Worker::new(store),
            blob,
            share,
            tx,
            stall,
            deadline,
        ));

        // Nobody reads: the first part waits in the channel, and the second
        // stalls and ends the reply.
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
    async fn a_read_file_reply_ends_at_its_deadline() {
        let data = content(3);
        let (_dir, store, _, blob) = file_store(&data);
        let worker = Worker::new(store);
        let reads = Arc::new(Semaphore::new(2));
        let stall = Duration::from_secs(60);
        let reply = |deadline| {
            let share = Arc::clone(&reads).try_acquire_owned().unwrap();
            let (tx, rx) = mpsc::channel(2);
            let task = send_file(worker.clone(), blob, share, tx, stall, deadline);
            (tokio::spawn(task), rx)
        };
        let deadline_exceeded = |part: Option<Part>| {
            let status = part.unwrap().unwrap_err();
            assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
        };

        // Past its deadline, a reply sends nothing, though its client reads.
        let (_, mut rx) = reply(Instant::now());
        deadline_exceeded(rx.recv().await);
        assert!(rx.recv().await.is_none());

        // A client that takes nothing: the reply ends at its deadline, long
        // before the stall limit.
        let (task, mut rx) = reply(Instant::now() + Duration::from_millis(200));
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the reply ends at its deadline")
            .unwrap();
        let first = rx.recv().await.unwrap().unwrap();
        assert!(first.data == data[..CHUNK_BYTES], "the first part");
        deadline_exceeded(rx.recv().await);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_read_file_call_waits_for_its_share_before_it_reads() {
        let data = content(2);
        let (_dir, store, cs, _) = file_store(&data);
        // Room for one reply of this file, not two.
        let service = Arc::new(ScionService {
            reads: Arc::new(Semaphore::new(read_share(data.len()))),
            ..ScionService::new(Worker::new(store))
        });
        let request = move || {
            Request::new(ReadFileRequest {
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

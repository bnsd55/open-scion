//! The gRPC front end. Parses requests, runs the work on a blocking thread
//! (`SQLite` and the object folder are synchronous), and maps errors to status
//! codes.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use scion_core::{BlobId, Changeset, ChangesetId, FileState, RepoPath};
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
use scion_proto::{Bytes, CHUNK_BYTES, MAX_MESSAGE_BYTES};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;

use crate::metrics::{Metrics, MetricsLayer};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::Instrument as _;

use crate::access::Access;
use crate::auth::Caller;
use crate::backend::Backend;
use crate::deadline::Deadline;
use crate::error::Result;
use crate::land::{self, LandOutcome, LandState, Lands};
use crate::meta::{Meta as _, MetaTx as _};
use crate::objects::ObjectsService;
use crate::quotas::{Call, Quotas};
use crate::store::{MAX_OBJECT_BYTES, READER_BYTES};
use crate::tickets::Keys;
use crate::{blame, names, read, views, write};

/// Most changeset bytes in one `Log` reply; the client pages for more.
/// Most store operations running at once.
const MAX_BLOCKING: usize = 256;
/// Most uploads read at once. Each holds one request message in memory, of up
/// to `MAX_MESSAGE_BYTES`, and a spool file of up to `MAX_OBJECT_BYTES`.
const MAX_UPLOADS: usize = 16;
/// Most `Blame` calls running at once. One call can diff for seconds, and
/// a call that waits for its memory budget holds one of the `MAX_BLOCKING`
/// permits that writes and lands share for the rest of its deadline. Four
/// of the 256 permits can never starve a land or a read; a blame queue
/// beyond this waits here, in async code, for a slot to free.
const MAX_BLAMES: usize = 4;
/// The bytes of memory that all `Blame` calls share: a call takes the bytes
/// of every version it reads, and `memory::BYTES_PER_LINE` per line of the
/// blamed file, so together they hold no more than this.
const BLAME_MEMORY_BUDGET: usize = 1 << 30;
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

/// How often the stale-land sweep runs after the first one.
const SWEEP_PERIOD: Duration = Duration::from_secs(60 * 60);

/// How often the tx-age gauge is refreshed: a scrape reads the gauge, and
/// an age that grows without a pool event must still show.
const TX_AGE_PERIOD: Duration = Duration::from_secs(5);

/// Keeps the `scion_pg_tx_age_seconds` gauge current: the age of the
/// oldest open Postgres read transaction, sampled every [`TX_AGE_PERIOD`]
/// until `stop` resolves. On SQLite the sample is always zero.
async fn pg_tx_age_sample(store: Backend, metrics: Arc<Metrics>, mut stop: watch::Receiver<()>) {
    let mut tick = tokio::time::interval(TX_AGE_PERIOD);
    loop {
        let store = store.clone();
        let age = tokio::task::spawn_blocking(move || store.pg_tx_age()).await;
        match age {
            Ok(age) => metrics.pg_tx_age(age),
            Err(e) => tracing::warn!(error = %e, "the tx-age sample failed"),
        }
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => break,
        }
    }
}

/// Runs the stale-land sweep at once, then once per [`SWEEP_PERIOD`], until
/// `stop` resolves. The first sweep settles what a restart left pending
/// before the first client asks.
async fn sweeps(worker: Worker, lands: Arc<Lands>, stale_secs: i64, mut stop: watch::Receiver<()>) {
    loop {
        let (lands, store) = (Arc::clone(&lands), worker.store());
        let rejected =
            tokio::task::spawn_blocking(move || lands.sweep_stale(&store, stale_secs)).await;
        match rejected {
            Ok(Ok(0)) => {}
            Ok(Ok(n)) => tracing::info!(rejected = n, "rejected stale pending lands"),
            Ok(Err(e)) => tracing::warn!(error = %e, "the stale-land sweep failed"),
            Err(e) => tracing::warn!(error = %e, "the stale-land sweep task failed"),
        }
        tokio::select! {
            () = tokio::time::sleep(SWEEP_PERIOD) => {}
            _ = stop.changed() => break,
        }
    }
}

// A connection that is asked to close keeps its calls for at least as long
// as the longest call takes.
const _: () = assert!(REQUEST_TIMEOUT.as_secs() <= listen::CONNECTION_GRACE.as_secs());

// Any one reply fits in the budget, and the budget counts in u32 permits.
const _: () = assert!(read_share(MAX_OBJECT_BYTES) <= READ_BUDGET_BYTES);
const _: () = assert!(READ_BUDGET_BYTES <= u32::MAX as usize);

/// What a server serves with, besides the data directory and the access
/// rules: its limits, its per-principal quotas, its TLS, and its metrics.
pub struct ServeOptions {
    /// The connection and stream caps of `listen`.
    pub limits: Limits,
    /// What one principal may hold at once.
    pub quotas: Quotas,
    /// The most pending lands one principal may hold.
    pub pending_lands: Option<usize>,
    /// A land pending longer than this is rejected by the sweep.
    pub land_stale_secs: i64,
    /// With TLS, every connection is a TLS connection, whose handshake
    /// `listen` ends within its timeout.
    pub tls: Option<ServerTls>,
    /// The registry that counts every call, whatever `--metrics-listen`
    /// exports. A server without the export still counts, so the flag can
    /// turn on without a restart.
    pub metrics: Arc<Metrics>,
}

/// Serves the API on `listener` within `options`, until `shutdown` resolves,
/// to the tokens and with the rules of `access`: an [`Access`], or one token
/// (`&str`) that may do everything. `options.quotas` bounds what one
/// principal may hold at once, and each principal at most
/// `pending_lands` lands pending. A land that has been pending for longer
/// than `land_stale_secs` is rejected by the sweep that runs at start and
/// then once per hour. With `options.tls`, every connection is a TLS
/// connection, whose handshake `listen` ends within its timeout.
///
/// # Errors
///
/// Transport errors.
pub async fn serve(
    store: Backend,
    listener: TcpListener,
    access: impl Into<Access>,
    options: ServeOptions,
    tickets: Option<Arc<Keys>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), tonic::transport::Error> {
    let ServeOptions {
        limits,
        quotas,
        pending_lands,
        land_stale_secs,
        tls,
        metrics,
    } = options;
    let access = Arc::new(access.into());
    let auth = access.auth();
    let fence = store.fence_flag();
    // The Postgres reader pool records into the registry: the wait for a
    // reader, the busy readers, and the tx age, which this task keeps
    // fresh. On SQLite this is a no-op.
    store.set_metrics(Arc::clone(&metrics));
    let worker = Worker::new(store);
    let lands = Arc::new(Lands::new(pending_lands, Some(Arc::clone(&metrics))));
    let (stop_tx, stop_rx) = watch::channel(());
    // The fence of a Postgres server that lost its database to another
    // process: it resolves the same shutdown the operator's signal does,
    // so the health service answers NOT_SERVING and tonic finishes the
    // open calls; a short grace later the process exits, and a supervisor
    // restart either takes the lock back or is refused at open. SQLite
    // has no fence (None), so this task never runs.
    let fence_stop = fence.clone().map(|fence| (fence, stop_tx.clone()));
    tokio::spawn(async move {
        shutdown.await;
        let _ = stop_tx.send(());
    });
    tokio::spawn(pg_tx_age_sample(
        worker.store(),
        Arc::clone(&metrics),
        stop_rx.clone(),
    ));
    let shutdown = {
        let mut stopped = stop_rx.clone();
        async move {
            let _ = stopped.changed().await;
        }
    };
    if let Some((fence, stop_tx)) = fence_stop {
        tokio::spawn(async move {
            while !fence.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            tracing::error!(
                "this server process lost the database to another server \
                 process; it stops serving and exits"
            );
            let _ = stop_tx.send(());
            tokio::time::sleep(Duration::from_secs(2)).await;
            std::process::exit(70);
        });
    }
    tokio::spawn(sweeps(
        worker.clone(),
        Arc::clone(&lands),
        land_stale_secs,
        stop_rx,
    ));
    // Outside the access rules, so a refused call counts too. The layer
    // only wraps replies in a counting body; the handlers stay as they are.
    let scion = MetricsLayer::new(Arc::clone(&metrics)).layer(InterceptedService::new(
        ScionService::new(
            worker.clone(),
            Arc::clone(&access),
            lands,
            quotas.clone(),
            Arc::clone(&metrics),
        )
        .into_server(),
        auth.clone(),
    ));
    // No token interceptor on `objects`: `GetObjects` authenticates by
    // ticket alone, and the service's other calls check the token
    // themselves. So a tier needs no token of the server to fetch objects.
    // The counting layer wraps it all the same, outside the ticket checks.
    let objects = MetricsLayer::new(Arc::clone(&metrics))
        .layer(ObjectsService::new(worker, access, quotas, tickets, metrics).into_server());
    // The health service answers `grpc.health.v1.Check` and `Watch` on the
    // same port, without a token: a probe carries none. The store opens
    // before this call, so the server starts serving. A fence trip
    // resolves the shutdown, so the names flip to NOT_SERVING on the
    // service's own path, and the process exits a grace later.
    let (health, shutdown) =
        listen::health(&["scion.v1.Scion", "scion.v1.Objects"], shutdown).await;
    let router = |server: Server| {
        server
            .timeout(REQUEST_TIMEOUT)
            .add_service(scion)
            .add_service(objects)
            .add_service(health)
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
    /// What one principal may hold at once.
    quotas: Quotas,
    /// The registry that counts the view derivations.
    metrics: Arc<Metrics>,
    /// Bounds uploads read at once (`MAX_UPLOADS`): their messages in memory
    /// and their spool files in `tmp/`.
    uploads: Arc<Semaphore>,
    /// Bounds blames running at once (`MAX_BLAMES`).
    blames: Arc<Semaphore>,
    /// The memory that blames share (`BLAME_MEMORY_BUDGET`).
    blame_memory: crate::memory::Bytes,
    /// Bounds graph queries running at once (`MAX_GRAPH_QUERIES`).
    graph_queries: Arc<Semaphore>,
    /// Bounds diffs running at once (`MAX_DIFFS`).
    diffs: Arc<Semaphore>,
    /// `READ_BUDGET_BYTES` permits, one per byte, shared by all `ReadFile`
    /// replies.
    reads: Arc<Semaphore>,
}

impl ScionService {
    /// Sends `data` in parts of up to `CHUNK_BYTES`, as `ReadFile` sends a
    /// file. Holds the whole bytes, so the share is their size.
    async fn stream_whole<T: Send + 'static>(
        &self,
        call: Call,
        data: Vec<u8>,
        stalled: Option<scion_proto::listen::Stalled>,
        make: fn(Vec<u8>) -> T,
    ) -> Result<Response<ReceiverStream<Part<T>>>, Status> {
        // Taken after the bytes are read, so a call that waits holds a
        // budget share for memory that another reply may need sooner.
        let share = Arc::clone(&self.reads)
            .acquire_many_owned(whole_share(data.len()))
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(send_parts(
            Parts::Whole {
                data: data.into(),
                offset: 0,
            },
            share,
            call,
            tx,
            READ_STALL_TIMEOUT,
            stalled,
            make,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    fn new(
        worker: Worker,
        access: Arc<Access>,
        lands: Arc<Lands>,
        quotas: Quotas,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            worker,
            access,
            lands,
            quotas,
            metrics,
            uploads: Arc::new(Semaphore::new(MAX_UPLOADS)),
            blames: Arc::new(Semaphore::new(MAX_BLAMES)),
            blame_memory: crate::memory::Bytes::new(BLAME_MEMORY_BUDGET),
            graph_queries: Arc::new(Semaphore::new(MAX_GRAPH_QUERIES)),
            diffs: Arc::new(Semaphore::new(MAX_DIFFS)),
            reads: Arc::new(Semaphore::new(READ_BUDGET_BYTES)),
        }
    }

    fn into_server(self) -> ScionServer<Self> {
        into_server(self)
    }

    /// Runs a `MergeBase`, `IsAncestor` or `Log` with a path: at most
    /// `MAX_GRAPH_QUERIES` at once, each until its call ends or its
    /// `deadline` passes.
    async fn graph_query<T, F>(&self, deadline: Deadline, query: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Backend, &Deadline) -> Result<T> + Send + 'static,
    {
        run_graph_query(&self.worker, &self.graph_queries, deadline, query).await
    }
}

/// Wraps a service into the gRPC server, with the message limits of the
/// protocol: a message over them fails the call on whichever side finds it
/// first (section 2 of protocol-v1).
///
/// SHORTCUT: a unary reply's bytes stay in HTTP/2 uncounted until the
/// client reads them or the connection closes; the largest one left is
/// `Diff` (about 120 KB), so a client that never reads holds at most its
/// connections x streams of those. Every reply that can grow with its
/// input streams; a new unary RPC must stream, or bound its reply, before
/// it ships.
pub fn into_server<S: Scion + Send + 'static>(service: S) -> ScionServer<S> {
    ScionServer::new(service)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
}

/// Runs synchronous store work off the async runtime. Clones share one bound
/// on the work in flight, so both services queue behind the same limit.
#[derive(Clone)]
pub(crate) struct Worker {
    store: Backend,
    /// Bounds blocking work, so a flood of requests queues instead of
    /// spawning unbounded threads.
    permits: Arc<Semaphore>,
}

impl Worker {
    pub(crate) fn new(store: Backend) -> Self {
        Self {
            store,
            permits: Arc::new(Semaphore::new(MAX_BLOCKING)),
        }
    }

    /// A handle to the store, for work outside the permit queue, such as the
    /// stale-land sweep.
    pub(crate) fn store(&self) -> Backend {
        self.store.clone()
    }

    /// Runs `f` on a blocking thread once a permit is free.
    pub(crate) async fn run<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(Backend) -> Result<T> + Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let store = self.store.clone();
        // The job runs on another thread, so it takes the call's span with
        // it: a refusal or a failure that the job decides logs with the
        // call's fields.
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _span = span.enter();
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

/// The refusal of every RPC that a partial reader may not use, uniform for
/// every principal with folder read rules: the feature is off for such a
/// caller, with no per-object difference. `check_reader` first, so a repo
/// that the caller may not see answers as a missing one.
pub(crate) fn check_whole_reader(
    access: &Access,
    caller: &Caller,
    repo: &str,
) -> Result<(), Status> {
    check_reader(access, caller, repo)?;
    if access.partial(caller, repo) {
        return Err(Status::unimplemented(
            "not available to a principal with folder read rules",
        ));
    }
    Ok(())
}

/// `ResolveRef`, served by both services.
pub(crate) async fn resolve_ref(
    worker: &Worker,
    access: &Access,
    quotas: &Quotas,
    request: Request<ResolveRefRequest>,
) -> Result<ResolveRefResponse, Status> {
    let caller = caller(&request)?;
    let _call = quotas.enter(&caller).map_err(Status::from)?;
    let ResolveRefRequest { repo, name } = request.into_inner();
    check_reader(access, &caller, &repo)?;
    check_name("ref", &name)?;
    let id = worker
        .run(move |store| store.resolve_ref(&repo, &name))
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
    let caller = request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("no caller"))?;
    // The call's span gets the principal once. It shows on every line that
    // the call makes.
    tracing::Span::current().record("principal", caller.0.as_str());
    Ok(caller)
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

/// A part of a streamed reply, or the error that ends it early.
type Part<T> = Result<T, Status>;

type ReadFileStream = ReceiverStream<Part<ReadFileResponse>>;

type GetChangesetStream = ReceiverStream<Part<GetChangesetResponse>>;

type ListDirStream = ReceiverStream<Part<ListDirResponse>>;

type LogStream = ReceiverStream<Part<LogResponse>>;

type BlameStream = ReceiverStream<Part<BlameResponse>>;

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

/// The share of `READ_BUDGET_BYTES` that a reply holds for `size` bytes that
/// are all in memory at once, as a `ListDir` or `GetChangeset` reply's object
/// is. `ReadFile` reads its blob part by part, but these replies have the
/// whole object in one buffer, so the share is its whole size.
// SHORTCUT: a reply holds its object even when its share is capped at the
// budget, so a directory whose bytes are over the budget holds what the
// budget cannot pay for. The budget bounds how many such replies run at
// once, not their size. Cap the encoded size if that ever matters.
fn whole_share(size: usize) -> u32 {
    u32::try_from(size.clamp(1, READ_BUDGET_BYTES)).unwrap_or(u32::MAX)
}

/// Where a reply's parts come from.
enum Parts {
    /// Parts read from a blob, one job each, so a long file is never all in
    /// memory. The reader lives between parts.
    Blob {
        worker: Worker,
        blob: BlobId,
        open: Option<Box<crate::store::BlobReader>>,
        /// The reader has ended: no part is left, and the next call ends the
        /// reply without opening the blob again.
        ended: bool,
    },
    /// Parts copied out of one in-memory buffer, as a `ListDir` or
    /// `GetChangeset` reply has. Each part is its own buffer, so a part that
    /// waits for a client that stopped reading never keeps the rest.
    Whole { data: Bytes, offset: usize },
}

impl Parts {
    /// Reads the next part of up to `CHUNK_BYTES`, or `None` after the last.
    async fn next_part(&mut self) -> Result<Option<Vec<u8>>, Status> {
        match self {
            Parts::Blob {
                worker,
                blob,
                open,
                ended,
            } => {
                if *ended {
                    return Ok(None);
                }
                let blob = *blob;
                let read = {
                    let open = open.take();
                    worker
                        .run(move |store| {
                            let mut reader = match open {
                                Some(reader) => *reader,
                                None => store.blobs().reader(blob)?,
                            };
                            let part = reader.read_part(CHUNK_BYTES)?;
                            // A reader that has ended has freed its decoder.
                            Ok((part, (!reader.ended()).then_some(reader)))
                        })
                        .await?
                };
                *ended = read.1.is_none();
                *open = read.1.map(Box::new);
                Ok(read.0)
            }
            Parts::Whole { data, offset } => {
                let end = (*offset + CHUNK_BYTES).min(data.len());
                let part = if *offset >= data.len() {
                    None
                } else {
                    Some(data[*offset..end].to_vec())
                };
                *offset = end;
                Ok(part)
            }
        }
    }
}

/// Sends `parts` to `tx` in parts of up to `CHUNK_BYTES`, as the client takes
/// them, and wraps each in `make`. Holds `share` until the client has taken
/// the last part, or until no part is taken within `stall`. A part goes into
/// the channel only when the channel is empty, so its second slot is always
/// free for the error that ends a reply early: a store error, or the stall.
/// A client never takes a short reply for a whole one.
/// Runs a graph query on `permits`, the way [`ScionService::graph_query`]
/// does, for a page of a streamed `Log` reply. The query checks the
/// deadline at each read: a request timeout, or a client that cancels,
/// drops this call but not a job that is running. The guard ends the
/// deadline when this call ends or is dropped. The slot is taken before
/// the worker permit, and held until the job ends, also when the client
/// leaves first.
async fn run_graph_query<T, F>(
    worker: &Worker,
    permits: &Arc<Semaphore>,
    deadline: Deadline,
    query: F,
) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce(&Backend, &Deadline) -> Result<T> + Send + 'static,
{
    // The query checks it at each read: a request timeout, or a client
    // that cancels, drops this call but not a job that is running. The
    // guard ends the deadline when this call ends or is dropped.
    let _call = deadline.guard();
    let slot = Arc::clone(permits)
        .acquire_owned()
        .await
        .map_err(|_| Status::unavailable("server is shutting down"))?;
    worker
        .run(move |store| {
            let _slot = slot;
            query(&store, &deadline)
        })
        .await
}

/// What a streamed `Log` reply still has to walk: where the last page
/// stopped, how many entries the call still owes, and the bounds of the
/// next page.
struct LogWalk {
    worker: Worker,
    repo: String,
    graph_queries: Arc<Semaphore>,
    /// The request slot, held until the reply ends.
    _call: Call,
    next: Option<ChangesetId>,
    left: usize,
    path: Option<RepoPath>,
    deadline: Deadline,
    stalled: Option<scion_proto::listen::Stalled>,
}

/// Splits a fetched page into parts: groups of entries whose encoded bytes
/// stay within [`CHUNK_BYTES`], one part each. The last part carries where
/// the walk continues; the walk stops, with the rest of the page unsent, at
/// the `limit` of the call.
fn log_page_parts(entries: Vec<(ChangesetId, Changeset)>, walk: &mut LogWalk) -> Vec<LogResponse> {
    let mut parts = Vec::new();
    let mut group: Vec<LogEntry> = Vec::new();
    let mut bytes = 0usize;
    for (id, changeset) in entries {
        if walk.left == 0 {
            // Unreachable while `log_parts` fetches every page with
            // `limit = walk.left.min(MAX_SCAN)`: a page then never holds
            // more entries than the call still owes, so the page ends
            // before the call's credit does. If a page ever did hold more,
            // this entry is the first uncovered one, and the next call
            // starts here.
            debug_assert!(false, "a page held more than the call still owes");
            break;
        }
        let body = changeset.encode();
        if !group.is_empty() && bytes + body.len() > CHUNK_BYTES {
            bytes = 0;
            parts.push(LogResponse {
                entries: std::mem::take(&mut group),
                next_changeset_id: Vec::new(),
            });
        }
        bytes += body.len();
        walk.left -= 1;
        group.push(LogEntry {
            changeset_id: id.as_bytes().to_vec(),
            changeset: body,
        });
    }
    parts.push(LogResponse {
        entries: group,
        next_changeset_id: id_bytes(walk.next),
    });
    parts
}

/// What waiting for the client got the reply's producer.
enum Taken<'a, T> {
    /// Two free slots of the reply's channel: the client has taken the
    /// parts before, so one slot stays free for the error that ends the
    /// reply early.
    Slots(tokio::sync::mpsc::PermitIterator<'a, Part<T>>),
    /// The client has gone with the connection.
    Gone,
    /// The client took nothing for `stall`: the parts of the reply stay in
    /// HTTP/2 until the stream ends or the connection closes, and the
    /// connection counts them.
    Stalled,
}

/// Waits `stall` for two free slots of the reply's channel, as
/// [`send_parts`] and [`log_parts`] do.
async fn reserve<'a, T>(
    tx: &'a mpsc::Sender<Part<T>>,
    stall: Duration,
    stalled: Option<&'a scion_proto::listen::Stalled>,
) -> Taken<'a, T> {
    match tokio::time::timeout(stall, tx.reserve_many(2)).await {
        Ok(Ok(slots)) => Taken::Slots(slots),
        // The client has gone with the connection.
        Ok(Err(_)) => Taken::Gone,
        Err(_) => {
            if let Some(stalled) = stalled {
                stalled.add(scion_proto::reply::STALLED_REPLY_BYTES);
            }
            Taken::Stalled
        }
    }
}

/// What a streamed `Blame` reply sends: the ranges of the walk and the
/// changesets they name, where the walk continues, and the state the reply
/// holds until its client takes it.
struct BlameSend {
    ranges: Vec<BlameRange>,
    changesets: Vec<BlameChangeset>,
    cursor: Vec<u8>,
    /// The request slot, held until the reply ends.
    _call: Call,
    stalled: Option<scion_proto::listen::Stalled>,
}

/// Splits a computed blame into parts: groups of ranges whose encoded
/// bytes, with the changesets they name that no earlier part carried, stay
/// within [`CHUNK_BYTES`]. The last part carries where the walk continues.
fn blame_page_parts(send: &mut BlameSend) -> Vec<BlameResponse> {
    // Where each named changeset sits, so a range names its changeset's
    // part: the first part that names it carries it.
    let mut at: HashMap<Vec<u8>, usize> = HashMap::with_capacity(send.changesets.len());
    for (i, changeset) in send.changesets.iter().enumerate() {
        at.insert(changeset.changeset_id.clone(), i);
    }
    let mut carried = vec![false; send.changesets.len()];
    let mut parts = Vec::new();
    let mut ranges: Vec<BlameRange> = Vec::new();
    let mut changesets: Vec<BlameChangeset> = Vec::new();
    let mut bytes = 0usize;
    for range in std::mem::take(&mut send.ranges) {
        let range_bytes = range.encoded_len();
        let named = at.get(&range.changeset_id).copied();
        let mut new_bytes = 0usize;
        if let Some(i) = named.filter(|i| !carried[*i]) {
            new_bytes = send.changesets[i].encoded_len();
        }
        if !ranges.is_empty() && bytes + range_bytes + new_bytes > CHUNK_BYTES {
            bytes = 0;
            parts.push(BlameResponse {
                ranges: std::mem::take(&mut ranges),
                changesets: std::mem::take(&mut changesets),
                next_cursor: Vec::new(),
            });
        }
        bytes += range_bytes + new_bytes;
        if let Some(i) = named.filter(|i| !carried[*i]) {
            changesets.push(std::mem::take(&mut send.changesets[i]));
            carried[i] = true;
        }
        ranges.push(range);
    }
    parts.push(BlameResponse {
        ranges,
        changesets,
        next_cursor: std::mem::take(&mut send.cursor),
    });
    parts
}

/// Streams a `Blame` reply: sends the parts of the computed blame, until
/// the client has them all or stops taking parts. A stall counts
/// [`STALLED_REPLY_BYTES`] on the connection, as any streamed reply does.
async fn blame_parts(mut send: BlameSend, tx: mpsc::Sender<Part<BlameResponse>>) {
    for part in blame_page_parts(&mut send) {
        match reserve(&tx, READ_STALL_TIMEOUT, send.stalled.as_ref()).await {
            Taken::Slots(mut slots) => {
                if let Some(slot) = slots.next() {
                    slot.send(Ok(part));
                }
            }
            Taken::Gone => return,
            Taken::Stalled => {
                let detail = format!("the client took no data for {READ_STALL_TIMEOUT:?}");
                let _ = tx.try_send(Err(Status::deadline_exceeded(detail)));
                return;
            }
        }
    }
    // Keeps the request slot until the client has taken the last part; a
    // client that never does is counted.
    reserve(&tx, READ_STALL_TIMEOUT, send.stalled.as_ref()).await;
}

/// Fetches one page of the log: a graph query when `path` is set, because
/// it may derive the path history of a long history first, a plain read
/// otherwise.
async fn log_page(
    worker: &Worker,
    graph_queries: &Arc<Semaphore>,
    repo: String,
    path: Option<RepoPath>,
    deadline: Deadline,
    from: ChangesetId,
    limit: usize,
) -> Result<read::LogPage, Status> {
    if let Some(path) = path {
        run_graph_query(worker, graph_queries, deadline, move |store, deadline| {
            read::log_page(store, &repo, from, Some(&path), limit, deadline)
        })
        .await
    } else {
        worker
            .run(move |store| read::log_page(&store, &repo, from, None, limit, &deadline))
            .await
    }
}

/// Streams a `Log` reply: sends the parts of the first page, then fetches
/// the page after it and sends that, until the history ends, the call has
/// served `limit` entries, or the client stops taking parts. A stall counts
/// [`STALLED_REPLY_BYTES`] on the connection, as any streamed reply does.
async fn log_parts(mut walk: LogWalk, page: read::LogPage, tx: mpsc::Sender<Part<LogResponse>>) {
    let fail = |status: Status| {
        // Fails only if the client has gone.
        let _ = tx.try_send(Err(status));
    };
    let mut page = page;
    loop {
        for part in log_page_parts(std::mem::take(&mut page.entries), &mut walk) {
            match reserve(&tx, READ_STALL_TIMEOUT, walk.stalled.as_ref()).await {
                Taken::Slots(mut slots) => {
                    if let Some(slot) = slots.next() {
                        slot.send(Ok(part));
                    }
                }
                Taken::Gone => return,
                Taken::Stalled => {
                    let detail = format!("the client took no data for {READ_STALL_TIMEOUT:?}");
                    return fail(Status::deadline_exceeded(detail));
                }
            }
        }
        if walk.left == 0 || walk.next.is_none() {
            // Keeps the request slot until the client has taken the last
            // part; a client that never does is counted.
            reserve(&tx, READ_STALL_TIMEOUT, walk.stalled.as_ref()).await;
            return;
        }
        let from = walk.next.unwrap();
        let limit = walk.left.min(read::MAX_SCAN);
        page = match log_page(
            &walk.worker,
            &walk.graph_queries,
            walk.repo.clone(),
            walk.path.clone(),
            walk.deadline.clone(),
            from,
            limit,
        )
        .await
        {
            Ok(page) => page,
            Err(status) => return fail(status),
        };
        // The next page's continuation is the walk's: without this, the
        // page after the second one starts at the second page's start, and
        // the reply repeats it.
        walk.next = page.next;
    }
}

async fn send_parts<T: Send + 'static>(
    mut parts: Parts,
    _share: OwnedSemaphorePermit,
    _call: Call,
    tx: mpsc::Sender<Part<T>>,
    stall: Duration,
    stalled: Option<scion_proto::listen::Stalled>,
    make: fn(Vec<u8>) -> T,
) {
    let fail = |status: Status| {
        // Fails only if the client has gone.
        let _ = tx.try_send(Err(status));
    };
    loop {
        let part = match parts.next_part().await {
            Ok(part) => part,
            Err(status) => return fail(status),
        };
        if let Some(data) = part {
            // Both slots free: the client has taken the part before.
            match reserve(&tx, stall, stalled.as_ref()).await {
                Taken::Slots(mut slots) => {
                    if let Some(slot) = slots.next() {
                        slot.send(Ok(make(data)));
                    }
                }
                Taken::Gone => return,
                Taken::Stalled => {
                    // The client took no part: it is owed the error, so it
                    // never takes the file for a whole one.
                    let detail = format!("the client took no data for {stall:?}");
                    return fail(Status::deadline_exceeded(detail));
                }
            }
        } else {
            // Keeps the share until the client has taken the last part.
            reserve(&tx, stall, stalled.as_ref()).await;
            return;
        }
    }
}

/// Sends a blob to `tx` in parts of up to `CHUNK_BYTES`, as the client takes
/// them.
async fn send_file(
    worker: Worker,
    blob: BlobId,
    share: OwnedSemaphorePermit,
    call: Call,
    tx: mpsc::Sender<Part<ReadFileResponse>>,
    stall: Duration,
    stalled: Option<scion_proto::listen::Stalled>,
) {
    send_parts(
        Parts::Blob {
            worker,
            blob,
            open: None,
            ended: false,
        },
        share,
        call,
        tx,
        stall,
        stalled,
        |data| ReadFileResponse { data },
    )
    .await;
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
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty upload"))??;
        check_name("repo", &first.repo)?;
        // An upload names no path, so a caller that may change some path of
        // the repo may upload. A partial reader uploads nothing in v0.1 of
        // the folder rules ([#121]).
        if self.access.partial(&caller, &first.repo) {
            return Err(Status::unimplemented(
                "not available to a principal with folder read rules",
            ));
        }
        self.access.check_writer(&caller, &first.repo)?;
        // The first message's bytes, and the budget that the later messages
        // of this upload take from. Held until the reply, on every path.
        let mut quota = self
            .quotas
            .upload(&caller, first.data.len())
            .map_err(Status::from)?;
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
            quota.add(chunk.data.len()).map_err(Status::from)?;
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
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let CreateChangesetRequest { repo, changeset } = request.into_inner();
        check_whole_reader(&self.access, &caller, &repo)?;
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
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
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
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let GetLandRequest { repo, land_id } = request.into_inner();
        check_whole_reader(&self.access, &caller, &repo)?;
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
        resolve_ref(&self.worker, &self.access, &self.quotas, request)
            .await
            .map(Response::new)
    }

    async fn get_changeset(
        &self,
        request: Request<GetChangesetRequest>,
    ) -> Result<Response<GetChangesetStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let data = self
            .worker
            .run(move |store| Ok(read::get_changeset(&store, &req.repo, id)?.encode()))
            .await?;
        self.stream_whole(call, data, stalled, |data| GetChangesetResponse {
            changeset: data,
        })
        .await
    }

    type ReadFileStream = ReadFileStream;

    type GetChangesetStream = GetChangesetStream;

    type ListDirStream = ListDirStream;

    type LogStream = LogStream;

    type BlameStream = BlameStream;

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        // The derivation of a first view walks the real tree, under the
        // call's deadline.
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let metrics = Arc::clone(&self.metrics);
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_path(req.path)?;
        // A partial reader reads its view: the paths that its read rules
        // cover, through trees the server derived for it ([#121]). A hidden
        // path is not in the view, so it answers as a missing one.
        let view = match self.access.read_digest(&caller, &req.repo) {
            Some(digest) => {
                let prefixes = self
                    .access
                    .read_rule_set(&caller, &req.repo)
                    .unwrap_or_default();
                let live = self.access.view_digests(&req.repo);
                Some((digest, prefixes, live))
            }
            None => None,
        };
        let (blob, size) = self
            .worker
            .run(move |store| {
                let blob = match &view {
                    Some((digest, prefixes, live)) => {
                        let repo_id = store.read(|c| c.repo_id(&req.repo))?;
                        let root = views::view_root(
                            &store, repo_id, digest, id, prefixes, live, &metrics, &deadline,
                        )?;
                        read::file_blob_at(&store, root, &path)?
                    }
                    None => read::file_blob(&store, &req.repo, id, &path)?,
                };
                Ok((blob, store.blobs().size(blob)?))
            })
            .await?;
        // Taken before any content is read, so a call that waits holds none.
        let share = u32::try_from(read_share(size)).expect("asserted to fit in u32");
        let share = Arc::clone(&self.reads)
            .acquire_many_owned(share)
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        // The request slot lives until the reply ends, not until this
        // handler returns: the reply's work goes on in the producer.
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(
            send_file(
                self.worker.clone(),
                blob,
                share,
                call,
                tx,
                READ_STALL_TIMEOUT,
                stalled,
            )
            .instrument(tracing::Span::current()),
        );
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        // The derivation of a first view walks the real tree, under the
        // call's deadline.
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let metrics = Arc::clone(&self.metrics);
        let req = request.into_inner();
        check_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_optional_path(req.path)?;
        // A partial reader lists its view: only the entries its read rules
        // cover, and a hidden path answers as a missing one ([#121]).
        let view = match self.access.read_digest(&caller, &req.repo) {
            Some(digest) => {
                let prefixes = self
                    .access
                    .read_rule_set(&caller, &req.repo)
                    .unwrap_or_default();
                let live = self.access.view_digests(&req.repo);
                Some((digest, prefixes, live))
            }
            None => None,
        };
        let data = self
            .worker
            .run(move |store| {
                let tree = match &view {
                    Some((digest, prefixes, live)) => {
                        let repo_id = store.read(|c| c.repo_id(&req.repo))?;
                        let root = views::view_root(
                            &store, repo_id, digest, id, prefixes, live, &metrics, &deadline,
                        )?;
                        read::list_dir_at(&store, root, path.as_ref())?
                    }
                    None => read::list_dir(&store, &req.repo, id, path.as_ref())?,
                };
                Ok(tree.encode())
            })
            .await?;
        self.stream_whole(call, data, stalled, |data| ListDirResponse { tree: data })
            .await
    }

    async fn log(&self, request: Request<LogRequest>) -> Result<Response<LogStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        // `read::MAX_SCAN` also bounds the entries.
        let limit = (req.limit as usize).max(1);
        let path = parse_optional_path(req.path)?;
        let page = log_page(
            &self.worker,
            &self.graph_queries,
            req.repo.clone(),
            path.clone(),
            deadline.clone(),
            id,
            limit,
        )
        .await?;
        // The first page goes out as the first parts; the rest follow,
        // page by page, until the history or `limit` ends the walk. The
        // reply's work goes on in the producer, so the request slot lives
        // until the reply ends.
        let walk = LogWalk {
            worker: self.worker.clone(),
            repo: req.repo.clone(),
            graph_queries: Arc::clone(&self.graph_queries),
            _call: call,
            next: page.next,
            left: limit,
            path,
            deadline,
            stalled,
        };
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(log_parts(walk, page, tx).instrument(tracing::Span::current()));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn blame(&self, request: Request<BlameRequest>) -> Result<Response<BlameStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is
        // taken apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::listen::Stalled::of(&request);
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
        let id = parse_id(&req.changeset_id, "changeset_id")?;
        let path = parse_path(req.path.clone())?;
        // Taken before the worker permit, and held until the job ends, also
        // when the client leaves first.
        let slot = Arc::clone(&self.blames)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))?;
        let memory = self.blame_memory.clone();
        let until = Some(deadline.at());
        let repo = req.repo.clone();
        let cursor_bytes = req.cursor.clone();
        let walk_path = path.clone();
        let blame = self
            .worker
            .run(move |store| {
                let _slot = slot;
                let _call = deadline.guard();
                // The client sends the cursor back unchanged. A cursor made
                // for another repo, changeset or path is refused here, and
                // its changesets must be the repo's. Its memory takes the
                // blame budget, and stays taken until the call ends.
                let (resume, cursor) = if cursor_bytes.is_empty() {
                    (blame::Resume::default(), None)
                } else {
                    let (resume, hold) = blame::decode_cursor(
                        &store,
                        &repo,
                        &cursor_bytes,
                        id,
                        &walk_path,
                        &memory,
                        until,
                    )?;
                    (resume, Some(hold))
                };
                let _cursor = cursor;
                blame::blame(&store, &repo, id, &walk_path, &resume, &memory, until)
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
        let next_cursor = blame
            .resume
            .map(|resume| blame::encode_cursor(&resume, &req.repo, id, &path))
            .unwrap_or_default();
        // The blame is computed; the reply goes on in the producer, so the
        // request slot lives until the reply ends.
        let send = BlameSend {
            ranges,
            changesets,
            cursor: next_cursor,
            _call: call,
            stalled,
        };
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(blame_parts(send, tx).instrument(tracing::Span::current()));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn merge_base(
        &self,
        request: Request<MergeBaseRequest>,
    ) -> Result<Response<MergeBaseResponse>, Status> {
        let caller = caller(&request)?;
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
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
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
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
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
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
    use crate::backend::Backend;
    use crate::db;
    use crate::store::Store;
    use std::time::Instant;

    use scion_core::{Changeset, FileState, IdentityTime};
    use tonic::Code;

    use super::*;

    /// Receives one step's go signal, or panics naming the step: a
    /// release that never comes fails the test in seconds, not the
    /// 30-minute CI cancel a stuck barrier costs.
    pub(crate) fn expect_step(rx: &std::sync::mpsc::Receiver<()>, step: &str) {
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .unwrap_or_else(|_| panic!("the test never released {step}"));
    }

    /// Lets one step go, or panics naming the step when its receiver is
    /// gone.
    pub(crate) fn release_step(tx: &std::sync::mpsc::Sender<()>, step: &str) {
        tx.send(())
            .unwrap_or_else(|_| panic!("the step {step} is gone"));
    }

    /// The log of the test binary, into this one buffer.
    static LOG: std::sync::LazyLock<Captured> = std::sync::LazyLock::new(Captured::default);

    /// One global subscriber, set once, so a line that a worker thread
    /// logs lands in the same buffer. Each log test reads only the lines
    /// that name its repo. A test calls [`init_log`] before it makes the
    /// call that logs.
    fn init_log() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::level_filters::LevelFilter::TRACE)
                .with_writer(Captured::clone(&*LOG))
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
        });
    }

    /// A service whose one principal may do everything.
    fn test_service(store: Backend) -> ScionService {
        ScionService::new(
            Worker::new(store),
            Arc::new(Access::single("t")),
            Arc::default(),
            Quotas::off(),
            Arc::default(),
        )
    }

    fn log_text() -> String {
        String::from_utf8(LOG.0.lock().unwrap().clone()).unwrap()
    }

    /// A log writer that collects what the test binary logs.
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// The lines that name `repo`, in the order that they were logged.
    fn log_lines(repo: &str) -> Vec<String> {
        log_text()
            .lines()
            .filter(|line| line.contains(repo))
            .map(std::borrow::ToOwned::to_owned)
            .collect()
    }

    /// An access whose principal `p` may read repo `log-open`, and change
    /// nothing in it.
    fn read_only_access() -> Access {
        let digest = blake3::hash(b"t").to_hex();
        Access::parse(&format!("token\tp\t{digest}\nread\tp\tlog-open\n")).unwrap()
    }

    /// The one span per call that the server opens, with `rpc` in it and
    /// the principal and the repo still empty.
    fn call_span(rpc: &str) -> tracing::Span {
        tracing::error_span!(
            "call",
            rpc = rpc,
            peer = tracing::field::Empty,
            principal = tracing::field::Empty,
            repo = tracing::field::Empty,
        )
    }

    fn file_store(content: &[u8]) -> (tempfile::TempDir, Backend, ChangesetId, BlobId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
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
        store: Backend,
        blob: BlobId,
        budget: usize,
        stall: Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<Part<ReadFileResponse>>,
        Arc<Semaphore>,
    ) {
        let reads = Arc::new(Semaphore::new(budget));
        let share = u32::try_from(budget).unwrap();
        let share = Arc::clone(&reads).acquire_many_owned(share).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        let task = tokio::spawn(send_file(
            Worker::new(store),
            blob,
            share,
            Call::default(),
            tx,
            stall,
            None,
        ));
        (task, rx, reads)
    }

    /// Starts a [`send_parts`] reply whose parts come from one in-memory
    /// buffer, with all of a budget of `budget`. Returns the task, the reply
    /// and the budget.
    async fn whole_reply(
        data: Vec<u8>,
        budget: usize,
        stall: Duration,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<Part<ListDirResponse>>,
        Arc<Semaphore>,
    ) {
        let reads = Arc::new(Semaphore::new(budget));
        let share = u32::try_from(budget).unwrap();
        let share = Arc::clone(&reads).acquire_many_owned(share).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        let task = tokio::spawn(send_parts(
            Parts::Whole {
                data: data.into(),
                offset: 0,
            },
            share,
            Call::default(),
            tx,
            stall,
            None,
            |data| ListDirResponse { tree: data },
        ));
        (task, rx, reads)
    }

    #[tokio::test]
    async fn a_whole_reply_splits_at_chunk_bytes_and_stays_whole() {
        let data = content(3);
        let budget = whole_share(data.len()) as usize;
        let (task, mut rx, reads) =
            whole_reply(data.clone(), budget, Duration::from_secs(60)).await;
        let mut sent = Vec::new();
        while let Some(part) = rx.recv().await {
            let part = part.unwrap();
            assert!(part.tree.len() <= CHUNK_BYTES, "a part over the cap");
            sent.extend(part.tree);
        }
        assert_eq!(sent, data, "the parts are the bytes, in order");
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the reply ends")
            .unwrap();
        assert_eq!(reads.available_permits(), budget, "the share is free");
    }

    #[tokio::test]
    async fn a_whole_reply_whose_client_stops_mid_reply_frees_its_share() {
        let data = content(3);
        let budget = whole_share(data.len()) as usize;
        let (task, mut rx, reads) =
            whole_reply(data.clone(), budget, Duration::from_secs(60)).await;

        // The client takes one part and stops reading.
        let first = rx.recv().await.unwrap().unwrap();
        assert!(first.tree == data[..CHUNK_BYTES], "the first part");
        drop(rx);

        // The reply ends without the stall, and its share comes back.
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the reply ends when the client goes")
            .unwrap();
        assert_eq!(reads.available_permits(), budget, "the share is free");
    }

    /// Polls `run` on one thread.
    fn block_on<T>(run: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run)
    }

    /// `message` as a request of that principal.
    fn as_default<T>(message: T) -> Request<T> {
        let mut request = Request::new(message);
        let caller = Caller(scion_proto::auth::DEFAULT_PRINCIPAL.into());
        request.extensions_mut().insert(caller);
        request
    }

    #[test]
    fn a_denied_write_makes_one_line() {
        init_log();
        let (_dir, store, _, _) = file_store(b"x");
        let service = ScionService::new(
            Worker::new(store.clone()),
            Arc::new(read_only_access()),
            Arc::default(),
            Quotas::off(),
            Arc::default(),
        );
        // A valid changeset, which puts `f`: decode succeeds, the rules
        // deny the write. The check runs in the job, so this also shows
        // that the job stays in the call's span.
        let state = FileState::Regular {
            content: store.blobs().put(b"x").unwrap(),
            executable: false,
        };
        let cs = Changeset {
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
        let mut request = Request::new(CreateChangesetRequest {
            repo: "log-open".to_owned(),
            changeset: cs.encode(),
        });
        request.extensions_mut().insert(Caller("p".to_owned()));

        let span = call_span("CreateChangeset");
        let guard = span.enter();
        let status = block_on(service.create_changeset(request)).expect_err("denied");
        drop(guard);
        assert_eq!(status.code(), Code::PermissionDenied);
        // One line, with who and what, and no token.
        let lines = log_lines("repo=\"log-open\"");
        assert_eq!(lines.len(), 1, "ALL:\n{}", log_text());
        assert!(lines[0].contains("WARN"), "{}", lines[0]);
        assert!(lines[0].contains("may not write the path"), "{}", lines[0]);
        assert!(lines[0].contains("principal=\"p\""), "{}", lines[0]);
        assert!(lines[0].contains("path=\"f\""), "{}", lines[0]);
        assert!(lines[0].contains("rpc=\"CreateChangeset\""), "{}", lines[0]);
    }

    #[test]
    fn a_failed_call_makes_one_line() {
        init_log();
        // The reply of a failure keeps its detail in the log. Inside the
        // call's span, that line also says who and which repo.
        let span = call_span("GetLand");
        let guard = span.enter();
        tracing::Span::current().record("principal", "default");
        tracing::Span::current().record("repo", "log-no-such");
        let status = Status::from(crate::error::Error::Io(std::io::Error::other(
            "the disk went away",
        )));
        drop(guard);
        assert_eq!(status.code(), Code::Internal);
        // One line, with who and what, and no token.
        let lines = log_lines("repo=\"log-no-such\"");
        assert_eq!(lines.len(), 1, "ALL:\n{}", log_text());
        assert!(lines[0].contains("ERROR"), "{}", lines[0]);
        assert!(lines[0].contains("internal error"), "{}", lines[0]);
        assert!(lines[0].contains("the disk went away"), "{}", lines[0]);
        assert!(lines[0].contains("rpc=\"GetLand\""), "{}", lines[0]);
        assert!(lines[0].contains("principal=\"default\""), "{}", lines[0]);
    }

    #[test]
    fn a_hidden_repo_makes_one_warn() {
        init_log();
        let (_dir, store, _, _) = file_store(b"x");
        let service = test_service(store);
        // `p` has no grant on `log-secret`: the reply says that it does not
        // exist, and the log says why.
        let mut request = Request::new(GetLandRequest {
            repo: "log-secret".to_owned(),
            land_id: "1".to_owned(),
        });
        request.extensions_mut().insert(Caller("p".to_owned()));

        let span = call_span("GetLand");
        let guard = span.enter();
        let status = block_on(service.get_land(request)).expect_err("hidden");
        drop(guard);
        assert_eq!(status.code(), Code::NotFound);
        // One line, with who and what.
        let lines = log_lines("repo=\"log-secret\"");
        assert_eq!(lines.len(), 1, "{}", log_text());
        assert!(lines[0].contains("WARN"), "{}", lines[0]);
        assert!(lines[0].contains("may not read the repo"), "{}", lines[0]);
        assert!(lines[0].contains("principal=\"p\""), "{}", lines[0]);
        assert!(lines[0].contains("rpc=\"GetLand\""), "{}", lines[0]);
    }

    #[tokio::test]
    async fn a_read_file_reply_of_many_parts_stays_exactly_the_file() {
        let data = content(3);
        let (_dir, store, _, blob) = file_store(&data);
        let budget = read_share(data.len());
        let (task, mut rx, reads) = reply(store, blob, budget, Duration::from_secs(60)).await;
        let mut parts = 0;
        let mut read = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(part) = rx.recv().await {
                let part = part.unwrap();
                assert!(part.data.len() <= CHUNK_BYTES, "a part over the cap");
                read.extend(part.data);
                parts += 1;
            }
        })
        .await
        .expect("the reply ends; it never repeats a part");
        assert_eq!(parts, 3, "one part per chunk, no replay");
        assert_eq!(read, data, "the parts are the file, in order");
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the reply ends")
            .unwrap();
        assert_eq!(reads.available_permits(), budget, "the share is free");
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
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
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
            .read(|c| Ok(c.sqlite().unwrap().query_row(sql, [], |row| row.get(0))?))
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

    type Blocker = (
        std::thread::JoinHandle<Result<(), crate::error::Error>>,
        std::sync::mpsc::Sender<()>,
    );

    /// Holds every read connection of `store`: one thread per reader,
    /// each parked in a read until its own release, announcing itself on
    /// the returned channel. A blocker that dies before its release fails
    /// its step in seconds, and cannot trap its siblings in a barrier.
    fn hold_read_connections(store: &Backend) -> (Vec<Blocker>, std::sync::mpsc::Receiver<()>) {
        let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
        let blockers = (0..crate::store::READERS)
            .map(|i| {
                let (store, arrived_tx) = (store.clone(), arrived_tx.clone());
                let (go_tx, go_rx) = std::sync::mpsc::channel();
                let handle = std::thread::spawn(move || {
                    store.read(|_| {
                        arrived_tx.send(()).unwrap();
                        go_rx
                            .recv_timeout(std::time::Duration::from_secs(30))
                            .unwrap_or_else(|_| {
                                panic!("the test never released read connection {i}")
                            });
                        Ok(())
                    })
                });
                (handle, go_tx)
            })
            .collect();
        (blockers, arrived_rx)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn at_most_max_graph_queries_run_at_once() {
        // root ─ a, and root ─ b.
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::Sqlite(Store::open(dir.path()).unwrap());
        let ids = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                db::testing::insert_graph(c.sqlite().unwrap(), repo, &[vec![], vec![0], vec![0]])
            })
            .unwrap();
        let bytes = |i: usize| ids[i].as_bytes().to_vec();
        let service = Arc::new(test_service(store.clone()));
        let busy = || MAX_BLOCKING - service.worker.permits.available_permits();

        // Hold every read connection, so each job waits at its first read.
        // One channel per blocker: a blocker that dies before its release
        // fails its step in seconds, and cannot trap its siblings in a
        // barrier.
        let (blockers, arrived_rx) = hold_read_connections(&store);
        for _ in 0..crate::store::READERS {
            expect_step(&arrived_rx, "a read connection took its seat");
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
                            tokio_stream::StreamExt::collect::<Vec<_>>(reply.map(|page| {
                                page.unwrap()
                                    .entries
                                    .into_iter()
                                    .map(|e| e.changeset_id)
                                    .collect::<Vec<_>>()
                            }))
                            .await
                            .into_iter()
                            .flatten()
                            .collect();
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

        // The reads held; release them, where the old barrier's `go.wait()`
        // stood, so the parked jobs get their connection.
        for (handle, go_tx) in &blockers {
            assert!(!handle.is_finished(), "a blocker died before it held");
            release_step(go_tx, "the held read connections");
        }
        let blockers: Vec<_> = blockers.into_iter().map(|(handle, _)| handle).collect();
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
        let worker = Worker::new(Backend::Sqlite(Store::open(dir.path()).unwrap()));
        let status = worker
            .run(|_| -> Result<()> { panic!("secret detail") })
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
    }
}

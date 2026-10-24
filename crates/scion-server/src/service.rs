//! The gRPC front end. Parses requests, runs the work on a blocking thread
//! (`SQLite` and the object folder are synchronous), and maps errors to status
//! codes.

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs), and unreachable_pub does not see
// through a re-export, so this marker would fire on all of them
// under any feature set.
#![allow(unreachable_pub)]
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use prost::Message as _;
use scion_core::{BlobId, Changeset, ChangesetId, FileState, RepoPath};
use scion_proto::runtime::listen::{self, Limits};
use scion_proto::runtime::tls::ServerTls;
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

use crate::metrics::{Metrics, MetricsLayer, code_label};
use serde_json::Value;
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
/// budget (`scion_proto::runtime::listen`).
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

/// Polls the free space of the data directory's filesystem into the
/// gauge. The latest poll is the value; a failed stat keeps the last
/// good value and logs once a poll. The poll stops when the server
/// does.
fn spawn_free_space_poll(
    data: std::path::PathBuf,
    metrics: Arc<Metrics>,
    stop: tokio::sync::watch::Receiver<()>,
) {
    tokio::spawn(async move {
        let mut stop = stop;
        loop {
            let data = data.clone();
            let free = tokio::task::spawn_blocking(move || crate::store::free_bytes(&data));
            match free.await {
                Ok(Ok(bytes)) => metrics.data_free_bytes(bytes),
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "cannot stat the data directory's free space");
                }
                Err(e) => tracing::warn!(error = %e, "the free-space poll failed"),
            }
            tokio::select! {
                () = tokio::time::sleep(FREE_SPACE_POLL) => {}
                _ = stop.changed() => break,
            }
        }
    });
}

/// Set when the server's first stale-land sweep has finished. A test
/// hook: the land round-trip guard waits for it before it resets the
/// statement counter, so the sweep's own statements cannot fall inside
/// the counted window - the only database-speaking background task the
/// server runs is this sweep, and after its first pass the next one is
/// [`SWEEP_PERIOD`] away.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) static TEST_FIRST_SWEEP_DONE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// The test seam of the land job's claim: after the job's `LAND_STARTED`
/// swap wins, it records the land's request key here. A test that aborts
/// a land's call waits for its key, so the abort never lands in the
/// permit-to-swap window, where the handler's drop wins the answer and
/// the job records nothing.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) static TEST_LAND_CLAIMED: std::sync::Mutex<Vec<String>> =
    std::sync::Mutex::new(Vec::new());

/// Runs the stale-land sweep at once, then once per [`SWEEP_PERIOD`], until
/// `stop` resolves. The first sweep settles what a restart left pending
/// before the first client asks.
async fn sweeps(
    worker: Worker,
    lands: Arc<Lands>,
    stale_secs: i64,
    audit: crate::audit::Audit,
    mut stop: watch::Receiver<()>,
) {
    loop {
        let (lands, store) = (Arc::clone(&lands), worker.store());
        let audit = audit.clone();
        let rejected =
            tokio::task::spawn_blocking(move || lands.sweep_stale(&store, stale_secs, &audit))
                .await;
        #[cfg(any(test, feature = "test-hooks"))]
        TEST_FIRST_SWEEP_DONE.store(true, std::sync::atomic::Ordering::Release);
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

/// How often the free-space gauge of the data directory is refreshed.
const FREE_SPACE_POLL: Duration = Duration::from_secs(30);

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
    /// The data directory the server serves, for the free-space gauge of
    /// its filesystem.
    pub data: std::path::PathBuf,
    /// The audit log. Every server has one; the caller opens it (or
    /// passes the off handle of the tests) and records its start line.
    pub audit: crate::audit::Audit,
    /// The bytes of unary replies the server holds at once. The tests
    /// shrink it, so a small store can exercise the budget; the default
    /// is the shipped [`crate::reply::REPLY_MEMORY_BUDGET`].
    pub reply_memory_budget: usize,
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
// The service wiring reads best inline: the layers wrap one another in
// the order a reply travels, and splitting it hides that order.
#[allow(clippy::too_many_lines)]
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
        reply_memory_budget,
        data,
        audit,
    } = options;
    let access = Arc::new(access.into());
    let auth = access.auth();
    let fence = store.fence_flag();
    // The backend records into the registry: the Postgres pool's wait,
    // busy readers and tx age (which this task keeps fresh), and on both
    // backends the blob full flushes, the derive-walk gauges and the
    // collector-key wait.
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
    // The free space of the data directory's filesystem, so the operator
    // sees a full drive before the server hits it.
    spawn_free_space_poll(data, Arc::clone(&metrics), stop_rx.clone());
    // Refused authentication is counted per peer and flushed as one
    // line per peer, so a stranger who spams bad tokens cannot fill the
    // audit log or the disk with refusal lines.
    let meter = Arc::new(crate::audit::Meter::default());
    tokio::spawn(crate::audit::meter_task(
        Arc::clone(&meter),
        audit.clone(),
        stop_rx.clone(),
    ));
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
        audit.clone(),
        stop_rx,
    ));
    let reply_memory =
        crate::reply::ReplyMemoryLayer::with_budget(reply_memory_budget, Arc::clone(&metrics));
    let scion = reply_memory.clone().layer(
        MetricsLayer::new(Arc::clone(&metrics)).layer(InterceptedService::new(
            ScionService::new(
                worker.clone(),
                Arc::clone(&access),
                lands,
                quotas.clone(),
                Arc::clone(&metrics),
                audit.clone(),
            )
            .into_server(),
            crate::audit::AuditedAuth::new(auth.clone(), Arc::clone(&meter)),
        )),
    );
    let objects = reply_memory.layer(MetricsLayer::new(Arc::clone(&metrics)).layer(
        ObjectsService::new(worker, access, quotas, tickets, Arc::clone(&metrics)).into_server(),
    ));
    // The health service answers `grpc.health.v1.Check` and `Watch` on the
    // same port, without a token: a probe carries none. The store opens
    // before this call, so the server starts serving. A fence trip
    // resolves the shutdown, so the names flip to NOT_SERVING on the
    // service's own path, and the process exits a grace later. An audit
    // line that was not made durable flips the names too, and latches:
    // the log has a hole, so the server must not be picked for writes.
    let (health, shutdown) = listen::health(
        &["scion.v1.Scion", "scion.v1.Objects"],
        shutdown,
        audit.ready(),
    )
    .await;
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
            Arc::clone(&metrics),
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
    /// The audit log: state changes record an intent line before they
    /// run and a result line after.
    audit: crate::audit::Audit,
}

/// One state per land call's answer: the intent is recorded, and
/// exactly one of the handler's drop and the store job owns the result
/// line. The job moves it to [`LAND_STARTED`] as its first statement;
/// the handler's drop moves it to [`LAND_ABANDONED`] and answers only
/// if its swap won. [`LAND_VOID`] retires the debt when the intent
/// itself was never made durable: no intent, no answer owed.
const LAND_INTENT: u8 = 0;
const LAND_STARTED: u8 = 1;
const LAND_ABANDONED: u8 = 2;
const LAND_VOID: u8 = 3;

/// The result line a land call owes its intent. Held by the handler
/// from before the intent is recorded to the call's end, returned or
/// dropped: its drop answers with `cancelled` only when the job never
/// claimed the call, and it hands the line to the audit's writer off
/// this thread, because a `Drop` must not fsync.
struct LandAnswer {
    state: Arc<AtomicU8>,
    audit: crate::audit::Audit,
    request_id: String,
    principal: String,
    repo: String,
    target_ref: String,
}

impl Drop for LandAnswer {
    fn drop(&mut self) {
        if self.state.swap(LAND_ABANDONED, Ordering::AcqRel) != LAND_INTENT {
            // The job claimed the call: its own lines answer the intent.
            return;
        }
        let line = ScionService::land_failed_line(
            &self.request_id,
            &self.principal,
            &self.repo,
            &self.target_ref,
            "cancelled",
        );
        let audit = self.audit.clone();
        let write = move || {
            if let Err(why) = audit.record_blocking(&line) {
                tracing::error!(
                    reason = %why,
                    "a cancelled land's audit line was not made durable"
                );
            }
        };
        // Off this thread: a Drop runs on a runtime worker, and a
        // blocking append there would stall it.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(write);
            }
            Err(_) => {
                std::thread::spawn(write);
            }
        }
    }
}

impl ScionService {
    /// Sends `data` in parts of up to `CHUNK_BYTES`, as `ReadFile` sends a
    /// file. Holds the whole bytes, so the share is their size.
    /// The budget share a whole-object reply needs: the object's own
    /// bytes, taken BEFORE the object is read and built, so the count
    /// of replies in flight bounds the memory they hold. An object over
    /// the cap is refused here, before any of its bytes are read.
    async fn whole_share_of(&self, size: usize) -> Result<OwnedSemaphorePermit, Status> {
        if size > MAX_OBJECT_BYTES {
            return Err(Status::resource_exhausted(format!(
                "the object is {size} bytes, over the {MAX_OBJECT_BYTES}-byte reply cap"
            )));
        }
        Arc::clone(&self.reads)
            .acquire_many_owned(u32::try_from(size).expect("the cap asserts u32 fit"))
            .await
            .map_err(|_| Status::unavailable("server is shutting down"))
    }

    /// Sends `data` in parts, holding `share` for the whole reply.
    fn stream_whole_holding<T: Send + 'static>(
        call: Call,
        data: Vec<u8>,
        share: OwnedSemaphorePermit,
        stalled: Option<scion_proto::runtime::listen::Stalled>,
        make: fn(Vec<u8>) -> T,
    ) -> Response<ReceiverStream<Part<T>>> {
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
        Response::new(ReceiverStream::new(rx))
    }

    fn new(
        worker: Worker,
        access: Arc<Access>,
        lands: Arc<Lands>,
        quotas: Quotas,
        metrics: Arc<Metrics>,
        audit: crate::audit::Audit,
    ) -> Self {
        Self {
            worker,
            access,
            lands,
            quotas,
            metrics,
            audit,
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

    /// Records the audit's intent line for a state change, and waits
    /// until it is durable: evidence first, so a change whose evidence
    /// was not made durable does not happen.
    async fn audit_intent(
        &self,
        request_id: &str,
        principal: &str,
        event: &'static str,
        fields: serde_json::Value,
    ) -> Result<(), Status> {
        let line =
            crate::audit::Event::new(event, "intent", json_fields(request_id, principal, &fields));
        self.audit.record(line).await.map_err(|why| {
            tracing::error!(reason = %why, "an audit intent was not made durable");
            Status::unavailable("the audit line was not made durable")
        })
    }

    /// The audit's answer the land job owes its intent: the outcome's
    /// own lines when the land ran, else the one failed line. The why
    /// is the code label of the one Status the job built, the one the
    /// reply carries too, so one error logs once. The checks before
    /// the land refuse a call after the guard already said STARTED, so
    /// the job owes the intent its answer there too.
    fn audit_land_outcome(
        audit: &crate::audit::Audit,
        request_id: &str,
        principal: &str,
        repo: &str,
        target_ref: &str,
        outcome: &Result<land::LandOutcome, Status>,
    ) {
        match outcome {
            Ok(outcome) => {
                for line in
                    Self::land_result_lines(request_id, principal, repo, target_ref, outcome)
                {
                    Self::audit_result_blocking(audit, &line);
                }
            }
            Err(status) => {
                Self::audit_result_blocking(
                    audit,
                    &Self::land_failed_line(
                        request_id,
                        principal,
                        repo,
                        target_ref,
                        code_label(status.code()),
                    ),
                );
            }
        }
    }

    /// The failed line a land owes its intent when the call ends in an
    /// error: the why is the code label the caller read.
    fn land_failed_line(
        request_id: &str,
        principal: &str,
        repo: &str,
        target_ref: &str,
        why: &str,
    ) -> crate::audit::Event {
        crate::audit::Event::new(
            "land",
            "failed",
            json_fields(
                request_id,
                principal,
                &serde_json::json!({
                    "rpc": "Land",
                    "repo": repo,
                    "ref": target_ref,
                    "why": why,
                }),
            ),
        )
    }

    /// The audit's result lines a state change leaves, written by the
    /// store job that made the change, after it and outside any write
    /// transaction: a call cut between the intent and the result (a
    /// client that left, a deadline, a shutdown) still leaves the
    /// audit's answer to its intent. A free function on the audit
    /// handle, because the job that calls it is not a method on the
    /// service.
    fn audit_result_blocking(audit: &crate::audit::Audit, line: &crate::audit::Event) {
        if let Err(why) = audit.record_blocking(line) {
            tracing::error!(reason = %why, "an audit result was not made durable");
        }
    }

    /// The audit's result lines a land that ran leaves: the land's
    /// result line, and — when this call moved the ref — the move's
    /// own line. A replay of a landed request reports the first move's
    /// outcome and moved nothing itself: the move's line exists from
    /// the call that made it.
    fn land_result_lines(
        request_id: &str,
        principal: &str,
        repo: &str,
        target_ref: &str,
        outcome: &land::LandOutcome,
    ) -> Vec<crate::audit::Event> {
        let mut fields = serde_json::json!({
            "rpc": "Land",
            "repo": repo,
            "ref": target_ref,
            "state": outcome.state.as_str(),
            "land_id": outcome.land_id,
        });
        if let Some(landed) = outcome.result {
            fields["new"] = serde_json::json!(hex_id(landed.as_bytes()));
            if let Some(from) = outcome.previous.as_ref() {
                fields["old"] = serde_json::json!(hex_id(from.as_bytes()));
            }
        }
        if !outcome.detail.is_empty() {
            fields["detail"] = serde_json::json!(outcome.detail);
        }
        let mut lines = vec![crate::audit::Event::new(
            "land",
            "ok",
            json_fields(request_id, principal, &fields),
        )];
        // The move's own line: which principal moved which ref from
        // what to what. The land's intent line made the evidence
        // first; this line is the move's record.
        if let (true, Some(landed)) = (outcome.moved, outcome.result) {
            lines.push(crate::audit::Event::new(
                "ref",
                "ok",
                json_fields(
                    request_id,
                    principal,
                    &serde_json::json!({
                        "rpc": "Land",
                        "repo": repo,
                        "ref": target_ref,
                        "old": outcome.previous.as_ref().map(|id| hex_id(id.as_bytes())),
                        "new": hex_id(landed.as_bytes()),
                    }),
                ),
            ));
        }
        lines
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
/// SHORTCUT: the reply-memory budget (#586) bounds the unary replies
/// parked in HTTP/2, so a client that never reads holds the budget -
/// `REPLY_MEMORY_BUDGET`, 128 MiB across every connection - not the
/// server's memory. The residual: the count is the reply's encoded
/// length set by its handler, not the wire bytes (the 5-byte gRPC
/// frame prefix of each parked reply stays uncounted), and a
/// committed write's reply goes out uncounted by design. A
/// whole-object reply (`ListDir`, `GetChangeset`; `ReadFile` streams
/// part by part and takes the read budget per part) is bounded twice:
/// its built body takes the read budget (`READ_BUDGET_BYTES`) before
/// the body is built, and the parts it has sent ride the
/// per-connection stalled-bytes cap, whose connection is closed over
/// the cap. A new unary RPC must still mark its reply, or stream,
/// before it ships: the layer's test seam fails the handler that
/// forgets.
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

/// Marks a unary reply so the reply-memory layer counts its
/// parked bytes: the handler knows the encoded size here, before
/// the response returns, which the encoded body never reveals.
pub(crate) fn count_reply<M: prost::Message>(response: Response<M>) -> Response<M> {
    let mut response = response;
    let size = response.get_ref().encoded_len();
    response
        .extensions_mut()
        .insert(crate::reply::ReplySize(size));
    response
}

/// Marks a committed write's reply so the reply-memory layer never
/// refuses it: the write has landed, and the client must read the
/// truth, not a retryable error. The layer sends it uncounted when
/// the budget is spent, with a warn line.
pub(crate) fn count_committed_reply<M: prost::Message>(response: Response<M>) -> Response<M> {
    let mut response = count_reply(response);
    response
        .extensions_mut()
        .insert(crate::reply::ReplyMustDeliver);
    response
}

/// Marks a streaming reply so the reply-memory layer knows its bytes
/// are counted elsewhere: the read-budget share it took before the
/// object was built. The layer's test seam panics on a reply with
/// neither mark, so a new handler that forgets both fails a test.
pub(crate) fn mark_stream<T>(response: Response<T>) -> Response<T> {
    let mut response = response;
    response.extensions_mut().insert(crate::reply::ReplyStreams);
    response
}

pub(crate) use scion_proto::runtime::check::id as parse_id;

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

/// The shared fields of an audit line: who, which call, on which RPC.
pub(crate) fn json_fields(
    request_id: &str,
    principal: &str,
    fields: &serde_json::Value,
) -> serde_json::Value {
    let mut all = serde_json::json!({
        "request_id": request_id,
        "principal": principal,
    });
    if let (Value::Object(all), Value::Object(extra)) = (&mut all, fields) {
        for (key, value) in extra {
            all.insert(key.clone(), value.clone());
        }
    }
    all
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

/// The caller's request ID, if it sent one that fits: the audit's
/// intent and result lines join by it. A call that sent no usable ID
/// gets one the server made, so every line it leaves still carries a
/// `request_id` its pair can be found by.
fn request_id_of<T>(request: &Request<T>) -> String {
    scion_proto::runtime::request_id::sanitize(
        request
            .metadata()
            .get(scion_proto::runtime::request_id::REQUEST_ID)
            .map(tonic::metadata::MetadataValue::as_bytes),
    )
    .unwrap_or_else(scion_proto::runtime::request_id::generate)
}

/// The hex of a content ID, as the audit's old and new ref values.
fn hex_id(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
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
    stalled: Option<scion_proto::runtime::listen::Stalled>,
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
    /// connection counts them until then. The count is the parked
    /// [`Stall`](scion_proto::runtime::listen::Stall), to give back when
    /// the body drops.
    Stalled(Option<scion_proto::runtime::listen::Stall>),
}

/// Parks a stalled reply's count until its body drops: the parts stay in
/// HTTP/2 until the client resets the stream or the connection closes,
/// which drops the reply's body and closes its channel. This is what keeps
/// a connection that serves stall after stall from ratcheting its count up
/// to the close limit for bytes it no longer holds. The reply's request
/// slot and budget share, if it holds any, stay with it as long as its
/// parts do.
async fn park_until_body_gone<T>(
    parked: Option<scion_proto::runtime::listen::Stall>,
    tx: &mpsc::Sender<Part<T>>,
) {
    if let Some(parked) = parked {
        tx.closed().await;
        drop(parked);
    }
}

/// Waits `stall` for two free slots of the reply's channel, as
/// [`send_parts`] and [`log_parts`] do.
async fn reserve<'a, T>(
    tx: &'a mpsc::Sender<Part<T>>,
    stall: Duration,
    stalled: Option<&'a scion_proto::runtime::listen::Stalled>,
) -> Taken<'a, T> {
    match tokio::time::timeout(stall, tx.reserve_many(2)).await {
        Ok(Ok(slots)) => Taken::Slots(slots),
        // The client has gone with the connection.
        Ok(Err(_)) => Taken::Gone,
        Err(_) => Taken::Stalled(
            stalled.map(|stalled| stalled.hold(scion_proto::runtime::reply::STALLED_REPLY_BYTES)),
        ),
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
    stalled: Option<scion_proto::runtime::listen::Stalled>,
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
            Taken::Stalled(parked) => {
                let detail = format!("the client took no data for {READ_STALL_TIMEOUT:?}");
                let _ = tx.try_send(Err(Status::deadline_exceeded(detail)));
                park_until_body_gone(parked, &tx).await;
                return;
            }
        }
    }
    // Keeps the request slot until the client has taken the last part; a
    // client that never does is counted until its body drops.
    if let Taken::Stalled(parked) = reserve(&tx, READ_STALL_TIMEOUT, send.stalled.as_ref()).await {
        park_until_body_gone(parked, &tx).await;
    }
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
                Taken::Stalled(parked) => {
                    let detail = format!("the client took no data for {READ_STALL_TIMEOUT:?}");
                    let _ = tx.try_send(Err(Status::deadline_exceeded(detail)));
                    park_until_body_gone(parked, &tx).await;
                    return;
                }
            }
        }
        if walk.left == 0 || walk.next.is_none() {
            // Keeps the request slot until the client has taken the last
            // part; a client that never does is counted until its body
            // drops.
            if let Taken::Stalled(parked) =
                reserve(&tx, READ_STALL_TIMEOUT, walk.stalled.as_ref()).await
            {
                park_until_body_gone(parked, &tx).await;
            }
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
    stalled: Option<scion_proto::runtime::listen::Stalled>,
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
                Taken::Stalled(parked) => {
                    // The client took no part: it is owed the error, so it
                    // never takes the file for a whole one.
                    let detail = format!("the client took no data for {stall:?}");
                    let _ = tx.try_send(Err(Status::deadline_exceeded(detail)));
                    park_until_body_gone(parked, &tx).await;
                    return;
                }
            }
        } else {
            // Keeps the share until the client has taken the last part; a
            // client that never does is counted until its body drops.
            if let Taken::Stalled(parked) = reserve(&tx, stall, stalled.as_ref()).await {
                park_until_body_gone(parked, &tx).await;
            }
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
    stalled: Option<scion_proto::runtime::listen::Stalled>,
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
        let request_id = request_id_of(&request);
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
        let repo_name = first.repo.clone();
        let principal = caller.0.clone();
        let audit = self.audit.clone();
        let mut bytes_total = first.data.len();
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
            bytes_total += chunk.data.len();
            quota.add(chunk.data.len()).map_err(Status::from)?;
            upload = self.worker.run(move |_| upload.append(&chunk.data)).await?;
        }
        let bytes = bytes_total;
        self.audit_intent(
            &request_id,
            caller.0.as_str(),
            "write",
            serde_json::json!({ "rpc": "PutBlob", "repo": repo_name, "bytes": bytes }),
        )
        .await?;
        let written = self
            .worker
            .run(move |store| {
                let written = write::put_blob(&store, repo, upload).map_err(Status::from);
                // The job that changed the state writes the state
                // change's own line, after the commit and outside the
                // write transaction: the evidence does not depend on the
                // call that asked for the write living on.
                let mut fields =
                    serde_json::json!({ "rpc": "PutBlob", "repo": repo_name, "bytes": bytes });
                match &written {
                    Ok(_) => Self::audit_result_blocking(
                        &audit,
                        &crate::audit::Event::new(
                            "write",
                            "ok",
                            json_fields(&request_id, &principal, &fields),
                        ),
                    ),
                    Err(status) => {
                        fields["why"] = serde_json::json!(code_label(status.code()));
                        Self::audit_result_blocking(
                            &audit,
                            &crate::audit::Event::new(
                                "write",
                                "failed",
                                json_fields(&request_id, &principal, &fields),
                            ),
                        );
                    }
                }
                Ok(written)
            })
            .await;
        let id = written??;
        Ok(count_committed_reply(Response::new(PutBlobResponse {
            blob_id: id.as_bytes().to_vec(),
        })))
    }

    async fn create_changeset(
        &self,
        request: Request<CreateChangesetRequest>,
    ) -> Result<Response<CreateChangesetResponse>, Status> {
        // The job checks it in its blob loop: a request timeout, or a
        // client that cancels, drops this call but not a job that is
        // running. The guard ends the deadline when this call ends or is
        // dropped. The blob check re-hashes every distinct blob, so a big
        // changeset would otherwise outlive the call that asked for it.
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let _call = deadline.guard();
        let caller = caller(&request)?;
        let request_id = request_id_of(&request);
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let CreateChangesetRequest { repo, changeset } = request.into_inner();
        check_whole_reader(&self.access, &caller, &repo)?;
        let principal = caller.0.clone();
        self.audit_intent(
            &request_id,
            principal.as_str(),
            "write",
            serde_json::json!({ "rpc": "CreateChangeset", "repo": repo }),
        )
        .await?;
        let access = Arc::clone(&self.access);
        let repo_for_write = repo.clone();
        let job_repo = repo.clone();
        let audit = self.audit.clone();
        let job_deadline = deadline.clone();
        let outcome = self
            .worker
            .run(move |store| {
                let outcome = (|| {
                    // The rules first, so a change that the caller may not make
                    // reads no blob. Decoding twice costs little next to reading
                    // every blob back.
                    let (cs, _) = write::decode(&changeset)?;
                    access.check_write(&caller, &repo_for_write, cs.changes().keys())?;
                    Ok(
                        write::create_changeset(&store, &repo_for_write, &changeset, &job_deadline)
                            .map_err(Status::from),
                    )
                })();
                // The job that changed the state writes the state change's
                // own line, after the commit and outside the write
                // transaction: the evidence does not depend on the call
                // that asked for the write living on.
                match &outcome {
                    Ok(Ok(id)) => {
                        Self::audit_result_blocking(
                            &audit,
                            &crate::audit::Event::new(
                                "write",
                                "ok",
                                json_fields(
                                    &request_id,
                                    &principal,
                                    // A changeset ID names content the caller
                                    // already had; the audit never names a blob
                                    // or a tree.
                                    &serde_json::json!({
                                        "rpc": "CreateChangeset",
                                        "repo": job_repo,
                                        "changeset": hex_id(id.as_bytes()),
                                    }),
                                ),
                            ),
                        );
                    }
                    Ok(Err(status)) => {
                        Self::audit_result_blocking(
                            &audit,
                            &crate::audit::Event::new(
                                "write",
                                "failed",
                                json_fields(
                                    &request_id,
                                    &principal,
                                    &serde_json::json!({
                                        "rpc": "CreateChangeset",
                                        "repo": job_repo,
                                        "why": code_label(status.code()),
                                    }),
                                ),
                            ),
                        );
                    }
                    Err(_) => {}
                }
                outcome
            })
            .await;
        let id = outcome??;
        Ok(count_committed_reply(Response::new(
            CreateChangesetResponse {
                changeset_id: id.as_bytes().to_vec(),
            },
        )))
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
        let request_id = request_id_of(&request);
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
        check_name("target_ref", &req.target_ref)?;
        check_name("request_key", &req.request_key)?;
        // The changeset is parsed before anything is recorded: a bad ID
        // is refused with `invalid_argument`, and the audit owes no
        // intent for a call that named nothing to land.
        let changeset = parse_id(&req.changeset_id, "changeset_id")?;
        let principal = caller.0.clone();
        // The answer the intent will owe: armed before the intent is
        // recorded, so a call cut while the intent's append is in
        // flight is still answered by the drop - a gap here would leave
        // an intent that nothing ever answers. The guard is voided when
        // the intent itself fails: no intent, no answer owed. The job
        // claims the answer as its first statement; if this call dies
        // first, the guard's drop answers `cancelled`. One of the two
        // answers, never both, never neither.
        let answer = Arc::new(AtomicU8::new(LAND_INTENT));
        let _answering = LandAnswer {
            state: Arc::clone(&answer),
            audit: self.audit.clone(),
            request_id: request_id.clone(),
            principal: principal.clone(),
            repo: req.repo.clone(),
            target_ref: req.target_ref.clone(),
        };
        self.audit_intent(
            &request_id,
            principal.as_str(),
            "land",
            serde_json::json!({
                "rpc": "Land",
                "repo": req.repo,
                "ref": req.target_ref,
                "request_key": req.request_key,
            }),
        )
        .await
        // The intent was not made durable, so no result line is owed:
        // the drop must answer nothing.
        .inspect_err(|_| answer.store(LAND_VOID, Ordering::Release))?;
        let req = land::LandRequest {
            changeset,
            repo: req.repo,
            target_ref: req.target_ref,
            request_key: req.request_key,
            deadline,
        };
        let (lands, access) = (Arc::clone(&self.lands), Arc::clone(&self.access));
        let (repo, target_ref) = (req.repo.clone(), req.target_ref.clone());
        // The job writes the land's own result lines, so the evidence
        // does not depend on this handler living: the job needs its own
        // copies of what the lines name.
        let (job_request_id, job_principal) = (request_id.clone(), principal.clone());
        let (job_repo, job_ref) = (repo.clone(), target_ref.clone());
        let audit = self.audit.clone();
        // The land's own result comes back inside an Ok, so this handler,
        // not the worker, builds the reply's Status: a land that ends in
        // an error moves the land counter, by the code of the reply the
        // caller saw.
        let ran = self
            .worker
            .run(move |store| {
                // The job's first statement claims the call's answer. If
                // the handler's drop won the race, the call is gone and
                // its drop has answered `cancelled`: run nothing, answer
                // nothing more.
                if answer.swap(LAND_STARTED, Ordering::AcqRel) == LAND_ABANDONED {
                    return Err(crate::error::Error::Cancelled(
                        "the call went before the land started".to_owned(),
                    ));
                }
                // The claim is a fact a test may wait for: the permit the
                // worker held said only that the job runs, not that it
                // claimed the call's answer.
                #[cfg(any(test, feature = "test-hooks"))]
                TEST_LAND_CLAIMED
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(req.request_key.clone());
                // A rebase keeps the changes, so these are the paths that the
                // land changes, on any ref of the repo. Checked on every call,
                // before the land records anything, also on a retry. Every
                // refusal becomes its one Status here: the audit's label
                // and the reply read the same value, so one error logs
                // once.
                let outcome: Result<land::LandOutcome, Status> = (|| {
                    let cs = read::get_changeset(&store, &req.repo, req.changeset)
                        .map_err(Status::from)?;
                    access
                        .check_write(&caller, &req.repo, cs.changes().keys())
                        .map_err(Status::from)?;
                    lands.land(&store, &caller, &req).map_err(Status::from)
                })();
                // The job that changed the state writes the state change's
                // own lines, after the commit and outside the write
                // transaction: the evidence does not depend on the call
                // that asked for the land living on. The worker's own
                // failures (a panic, a shutdown) built no land outcome,
                // and the worker logged them.
                Self::audit_land_outcome(
                    &audit,
                    &job_request_id,
                    &job_principal,
                    &job_repo,
                    &job_ref,
                    &outcome,
                );
                Ok(outcome)
            })
            .await;
        match ran {
            Ok(Ok(outcome)) => Ok(count_committed_reply(Response::new(land_response(outcome)))),
            Ok(Err(status)) => {
                self.metrics.land(code_label(status.code()));
                Err(status)
            }
            // The worker's own failures (a panic, a shutdown) built no
            // land outcome, and the worker logged them.
            Err(status) => Err(status),
        }
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
        Ok(count_reply(Response::new(land_response(outcome))))
    }

    async fn resolve_ref(
        &self,
        request: Request<ResolveRefRequest>,
    ) -> Result<Response<ResolveRefResponse>, Status> {
        resolve_ref(&self.worker, &self.access, &self.quotas, request)
            .await
            .map(|r| count_reply(Response::new(r)))
    }

    async fn get_changeset(
        &self,
        request: Request<GetChangesetRequest>,
    ) -> Result<Response<GetChangesetStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
        let req = request.into_inner();
        check_whole_reader(&self.access, &caller, &req.repo)?;
        let id: ChangesetId = parse_id(&req.changeset_id, "changeset_id")?;
        // The share is taken before the object is built, sized from the
        // stored body's length: the count of replies in flight bounds
        // the memory they hold, and an over-cap object is refused
        // before any of its bytes are read.
        let size = self
            .worker
            .run(move |store| {
                let stored =
                    store.read(|c| c.stored_changeset_body_lengths(&[id.as_bytes().to_vec()]))?;
                let len = stored
                    .iter()
                    .find(|(stored_id, _)| *stored_id == id.as_bytes())
                    .map_or(0, |(_, len)| usize::try_from(*len).unwrap_or(0));
                // A missing object surfaces in the build below, with
                // its real error.
                Ok(len)
            })
            .await?;
        let share = self.whole_share_of(size).await?;
        let data = self
            .worker
            .run(move |store| Ok(read::get_changeset(&store, &req.repo, id)?.encode()))
            .await?;
        Ok(mark_stream(Self::stream_whole_holding(
            call,
            data,
            share,
            stalled,
            |data| GetChangesetResponse { changeset: data },
        )))
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
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
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
                    None => read::file_blob(&store, &req.repo, id, &path, &deadline)?,
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
        Ok(mark_stream(Response::new(ReceiverStream::new(rx))))
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
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
        // One worker pass resolves the root and the path (a view root
        // is not cheap, so it is resolved once), and returns the
        // directory's id with its stored body's length: the body IS
        // the reply's bytes, so the length is exact. The share is
        // taken from that length, before the build reads the row it
        // already named; a changeset-pinned tree cannot change between
        // the two reads.
        let (dir_id, size) = self
            .worker
            .run(move |store| {
                let root = match &view {
                    Some((digest, prefixes, live)) => {
                        let repo_id = store.read(|c| c.repo_id(&req.repo))?;
                        views::view_root(
                            &store, repo_id, digest, id, prefixes, live, &metrics, &deadline,
                        )?
                    }
                    None => read::root_tree(&store, &req.repo, id, &deadline)?,
                };
                let dir_id = store.read(|c| match path.as_ref() {
                    None => Ok(root),
                    Some(path) => match crate::tree::lookup(c, root, path)? {
                        Some(scion_core::Entry::Dir(dir_id)) => Ok(dir_id),
                        Some(scion_core::Entry::File(_)) => {
                            Err(crate::Error::InvalidArgument(format!("{path:?} is a file")))
                        }
                        None => Err(crate::Error::NotFound(format!("path {path:?}"))),
                    },
                })?;
                let size = store.read(|c| c.tree_size(dir_id))?;
                Ok((dir_id, size))
            })
            .await?;
        let share = self.whole_share_of(size).await?;
        let data = self
            .worker
            .run(move |store| Ok(store.read(|c| c.get_tree(dir_id))?.encode()))
            .await?;
        Ok(mark_stream(Self::stream_whole_holding(
            call,
            data,
            share,
            stalled,
            |data| ListDirResponse { tree: data },
        )))
    }

    async fn log(&self, request: Request<LogRequest>) -> Result<Response<LogStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is taken
        // apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
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
        Ok(mark_stream(Response::new(ReceiverStream::new(rx))))
    }

    async fn blame(&self, request: Request<BlameRequest>) -> Result<Response<BlameStream>, Status> {
        let caller = caller(&request)?;
        let call = self.quotas.enter(&caller).map_err(Status::from)?;
        // The handle of the caller's connection, before the request is
        // taken apart: the reply counts what it leaves in HTTP/2 on it.
        let stalled = scion_proto::runtime::listen::Stalled::of(&request);
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
                author_name: author.name().to_vec(),
                author_email: author.email().to_vec(),
                author_unix_secs: author.unix_secs(),
                author_tz_offset_minutes: author.tz_offset_minutes(),
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
        Ok(mark_stream(Response::new(ReceiverStream::new(rx))))
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
        Ok(count_reply(Response::new(MergeBaseResponse {
            changeset_ids: bases.iter().map(|id| id.as_bytes().to_vec()).collect(),
        })))
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
        Ok(count_reply(Response::new(IsAncestorResponse {
            is_ancestor,
        })))
    }

    async fn diff(&self, request: Request<DiffRequest>) -> Result<Response<DiffResponse>, Status> {
        let caller = caller(&request)?;
        let _call = self.quotas.enter(&caller).map_err(Status::from)?;
        let deadline = Deadline::of_call(&request, REQUEST_TIMEOUT);
        let _deadline = deadline.guard();
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
                read::diff(
                    &store,
                    &req.repo,
                    old,
                    new,
                    path.as_ref(),
                    after.as_ref(),
                    &deadline,
                )
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
        Ok(count_reply(Response::new(DiffResponse {
            entries,
            next_after: path_bytes(page.next_after),
            renames_incomplete: page.renames_incomplete,
        })))
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
            crate::audit::Audit::off(),
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
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let blob = store.blobs().put(content).unwrap();
        let state = FileState::Regular {
            content: blob,
            executable: false,
        };
        let changeset = Changeset::new(
            vec![],
            IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                .expect("identity fits the format"),
            None,
            vec![],
            [(RepoPath::new("f").unwrap(), Some(state))].into(),
        )
        .expect("changeset fits the format");
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
        let budget = data.len();
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
        let budget = data.len();
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
        let caller = Caller(scion_proto::runtime::auth::DEFAULT_PRINCIPAL.into());
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
            crate::audit::Audit::off(),
        );
        // A valid changeset, which puts `f`: decode succeeds, the rules
        // deny the write. The check runs in the job, so this also shows
        // that the job stays in the call's span.
        let state = FileState::Regular {
            content: store.blobs().put(b"x").unwrap(),
            executable: false,
        };
        let cs = Changeset::new(
            vec![],
            IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                .expect("identity fits the format"),
            None,
            vec![],
            [(RepoPath::new("f").unwrap(), Some(state))].into(),
        )
        .expect("changeset fits the format");
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

    /// The store of the dropped-call tests: repo `r`, `main` at B, and a
    /// changeset on root, below the floor of a new path index at B, so
    /// the land's plan walks back.
    fn dropped_call_fixture() -> (tempfile::TempDir, Backend, ChangesetId, ChangesetId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let blob = store.blobs().put(b"x").unwrap();
        let put = |parents, path: &str| {
            Changeset::new(
                parents,
                IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                path.as_bytes().to_vec(),
                [(
                    RepoPath::new(path).unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )]
                .into(),
            )
            .expect("changeset fits the format")
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
        (dir, store, b, cs)
    }

    /// Waits until a land job claimed a call whose request key is `key`:
    /// the claim, not the worker permit, is what an abort must follow.
    async fn wait_land_claimed(key: &str, started: Instant) {
        loop {
            let claimed = TEST_LAND_CLAIMED
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|k| k == key);
            if claimed {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the job claimed the call"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Waits until the audit file holds a line with `outcome` and
    /// `marker`: the audit's writer answers a record only once the line
    /// is durable, so the file is the test's signal.
    async fn wait_audit_line(path: &std::path::Path, outcome: &str, marker: &str, what: &str) {
        let started = Instant::now();
        loop {
            let text = std::fs::read_to_string(path).unwrap();
            let line = text.lines().any(|line| {
                line.contains(&format!("\"outcome\":\"{outcome}\"")) && line.contains(marker)
            });
            if line {
                return;
            }
            assert!(started.elapsed() < Duration::from_secs(10), "{what}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_whose_call_is_dropped_stops_and_stays_pending() {
        let (_dir, store, b, cs) = dropped_call_fixture();
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
        // Only this test's land uses this key, so clearing here leaves
        // nothing for the wait below to read but this call's claim.
        TEST_LAND_CLAIMED
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        let request = as_default(scion_proto::v1::LandRequest {
            repo: "r".into(),
            changeset_id: cs.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "claim-wait".into(),
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

        // Abort only once the job claimed the call's answer: taking a
        // permit says only that the job runs, not that it claimed. An
        // abort in the window between the two wins the handler's drop,
        // the job answers nothing, and the land records no row at all.
        wait_land_claimed("claim-wait", started).await;

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
        let sql = "SELECT state FROM lands WHERE request_key = 'claim-wait'";
        let state: String = store
            .read(|c| {
                c.sqlite()
                    .unwrap()
                    .query_row(sql, [], |row| row.get(0))
                    .map_err(crate::error::from_sqlite)
            })
            .unwrap();
        assert_eq!(state, "pending");
        let main = store.read(|c| {
            let repo = c.repo_id("r")?;
            c.get_ref(repo, "main")
        });
        assert_eq!(main.unwrap().unwrap().target, b, "main did not move");
    }

    /// Parks one job on every worker permit, so the caller holds the
    /// whole worker: a land call started while the worker is full sits
    /// after its durable intent, before its claim. Returns the gates to
    /// drop to free the permits, and the parked tasks to join.
    async fn park_the_worker(
        service: &Arc<ScionService>,
    ) -> (
        Vec<std::sync::mpsc::Sender<()>>,
        Vec<tokio::task::JoinHandle<()>>,
    ) {
        let mut gates = Vec::new();
        let mut parked = Vec::new();
        for _ in 0..MAX_BLOCKING {
            let (free_tx, free_rx) = std::sync::mpsc::channel::<()>();
            gates.push(free_tx);
            parked.push(tokio::spawn({
                let service = Arc::clone(service);
                async move {
                    let _ = service
                        .worker
                        .run(move |_store| {
                            let _ = free_rx.recv();
                            Ok(())
                        })
                        .await;
                }
            }));
        }
        let started = Instant::now();
        while service.worker.permits.available_permits() > 0 {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the worker is full"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        (gates, parked)
    }

    /// A call aborted while its job could not claim it: the intent is
    /// durable, the job never claimed, the drop answers it `cancelled`,
    /// and no land row is recorded. This is the window the test above
    /// once aborted in by accident, when it aborted on the permit.
    #[tokio::test]
    async fn a_call_aborted_before_its_job_claims_it_answers_cancelled_and_records_nothing() {
        let (dir, store, b, cs) = dropped_call_fixture();
        let audit_path = dir.path().join("audit.log");
        let service = Arc::new(ScionService::new(
            Worker::new(store.clone()),
            Arc::new(Access::single("t")),
            Arc::default(),
            Quotas::off(),
            Arc::default(),
            crate::audit::Audit::open(&audit_path).unwrap(),
        ));
        let idle = || service.worker.permits.available_permits() == MAX_BLOCKING;

        // Park a job on every permit, so the land's job waits for one:
        // the call sits after its durable intent, before its claim.
        let (gates, parked) = park_the_worker(&service).await;

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
        // The intent is durable when the handler writes it, and no permit
        // was free, so the job cannot have claimed: this is the window
        // between the two, held open on purpose.
        wait_audit_line(
            &audit_path,
            "intent",
            "\"request_key\":\"k\"",
            "the intent lands",
        )
        .await;

        // The abort wins the answer: the drop records one cancelled line,
        // and the job, once it runs, sees the abandonment and records
        // nothing.
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        wait_audit_line(
            &audit_path,
            "failed",
            "\"why\":\"cancelled\"",
            "the drop answers the intent",
        )
        .await;
        // Free the permits: the queued job claims, sees the abandonment,
        // records nothing.
        drop(gates);
        for handle in parked {
            handle.await.unwrap();
        }
        let started = Instant::now();
        while !idle() {
            assert!(started.elapsed() < Duration::from_secs(10), "the job ends");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let sql = "SELECT state FROM lands WHERE request_key = 'k'";
        let row = store
            .read(|c| {
                c.sqlite()
                    .unwrap()
                    .query_row(sql, [], |row| row.get::<_, String>(0))
                    .map_err(crate::error::from_sqlite)
            })
            .unwrap_err();
        assert!(
            format!("{row:?}").contains("QueryReturnedNoRows"),
            "no land was recorded: {row:?}"
        );
        let text = std::fs::read_to_string(&audit_path).unwrap();
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("\"outcome\":\"intent\""))
                .count(),
            1,
            "one intent: {text}"
        );
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("\"outcome\":\"failed\""))
                .count(),
            1,
            "the drop's cancelled answer: {text}"
        );
        let main = store.read(|c| {
            let repo = c.repo_id("r")?;
            c.get_ref(repo, "main")
        });
        assert_eq!(main.unwrap().unwrap().target, b, "main did not move");
    }

    /// A call cut while its `CreateChangeset` hashed blobs: the job stops
    /// at its next blob, the dead call's answer is the abort, the audit
    /// gets its one failed line, and nothing is written.
    #[tokio::test]
    async fn a_create_cut_mid_check_stops_at_its_next_blob_and_records_nothing() {
        let (dir, store, repo_id, id, bytes, one) = create_cut_fixture();

        let audit_path = dir.path().join("audit.log");
        let service = Arc::new(ScionService::new(
            Worker::new(store.clone()),
            Arc::new(Access::single("t")),
            Arc::default(),
            Quotas::off(),
            Arc::default(),
            crate::audit::Audit::open(&audit_path).unwrap(),
        ));

        // The check parks after the first blob's keep; the call is cut
        // while it waits, and the park then lets the job reach the next
        // blob, whose check stops it.
        let hooks = dir.path().join("hooks");
        std::fs::create_dir_all(&hooks).unwrap();
        write::PARK_CHECK.arm(&hooks);
        std::fs::write(hooks.join(format!("park-{one}")), b"").unwrap();
        let call = tokio::spawn({
            let service = Arc::clone(&service);
            async move {
                service
                    .create_changeset(as_default(scion_proto::v1::CreateChangesetRequest {
                        repo: "r".into(),
                        changeset: bytes,
                    }))
                    .await
            }
        });
        let parked = hooks.join(format!("parked-{one}"));
        let started = Instant::now();
        while !parked.exists() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(10),
                "the check never parked"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        call.abort();
        // The cut ends the deadline when the aborted task's future drops
        // the call's guard, not when `abort` returns; joining the task
        // puts the drop before the park's lift, so the job's next check
        // reads the cut, never the instant before it.
        assert!(call.await.unwrap_err().is_cancelled());
        std::fs::remove_file(hooks.join(format!("park-{one}"))).unwrap();
        write::PARK_CHECK.lift();

        wait_audit_line(
            &audit_path,
            "failed",
            "\"why\":\"cancelled\"",
            "the job answers the cut call's intent",
        )
        .await;
        let text = std::fs::read_to_string(&audit_path).unwrap();
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("\"outcome\":\"intent\""))
                .count(),
            1,
            "one intent: {text}"
        );
        assert_eq!(
            text.lines()
                .filter(|l| l.contains("\"outcome\":\"failed\""))
                .count(),
            1,
            "one failed line: {text}"
        );
        let there = store.read(|c| c.has_changeset(repo_id, id));
        assert!(!there.unwrap(), "nothing was written");
    }

    /// A store whose repo `r` holds two stored blobs, a root on `main`,
    /// and the encoded `CreateChangeset` request that puts both blobs on
    /// top of the root: the blob check's raw material, with the id of the
    /// changeset it would write and its first put's blob.
    fn create_cut_fixture() -> (
        tempfile::TempDir,
        Backend,
        crate::meta::RepoId,
        ChangesetId,
        Vec<u8>,
        scion_core::BlobId,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let one = store.blobs().put(b"one\n").unwrap();
        let two = store.blobs().put(b"two\n").unwrap();
        let state = |blob| {
            Some(FileState::Regular {
                content: blob,
                executable: false,
            })
        };
        // The parent the changeset hangs off, through the seam, as the
        // dropped-call fixture does.
        let root = Changeset::new(
            vec![],
            IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                .expect("changeset fits the format"),
            None,
            b"root".to_vec(),
            [
                (RepoPath::new("seed").unwrap(), state(one)),
                (RepoPath::new("seed2").unwrap(), state(two)),
            ]
            .into(),
        )
        .expect("changeset fits the format");
        let (root, repo_id) = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                // The puts' blobs must be the repo's, as an upload makes
                // them: otherwise the member check refuses before the
                // blob check ever runs.
                c.add_repo_blobs(repo, &[one, two])?;
                let root = c.insert_changeset(repo, &root)?;
                assert!(c.cas_ref(repo, "main", None, root)?);
                Ok((root, repo))
            })
            .unwrap();
        let cs = Changeset::new(
            vec![root],
            IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                .expect("changeset fits the format"),
            None,
            b"two puts".to_vec(),
            [
                (RepoPath::new("a").unwrap(), state(one)),
                (RepoPath::new("b").unwrap(), state(two)),
            ]
            .into(),
        )
        .expect("changeset fits the format");
        let bytes = cs.encode();
        let (_, id) = write::decode(&bytes).unwrap();
        (dir, store, repo_id, id, bytes, one)
    }

    /// A service whose land counters the test reads: the service and its
    /// land worker share one metrics registry.
    fn counting_service(store: &Backend, metrics: &Arc<Metrics>) -> ScionService {
        ScionService::new(
            Worker::new(store.clone()),
            Arc::new(Access::single("t")),
            Arc::new(Lands::new(None, Some(Arc::clone(metrics)))),
            Quotas::off(),
            Arc::clone(metrics),
            crate::audit::Audit::off(),
        )
    }

    /// A land whose call already ran past its deadline moves the land
    /// counter for its outcome: the caller read `deadline_exceeded`, and
    /// the operator sees the failed land.
    #[tokio::test]
    async fn a_land_whose_call_failed_moves_its_counter() {
        // main moved past the changeset's parent, so the land's plan has
        // a walk to make: the spent deadline stops it at its first check.
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let blob = store.blobs().put(b"x").unwrap();
        let put = |parents, path: &str| {
            Changeset::new(
                parents,
                IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                path.as_bytes().to_vec(),
                [(
                    RepoPath::new(path.as_bytes().to_vec()).unwrap(),
                    Some(FileState::Regular {
                        content: blob,
                        executable: false,
                    }),
                )]
                .into(),
            )
            .expect("changeset fits the format")
        };
        let cs = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                let root = c.insert_changeset(repo, &put(vec![], "a"))?;
                let b = c.insert_changeset(repo, &put(vec![root], "b"))?;
                assert!(c.cas_ref(repo, "main", None, b)?);
                c.insert_changeset(repo, &put(vec![root], "c"))
            })
            .unwrap();
        let metrics = Arc::new(Metrics::default());
        let service = counting_service(&store, &metrics);
        let mut request = as_default(scion_proto::v1::LandRequest {
            repo: "r".into(),
            changeset_id: cs.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k".into(),
        });
        // A timeout the call has already spent: the request's
        // grpc-timeout header, one nanosecond.
        request
            .metadata_mut()
            .insert("grpc-timeout", "1n".parse().unwrap());
        let status = service.land(request).await.expect_err("past deadline");
        assert_eq!(status.code(), Code::DeadlineExceeded);
        let text = metrics.render();
        assert!(
            text.contains("scion_lands_total{outcome=\"deadline_exceeded\"} 1\n"),
            "{text}"
        );
        assert!(!text.contains("outcome=\"landed\""), "{text}");
    }

    /// A land whose call fails on a full disk moves the land counter for
    /// the class the caller read: the server is out of space, so the
    /// reply is `resource_exhausted`, and the operator sees the failed
    /// land.
    #[tokio::test]
    async fn a_land_that_hits_a_full_disk_moves_its_counter() {
        let (_dir, store, cs) = landed_store();
        if Backend::test_is_postgres() {
            // The page cap is a data-directory hook; the Postgres disk
            // full is the SQLSTATE 53100 arm of the pg mapping, a unit
            // test of pg.rs, and it feeds the same counter code.
            return;
        }
        let store = store
            .as_sqlite()
            .expect("the page cap is a data-directory test");
        // The land's own changeset must grow the file past its cap: two
        // thousand files, far past any space the setup left free.
        store.cap_database_pages(0);
        let metrics = Arc::new(Metrics::default());
        let service = counting_service(&Backend::sqlite(store.clone()), &metrics);
        let request = as_default(scion_proto::v1::LandRequest {
            repo: "r".into(),
            changeset_id: cs.as_bytes().to_vec(),
            target_ref: "main".into(),
            request_key: "k".into(),
        });
        let status = service.land(request).await.expect_err("out of space");
        assert_eq!(status.code(), Code::ResourceExhausted);
        let text = metrics.render();
        assert!(
            text.contains("scion_lands_total{outcome=\"resource_exhausted\"} 1\n"),
            "{text}"
        );
    }

    /// A repo with a root changeset under `main`, and a child changeset
    /// ready to land: two thousand files, so its landed changeset must
    /// grow the metadata file.
    fn landed_store() -> (tempfile::TempDir, Backend, ChangesetId) {
        let dir = tempfile::tempdir().unwrap();
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
        let blob = store.blobs().put(b"x").unwrap();
        let mut files = Vec::new();
        for i in 0..2000 {
            files.push((
                RepoPath::new(format!("f{i:04}").into_bytes()).unwrap(),
                Some(FileState::Regular {
                    content: blob,
                    executable: false,
                }),
            ));
        }
        let changeset = |parents, changes| {
            Changeset::new(
                parents,
                IdentityTime::new(b"t".to_vec(), b"t@x".to_vec(), 0, 0)
                    .expect("identity fits the format"),
                None,
                vec![],
                changes,
            )
            .expect("changeset fits the format")
        };
        let cs = store
            .write(|c| {
                let repo = c.create_repo("r")?;
                let root = c.insert_changeset(repo, &changeset(vec![], [].into()))?;
                assert!(c.cas_ref(repo, "main", None, root)?);
                c.insert_changeset(repo, &changeset(vec![root], files.into_iter().collect()))
            })
            .unwrap();
        (dir, store, cs)
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
        let store = Backend::sqlite(Store::open(dir.path()).unwrap());
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
            // A liveness bound, not a speed claim: only a call that
            // never takes its slot fails here. A slow runner (the arm64
            // debug build) may take a while to schedule the jobs; 120 s
            // is generous to it and still fails a hang in a test's
            // time.
            assert!(started.elapsed() < Duration::from_secs(120), "the jobs run");
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
            // A liveness bound, not a speed claim: a parked job waits
            // for a read connection the blockers hold, then runs a
            // handful of queries; only a starved or deadlocked call
            // fails a 120 s timeout, on any runner's speed.
            let answer = tokio::time::timeout(Duration::from_secs(120), call)
                .await
                .expect("every call ends")
                .unwrap();
            assert!(answer, "every call answers");
        }
    }

    #[tokio::test]
    async fn a_worker_panic_does_not_reach_the_client() {
        let dir = tempfile::tempdir().unwrap();
        let worker = Worker::new(Backend::sqlite(Store::open(dir.path()).unwrap()));
        let status = worker
            .run(|_| -> Result<()> { panic!("secret detail") })
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "internal error");
    }
}

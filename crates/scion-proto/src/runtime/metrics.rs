//! Metrics for an operator: counters and one latency histogram per RPC,
//! exported in the Prometheus text format on a plain HTTP listener.
//!
//! The registry is fixed arrays of atomics: the RPCs of the two services
//! and the gRPC codes are known at compile time, so a request never takes
//! a lock or allocates. A tower layer counts, and the HTTP export renders
//! the text ([`serve`]). The server and a tier both use this module: the
//! tier counts the `Objects` calls in the same registry and adds its two
//! cache counters.

use std::fmt::Write as _;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::net::TcpListener;

/// The RPCs that can be counted, as `service/method`. A request outside
/// the list (a bad path) counts nothing.
const RPCS: [&str; 16] = [
    "Scion/PutBlob",
    "Scion/CreateChangeset",
    "Scion/Land",
    "Scion/GetLand",
    "Scion/ResolveRef",
    "Scion/GetChangeset",
    "Scion/ReadFile",
    "Scion/ListDir",
    "Scion/Log",
    "Scion/Blame",
    "Scion/MergeBase",
    "Scion/IsAncestor",
    "Scion/Diff",
    "Objects/ResolveRef",
    "Objects/GetRootTree",
    "Objects/GetObjects",
];

/// The gRPC codes a reply can end with, by number, plus the ones that are
/// not a code: `ok` is code 0, and `cancelled` is what a call whose client
/// went away before its trailers counts as.
const CODES: [&str; 18] = [
    "ok",
    "cancelled",
    "unknown",
    "invalid_argument",
    "deadline_exceeded",
    "not_found",
    "already_exists",
    "permission_denied",
    "resource_exhausted",
    "failed_precondition",
    "aborted",
    "out_of_range",
    "unimplemented",
    "internal",
    "unavailable",
    "data_loss",
    "unauthenticated",
    "unrecognised",
];

/// The upper bounds of the histogram buckets, in microseconds: 1, 4, 16,
/// 64, 256 ms, 1, 4, 16, 64 s, and the infinite bucket.
const BUCKETS: [u64; 10] = [
    1_000,
    4_000,
    16_000,
    64_000,
    256_000,
    1_000_000,
    4_000_000,
    16_000_000,
    64_000_000,
    u64::MAX,
];

/// The outcomes of a land, by how the server's land path decides them,
/// then the classes of a land whose call ended in an error: the names of
/// the gRPC codes the land's reply can answer.
const LANDS: [&str; 14] = [
    "landed",
    "conflict",
    "rejected",
    "retry",
    "aborted",
    "deadline_exceeded",
    "resource_exhausted",
    "unavailable",
    "not_found",
    "invalid_argument",
    "permission_denied",
    "failed_precondition",
    "cancelled",
    "internal",
];

/// The land counter's label for a land whose call ended in an error, by
/// the gRPC code of the reply the caller saw: one source, the reply's own
/// code, so the counter cannot drift from what callers read. The label is
/// the text the registry renders, so a renumbering of the table cannot
/// change a label's meaning. The land handler counts after it builds the
/// reply's `Status`.
#[must_use]
pub fn code_label(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Aborted => "aborted",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::NotFound => "not_found",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::FailedPrecondition => "failed_precondition",
        // The call went while its land was planned: the land stays pending,
        // and a retry with the same request key resumes it.
        tonic::Code::Cancelled => "cancelled",
        // Every other code the land's reply can answer, the caller reads
        // as an internal error.
        _ => "internal",
    }
}

/// What the processes count. Every field is atomics: a request only adds.
#[derive(Default)]
pub struct Metrics {
    /// Requests by RPC and the code their reply ended with.
    requests: [[AtomicU64; CODES.len()]; RPCS.len()],
    /// Latency by RPC: each bucket holds the replies that ended at or
    /// below its bound, and the last two hold the sum in microseconds and
    /// the count.
    latency: [[AtomicU64; BUCKETS.len() + 2]; RPCS.len()],
    /// Bytes of requests and replies by RPC.
    bytes_in: [AtomicU64; RPCS.len()],
    bytes_out: [AtomicU64; RPCS.len()],
    /// Lands by outcome.
    lands: [AtomicU64; LANDS.len()],
    /// Lands whose ref moved under them and were planned again.
    land_replans: AtomicU64,
    /// Objects a tier served from its store, and fetched from upstream.
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    /// Real trees a view derivation fetched, folders of a partial
    /// reader's view (folder read rules).
    view_trees_walked: AtomicU64,
    /// Durations of a blob batch's full flush of the drive cache, in
    /// microseconds, as [`scion_blob_sync_seconds`].
    blob_sync: [AtomicU64; BUCKETS.len() + 2],
    /// Waits for the collector's advisory key before a Postgres write
    /// transaction, in microseconds.
    collector_key_wait: [AtomicU64; BUCKETS.len() + 2],
    /// Free bytes of the filesystem that holds the data directory, as the
    /// latest poll saw them (`data_free_set` says whether any poll ran).
    data_free_bytes: AtomicU64,
    data_free_set: std::sync::atomic::AtomicBool,
    /// Derive walks in flight, and walks waiting for a permit.
    derive_walks_in_flight: AtomicU64,
    derive_walks_waiting: AtomicU64,
    /// The Postgres reader pool: the time calls waited for a reader, as
    /// the same buckets as `latency`, with the sum and the count. The
    /// last two fields are the gauges: readers out of the pool, and the
    /// age of the oldest open read transaction, both in microseconds.
    pg_pool_wait: [AtomicU64; BUCKETS.len() + 2],
    pg_readers_busy: AtomicU64,
    pg_tx_age: AtomicU64,
    /// Bytes that stalled replies of live connections hold, as one
    /// [`crate::STALLED_REPLY_BYTES`]-sized estimate per stalled reply,
    /// counted by [`crate::runtime::listen::Stalled`] and given back when
    /// the stalled reply's stream ends.
    stalled_reply_bytes: AtomicU64,
    /// Calls the counting layer has seen whose reply body it has not
    /// given back yet: the calls a drain still waits for, as the
    /// [`scion_calls_in_flight`] gauge. A call that never reaches a
    /// counting body (the future is dropped before the reply) is given
    /// back by the same guard that counted it.
    calls_in_flight: AtomicU64,
}

/// A metric name, as the index of its RPC.
#[derive(Clone, Copy)]
pub struct Rpc(usize);

impl Metrics {
    /// The counter of the RPC at `path` (like `/scion.v1.Scion/Blame`).
    #[must_use]
    pub fn rpc(&self, path: &str) -> Option<Rpc> {
        let (service, method) = path.rsplit_once('/')?;
        let service = service.rsplit('.').next()?;
        RPCS.iter()
            .position(|known| *known == format!("{service}/{method}"))
            .map(Rpc)
    }

    /// Counts one request of `rpc` that ended with the gRPC code `code`
    /// (0 is `ok`), `started` when the request arrived, and how many bytes
    /// it moved.
    pub fn request(&self, rpc: Rpc, code: u32, started: Instant, bytes_in: u64, bytes_out: u64) {
        let code = code as usize;
        let code = if code < CODES.len() - 1 {
            code
        } else {
            // A code this binary does not know.
            CODES.len() - 1
        };
        self.requests[rpc.0][code].fetch_add(1, Ordering::Relaxed);
        let micros = elapsed_micros(started);
        let buckets = &self.latency[rpc.0];
        for (i, bound) in BUCKETS.iter().enumerate() {
            if micros <= *bound {
                buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        buckets[BUCKETS.len()].fetch_add(micros, Ordering::Relaxed);
        buckets[BUCKETS.len() + 1].fetch_add(1, Ordering::Relaxed);
        self.bytes_in[rpc.0].fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out[rpc.0].fetch_add(bytes_out, Ordering::Relaxed);
    }

    /// Counts a call whose client went away before its trailers: no code
    /// arrived, so it counts as `cancelled`.
    pub fn cancelled(&self, rpc: Rpc, started: Instant, bytes_in: u64, bytes_out: u64) {
        self.requests[rpc.0][1].fetch_add(1, Ordering::Relaxed);
        let micros = elapsed_micros(started);
        let buckets = &self.latency[rpc.0];
        for (i, bound) in BUCKETS.iter().enumerate() {
            if micros <= *bound {
                buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        buckets[BUCKETS.len()].fetch_add(micros, Ordering::Relaxed);
        buckets[BUCKETS.len() + 1].fetch_add(1, Ordering::Relaxed);
        self.bytes_in[rpc.0].fetch_add(bytes_in, Ordering::Relaxed);
        self.bytes_out[rpc.0].fetch_add(bytes_out, Ordering::Relaxed);
    }

    /// Counts one call the counting layer has taken: the gauge rises at
    /// the call and falls when its reply body ends, so it is the set of
    /// calls a drain still waits for.
    pub(crate) fn call_started(&self) {
        self.calls_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn call_finished(&self) {
        let held = self.calls_in_flight.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(held > 0, "the in-flight gauge fell under zero");
    }

    /// The gauge of calls whose reply body has not ended: what a drain
    /// still waits for.
    #[must_use]
    pub fn calls_in_flight(&self) -> u64 {
        self.calls_in_flight.load(Ordering::Relaxed)
    }

    /// Counts one land that ended as `outcome`: how the server's land path
    /// decided it, or the class of the error it ended in, as the label the
    /// registry renders. A label outside the table counts nothing.
    pub fn land(&self, outcome: &'static str) {
        if let Some(at) = LANDS.iter().position(|known| *known == outcome) {
            self.lands[at].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Counts one land whose ref moved under it, so it plans again.
    pub fn land_replan(&self) {
        self.land_replans.fetch_add(1, Ordering::Relaxed);
    }

    /// The replan counter, for a test that reads it without a scrape.
    #[must_use]
    pub fn land_replans(&self) -> u64 {
        self.land_replans.load(Ordering::Relaxed)
    }

    /// Counts an object a tier served from its store.
    pub fn cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts an object a tier fetched from upstream.
    pub fn cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds one full flush of the drive cache for a blob batch, `micros`
    /// long. Callers that batch at the same time share one flush, so each
    /// caller's duration includes the wait for the flush in progress.
    pub fn blob_sync(&self, micros: u64) {
        add_hist(&self.blob_sync, micros);
    }

    /// Adds one wait for the collector's advisory key, which every
    /// Postgres write transaction takes before it writes.
    pub fn collector_key_wait(&self, micros: u64) {
        add_hist(&self.collector_key_wait, micros);
    }

    /// Sets the free bytes of the filesystem that holds the data
    /// directory, from the latest poll.
    pub fn data_free_bytes(&self, bytes: u64) {
        self.data_free_bytes.store(bytes, Ordering::Relaxed);
        self.data_free_set.store(true, Ordering::Relaxed);
    }

    /// Counts one derive walk that took its permit.
    pub fn derive_walk_started(&self) {
        self.derive_walks_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one derive walk that gave its permit back.
    pub fn derive_walk_ended(&self) {
        self.derive_walks_in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Counts one walk that waits for a permit; the pair brackets each
    /// wait, so the gauge is the walks in the wait, not ever waited.
    pub fn derive_walk_wait_started(&self) {
        self.derive_walks_waiting.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one walk that got a permit or gave up waiting.
    pub fn derive_walk_wait_ended(&self) {
        self.derive_walks_waiting.fetch_sub(1, Ordering::Relaxed);
    }

    /// Counts `n` real trees a view derivation fetched.
    pub fn view_tree_walked(&self, n: u64) {
        self.view_trees_walked.fetch_add(n, Ordering::Relaxed);
    }

    /// The view-walk counter, for a test that reads it without a scrape.
    #[must_use]
    pub fn view_trees_walked(&self) -> u64 {
        self.view_trees_walked.load(Ordering::Relaxed)
    }

    /// Records one wait for a Postgres reader that took `waited`.
    pub fn pg_pool_wait(&self, waited: std::time::Duration) {
        let micros = u64::try_from(waited.as_micros()).unwrap_or(u64::MAX);
        for (i, bound) in BUCKETS.iter().enumerate() {
            if micros <= *bound {
                self.pg_pool_wait[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.pg_pool_wait[BUCKETS.len()].fetch_add(micros, Ordering::Relaxed);
        self.pg_pool_wait[BUCKETS.len() + 1].fetch_add(1, Ordering::Relaxed);
    }

    /// Sets the gauge of Postgres read connections that are out of the
    /// pool.
    pub fn pg_readers_busy(&self, busy: u64) {
        self.pg_readers_busy.store(busy, Ordering::Relaxed);
    }

    /// Sets the gauge of the age of the oldest open read transaction.
    pub fn pg_tx_age(&self, age: std::time::Duration) {
        self.pg_tx_age.store(
            u64::try_from(age.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// The cache counters, for a test that reads them without a scrape.
    #[must_use]
    pub fn cache_counts(&self) -> (u64, u64) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }

    /// Counts `bytes` more that stalled replies hold, in HTTP/2 on their
    /// connections.
    pub(crate) fn stalled_reply_held(&self, bytes: u64) {
        self.stalled_reply_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Gives `bytes` of the stalled replies back: the stalled reply's
    /// stream ended, so its parts went. One count is given back once, so a
    /// give below zero means a count was given back twice.
    pub(crate) fn stalled_reply_freed(&self, bytes: u64) {
        let held = self.stalled_reply_bytes.fetch_sub(bytes, Ordering::Relaxed);
        assert!(
            held >= bytes,
            "the stalled-reply gauge fell under zero: {held} held, {bytes} given back"
        );
    }

    /// What stalled replies hold now, for the tests that count them.
    #[cfg(test)]
    pub(crate) fn stalled_reply_bytes(&self) -> u64 {
        self.stalled_reply_bytes.load(Ordering::Relaxed)
    }

    /// Renders the registry as the Prometheus text format.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = String::new();
        self.render_requests(&mut text);
        self.render_latency(&mut text);
        self.render_bytes(&mut text);
        self.render_lands(&mut text);
        self.render_view_walks(&mut text);
        self.render_land_replans(&mut text);
        self.render_cache(&mut text);
        self.render_blob_sync(&mut text);
        self.render_collector_key_wait(&mut text);
        if self.data_free_set.load(Ordering::Relaxed) {
            let _ = writeln!(
                text,
                "# HELP scion_data_free_bytes Free bytes of the filesystem that holds the data directory.\n\
                 # TYPE scion_data_free_bytes gauge\n\
                 scion_data_free_bytes {}",
                self.data_free_bytes.load(Ordering::Relaxed)
            );
        }
        self.render_derive_walks(&mut text);
        self.render_pg(&mut text);
        self.render_stalled(&mut text);
        text
    }

    /// The per-RPC latency histograms, one series per RPC that counted.
    fn render_latency(&self, text: &mut String) {
        use std::fmt::Write as _;
        text.push_str("# HELP scion_request_seconds A request's duration by RPC, in seconds.\n");
        text.push_str("# TYPE scion_request_seconds histogram\n");
        for (rpc, known) in RPCS.iter().enumerate() {
            let buckets = &self.latency[rpc];
            if buckets[BUCKETS.len() + 1].load(Ordering::Relaxed) == 0 {
                continue;
            }
            for (i, bound) in BUCKETS.iter().enumerate() {
                let le = if *bound == u64::MAX {
                    "+Inf".to_owned()
                } else {
                    format!("{}", seconds(*bound))
                };
                let _ = writeln!(
                    text,
                    "scion_request_seconds_bucket{{rpc=\"{known}\",le=\"{le}\"}} {}",
                    buckets[i].load(Ordering::Relaxed)
                );
            }
            let count = buckets[BUCKETS.len() + 1].load(Ordering::Relaxed);
            let sum = buckets[BUCKETS.len()].load(Ordering::Relaxed);
            let _ = writeln!(
                text,
                "scion_request_seconds_sum{{rpc=\"{known}\"}} {}",
                seconds(sum)
            );
            let _ = writeln!(
                text,
                "scion_request_seconds_count{{rpc=\"{known}\"}} {count}"
            );
        }
    }

    /// The per-RPC byte counters, for the RPCs that counted any.
    fn render_bytes(&self, text: &mut String) {
        use std::fmt::Write as _;
        text.push_str("# HELP scion_rpc_bytes_total Request and reply bytes by RPC.\n");
        text.push_str("# TYPE scion_rpc_bytes_total counter\n");
        for (rpc, known) in RPCS.iter().enumerate() {
            let bytes_in = self.bytes_in[rpc].load(Ordering::Relaxed);
            let bytes_out = self.bytes_out[rpc].load(Ordering::Relaxed);
            if bytes_in > 0 {
                let _ = writeln!(
                    text,
                    "scion_rpc_bytes_total{{rpc=\"{known}\",direction=\"in\"}} {bytes_in}"
                );
            }
            if bytes_out > 0 {
                let _ = writeln!(
                    text,
                    "scion_rpc_bytes_total{{rpc=\"{known}\",direction=\"out\"}} {bytes_out}"
                );
            }
        }
    }

    /// The request counter, by RPC and code, for the counted calls.
    fn render_requests(&self, text: &mut String) {
        use std::fmt::Write as _;
        text.push_str("# HELP scion_requests_total Requests by RPC and code.\n");
        text.push_str("# TYPE scion_requests_total counter\n");
        for (rpc, known) in RPCS.iter().enumerate() {
            for (code, name) in CODES.iter().enumerate() {
                let value = self.requests[rpc][code].load(Ordering::Relaxed);
                if value > 0 {
                    let _ = writeln!(
                        text,
                        "scion_requests_total{{rpc=\"{known}\",code=\"{name}\"}} {value}"
                    );
                }
            }
        }
    }

    /// The land, view-walk, replan and cache series.
    fn render_lands(&self, text: &mut String) {
        use std::fmt::Write as _;
        text.push_str("# HELP scion_lands_total Lands by outcome.\n");
        text.push_str("# TYPE scion_lands_total counter\n");
        for (outcome, name) in LANDS.iter().enumerate() {
            let value = self.lands[outcome].load(Ordering::Relaxed);
            if value > 0 {
                let _ = writeln!(text, "scion_lands_total{{outcome=\"{name}\"}} {value}");
            }
        }
    }

    /// The view-walk counter.
    fn render_view_walks(&self, text: &mut String) {
        use std::fmt::Write as _;
        let _ = writeln!(
            text,
            "# HELP scion_view_trees_walked_total Real trees a view derivation fetched.\n\
             # TYPE scion_view_trees_walked_total counter\n\
             scion_view_trees_walked_total {}",
            self.view_trees_walked.load(Ordering::Relaxed)
        );
    }

    /// The replan counter, when any land planned again.
    fn render_land_replans(&self, text: &mut String) {
        use std::fmt::Write as _;
        let replans = self.land_replans.load(Ordering::Relaxed);
        if replans > 0 {
            let _ = writeln!(
                text,
                "# HELP scion_land_replans_total Lands whose ref moved under them, planned again.\n\
                 # TYPE scion_land_replans_total counter\n\
                 scion_land_replans_total {replans}"
            );
        }
    }

    /// The cache counters, while a tier served or fetched anything.
    fn render_cache(&self, text: &mut String) {
        use std::fmt::Write as _;
        text.push_str("# HELP scion_cache_hits_total Objects a tier served from its store.\n");
        text.push_str("# TYPE scion_cache_hits_total counter\n");
        text.push_str("# HELP scion_cache_misses_total Objects a tier fetched from upstream.\n");
        text.push_str("# TYPE scion_cache_misses_total counter\n");
        for (name, counter) in [("hits", &self.cache_hits), ("misses", &self.cache_misses)] {
            let value = counter.load(Ordering::Relaxed);
            if value > 0 {
                let _ = writeln!(text, "scion_cache_{name}_total {value}");
            }
        }
    }

    /// The blob-sync histogram, when a flush ran.
    fn render_blob_sync(&self, text: &mut String) {
        let help = "One fsync attempt on the blob write path, failed ones \
                    included: a temp file, a stored file, a blob directory, \
                    or the drive-cache flush that concurrent callers share.";
        render_hist(text, "scion_blob_sync_seconds", help, &self.blob_sync);
    }

    /// The collector-key-wait histogram, when a Postgres write waited.
    fn render_collector_key_wait(&self, text: &mut String) {
        let help = "A write transaction's wait for the collector's advisory key \
                    (Postgres).";
        render_hist(
            text,
            "scion_pg_collector_key_wait_seconds",
            help,
            &self.collector_key_wait,
        );
    }

    /// The derive-walk gauges, while walks hold or wait for permits.
    fn render_derive_walks(&self, text: &mut String) {
        use std::fmt::Write as _;
        for (name, help, gauge) in [
            (
                "scion_derive_walks_in_flight",
                "Derive walks holding a permit now.",
                &self.derive_walks_in_flight,
            ),
            (
                "scion_derive_walks_waiting",
                "Derive walks waiting for a permit.",
                &self.derive_walks_waiting,
            ),
        ] {
            let value = gauge.load(Ordering::Relaxed);
            if value > 0 {
                let _ = writeln!(
                    text,
                    "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}"
                );
            }
        }
    }

    /// The Postgres reader-pool metrics: the wait histogram and the two
    /// gauges. [`Metrics::render`] hands the text over.
    fn render_pg(&self, text: &mut String) {
        text.push_str(
            "# HELP scion_pg_pool_wait_seconds Time a call waited for a Postgres reader, in seconds.\n",
        );
        text.push_str("# TYPE scion_pg_pool_wait_seconds histogram\n");
        let buckets = &self.pg_pool_wait;
        if buckets[BUCKETS.len() + 1].load(Ordering::Relaxed) > 0 {
            for (i, bound) in BUCKETS.iter().enumerate() {
                let le = if *bound == u64::MAX {
                    "+Inf".to_owned()
                } else {
                    format!("{}", seconds(*bound))
                };
                let _ = writeln!(
                    text,
                    "scion_pg_pool_wait_seconds_bucket{{le=\"{le}\"}} {}",
                    buckets[i].load(Ordering::Relaxed)
                );
            }
            let count = buckets[BUCKETS.len() + 1].load(Ordering::Relaxed);
            let sum = buckets[BUCKETS.len()].load(Ordering::Relaxed);
            let _ = writeln!(text, "scion_pg_pool_wait_seconds_sum {}", seconds(sum));
            let _ = writeln!(text, "scion_pg_pool_wait_seconds_count {count}");
        }
        text.push_str(
            "# HELP scion_pg_readers_busy Postgres read connections of the pool that are in use.\n",
        );
        text.push_str("# TYPE scion_pg_readers_busy gauge\n");
        let _ = writeln!(
            text,
            "scion_pg_readers_busy {}",
            self.pg_readers_busy.load(Ordering::Relaxed)
        );
        text.push_str(
            "# HELP scion_pg_tx_age_seconds Age of the oldest open Postgres read transaction, in seconds.\n",
        );
        text.push_str("# TYPE scion_pg_tx_age_seconds gauge\n");
        let _ = writeln!(
            text,
            "scion_pg_tx_age_seconds {}",
            seconds(self.pg_tx_age.load(Ordering::Relaxed))
        );
    }

    /// What the stalled replies of the process's connections hold. Nothing
    /// renders while no reply is stalled.
    fn render_stalled(&self, text: &mut String) {
        use std::fmt::Write as _;
        let value = self.stalled_reply_bytes.load(Ordering::Relaxed);
        if value > 0 {
            let _ = writeln!(
                text,
                "# HELP scion_stalled_reply_bytes Bytes that stalled replies hold in HTTP/2, as one estimate per stalled reply.\n\
                 # TYPE scion_stalled_reply_bytes gauge\n\
                 scion_stalled_reply_bytes {value}"
            );
        }
        // The in-flight gauge always renders: a drain watcher reads it
        // before and during the stop, and zero is a reading.
        let _ = writeln!(
            text,
            "# HELP scion_calls_in_flight Calls whose reply body has not ended, as the count a drain still waits for.\n\
             # TYPE scion_calls_in_flight gauge\n\
             scion_calls_in_flight {}",
            self.calls_in_flight.load(Ordering::Relaxed)
        );
    }
}

/// One histogram series without labels, in the Prometheus text shape:
/// every bucket, then the sum and the count. Nothing renders while the
/// series counted nothing.
fn render_hist(text: &mut String, name: &str, help: &str, hist: &[AtomicU64]) {
    use std::fmt::Write as _;
    if hist[BUCKETS.len() + 1].load(Ordering::Relaxed) == 0 {
        return;
    }
    let _ = writeln!(text, "# HELP {name} {help}\n# TYPE {name} histogram");
    for (i, bound) in BUCKETS.iter().enumerate() {
        let le = if *bound == u64::MAX {
            "+Inf".to_owned()
        } else {
            format!("{}", seconds(*bound))
        };
        let _ = writeln!(
            text,
            "{name}_bucket{{le=\"{le}\"}} {}",
            hist[i].load(Ordering::Relaxed)
        );
    }
    let count = hist[BUCKETS.len() + 1].load(Ordering::Relaxed);
    let sum = hist[BUCKETS.len()].load(Ordering::Relaxed);
    let _ = writeln!(text, "{name}_sum {}", seconds(sum));
    let _ = writeln!(text, "{name}_count {count}");
}

/// Adds `micros` to every bucket of `hist` whose bound covers it, to the
/// sum, and to the count: the shape of the per-RPC latency code, without
/// the RPC label.
fn add_hist(hist: &[AtomicU64], micros: u64) {
    for (i, bound) in BUCKETS.iter().enumerate() {
        if micros <= *bound {
            hist[i].fetch_add(1, Ordering::Relaxed);
        }
    }
    hist[BUCKETS.len()].fetch_add(micros, Ordering::Relaxed);
    hist[BUCKETS.len() + 1].fetch_add(1, Ordering::Relaxed);
}

/// A call's duration in microseconds, saturated at `u64::MAX`.
fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

/// A microsecond count as seconds, the unit Prometheus exports. The
/// conversion loses precision past 2^53 microseconds (285 years): fine for
/// a sum of call durations.
#[allow(clippy::cast_precision_loss)]
fn seconds(micros: u64) -> f64 {
    micros as f64 / 1_000_000.0
}

/// How long the export waits for a request's headers: a scraper sends
/// them at once, and a silent connection cannot hold the export.
const HEADER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Refuses `address` for the metrics listener, which is plain HTTP with no
/// token, unless `allow_insecure` says otherwise.
///
/// # Errors
///
/// `address` is not loopback and `allow_insecure` is false.
pub fn loopback_or_allow(
    address: std::net::SocketAddr,
    allow_insecure: bool,
) -> Result<(), String> {
    if address.ip().is_loopback() || allow_insecure {
        Ok(())
    } else {
        Err(format!(
            "{address} is not a loopback address; the metrics travel unencrypted \
             over plain HTTP. Pass --insecure-listen to allow it."
        ))
    }
}

/// The drain bound, for a binary that serves until a signal: when
/// `drain_started` resolves (the first signal's watch), the in-flight
/// calls have `timeout` to finish. On expiry the calls still running are
/// cut, the log names the count, and the process exits non-zero, so a
/// supervisor restarts it instead of waiting on a call that never ends.
/// A second signal cuts at once, because the same watch ends the
/// caller's own serve future.
pub fn spawn_drain_bound(
    mut drain_started: tokio::sync::watch::Receiver<()>,
    timeout: std::time::Duration,
    metrics: std::sync::Arc<Metrics>,
) {
    tokio::spawn(async move {
        let _ = drain_started.changed().await;
        tokio::time::sleep(timeout).await;
        let open = metrics.calls_in_flight();
        tracing::warn!(
            open,
            "the drain timeout passed; the calls still running were cut"
        );
        std::process::exit(1);
    });
}

/// Resolves when the process is asked to end: Ctrl+C, or SIGTERM on Unix.
///
/// # Panics
///
/// The SIGTERM handler cannot be installed (only under a runtime without
/// signal drivers, which this crate's users do not build).
pub async fn shutdown_signal() {
    let term = async {
        #[cfg(unix)]
        {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler")
                .recv()
                .await;
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    tokio::select! {
        stopped = tokio::signal::ctrl_c() => {
            let _ = stopped;
        }
        () = term => {}
    }
}

/// Serves the rendered registry as `text/plain` at `GET /metrics` on
/// `listener`, until `shutdown` completes. One connection at a time, no
/// keep-alive, and a header read timeout, so a silent client cannot hold
/// the export.
///
/// # Errors
///
/// `listener.accept` failing for a reason other than an interrupted call.
///
/// # Panics
///
/// Never, in the paths this function runs; the panic in the reply builder
/// is on a static response that always builds.
pub async fn serve(
    listener: TcpListener,
    metrics: std::sync::Arc<Metrics>,
    shutdown: impl std::future::Future<Output = ()>,
) -> io::Result<()> {
    use http_body_util::Full;
    use hyper::service::service_fn;
    let mut shutdown = Box::pin(shutdown);
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .header_read_timeout(Some(HEADER_READ_TIMEOUT))
        .timer(hyper_util::rt::TokioTimer::new())
        .keep_alive(false);
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut shutdown => return Ok(()),
        };
        let Ok(accepted) = accepted else {
            continue;
        };
        let (stream, _peer) = accepted;
        let stream = hyper_util::rt::TokioIo::new(stream);
        let metrics = std::sync::Arc::clone(&metrics);
        let conn = builder.serve_connection(
            stream,
            service_fn(move |request: http::Request<hyper::body::Incoming>| {
                let metrics = std::sync::Arc::clone(&metrics);
                async move {
                    type Reply = http::Response<Full<bytes::Bytes>>;
                    if request.uri().path() == "/metrics" {
                        let body = metrics.render();
                        Ok(http::Response::builder()
                            .header("content-type", "text/plain; version=0.0.4")
                            .body(Full::new(body.into_bytes().into()))
                            .expect("static response"))
                            as Result<Reply, std::convert::Infallible>
                    } else {
                        Ok(http::Response::builder()
                            .status(http::StatusCode::NOT_FOUND)
                            .body(Full::new(bytes::Bytes::new()))
                            .expect("static response"))
                            as Result<Reply, std::convert::Infallible>
                    }
                }
            }),
        );
        // One scrape at a time: the connection is awaited here, not spawned.
        tokio::pin!(conn);
        tokio::select! {
            result = conn.as_mut() => {
                let _ = result;
            }
            () = &mut shutdown => {
                conn.as_mut().graceful_shutdown();
                let _ = conn.as_mut().await;
                return Ok(());
            }
        }
    }
}

/// Boxes a reply body so [`CountingBody`] can poll it, whatever body type
/// the wrapped service answers with.
fn boxed<B>(body: B) -> BoxedBody
where
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    use http_body_util::BodyExt as _;
    Box::pin(body.map_err(std::convert::Into::into))
}

/// The body every reply hands to the router: the reply's own body behind a
/// boxed trait object, its error boxed with it.
type BoxedBody = std::pin::Pin<
    Box<
        dyn http_body::Body<Data = bytes::Bytes, Error = Box<dyn std::error::Error + Send + Sync>>
            + Send,
    >,
>;

/// The reply body the layer hands back: it counts the bytes of the data
/// frames, and on the trailers frame it counts the whole call, so a
/// streaming reply's code and bytes arrive when its last frame does. A body
/// that is dropped before its trailers counts the call as `cancelled`.
struct CountingBody {
    inner: BoxedBody,
    metrics: std::sync::Arc<Metrics>,
    rpc: Rpc,
    started: Instant,
    bytes_in: u64,
    bytes_out: u64,
    /// Set once the call is counted, by the trailers or by the drop.
    counted: bool,
    /// Counts the call in [`Metrics::calls_in_flight`] until this body
    /// ends: the drain waits for exactly this long.
    guard: Option<CallGuard>,
}

impl http_body::Body for CountingBody {
    type Data = bytes::Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // `Self` is `Unpin`: the body behind a `Pin<Box<..>>`, the rest data.
        let this = self.get_mut();
        match this.inner.as_mut().poll_frame(cx) {
            std::task::Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.bytes_out += data.len() as u64;
                }
                if frame.is_trailers() {
                    this.counted = true;
                    let code = frame
                        .trailers_ref()
                        .and_then(|t| t.get("grpc-status"))
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u32>().ok())
                        .unwrap_or(0);
                    this.metrics.request(
                        this.rpc,
                        code,
                        this.started,
                        this.bytes_in,
                        this.bytes_out,
                    );
                }
                std::task::Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for CountingBody {
    fn drop(&mut self) {
        // The guard's drop gives the call back to the in-flight gauge,
        // whether the reply ended or the transport dropped the body.
        drop(self.guard.take());
        if !self.counted {
            self.metrics
                .cancelled(self.rpc, self.started, self.bytes_in, self.bytes_out);
        }
    }
}

/// Wraps a service so that every call it serves is counted. It sits outside
/// the access rules, so a refused call counts too, and inside the router,
/// so its handler bodies stay untouched.
#[derive(Clone)]
pub struct MetricsLayer {
    metrics: std::sync::Arc<Metrics>,
}

impl MetricsLayer {
    /// Layers `inner` so that every call it serves is counted in `metrics`.
    #[must_use]
    pub fn new(metrics: std::sync::Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl MetricsLayer {
    /// Wraps `inner` so that every call it serves is counted.
    #[must_use]
    pub fn layer<S>(self, inner: S) -> MetricsService<S> {
        MetricsService {
            inner,
            metrics: self.metrics,
        }
    }
}

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: std::sync::Arc::clone(&self.metrics),
        }
    }
}

/// The wrapped service: it times the call and hands the reply's frames to
/// `CountingBody`.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: std::sync::Arc<Metrics>,
}

// The router picks a service by its name, which the wrap must keep.
impl<S: tonic::server::NamedService> tonic::server::NamedService for MetricsService<S> {
    const NAME: &'static str = S::NAME;
}

// The wrapped service answers a request of `tonic::body::Body` (what the
// router feeds) with a reply of any body type, behind an interceptor or
// not; the reply goes back to the router as a `tonic::body::Body` again,
// around a [`CountingBody`].
/// Counts one call from the moment the layer takes it to the moment its
/// reply body ends, so the in-flight gauge names what a drain still waits
/// for. Dropping the guard gives the call back, so a future the transport
/// abandons before its reply cannot leak a count.
struct CallGuard {
    metrics: std::sync::Arc<Metrics>,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.metrics.call_finished();
    }
}

impl<S, B> tower::Service<http::Request<tonic::body::Body>> for MetricsService<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<B>,
            Error = std::convert::Infallible,
        >,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let metrics = self.metrics.clone();
        let rpc = metrics.rpc(request.uri().path());
        let started = Instant::now();
        let guard = CallGuard {
            metrics: std::sync::Arc::clone(&metrics),
        };
        metrics.call_started();
        // The bytes of a request: its content-length, or what its body
        // hints, or nothing (a streaming request that sends no length).
        let bytes_in = request
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let future = self.inner.call(request);
        Box::pin(async move {
            // Every reply body goes through a `CountingBody` or, for a path
            // that is not one of the counted RPCs, through the plain boxed
            // body, so the reply type stays the same.
            let response = future.await?;
            // A refusal answers without a body: the code is in the reply's
            // headers (a trailers-only reply), and no trailer frame ever
            // flows. Count it now, and hand the body back already counted,
            // so the drop does not count it twice.
            let mut counted = false;
            let code = match rpc {
                Some(_) => response
                    .headers()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u32>().ok()),
                None => None,
            };
            if let (Some(rpc), Some(code)) = (rpc, code) {
                metrics.request(rpc, code, started, bytes_in, 0);
                counted = true;
            }
            Ok(response.map(|body| {
                if let Some(rpc) = rpc {
                    tonic::body::Body::new(CountingBody {
                        inner: boxed(body),
                        metrics,
                        rpc,
                        started,
                        bytes_in,
                        bytes_out: 0,
                        counted,
                        guard: Some(guard),
                    })
                } else {
                    // A path the registry does not count (the health service,
                    // an unknown name): its call leaves the gauge when its
                    // handler has answered, which is when its reply is ready.
                    drop(guard);
                    tonic::body::Body::new(boxed(body))
                }
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::StreamBody;
    use std::task::{Poll, Waker};

    /// Polls a boxed body to its end with a no-op waker: the frames of
    /// these tests are ready at once.
    fn drain(body: &mut CountingBody) {
        use http_body::Body as _;
        let waker = Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut inner = std::pin::pin!(body);
        loop {
            match inner.as_mut().poll_frame(&mut cx) {
                Poll::Ready(None) => return,
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(Some(Err(e))) => panic!("body error: {e}"),
                Poll::Pending => panic!("unexpected pending frame"),
            }
        }
    }

    /// The code of the trailers frame is the code the call is counted by.
    #[test]
    fn a_trailers_frame_counts_the_call_by_its_code() {
        let metrics = std::sync::Arc::new(Metrics::default());
        let rpc = metrics.rpc("/scion.v1.Scion/Blame").unwrap();
        let trailers = {
            let mut map = http::HeaderMap::new();
            map.insert("grpc-status", "5".parse().unwrap());
            map
        };
        let frames: Vec<Result<http_body::Frame<bytes::Bytes>, std::convert::Infallible>> = vec![
            Ok(http_body::Frame::data(bytes::Bytes::from_static(b"hi"))),
            Ok(http_body::Frame::trailers(trailers)),
        ];
        let mut body = CountingBody {
            inner: boxed(StreamBody::new(tokio_stream::iter(frames))),
            metrics: metrics.clone(),
            rpc,
            started: Instant::now(),
            bytes_in: 7,
            bytes_out: 0,
            counted: false,
            guard: None,
        };
        drain(&mut body);
        drop(body);
        let text = metrics.render();
        assert!(
            text.contains("scion_requests_total{rpc=\"Scion/Blame\",code=\"not_found\"} 1\n"),
            "{text}"
        );
        // The reply's data frame counted its bytes, and the request's
        // content-length came in as `in`.
        assert!(
            text.contains("scion_rpc_bytes_total{rpc=\"Scion/Blame\",direction=\"out\"} 2\n"),
            "{text}"
        );
        assert!(
            text.contains("scion_rpc_bytes_total{rpc=\"Scion/Blame\",direction=\"in\"} 7\n"),
            "{text}"
        );
        // Exactly one call in the histogram.
        assert!(
            text.contains("scion_request_seconds_count{rpc=\"Scion/Blame\"} 1\n"),
            "{text}"
        );
    }

    /// A body dropped before its trailers counts the call as cancelled.
    #[test]
    fn a_dropped_body_counts_the_call_as_cancelled() {
        let metrics = std::sync::Arc::new(Metrics::default());
        let rpc = metrics.rpc("/scion.v1.Scion/Blame").unwrap();
        let frames: Vec<Result<http_body::Frame<bytes::Bytes>, std::convert::Infallible>> =
            vec![Ok(http_body::Frame::data(bytes::Bytes::from_static(
                b"first",
            )))];
        let mut body = CountingBody {
            inner: boxed(StreamBody::new(tokio_stream::iter(frames))),
            metrics: metrics.clone(),
            rpc,
            started: Instant::now(),
            bytes_in: 0,
            bytes_out: 0,
            counted: false,
            guard: None,
        };
        // One frame leaves, and no trailer follows: the client went away.
        {
            let waker = Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            let inner = std::pin::pin!(&mut body);
            assert!(matches!(
                http_body::Body::poll_frame(inner, &mut cx),
                Poll::Ready(Some(Ok(_)))
            ));
        }
        drop(body);
        let text = metrics.render();
        assert!(
            text.contains("scion_requests_total{rpc=\"Scion/Blame\",code=\"cancelled\"} 1\n"),
            "{text}"
        );
    }

    /// The operator gauges and histograms render only when they counted
    /// something, and in the Prometheus shape the scrapers expect.
    #[test]
    fn the_operating_metrics_render_their_shape() {
        let metrics = Metrics::default();
        let text = metrics.render();
        assert!(!text.contains("scion_blob_sync_seconds"), "{text}");
        assert!(!text.contains("scion_data_free_bytes"), "{text}");
        assert!(!text.contains("scion_derive_walks"), "{text}");
        assert!(!text.contains("scion_pg_collector_key_wait"), "{text}");

        metrics.blob_sync(4_000);
        metrics.collector_key_wait(70_000);
        metrics.data_free_bytes(4_096);
        metrics.derive_walk_started();
        metrics.derive_walk_wait_started();
        let text = metrics.render();
        assert!(
            text.contains("scion_blob_sync_seconds_bucket{le=\"0.004\"} 1\n"),
            "{text}"
        );
        assert!(
            text.contains("scion_blob_sync_seconds_bucket{le=\"+Inf\"} 1\n"),
            "{text}"
        );
        assert!(text.contains("scion_blob_sync_seconds_count 1\n"), "{text}");
        assert!(
            !text.contains("scion_blob_sync_seconds_bucket{le=\"0.001\"} 1\n"),
            "4 ms is not in the 1 ms bucket: {text}"
        );
        assert!(
            text.contains("scion_pg_collector_key_wait_seconds_bucket{le=\"0.256\"} 1\n"),
            "{text}"
        );
        assert!(text.contains("scion_data_free_bytes 4096\n"), "{text}");
        assert!(text.contains("scion_derive_walks_in_flight 1\n"), "{text}");
        assert!(text.contains("scion_derive_walks_waiting 1\n"), "{text}");

        metrics.derive_walk_ended();
        metrics.derive_walk_wait_ended();
        let text = metrics.render();
        assert!(!text.contains("scion_derive_walks"), "{text}");
    }

    /// Every rpc of the proto file has a slot: the table cannot drift from
    /// the API. The proto file enters this crate through the link that
    /// `build.rs` compiles, so the test reads it where the crate builds.
    #[test]
    fn every_rpc_of_the_proto_file_has_a_slot() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto/scion.proto");
        let text = std::fs::read_to_string(&path).unwrap();
        let mut service = String::new();
        let mut found = 0;
        for line in text.lines() {
            if let Some(name) = line.trim().strip_prefix("service ") {
                service = name.trim_end_matches(" {").to_owned();
            } else if let Some(rest) = line.trim().strip_prefix("rpc ") {
                let method = rest.split(['(', ' ']).next().unwrap();
                let path = format!("/scion.v1.{service}/{method}");
                let metrics = Metrics::default();
                assert!(
                    metrics.rpc(&path).is_some(),
                    "{path} has no slot in the RPCS table"
                );
                found += 1;
            }
        }
        assert_eq!(found, RPCS.len(), "the proto has rpcs the table misses");
    }

    /// The Postgres pool metrics render with their names, units, and the
    /// values a caller recorded.
    #[test]
    fn the_pg_pool_metrics_render() {
        let metrics = Metrics::default();
        metrics.pg_pool_wait(std::time::Duration::from_millis(1500));
        metrics.pg_readers_busy(8);
        metrics.pg_tx_age(std::time::Duration::from_millis(2500));
        let text = metrics.render();
        assert!(
            text.contains("# TYPE scion_pg_pool_wait_seconds histogram\n"),
            "{text}"
        );
        assert!(
            text.contains("scion_pg_pool_wait_seconds_bucket{le=\"4\"}"),
            "{text}"
        );
        assert!(
            text.contains("scion_pg_pool_wait_seconds_count 1"),
            "{text}"
        );
        assert!(
            text.contains("# TYPE scion_pg_readers_busy gauge\n"),
            "{text}"
        );
        assert!(text.contains("scion_pg_readers_busy 8"), "{text}");
        assert!(
            text.contains("# TYPE scion_pg_tx_age_seconds gauge\n"),
            "{text}"
        );
        assert!(text.contains("scion_pg_tx_age_seconds 2.5"), "{text}");
        // An age of zero still renders: a quiet pool is a real state.
        let empty = Metrics::default();
        let text = empty.render();
        assert!(text.contains("scion_pg_tx_age_seconds 0"), "{text}");
        assert!(!text.contains("scion_pg_pool_wait_seconds_count"), "{text}");
    }
}

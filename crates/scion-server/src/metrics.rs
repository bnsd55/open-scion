//! Metrics for an operator: counters and one latency histogram per RPC,
//! exported in the Prometheus text format on a plain HTTP listener.
//!
//! The registry is fixed arrays of atomics: the RPCs of the two services
//! and the gRPC codes are known at compile time, so a request never takes
//! a lock or allocates. A tower layer counts, and a small HTTP server
//! renders the text ([`serve`]).

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

/// The outcomes of a [`Land`](crate::land), by how `land.rs` decides them.
const LANDS: [&str; 4] = ["landed", "conflict", "rejected", "retry"];

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

    /// Counts one land that ended as `outcome` (0-3, in [`LANDS`] order).
    pub fn land(&self, outcome: usize) {
        if outcome < LANDS.len() {
            self.lands[outcome].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Renders the registry as the Prometheus text format.
    #[must_use]
    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut text = String::new();
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
        text.push_str("# HELP scion_lands_total Lands by outcome.\n");
        text.push_str("# TYPE scion_lands_total counter\n");
        for (outcome, name) in LANDS.iter().enumerate() {
            let value = self.lands[outcome].load(Ordering::Relaxed);
            if value > 0 {
                let _ = writeln!(text, "scion_lands_total{{outcome=\"{name}\"}} {value}");
            }
        }
        text
    }
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

/// Serves the rendered registry as `text/plain` on `listener`, until
/// `shutdown` completes. One scrape at a time is enough: a scraper polls,
/// and the text renders in microseconds.
///
/// # Errors
///
/// `listener.accept` failing for a reason other than an interrupted call.
pub async fn serve(
    listener: TcpListener,
    metrics: std::sync::Arc<Metrics>,
    shutdown: impl std::future::Future<Output = ()>,
) -> io::Result<()> {
    let mut shutdown = Box::pin(shutdown);
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut shutdown => return Ok(()),
        };
        let Ok((mut socket, _peer)) = accepted else {
            continue;
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            // One request per connection: the answer says close, and a
            // scraper that wants more opens a new one. Read to the end of
            // the request headers, answer, close.
            use tokio::io::AsyncWriteExt;
            let mut buf = [0u8; 1024];
            let mut read = 0;
            loop {
                match socket.try_read(&mut buf[read..]) {
                    Ok(0) => break,
                    Ok(n) => {
                        read += n;
                        if buf[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                        if read == buf.len() {
                            break;
                        }
                    }
                    Err(e) if e.kind() != io::ErrorKind::WouldBlock => return,
                    Err(_) => {}
                }
            }
            let text = metrics.render();
            let answer = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
            let _ = socket.write_all(answer.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
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

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

/// The wrapped service: it times the call and hands the reply's frames to
/// [`CountingBody`].
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
// router feeds) with its own body type wrapped in the interceptor's
// `ResponseBody`; the reply goes back to the router as a `tonic::body::Body`
// again, around a [`CountingBody`].
impl<S> tower::Service<http::Request<tonic::body::Body>> for MetricsService<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::service::interceptor::ResponseBody<tonic::body::Body>>,
            Error = std::convert::Infallible,
        >,
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
            Ok(response.map(|body| match rpc {
                Some(rpc) => tonic::body::Body::new(CountingBody {
                    inner: boxed(body),
                    metrics,
                    rpc,
                    started,
                    bytes_in,
                    bytes_out: 0,
                    counted,
                }),
                None => tonic::body::Body::new(boxed(body)),
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
}

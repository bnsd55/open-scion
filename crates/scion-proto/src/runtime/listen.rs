//! How the server and the cache tiers serve connections. They share this
//! code, so every process that answers calls keeps the same bounds.
//!
//! A reply whose client stops reading keeps memory in HTTP/2 that no budget
//! counts, until its stream ends or its connection closes. A `GetObjects` or
//! `ReadFile` reply keeps three parts: the part in its channel, the part
//! that hyper has read ahead, and the part in the h2 send buffer, which also
//! keeps up to 400 KiB more. That is under 4 MiB. A unary reply keeps its
//! whole message, up to 64 MiB. hyper cannot reset such a stream from the
//! server side: while the stream waits for flow-control window, hyper does
//! not poll the reply body, so it never sees a body error.
//!
//! So [`Limits::serve`] bounds what stalled replies hold: at most
//! [`Limits::connections`] connections at once, each with at most
//! [`Limits::streams`] open streams, and each with at most
//! [`Limits::stalled_bytes`] bytes held by its stalled replies — when a
//! reply stalls, it adds what it leaves in HTTP/2 to its connection's
//! count, and a connection over the limit is closed, which frees its
//! stalled streams' buffers at once ([`Stalled`]). And it closes every
//! connection in time, so a connection that waits for a place gets one:
//!
//! - a client that does not send the HTTP/2 preface within
//!   [`HANDSHAKE_TIMEOUT`] is closed. hyper has no timeout of its own
//!   there, and its keepalive starts only after the preface.
//! - after the preface, HTTP/2 keepalive closes a connection whose client
//!   does not answer a ping.
//! - every connection is asked to close (`GOAWAY`) after
//!   [`MAX_CONNECTION_AGE`], and is closed [`CONNECTION_GRACE`] later, with
//!   any call still in it. A client then connects again.
//!
//! With TLS, a connection takes its place first, and one deadline from its
//! accept bounds the TLS handshake and the HTTP/2 preface together: the
//! handshake gets what is left of the deadline, and the preface timer gets
//! the rest. A peer that never starts the handshake, never ends it, or
//! stops right after it gives its place up within
//! [`TLS_HANDSHAKE_TIMEOUT`] of its accept, the same bound as a silent
//! plain peer.

use std::future::Future;
use std::io;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::runtime::request_id;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::Sleep;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::server::TlsStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::Server;
use tonic::transport::server::{Connected, Router, TcpIncoming};

/// The default of [`Limits::connections`]. Room for the 50 checkouts at once
/// of `scripts/bench-checkout.sh`, each on its own connection to one daemon.
pub const DEFAULT_CONNECTIONS: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// The default of [`Limits::streams`]. One connection can still fill all
/// 256 job slots of the server, and carry the 200 agents of `scn bench`.
pub const DEFAULT_STREAMS: NonZeroU32 = NonZeroU32::new(256).unwrap();

/// A client must send the HTTP/2 preface within this long after it gets its
/// place, or its connection is closed.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// A TLS handshake must end within this long, or the connection is dropped.
/// The same wait as [`HANDSHAKE_TIMEOUT`], and generous for one round trip
/// plus a certificate check.
pub(crate) const TLS_HANDSHAKE_TIMEOUT: Duration = HANDSHAKE_TIMEOUT;

/// After this long without a frame from a client, the process sends it an
/// HTTP/2 ping.
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// A connection whose client does not answer a ping within this long is
/// closed. So after the preface, a dead peer frees its connection, and all
/// that its replies hold, within [`KEEPALIVE_INTERVAL`] plus this of its
/// last frame.
pub(crate) const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// After this long, a connection is asked to close, so one that a client
/// keeps for life (a daemon keeps one to its cache) gives its place up.
pub(crate) const MAX_CONNECTION_AGE: Duration = Duration::from_secs(30 * 60);

/// A connection that was asked to close is closed this long later, with any
/// call still in it. At least the server's longest call (600 s).
pub const CONNECTION_GRACE: Duration = Duration::from_secs(600);

/// The bytes of the HTTP/2 client preface.
const PREFACE_BYTES: usize = 24;

/// A connection whose stalled replies hold more than this is closed, with
/// all that they hold. 16 stalled replies of under 4 MiB each. The flag is
/// `--max-stalled-bytes`.
pub const DEFAULT_STALLED_BYTES: NonZeroU64 = NonZeroU64::new(64 << 20).unwrap();

/// How many connections a process serves, and how many streams each may
/// have open.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Most connections served at once. A connection beyond that is
    /// accepted, and waits unread until another connection closes: within
    /// [`MAX_CONNECTION_AGE`] plus [`CONNECTION_GRACE`].
    pub connections: NonZeroUsize,
    /// Most streams (calls) that one connection may have open at once, as
    /// HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS`. A client that has this many
    /// waits for one to end before it opens another.
    pub streams: NonZeroU32,
    /// Most bytes that the stalled replies of one connection may hold. A
    /// connection over it is closed, and what its stalled replies hold goes.
    pub stalled_bytes: NonZeroU64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: DEFAULT_CONNECTIONS,
            streams: DEFAULT_STREAMS,
            stalled_bytes: DEFAULT_STALLED_BYTES,
        }
    }
}

/// What the stalled replies of one connection hold, counted when a reply
/// stalls. The count lives as long as the connection: a call gets the
/// handle in its extensions (as the connection's connect info), and passes
/// it to its reply. When the count passes the limit, the connection's read
/// side fails, so hyper closes the connection and frees what every stalled
/// stream of it holds — even though the client sends nothing, because the
/// count wakes the stored read waker.
#[derive(Clone)]
pub struct Stalled(Arc<Counted>);

struct Counted {
    /// Most bytes that the stalled replies of this connection may hold.
    limit: u64,
    /// Bytes that stalled replies are estimated to hold now.
    bytes: AtomicU64,
    /// The waker of the connection's last read poll, woken once the count
    /// passes the limit, so the close is seen without a frame from the
    /// client.
    waker: Mutex<Option<Waker>>,
    /// Whether the crossing has been logged.
    logged: AtomicBool,
}

impl Stalled {
    fn new(limit: u64) -> Self {
        Self(Arc::new(Counted {
            limit,
            bytes: AtomicU64::new(0),
            waker: Mutex::new(None),
            logged: AtomicBool::new(false),
        }))
    }

    /// The handle of `request`'s connection, if it is one of ours. A reply
    /// counts its stalled bytes on it.
    #[must_use]
    pub fn of<T>(request: &tonic::Request<T>) -> Option<Self> {
        request.extensions().get::<Self>().cloned()
    }

    /// `bytes` more that stalled replies leave in HTTP/2. Past the limit,
    /// logs once, and wakes the read side, which then fails the connection.
    ///
    /// # Panics
    ///
    /// Only if a lock is poisoned, which cannot happen: no holder panics.
    pub fn add(&self, bytes: u64) {
        let held = self.0.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if held >= self.0.limit && !self.0.logged.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                held,
                limit = self.0.limit,
                "closing a connection: its stalled replies hold too much"
            );
            if let Some(waker) = self
                .0
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                waker.wake();
            }
        }
    }

    /// Whether the connection must close now.
    fn over(&self) -> bool {
        self.0.bytes.load(Ordering::Relaxed) >= self.0.limit
    }

    /// Registers the read waker, so a crossing of the limit is seen at once.
    fn register(&self, read: &Waker) {
        let mut waker = self.0.waker.lock().unwrap_or_else(PoisonError::into_inner);
        *waker = Some(read.clone());
    }

    /// The I/O error that closes a connection over the limit.
    fn error(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            format!(
                "its stalled replies hold {} bytes, over the {} limit",
                self.0.bytes.load(Ordering::Relaxed),
                self.0.limit
            ),
        )
    }
}

/// The timers of a connection.
struct Timers {
    handshake: Duration,
    keepalive: Duration,
    keepalive_timeout: Duration,
    age: Duration,
    grace: Duration,
}

const TIMERS: Timers = Timers {
    handshake: HANDSHAKE_TIMEOUT,
    keepalive: KEEPALIVE_INTERVAL,
    keepalive_timeout: KEEPALIVE_TIMEOUT,
    age: MAX_CONNECTION_AGE,
    grace: CONNECTION_GRACE,
};

/// The gRPC health service (`grpc.health.v1`), on the same port as the
/// other services and without a token, so a probe needs none. It is
/// `SERVING` for the whole server (the empty name) and for every name in
/// `services`; when shutdown starts, every one of these names flips to
/// `NOT_SERVING`, before the drain, while the server still answers.
///
/// Returns the service to add to the router, and the wrapped shutdown.
pub async fn health(
    services: &[&'static str],
    shutdown: impl Future<Output = ()>,
) -> (
    tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health>,
    impl Future<Output = ()>,
) {
    let (reporter, service) = tonic_health::server::health_reporter();
    for name in std::iter::once("").chain(services.iter().copied()) {
        reporter
            .set_service_status(name, tonic_health::ServingStatus::Serving)
            .await;
    }
    let shutdown = async move {
        shutdown.await;
        for name in std::iter::once("").chain(services.iter().copied()) {
            reporter
                .set_service_status(name, tonic_health::ServingStatus::NotServing)
                .await;
        }
    };
    (service, shutdown)
}

impl Limits {
    /// Serves the services that `router` adds to a server, on `incoming`
    /// connections, within these limits and with the timers above, until
    /// `shutdown` resolves. A connection that waits for its place logs a
    /// warning.
    ///
    /// # Errors
    ///
    /// Transport errors.
    pub async fn serve<I, IO>(
        self,
        router: impl FnOnce(Server) -> Router,
        incoming: I,
        tls: Option<Arc<ServerConfig>>,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
    {
        self.serve_with(&TIMERS, router, incoming, tls, shutdown)
            .await
    }

    async fn serve_with<I, IO>(
        self,
        timers: &Timers,
        router: impl FnOnce(Server) -> Router,
        incoming: I,
        tls: Option<Arc<ServerConfig>>,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = io::Result<IO>> + Send + 'static,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
    {
        // One span per call: the RPC and the peer at the start, and the
        // principal and the repo once the call knows them. `error_span`, so
        // the span is open under any filter, and a `WARN` line under a
        // `warn` filter still shows its fields.
        let server = Server::builder()
            .trace_fn(|request| {
                let peer = request
                    .extensions()
                    .get::<tonic::transport::server::TcpConnectInfo>()
                    .and_then(tonic::transport::server::TcpConnectInfo::remote_addr)
                    .map(|addr| addr.to_string())
                    .unwrap_or_default();
                // The caller's request ID, if it sent one that fits. A
                // header that does not fit is dropped whole, not cleaned
                // up: nothing of it is logged.
                let request_id = request_id::sanitize(
                    request
                        .headers()
                        .get(request_id::REQUEST_ID)
                        .map(http::HeaderValue::as_bytes),
                )
                .unwrap_or_default();
                tracing::error_span!(
                    "call",
                    rpc = %request.uri().path(),
                    peer,
                    request_id = %request_id,
                    principal = tracing::field::Empty,
                    repo = tracing::field::Empty,
                )
            })
            .max_concurrent_streams(self.streams.get())
            .http2_keepalive_interval(Some(timers.keepalive))
            .http2_keepalive_timeout(Some(timers.keepalive_timeout))
            .max_connection_age(timers.age)
            .max_connection_age_grace(timers.grace);
        if let Some(config) = tls {
            let incoming = accept_tls(
                incoming,
                self.connections,
                timers.handshake,
                self.stalled_bytes,
                config,
            );
            return router(server)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await;
        }
        let incoming = accept(
            incoming,
            self.connections,
            timers.handshake,
            self.stalled_bytes,
        );
        router(server)
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await
    }
}

/// The TCP connections of `listener`, with Nagle's algorithm off, so a small
/// reply does not wait for the client's ACK. (tonic's own `tcp_nodelay`
/// applies only to a listener that tonic opens.)
#[must_use]
pub fn tcp(listener: TcpListener) -> TcpIncoming {
    TcpIncoming::from(listener).with_nodelay(Some(true))
}

/// The TLS connections of `incoming`, at most `max` at once, as
/// [`accept`] yields plain ones: a connection takes its place first (and
/// waits at the cap), and then its TLS handshake runs on its own task, under
/// `handshake`, while it holds that place. So the handshakes of different
/// connections run at the same time, and a peer that never starts or never
/// ends its handshake holds at most one of at most `max` places, for at most
/// `handshake` long. A handshake that failed or timed out drops its place,
/// with a warning. The decrypted connection goes on with the preface timer
/// of [`accept`], so a peer that stops right after the handshake is closed
/// by that timer too.
///
/// The error of `incoming` must be [`io::Error`], which is what a TCP or a
/// unix listener yields.
fn accept_tls<I, IO>(
    incoming: I,
    max: NonZeroUsize,
    handshake: Duration,
    stalled_bytes: NonZeroU64,
    config: Arc<ServerConfig>,
) -> ReceiverStream<io::Result<Connection<TlsStream<IO>>>>
where
    I: Stream<Item = io::Result<IO>> + Send + 'static,
    IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
{
    let places = Arc::new(Semaphore::new(max.get()));
    let (done, served) = mpsc::channel(max.get());
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    tokio::spawn(async move {
        let mut incoming = pin!(incoming);
        while let Some(accepted) = incoming.next().await {
            if done.is_closed() {
                break;
            }
            let io = match accepted {
                Ok(io) => io,
                Err(error) => {
                    if done.send(Err(error)).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            // The place first, as the plain path does: a silent peer holds
            // one of at most `max`, and frees it within the timeout.
            let place = if let Ok(place) = Arc::clone(&places).try_acquire_owned() {
                place
            } else {
                tracing::warn!(max, "a connection waits: the most connections are open");
                places
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("the semaphore of places is never closed")
            };
            // One deadline, from the accept: the TLS handshake and the
            // HTTP/2 preface that follows share it, so a silent peer holds
            // its place for at most `handshake` in all.
            let deadline = tokio::time::Instant::now() + handshake;
            let wait = tokio::time::timeout_at(deadline, acceptor.accept(io));
            let preface = Some((
                PREFACE_BYTES,
                Box::pin(tokio::time::sleep_until(deadline)) as Pin<Box<Sleep>>,
            ));
            let sender = done.clone();
            tokio::spawn(async move {
                match wait.await {
                    Ok(Ok(stream)) => {
                        // The place goes with the connection, and frees when
                        // it is dropped, which the gone receiver also does.
                        let _ = sender
                            .send(Ok(Connection {
                                io: stream,
                                _place: place,
                                preface,
                                stalled: Stalled::new(stalled_bytes.get()),
                            }))
                            .await;
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "a TLS handshake failed");
                    }
                    Err(_) => {
                        tracing::warn!(
                            timeout = ?handshake,
                            "no TLS handshake within the timeout"
                        );
                    }
                }
            });
        }
    });
    ReceiverStream::new(served)
}

/// The connections from `incoming`, at most `max` at once. Each keeps its
/// place until it is dropped, when it closes. Each must send the preface
/// within `handshake` once it has its place.
fn accept<I, IO, E>(
    incoming: I,
    max: NonZeroUsize,
    handshake: Duration,
    stalled_bytes: NonZeroU64,
) -> impl Stream<Item = Result<Connection<IO>, E>>
where
    I: Stream<Item = Result<IO, E>>,
{
    let max = max.get();
    let stalled_bytes = stalled_bytes.get();
    let places = Arc::new(Semaphore::new(max));
    incoming.then(move |accepted| {
        let places = Arc::clone(&places);
        async move {
            let io = accepted?;
            let place = if let Ok(place) = Arc::clone(&places).try_acquire_owned() {
                place
            } else {
                tracing::warn!(max, "a connection waits: the most connections are open");
                places
                    .acquire_owned()
                    .await
                    .expect("the semaphore of places is never closed")
            };
            Ok(Connection {
                io,
                _place: place,
                preface: Some((PREFACE_BYTES, Box::pin(tokio::time::sleep(handshake)))),
                stalled: Stalled::new(stalled_bytes),
            })
        }
    })
}

/// A connection that keeps its place of [`accept`] until it is dropped.
struct Connection<IO> {
    io: IO,
    _place: OwnedSemaphorePermit,
    /// Until the preface has come: its bytes still to come, and the end of
    /// the wait for them.
    preface: Option<(usize, Pin<Box<Sleep>>)>,
    /// What the stalled replies of this connection hold, and their limit.
    stalled: Stalled,
}

impl<IO> Connected for Connection<IO> {
    type ConnectInfo = Stalled;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stalled.clone()
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Connection<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        // Over the limit, the connection is closed, so hyper frees what its
        // stalled streams hold.
        if this.stalled.over() {
            return Poll::Ready(Err(this.stalled.error()));
        }
        // The waker, so a crossing of the limit is seen without a frame from
        // the client.
        this.stalled.register(cx.waker());
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.io).poll_read(cx, buf);
        if let Some((left, wait)) = &mut this.preface {
            match polled {
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len() - before;
                    *left = left.saturating_sub(read);
                    // The preface is in, or the client closed.
                    if *left == 0 || read == 0 {
                        this.preface = None;
                    }
                }
                Poll::Pending if wait.as_mut().poll(cx).is_ready() => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "no HTTP/2 preface within the handshake timeout",
                    )));
                }
                _ => {}
            }
        }
        polled
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Connection<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.stalled.over() {
            return Poll::Ready(Err(self.stalled.error()));
        }
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::SocketAddr;
    use std::path::Path;
    use std::pin::pin;
    use std::sync::Arc;

    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    };
    use tokio::io::AsyncReadExt as _;
    use tokio_rustls::rustls::pki_types::{
        CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _,
    };
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};
    use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
    use tonic::{Code, Request, Response, Status, Streaming};

    use super::*;
    use crate::Bytes;
    use crate::runtime::object::Header;
    use crate::runtime::reply::{self, Body, IN_FLIGHT_BYTES, Outgoing};
    use crate::runtime::tls::ServerTls;
    use crate::v1::objects_client::ObjectsClient;
    use crate::v1::objects_server::{Objects, ObjectsServer};
    use crate::v1::{
        Encoding, GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectKind,
        ObjectPart, ResolveRefRequest, ResolveRefResponse,
    };

    /// A wait that ends at once when nothing holds the call back.
    const HELD: Duration = Duration::from_millis(500);

    /// Short timers, each of 100 ms, except `slow`.
    fn timers(slow: &str) -> Timers {
        let (short, long) = (Duration::from_millis(100), Duration::from_secs(3600));
        let pick = |name| if name == slow { long } else { short };
        Timers {
            handshake: pick("handshake"),
            keepalive: pick("keepalive"),
            keepalive_timeout: pick("keepalive"),
            age: pick("age"),
            grace: pick("age"),
        }
    }

    /// Replies to `GetObjects` with one object in three parts of 128 KiB, so
    /// a client that does not read stalls the reply after its first part.
    /// The reply runs through the real [`reply::send_objects`], so it counts
    /// what it leaves on its connection. Refuses `ResolveRef` at once, with
    /// no data, so a full window does not hold that reply.
    #[derive(Clone)]
    struct Stalls;

    #[tonic::async_trait]
    impl Objects for Stalls {
        async fn resolve_ref(
            &self,
            _: Request<ResolveRefRequest>,
        ) -> Result<Response<ResolveRefResponse>, Status> {
            Err(Status::not_found("no ref"))
        }

        async fn get_root_tree(
            &self,
            _: Request<GetRootTreeRequest>,
        ) -> Result<Response<GetRootTreeResponse>, Status> {
            Err(Status::unimplemented("not in the stub"))
        }

        type GetObjectsStream = reply::ObjectStream;

        async fn get_objects(
            &self,
            request: Request<GetObjectsRequest>,
        ) -> Result<Response<Self::GetObjectsStream>, Status> {
            // More parts than a client's 64 KiB window and hyper's read
            // ahead take, so a client that does not read stalls the reply
            // after its first parts.
            let chunk = Bytes::from(vec![0; 128 << 10]);
            let parts = 10;
            let header = Header {
                kind: ObjectKind::Blob,
                id: [2; 32],
                encoding: Encoding::Raw,
                encoded_size: parts * chunk.len(),
                size: parts * chunk.len(),
            };
            let object = Outgoing {
                header,
                body: Body::Chunks(vec![chunk; parts]),
            };
            // The budget of this stub: one object at a time, as a reply
            // holds.
            let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
            let permit = reply::try_take(&budget, 1).unwrap();
            let (tx, stream) = reply::channel();
            // The stall is short, so the test does not wait for the real
            // one; the counting is what the test watches.
            let stall = Duration::from_millis(100);
            tokio::spawn(reply::send_objects(
                (Ok(object), permit),
                [(); 0],
                tx,
                stall,
                Stalled::of(&request),
                |()| async { None },
            ));
            Ok(Response::new(stream))
        }
    }

    /// A leaf certificate and its key, both PEM.
    struct TestCert {
        cert_pem: String,
        key_pem: String,
    }

    /// A certificate authority for one test run: it signs the server and
    /// client certificates of the test. Everything stays in memory, except
    /// the PEM files a test writes into its temp directory.
    struct TestAuthority {
        params: CertificateParams,
        key: KeyPair,
    }

    impl TestAuthority {
        fn new() -> Self {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
            Self {
                params,
                key: KeyPair::generate().unwrap(),
            }
        }

        /// Signs a leaf for `names`, used as `purpose`.
        fn sign(&self, names: &[&str], purpose: ExtendedKeyUsagePurpose) -> TestCert {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(
                names
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            params.extended_key_usages = vec![purpose];
            let issuer = Issuer::from_params(&self.params, &self.key);
            let cert = params.signed_by(&key, &issuer).unwrap();
            TestCert {
                cert_pem: cert.pem(),
                key_pem: key.serialize_pem(),
            }
        }

        /// The certificate of the authority itself, as a client trusts it.
        fn cert(&self) -> TestCert {
            let cert = self.params.self_signed(&self.key).unwrap();
            TestCert {
                cert_pem: cert.pem(),
                key_pem: self.key.serialize_pem(),
            }
        }
    }

    /// The certificates of one test: an authority, a server certificate for
    /// `localhost` and `127.0.0.1`, a client certificate of that authority,
    /// and a second authority with a client certificate of its own, so a
    /// test can trust or present the wrong one.
    struct TestCerts {
        authority: TestAuthority,
        server: TestCert,
        client: TestCert,
        stranger: TestAuthority,
        stranger_client: TestCert,
    }

    fn test_certs() -> TestCerts {
        let authority = TestAuthority::new();
        let server = authority.sign(
            &["localhost", "127.0.0.1"],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let client = authority.sign(&["client"], ExtendedKeyUsagePurpose::ClientAuth);
        let stranger = TestAuthority::new();
        let stranger_client = stranger.sign(&["client"], ExtendedKeyUsagePurpose::ClientAuth);
        TestCerts {
            authority,
            server,
            client,
            stranger,
            stranger_client,
        }
    }

    /// Writes `pem` into the temp directory of a test, as `name`.
    fn write(dir: &Path, name: &str, pem: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, pem).unwrap();
        path
    }

    /// A `ServerTls` for `server`, with mTLS when `client_ca` is given.
    fn server_tls(dir: &Path, server: &TestCert, client_ca: Option<&TestCert>) -> ServerTls {
        let cert = write(dir, "server.pem", &server.cert_pem);
        let key = write(dir, "server.key", &server.key_pem);
        let client_ca = client_ca.map(|ca| write(dir, "client-ca.pem", &ca.cert_pem));
        ServerTls::load(&cert, &key, client_ca.as_deref()).unwrap()
    }

    /// A client of the stub at `addr`, trusting the CA `ca`, presenting
    /// `identity` with mTLS. Fails when the server refuses the handshake.
    async fn client(
        addr: SocketAddr,
        ca: &TestCert,
        identity: Option<&TestCert>,
    ) -> Result<ObjectsClient<Channel>, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(format!("https://{addr}")).unwrap();
        let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&ca.cert_pem));
        if let Some(cert) = identity {
            tls = tls.identity(Identity::from_pem(&cert.cert_pem, &cert.key_pem));
        }
        let channel = endpoint.tls_config(tls)?.connect().await?;
        Ok(ObjectsClient::new(channel))
    }

    /// A raw TLS client of `addr`, trusting the CA `ca`, with `identity`.
    /// For tests that talk to the server by hand.
    async fn raw_client(
        addr: SocketAddr,
        ca: &TestCert,
        identity: Option<&TestCert>,
    ) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ca.cert_pem.as_bytes()).unwrap())
            .unwrap();
        let builder = ClientConfig::builder_with_provider(crate::runtime::tls::provider())
            .with_safe_default_protocol_versions()
            .unwrap();
        let config = match identity {
            None => builder.with_root_certificates(roots).with_no_client_auth(),
            Some(cert) => {
                let der = CertificateDer::from_pem_slice(cert.cert_pem.as_bytes()).unwrap();
                let key = PrivateKeyDer::from_pem_slice(cert.key_pem.as_bytes()).unwrap();
                builder
                    .with_root_certificates(roots)
                    .with_client_auth_cert(vec![der], key)
                    .unwrap()
            }
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap()
    }

    /// The stub, served within `limits` with `timers`, TLS with
    /// `server_tls` whose handshakes end within `tls_timeout`, and its
    /// address.
    async fn serve_tls(
        limits: Limits,
        timers: Timers,
        server_tls: &ServerTls,
        tls_timeout: Duration,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = server_tls.config();
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
            let pending = std::future::pending();
            let timers = Timers {
                handshake: tls_timeout,
                ..timers
            };
            Box::pin(limits.serve_with(&timers, router, tcp(listener), Some(config), pending)).await
        });
        addr
    }

    /// The stub, served within `limits` with `timers`, and its address.
    async fn serve(limits: Limits, timers: Timers) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stalls));
            let pending = std::future::pending();
            limits
                .serve_with(&timers, router, tcp(listener), None, pending)
                .await
        });
        addr
    }

    fn endpoint(addr: SocketAddr) -> Endpoint {
        Endpoint::from_shared(format!("http://{addr}")).unwrap()
    }

    /// A `GetObjects` reply that the caller never reads.
    async fn stall(client: &mut ObjectsClient<Channel>) -> Streaming<ObjectPart> {
        let request = GetObjectsRequest::default();
        client.get_objects(request).await.unwrap().into_inner()
    }

    /// The code of a call that the stub answers at once.
    async fn quick(client: &mut ObjectsClient<Channel>) -> Code {
        let request = ResolveRefRequest::default();
        client.resolve_ref(request).await.unwrap_err().code()
    }

    /// Whether the process closes a raw TCP connection that sends `sent` and
    /// then never answers, within 10 s.
    async fn closes_a_peer_that_sends(addr: SocketAddr, sent: &'static [u8]) -> bool {
        let closed = tokio::task::spawn_blocking(move || {
            let mut peer = std::net::TcpStream::connect(addr)?;
            peer.write_all(sent)?;
            peer.set_read_timeout(Some(Duration::from_secs(10)))?;
            // Ends when the process closes the connection, with an end of
            // file or a reset.
            peer.read_to_end(&mut Vec::new())
        });
        match closed.await.unwrap() {
            Ok(_) => true,
            Err(e) => !matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
        }
    }

    /// A client that stops reading keeps its streams open. With the streams
    /// of its connection all open, its next call waits, and goes on once one
    /// of them ends.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_opens_at_most_its_streams() {
        let streams = 4;
        let limits = Limits {
            streams: NonZeroU32::new(streams).unwrap(),
            ..Limits::default()
        };
        let addr = serve(limits, TIMERS).await;
        // The smallest windows, so no reply ends while it is unread.
        let channel = endpoint(addr)
            .initial_stream_window_size(65_535)
            .initial_connection_window_size(65_535)
            .connect()
            .await
            .unwrap();
        let mut client = ObjectsClient::new(channel);
        let mut stalled = Vec::new();
        for _ in 0..streams {
            stalled.push(stall(&mut client).await);
        }

        let mut next = client.clone();
        let mut next = pin!(quick(&mut next));
        assert!(
            tokio::time::timeout(HELD, &mut next).await.is_err(),
            "a stream beyond the cap was opened"
        );
        drop(stalled.pop());
        let code = tokio::time::timeout(Duration::from_secs(10), next)
            .await
            .expect("the call goes on once a stream ends");
        assert_eq!(code, Code::NotFound);
    }

    /// With all its connections open, a process leaves a new one unread, and
    /// serves it once another closes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_process_serves_at_most_its_connections() {
        let connections = 2;
        let limits = Limits {
            connections: NonZeroUsize::new(connections).unwrap(),
            ..Limits::default()
        };
        let endpoint = endpoint(serve(limits, TIMERS).await);
        let mut open = Vec::new();
        for _ in 0..connections {
            let mut client = ObjectsClient::new(endpoint.connect().await.unwrap());
            // Served, so the process holds a place for it.
            assert_eq!(quick(&mut client).await, Code::NotFound);
            open.push(client);
        }

        let mut extra = ObjectsClient::new(endpoint.connect_lazy());
        let mut call = pin!(quick(&mut extra));
        assert!(
            tokio::time::timeout(HELD, &mut call).await.is_err(),
            "a connection beyond the cap was served"
        );
        // Its connection closes once no handle is left.
        drop(open.pop());
        let code = tokio::time::timeout(Duration::from_secs(10), call)
            .await
            .expect("the connection is served once another closes");
        assert_eq!(code, Code::NotFound);
    }

    /// A client that keeps its connection, idle, for life still gives its
    /// place up once the connection is old, so a waiting one gets served.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_old_connection_gives_its_place_up() {
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            ..Limits::default()
        };
        let endpoint = endpoint(serve(limits, timers("keepalive")).await);
        let mut old = ObjectsClient::new(endpoint.connect().await.unwrap());
        assert_eq!(quick(&mut old).await, Code::NotFound);

        let mut new = ObjectsClient::new(endpoint.connect_lazy());
        let code = tokio::time::timeout(Duration::from_secs(10), quick(&mut new))
            .await
            .expect("the old connection gives its place up");
        assert_eq!(code, Code::NotFound);
        drop(old);
    }

    /// A peer that connects and sends nothing is closed: it gets no place
    /// for life, though keepalive never starts without the preface.
    #[tokio::test]
    async fn a_peer_without_the_preface_is_closed() {
        let addr = serve(Limits::default(), timers("age")).await;
        assert!(closes_a_peer_that_sends(addr, b"").await);
    }

    /// A peer that sends the HTTP/2 preface and then never answers is closed
    /// by keepalive: a dead client does not keep its connection, or what its
    /// replies hold, forever.
    #[tokio::test]
    async fn keepalive_closes_a_silent_peer() {
        let addr = serve(Limits::default(), timers("age")).await;
        // The client preface, then an empty SETTINGS frame. No frame of the
        // server is ever answered, so its ping is not either.
        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";
        assert!(closes_a_peer_that_sends(addr, preface).await);
    }

    /// One stalled reply is already over the limit, so a client that stops
    /// reading on one stream gets its connection closed, though it sends
    /// nothing more: the count wakes the read side. Another connection is
    /// served once the stalled one is closed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_connection_whose_stalled_replies_hold_too_much_is_closed() {
        let limits = Limits {
            connections: NonZeroUsize::MIN,
            stalled_bytes: NonZeroU64::new(reply::STALLED_REPLY_BYTES).unwrap(),
            ..Limits::default()
        };
        // Long keepalive and age, so only the count can close the first
        // connection.
        let long = Duration::from_secs(3600);
        let timers = Timers {
            handshake: Duration::from_millis(100),
            keepalive: long,
            keepalive_timeout: long,
            age: long,
            grace: long,
        };
        let addr = serve(limits, timers).await;
        let mut stalling = ObjectsClient::new(
            endpoint(addr)
                .initial_stream_window_size(65_535)
                .initial_connection_window_size(65_535)
                .connect()
                .await
                .unwrap(),
        );
        let mut stalled_stream = stall(&mut stalling).await;

        // The other connection waits for a place, and is served once the
        // process closes the stalled one.
        let mut other = ObjectsClient::new(endpoint(addr).connect_lazy());
        let mut next = pin!(quick(&mut other));
        let code = tokio::time::timeout(Duration::from_secs(10), &mut next)
            .await
            .expect(
                "the connection whose stalled replies hold too much is closed; the other is served",
            );
        assert_eq!(code, Code::NotFound);
        // The stalled stream of the closed connection holds nothing more.
        assert!(matches!(stalled_stream.message().await, Err(_) | Ok(None)));
    }

    /// TCP connections have Nagle's algorithm off, so a small reply does not
    /// wait for the client's ACK.
    #[tokio::test]
    async fn tcp_connections_have_no_delay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let accepted = pin!(tcp(listener)).next().await.unwrap().unwrap();
        assert!(accepted.nodelay().unwrap());
    }

    /// A client that trusts the CA of the server is served over TLS.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tls_client_is_served() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            TLS_HANDSHAKE_TIMEOUT,
        )
        .await;
        let mut client = client(addr, &certs.authority.cert(), None).await.unwrap();
        assert_eq!(quick(&mut client).await, Code::NotFound);
    }

    /// An mTLS server refuses a client without a certificate.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mtls_server_refuses_a_client_without_a_certificate() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            TLS_HANDSHAKE_TIMEOUT,
        )
        .await;
        // The connect of a client can end before the server has checked its
        // certificate, so the refusal may show in the connect or in the
        // call. A served client gets the stub's `NotFound`; anything else is
        // the refusal.
        match client(addr, &certs.authority.cert(), None).await {
            Err(_) => {}
            Ok(mut call) => {
                let code = call
                    .resolve_ref(ResolveRefRequest::default())
                    .await
                    .unwrap_err()
                    .code();
                assert_ne!(code, Code::NotFound, "the stub served the client");
            }
        }
    }

    /// An mTLS server serves a client with a certificate of its CA.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mtls_server_serves_a_client_with_a_certificate() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            TLS_HANDSHAKE_TIMEOUT,
        )
        .await;
        let mut client = client(addr, &certs.authority.cert(), Some(&certs.client))
            .await
            .unwrap();
        assert_eq!(quick(&mut client).await, Code::NotFound);
    }

    /// A client that trusts another CA is refused: the certificate of the
    /// server does not verify.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_with_the_wrong_ca_is_refused() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            TLS_HANDSHAKE_TIMEOUT,
        )
        .await;
        assert!(client(addr, &certs.stranger.cert(), None).await.is_err());
    }

    /// An mTLS server refuses a client whose certificate another CA signed.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_mtls_server_refuses_a_client_of_another_ca() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, Some(&certs.authority.cert()));
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            TLS_HANDSHAKE_TIMEOUT,
        )
        .await;
        // As above: the refusal may show in the connect or in the call.
        match client(addr, &certs.authority.cert(), Some(&certs.stranger_client)).await {
            Err(_) => {}
            Ok(mut call) => {
                let code = call
                    .resolve_ref(ResolveRefRequest::default())
                    .await
                    .unwrap_err()
                    .code();
                assert_ne!(code, Code::NotFound, "the stub served the client");
            }
        }
    }

    /// A peer that opens a TLS connection and sends nothing is dropped by
    /// the TLS handshake timeout.
    #[tokio::test]
    async fn a_peer_without_the_tls_handshake_is_dropped() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            Duration::from_millis(100),
        )
        .await;
        assert!(closes_a_peer_that_sends(addr, b"").await);
    }

    /// A peer that ends the TLS handshake and then sends nothing holds no
    /// place: the preface timer of the decrypted connection closes it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_peer_silent_after_the_tls_handshake_is_closed() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        // All timers are short (timers("age")), so the peer is closed soon
        // after it goes silent: the TLS handshake timeout can no longer
        // reach it once the handshake ended, so only the preface timer can.
        let addr = serve_tls(
            Limits::default(),
            timers("age"),
            &server_tls,
            Duration::from_millis(100),
        )
        .await;
        let mut peer = raw_client(addr, &certs.authority.cert(), None).await;
        // The server aborts the connection (no clean TLS close), so the
        // read ends with an error; what matters is that it ends: the peer
        // holds no place for life.
        let read =
            tokio::time::timeout(Duration::from_secs(10), peer.read_to_end(&mut Vec::new())).await;
        assert!(read.is_ok(), "the server closed the silent peer");
    }

    /// A peer whose TLS handshake ends late, and that goes silent right
    /// after it, is closed within one deadline from its accept: the
    /// handshake and the preface share the deadline, so the close comes
    /// before a second full handshake timeout could run.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_silent_tls_peer_is_closed_within_one_deadline() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        let deadline = Duration::from_secs(1);
        let addr = serve_tls(Limits::default(), TIMERS, &server_tls, deadline).await;
        // The peer connects at once, waits out most of the deadline, then
        // ends its handshake and sends nothing more. The clock runs from the
        // connect, which is the server's accept to within a millisecond.
        let started = std::time::Instant::now();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(deadline * 5 / 6).await;
        let mut roots = RootCertStore::empty();
        roots
            .add(
                CertificateDer::from_pem_slice(certs.authority.cert().cert_pem.as_bytes()).unwrap(),
            )
            .unwrap();
        let builder = ClientConfig::builder_with_provider(crate::runtime::tls::provider())
            .with_safe_default_protocol_versions()
            .unwrap();
        let config = builder.with_root_certificates(roots).with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let mut peer = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let read =
            tokio::time::timeout(Duration::from_secs(10), peer.read_to_end(&mut Vec::new())).await;
        assert!(read.is_ok(), "the server closed the silent peer");
        // One deadline from the accept, with slack for the handshake round
        // trip and a loaded runner. The old code (a full preface timer after
        // the handshake) closes at about deadline + delay, 1.83 s here,
        // past this bound.
        assert!(
            started.elapsed() < deadline * 3 / 2,
            "the silent peer held its place {:?}, past the deadline",
            started.elapsed()
        );
    }

    /// A peer that opens a TLS connection and sends nothing holds its place,
    /// but no accept: a second client is served at once, while the silent
    /// peer's handshake timeout is still far away. With the handshake run in
    /// sequence instead (the mutation), the second client would wait for the
    /// silent peer's timeout, past 20 s.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_silent_tls_handshake_does_not_block_other_clients() {
        let certs = test_certs();
        let dir = tempfile::tempdir().unwrap();
        let server_tls = server_tls(dir.path(), &certs.server, None);
        // The silent peer's handshake timeout: far beyond the next test
        // step, so only the blocking of a sequential handshake can end it.
        let addr = serve_tls(
            Limits::default(),
            TIMERS,
            &server_tls,
            Duration::from_secs(60),
        )
        .await;
        // A peer that connects and sends nothing: no ClientHello.
        let mut silent = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let served = tokio::time::timeout(Duration::from_secs(20), async {
            let mut client = client(addr, &certs.authority.cert(), None).await?;
            Ok::<_, tonic::transport::Error>(quick(&mut client).await)
        })
        .await
        .expect("a silent handshake does not block the next client")
        .expect("the client connects");
        assert_eq!(served, Code::NotFound);
        // The silent peer is still holding its connection: a read would
        // block, not return.
        let mut buf = [0; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), silent.read(&mut buf))
                .await
                .is_err(),
            "the silent peer is not yet closed"
        );
        drop(silent);
    }
}

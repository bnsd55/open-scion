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
//! So [`Limits::serve`] bounds how many such streams there are: at most
//! [`Limits::connections`] connections at once, each with at most
//! [`Limits::streams`] open streams. And it closes every connection in
//! time, so a connection that waits for a place gets one:
//!
//! - a client that does not send the HTTP/2 preface within
//!   [`HANDSHAKE_TIMEOUT`] is closed. hyper has no timeout of its own
//!   there, and its keepalive starts only after the preface.
//! - after the preface, HTTP/2 keepalive closes a connection whose client
//!   does not answer a ping.
//! - every connection is asked to close (`GOAWAY`) after
//!   [`MAX_CONNECTION_AGE`], and is closed [`CONNECTION_GRACE`] later, with
//!   any call still in it. A client then connects again.

use std::error::Error;
use std::future::Future;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Sleep;
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
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);

/// After this long without a frame from a client, the process sends it an
/// HTTP/2 ping.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// A connection whose client does not answer a ping within this long is
/// closed. So after the preface, a dead peer frees its connection, and all
/// that its replies hold, within [`KEEPALIVE_INTERVAL`] plus this of its
/// last frame.
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// After this long, a connection is asked to close, so one that a client
/// keeps for life (a daemon keeps one to its cache) gives its place up.
pub const MAX_CONNECTION_AGE: Duration = Duration::from_secs(30 * 60);

/// A connection that was asked to close is closed this long later, with any
/// call still in it. At least the server's longest call (600 s).
pub const CONNECTION_GRACE: Duration = Duration::from_secs(600);

/// The bytes of the HTTP/2 client preface.
const PREFACE_BYTES: usize = 24;

/// How many connections a process serves, and how many streams each may
/// have open.
// SHORTCUT: a live client that stops reading keeps what its stalled streams
// hold until its connection closes (at most the age plus the grace), so the
// ceiling is connections x streams x what one stream keeps (see the module
// docs). Upgrade (issue #113): count
// what the stalled replies of each connection hold, and close a connection
// that holds too much.
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
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: DEFAULT_CONNECTIONS,
            streams: DEFAULT_STREAMS,
        }
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

impl Limits {
    /// Serves the services that `router` adds to a server, on `incoming`
    /// connections, within these limits and with the timers above, until
    /// `shutdown` resolves. A connection that waits for its place logs a
    /// warning.
    ///
    /// # Errors
    ///
    /// Transport errors.
    // TLS (issue #14) adds a handshake of its own, which needs a timeout too.
    // `router` will then return a `Result`, for the TLS setup.
    pub async fn serve<I, IO, IE>(
        self,
        router: impl FnOnce(Server) -> Router,
        incoming: I,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = Result<IO, IE>>,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IE: Into<Box<dyn Error + Send + Sync>>,
    {
        self.serve_with(&TIMERS, router, incoming, shutdown).await
    }

    async fn serve_with<I, IO, IE>(
        self,
        timers: &Timers,
        router: impl FnOnce(Server) -> Router,
        incoming: I,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error>
    where
        I: Stream<Item = Result<IO, IE>>,
        IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
        IE: Into<Box<dyn Error + Send + Sync>>,
    {
        let server = Server::builder()
            .max_concurrent_streams(self.streams.get())
            .http2_keepalive_interval(Some(timers.keepalive))
            .http2_keepalive_timeout(Some(timers.keepalive_timeout))
            .max_connection_age(timers.age)
            .max_connection_age_grace(timers.grace);
        let incoming = accept(incoming, self.connections, timers.handshake);
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

/// The connections from `incoming`, at most `max` at once. Each keeps its
/// place until it is dropped, when it closes. Each must send the preface
/// within `handshake` once it has its place.
fn accept<I, IO, E>(
    incoming: I,
    max: NonZeroUsize,
    handshake: Duration,
) -> impl Stream<Item = Result<Connection<IO>, E>>
where
    I: Stream<Item = Result<IO, E>>,
{
    let max = max.get();
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
}

impl<IO: Connected> Connected for Connection<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Connection<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
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
    use std::pin::pin;

    use tonic::transport::{Channel, Endpoint};
    use tonic::{Code, Request, Response, Status, Streaming};

    use super::*;
    use crate::Bytes;
    use crate::v1::objects_client::ObjectsClient;
    use crate::v1::objects_server::{Objects, ObjectsServer};
    use crate::v1::{
        GetObjectsRequest, GetRootTreeRequest, GetRootTreeResponse, ObjectPart, ResolveRefRequest,
        ResolveRefResponse,
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

    /// Replies to `GetObjects` with one part of 128 KiB, more than a client's
    /// whole connection window of 64 KiB, so a client that does not read
    /// keeps the stream open. Refuses `ResolveRef` at once, with no data, so
    /// a full window does not hold that reply.
    struct Stub;

    #[tonic::async_trait]
    impl Objects for Stub {
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

        type GetObjectsStream = tokio_stream::Iter<std::vec::IntoIter<Result<ObjectPart, Status>>>;

        async fn get_objects(
            &self,
            _: Request<GetObjectsRequest>,
        ) -> Result<Response<Self::GetObjectsStream>, Status> {
            let part = ObjectPart {
                data: Bytes::from(vec![0; 128 << 10]),
                ..ObjectPart::default()
            };
            Ok(Response::new(tokio_stream::iter(vec![Ok(part)])))
        }
    }

    /// The stub, served within `limits` with `timers`, and its address.
    async fn serve(limits: Limits, timers: Timers) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let router = |mut server: Server| server.add_service(ObjectsServer::new(Stub));
            let pending = std::future::pending();
            limits
                .serve_with(&timers, router, tcp(listener), pending)
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
}

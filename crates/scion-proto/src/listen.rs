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
//! [`Limits::streams`] open streams. HTTP/2 keepalive closes a connection
//! whose client does not answer a ping, which frees all that it holds.

use std::error::Error;
use std::future::Future;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::Server;
use tonic::transport::server::{Connected, Router};

/// The default of [`Limits::connections`]. Room for the 50 checkouts at once
/// of `scripts/bench-checkout.sh`, each on its own connection to one daemon.
pub const DEFAULT_CONNECTIONS: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// The default of [`Limits::streams`]. One connection can still fill all
/// 256 job slots of the server, and carry the 200 agents of `scn bench`.
pub const DEFAULT_STREAMS: NonZeroU32 = NonZeroU32::new(256).unwrap();

/// After this long without a frame from a client, the process sends it an
/// HTTP/2 ping.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// A connection whose client does not answer a ping within this long is
/// closed. So a dead peer frees its connection, and all that its replies
/// hold, within [`KEEPALIVE_INTERVAL`] plus this.
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// How many connections a process serves, and how many streams each may
/// have open.
// SHORTCUT: a live client that stops reading keeps what its stalled streams
// hold until its connection closes, so the ceiling is connections x streams
// x what one stream keeps (see the module docs). Upgrade (issue #113): count
// what the stalled replies of each connection hold, and close a connection
// that holds too much.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Most connections served at once. A connection beyond that is
    /// accepted, and waits unread until another connection closes.
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

impl Limits {
    /// Serves the services that `router` adds to a server, on `incoming`
    /// connections, within these limits and with keepalive, until
    /// `shutdown` resolves. A connection that waits for its place logs a
    /// warning.
    ///
    /// # Errors
    ///
    /// Transport errors.
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
        router(builder(self.streams, KEEPALIVE_INTERVAL, KEEPALIVE_TIMEOUT))
            .serve_with_incoming_shutdown(accept(incoming, self.connections), shutdown)
            .await
    }
}

/// A server builder with the stream limit and keepalive.
fn builder(streams: NonZeroU32, keepalive: Duration, timeout: Duration) -> Server {
    Server::builder()
        .max_concurrent_streams(streams.get())
        .http2_keepalive_interval(Some(keepalive))
        .http2_keepalive_timeout(Some(timeout))
}

/// The connections from `incoming`, at most `max` at once. Each keeps its
/// place until it is dropped, when it closes.
fn accept<I, IO, E>(incoming: I, max: NonZeroUsize) -> impl Stream<Item = Result<Connection<IO>, E>>
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
            Ok(Connection { io, _place: place })
        }
    })
}

/// A connection that keeps its place of [`accept`] until it is dropped.
struct Connection<IO> {
    io: IO,
    _place: OwnedSemaphorePermit,
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
        Pin::new(&mut self.io).poll_read(cx, buf)
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
    use std::pin::pin;

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::service::Routes;
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

    /// The stub, served within `limits`, and its address.
    async fn serve(limits: Limits) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = |mut server: Server| server.add_service(ObjectsServer::new(Stub));
        let incoming = TcpListenerStream::new(listener);
        tokio::spawn(limits.serve(router, incoming, std::future::pending()));
        url
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
        let url = serve(limits).await;
        // The smallest windows, so no reply ends while it is unread.
        let channel = Endpoint::from_shared(url)
            .unwrap()
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
        let endpoint = Endpoint::from_shared(serve(limits).await).unwrap();
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

    /// A peer that sends the HTTP/2 preface and then never answers is closed
    /// by keepalive: a dead client does not keep its connection, or what its
    /// replies hold, forever.
    #[tokio::test]
    async fn keepalive_closes_a_silent_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ping = Duration::from_millis(100);
        let incoming = accept(TcpListenerStream::new(listener), DEFAULT_CONNECTIONS);
        tokio::spawn(
            builder(DEFAULT_STREAMS, ping, ping)
                .add_routes(Routes::default())
                .serve_with_incoming(incoming),
        );
        let closed = tokio::task::spawn_blocking(move || {
            let mut peer = std::net::TcpStream::connect(addr)?;
            // The client preface, then an empty SETTINGS frame. No frame of
            // the server is ever answered, so its ping is not either.
            peer.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")?;
            peer.set_read_timeout(Some(Duration::from_secs(10)))?;
            // Ends when the server closes the connection, with an end of
            // file or a reset.
            peer.read_to_end(&mut Vec::new())
        });
        if let Err(e) = closed.await.unwrap() {
            let timed_out = matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            );
            assert!(!timed_out, "the server kept the connection open");
        }
    }
}

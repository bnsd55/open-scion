//! How the server and the cache tiers accept connections. They share this
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
//! So this module bounds how many such streams there are: at most
//! [`Limits::connections`] connections at once, each with at most
//! [`Limits::streams`] open streams. HTTP/2 keepalive closes a connection
//! whose client does not answer a ping, which frees all that it holds.

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
use tonic::transport::server::Connected;

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
// hold until its connection closes. So the ceiling is connections x streams
// x (under 4 MiB for a streamed reply, up to 64 MiB for a unary one). hyper
// cannot reset a stalled stream from the server side. Upgrade (issue #113):
// count what the stalled replies of each connection hold, and close a
// connection that holds too much.
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
    /// A server builder with the stream limit and keepalive.
    #[must_use]
    pub fn server(self) -> Server {
        builder(self.streams, KEEPALIVE_INTERVAL, KEEPALIVE_TIMEOUT)
    }

    /// The connections from `incoming`, at most [`Limits::connections`] at
    /// once. Each keeps its place until it is dropped, when it closes. A
    /// connection that waits for a place logs a warning.
    pub fn accept<I, IO, E>(self, incoming: I) -> impl Stream<Item = Result<Connection<IO>, E>>
    where
        I: Stream<Item = Result<IO, E>>,
    {
        let max = self.connections.get();
        let places = Arc::new(Semaphore::new(max));
        incoming.then(move |accepted| {
            let places = Arc::clone(&places);
            async move {
                let io = accepted?;
                let place = if let Ok(place) = Arc::clone(&places).try_acquire_owned() {
                    Some(place)
                } else {
                    tracing::warn!(max, "a connection waits: the most connections are open");
                    places.acquire_owned().await.ok()
                };
                Ok(Connection { io, _place: place })
            }
        })
    }
}

fn builder(streams: NonZeroU32, keepalive: Duration, timeout: Duration) -> Server {
    Server::builder()
        .max_concurrent_streams(streams.get())
        .http2_keepalive_interval(Some(keepalive))
        .http2_keepalive_timeout(Some(timeout))
}

/// A connection that keeps one of the places of [`Limits::accept`] until it
/// is dropped.
#[derive(Debug)]
pub struct Connection<IO> {
    io: IO,
    /// `None` only if the semaphore of places were closed. It never is.
    _place: Option<OwnedSemaphorePermit>,
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

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::service::Routes;

    use super::*;

    /// A peer that sends the HTTP/2 preface and then never answers is closed
    /// by keepalive: a dead client does not keep its connection, or what its
    /// replies hold, forever.
    #[tokio::test]
    async fn keepalive_closes_a_silent_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ping = Duration::from_millis(100);
        let incoming = Limits::default().accept(TcpListenerStream::new(listener));
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

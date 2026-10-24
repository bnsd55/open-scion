//! The reply-memory budget.
//!
//! A unary reply's bytes sit in the server's memory from the moment the
//! handler builds them until the client takes them: HTTP/2 parks the
//! whole encoded message, uncounted by every other budget, for as long
//! as the client does not read it and the connection stays open. A
//! client that opens streams and never reads them holds that memory
//! forever, one reply per stream.
//!
//! [`ReplyMemoryLayer`] counts those bytes against one
//! [`REPLY_MEMORY_BUDGET`]: when a reply body's size is known and its
//! budget share cannot be taken, the reply is refused with
//! `resource_exhausted`, so a client that hoards streams cannot hold
//! more than the budget names. The permit lives in the reply's body
//! wrapper, so it is released on every way the body ends: the reply
//! finishes, the client resets the stream, or the connection closes.
//!
//! Replies that stream (whose size is not known ahead) do not go
//! through this budget: they take their own share from the read budget
//! before their object is built, which bounds the memory their
//! building holds.

use std::sync::Arc;
use std::task::{Context, Poll};

use http_body::Body as HttpBody;
use std::time::Instant;

use scion_proto::runtime::metrics::Metrics;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The bytes of unary replies the server holds at once, across every
/// connection and stream. The largest unary reply today is a `Diff`
/// of about 120 KB, so the budget holds roughly a thousand of them; a
/// client that never reads holds the budget, not the server's memory.
pub const REPLY_MEMORY_BUDGET: usize = 128 << 20;

/// Counts unary reply bytes against [`REPLY_MEMORY_BUDGET`].
#[derive(Clone)]
pub(crate) struct ReplyMemoryLayer {
    budget: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

impl ReplyMemoryLayer {
    pub(crate) fn with_budget(budget: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            budget: Arc::new(Semaphore::new(budget)),
            metrics,
        }
    }

    pub(crate) fn layer<S>(self, inner: S) -> ReplyMemoryService<S> {
        ReplyMemoryService {
            inner,
            budget: self.budget,
            metrics: self.metrics,
        }
    }
}

/// The mark of a streaming reply: its bytes are counted by the
/// read-budget share its handler took before the object was built,
/// not by this layer.
#[derive(Clone, Copy)]
pub(crate) struct ReplyStreams;

/// The refusal a spent budget answers with: a `resource_exhausted`
/// status carried as a whole http reply, with the content-type a gRPC
/// client expects (a reply without it is a protocol error to some
/// clients, whatever tonic accepts).
fn refusal(message: &'static str) -> http::Response<tonic::body::Body> {
    tonic::Status::resource_exhausted(message).into_http()
}

/// The encoded size of a unary reply, set by the handler that built
/// it, so the budget counts bytes it cannot learn from the body (a
/// tonic-encoded body carries no exact size hint).
#[derive(Clone, Copy)]
pub(crate) struct ReplySize(pub usize);

/// The mark of a reply that must be delivered even when the budget is
/// spent: its call changed state, so the write has committed and the
/// client must read the truth, never a retryable error. The layer
/// sends such a reply anyway, uncounted; its bytes are a few dozen.
#[derive(Clone, Copy)]
pub(crate) struct ReplyMustDeliver;

/// The reply body that holds the budget permit until it ends.
struct CountedReply<B> {
    inner: B,
    _permit: OwnedSemaphorePermit,
}

impl<B: HttpBody<Data = scion_proto::Bytes> + Unpin + Send + 'static> HttpBody for CountedReply<B> {
    type Data = scion_proto::Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::pin::Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Clone)]
pub(crate) struct ReplyMemoryService<S> {
    inner: S,
    budget: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for ReplyMemoryService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> tower::Service<http::Request<tonic::body::Body>> for ReplyMemoryService<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<B>,
            Error = std::convert::Infallible,
        >,
    B: HttpBody<Data = bytes::Bytes> + Unpin + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let budget = Arc::clone(&self.budget);
        let metrics = Arc::clone(&self.metrics);
        let path = request.uri().path().to_owned();
        let future = self.inner.call(request);
        // MUTATION levers for the tests' red proofs: "uncounted" never
        // takes the share (a parked reply holds nothing the budget
        // knows of); "held_forever" takes it and never gives it back
        // (a reset cannot free it). Release builds never read these.
        #[cfg(any(test, feature = "test-hooks"))]
        match std::env::var_os("SCION_TEST_REPLY_MUTATION").as_deref() {
            Some(x) if x == "uncounted" => {
                return Box::pin(async move {
                    let (parts, body) = future.await?.into_parts();
                    Ok(http::Response::from_parts(
                        parts,
                        tonic::body::Body::new(body),
                    ))
                });
            }
            Some(x) if x == "held_forever" => {
                return Box::pin(async move {
                    let response = future.await?;
                    let size = response.extensions().get::<ReplySize>().map_or(0, |s| s.0);
                    // Count as the real path does, but leak the permit:
                    // no reset can ever free it.
                    if size > 0 {
                        match budget
                            .clone()
                            .try_acquire_many_owned(u32::try_from(size).unwrap_or(u32::MAX))
                        {
                            Err(_) => {
                                return Ok(refusal(
                                    "the server's reply budget is spent; read what you opened",
                                ));
                            }
                            Ok(permit) => std::mem::forget(permit),
                        }
                    }
                    let (parts, body) = response.into_parts();
                    Ok(http::Response::from_parts(
                        parts,
                        tonic::body::Body::new(body),
                    ))
                });
            }
            _ => {}
        }
        Box::pin(async move {
            let response = future.await?;
            // A test seam, so a handler that forgets both marks fails a
            // test instead of passing the budget uncounted: an OK reply
            // (one that does not refuse in its headers) is either a
            // unary reply with a size, or a streaming reply with the
            // stream mark. Release builds never check.
            #[cfg(any(test, feature = "test-hooks"))]
            if response.headers().get(tonic::Status::GRPC_STATUS).is_none() {
                let size = response.extensions().get::<ReplySize>().is_some();
                let streams = response.extensions().get::<ReplyStreams>().is_some();
                assert!(
                    size || streams,
                    "a reply passed the reply-memory layer with neither mark: {path}"
                );
            }
            // The size is what the handler knew when it built the
            // reply (its encoded length, set as a response extension),
            // else the body's exact hint. A streaming reply names
            // neither and holds its own share from the read budget.
            let size = if let Some(ReplySize(size)) = response.extensions().get::<ReplySize>() {
                *size
            } else {
                usize::try_from(response.body().size_hint().exact().unwrap_or(0))
                    .unwrap_or(usize::MAX)
            };
            let permit = if size == 0 {
                None
            } else if let Ok(permit) = budget
                .clone()
                .try_acquire_many_owned(u32::try_from(size).unwrap_or(u32::MAX))
            {
                Some(permit)
            } else if response.extensions().get::<ReplyMustDeliver>().is_some() {
                // A call that changed state: the write has committed,
                // and the client must read the truth, never a
                // retryable error for a write that landed. Sent
                // uncounted; the reply is a few dozen bytes.
                tracing::warn!(
                    path,
                    size,
                    "a committed write's reply is over the reply budget: sent uncounted"
                );
                None
            } else {
                // The budget is out: refuse the reply rather
                // than hold memory no other reply can count
                // on. The built response is dropped here, so
                // the refusal frees what it held.
                tracing::warn!(size, "a unary reply refused: the reply budget is spent");
                // The outer counting body never sees this reply's
                // trailers (the status rides in the headers), so it
                // would record CANCELLED on its drop; the refusal is a
                // resource_exhausted, and an operator who looks for it
                // during an incident must find it.
                if let Some(rpc) = metrics.rpc(&path) {
                    metrics.request(rpc, 8, Instant::now(), 0, 0);
                }
                return Ok(refusal(
                    "the server's reply budget is spent; read what you opened",
                ));
            };
            let (parts, body) = response.into_parts();
            match permit {
                Some(permit) => {
                    let counted = CountedReply {
                        inner: body,
                        _permit: permit,
                    };
                    Ok(http::Response::from_parts(
                        parts,
                        tonic::body::Body::new(counted),
                    ))
                }
                None => Ok(http::Response::from_parts(
                    parts,
                    tonic::body::Body::new(body),
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::task::Poll;
    use tower::Service as _;

    /// An inner service that answers every call with a reply whose
    /// body's size hint is `size` bytes, as a unary reply's is.
    #[derive(Clone)]
    struct FixedReply {
        size: u64,
        never_read: bool,
        marked: bool,
        must_deliver: bool,
    }

    impl tower::Service<http::Request<tonic::body::Body>> for FixedReply {
        type Response = http::Response<FullBody>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: http::Request<tonic::body::Body>) -> Self::Future {
            let body = FullBody {
                size: self.size,
                never_read: self.never_read,
            };
            let mut reply = http::Response::new(body);
            if self.must_deliver {
                reply.extensions_mut().insert(ReplyMustDeliver);
            }
            if self.marked {
                if self.size == 0 {
                    reply.extensions_mut().insert(ReplyStreams);
                } else {
                    reply
                        .extensions_mut()
                        .insert(ReplySize(usize::try_from(self.size).unwrap_or(usize::MAX)));
                }
            }
            std::future::ready(Ok(reply))
        }
    }

    /// A body that is never polled: its bytes stay held, as a client
    /// that never reads leaves them.
    struct FullBody {
        size: u64,
        never_read: bool,
    }

    impl HttpBody for FullBody {
        type Data = scion_proto::Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            if self.never_read {
                Poll::Pending
            } else {
                Poll::Ready(None)
            }
        }

        fn size_hint(&self) -> http_body::SizeHint {
            http_body::SizeHint::with_exact(self.size)
        }
    }

    fn request() -> http::Request<tonic::body::Body> {
        http::Request::builder()
            .body(tonic::body::Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn a_unary_reply_holds_its_bytes_until_its_body_ends() {
        let budget = Arc::new(Semaphore::new(1_000));
        let mut service = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: false,
            marked: true,
            must_deliver: false,
        });
        let response = service.call(request()).await.unwrap();
        assert_eq!(
            budget.available_permits(),
            600,
            "the reply's bytes are counted"
        );
        drop(response);
        assert_eq!(
            budget.available_permits(),
            1_000,
            "the reply's end frees its bytes"
        );
    }

    #[tokio::test]
    async fn a_reply_the_client_abandons_frees_its_bytes_on_the_drop() {
        // The transport drops the body on a client reset or a
        // connection close; the permit goes with it either way.
        let budget = Arc::new(Semaphore::new(1_000));
        let mut service = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: true,
            marked: true,
            must_deliver: false,
        });
        let response = service.call(request()).await.unwrap();
        assert_eq!(budget.available_permits(), 600);
        drop(response);
        assert_eq!(
            budget.available_permits(),
            1_000,
            "an abandoned reply's bytes are free"
        );
    }

    #[tokio::test]
    async fn a_reply_over_the_spent_budget_is_refused_resource_exhausted() {
        let budget = Arc::new(Semaphore::new(1_000));
        let mut holder = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 1_000,
            never_read: true,
            marked: true,
            must_deliver: false,
        });
        let held = holder.call(request()).await.unwrap();
        assert_eq!(budget.available_permits(), 0);

        let mut refused = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: false,
            marked: true,
            must_deliver: false,
        });
        let reply = refused.call(request()).await.unwrap();
        assert_eq!(
            reply
                .headers()
                .get(tonic::Status::GRPC_STATUS)
                .and_then(|v| v.to_str().ok()),
            Some("8"),
            "the refusal is resource_exhausted"
        );
        drop(held);
        assert_eq!(budget.available_permits(), 1_000);
    }

    #[tokio::test]
    async fn a_committed_write_is_answered_when_the_budget_is_spent() {
        // The budget is spent; a read is refused, and a committed
        // write's reply goes out anyway, uncounted: the client must
        // read the truth for a write that landed.
        let budget = Arc::new(Semaphore::new(400));
        let mut holder = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: true,
            marked: true,
            must_deliver: false,
        });
        let held = holder.call(request()).await.unwrap();
        assert_eq!(budget.available_permits(), 0);

        let mut refused = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: false,
            marked: true,
            must_deliver: false,
        });
        let refusal = refused.call(request()).await.unwrap();
        assert_eq!(
            refusal
                .headers()
                .get(tonic::Status::GRPC_STATUS)
                .map(|v| v.to_str().unwrap()),
            Some("8"),
            "the read is refused"
        );

        let mut write = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: false,
            marked: true,
            must_deliver: true,
        });
        let reply = write.call(request()).await.unwrap();
        assert!(
            reply.headers().get(tonic::Status::GRPC_STATUS).is_none(),
            "a committed write is never refused"
        );
        drop(held);
        assert_eq!(budget.available_permits(), 400, "the write held nothing");
    }

    #[tokio::test]
    async fn a_refusal_is_counted_resource_exhausted_not_cancelled() {
        let metrics = Arc::new(Metrics::default());
        // The refusal counts under the call's own RPC, which the path
        // names, as a real call's does.
        let request = http::Request::builder()
            .uri("http://x/scion.v1.Scion/Diff")
            .body(tonic::body::Body::empty())
            .unwrap();
        // One 400-byte reply fits; a second does not, and is refused.
        let budget = Arc::new(Semaphore::new(600));
        let mut holder = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::clone(&metrics),
        }
        .layer(FixedReply {
            size: 400,
            never_read: true,
            marked: true,
            must_deliver: false,
        });
        let _held = holder.call(request).await.unwrap();
        let request = http::Request::builder()
            .uri("http://x/scion.v1.Scion/Diff")
            .body(tonic::body::Body::empty())
            .unwrap();

        let mut refused = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::clone(&metrics),
        }
        .layer(FixedReply {
            size: 400,
            never_read: false,
            marked: true,
            must_deliver: false,
        });
        let _refusal = refused.call(request).await.unwrap();
        let text = metrics.render();
        assert!(
            text.contains("code=\"resource_exhausted\"} 1"),
            "the refusal is a resource_exhausted: {text}"
        );
        assert!(
            !text.contains("code=\"cancelled\"} 1"),
            "the refusal is not a cancelled: {text}"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "neither mark")]
    async fn a_reply_with_neither_mark_is_a_test_failure() {
        // A handler that forgets both marks fails the test that calls
        // its RPC, instead of passing the budget uncounted.
        let budget = Arc::new(Semaphore::new(1_000));
        let mut service = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 400,
            never_read: false,
            marked: false,
            must_deliver: false,
        });
        let _ = service.call(request()).await;
    }

    #[tokio::test]
    async fn a_streaming_reply_takes_no_budget_share() {
        let budget = Arc::new(Semaphore::new(1_000));
        let mut service = ReplyMemoryLayer {
            budget: Arc::clone(&budget),
            metrics: Arc::new(Metrics::default()),
        }
        .layer(FixedReply {
            size: 0, // no exact hint: the size is unknown, as a stream's is
            never_read: true,
            marked: true,
            must_deliver: false,
        });
        let response = service.call(request()).await.unwrap();
        assert_eq!(
            budget.available_permits(),
            1_000,
            "a streaming reply holds its own share from the read budget"
        );
        drop(response);
    }
}

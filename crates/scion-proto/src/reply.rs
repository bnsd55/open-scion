//! How a `GetObjects` reply streams objects. The server and every cache tier
//! follow the same rules, so they share this code.
//!
//! A reply hands the parts of its objects ([`crate::object`]) to its client
//! one at a time, through a one-slot channel. Before a reply reads an
//! object's bytes, it takes the object's share of a byte budget: the bytes
//! that the reply will hold for it. Each part carries its piece of that
//! share, and frees it when the client takes the part. A reply sends the next
//! object only while it has sent less than [`REPLY_BUDGET_BYTES`]. It ends
//! when its client takes no part for [`STALL_TIMEOUT`], or when the budget
//! has no room for the next object for [`BUSY_TIMEOUT`].
//!
//! A reply finds its first object before the call answers. If the budget has
//! no room for it, the call fails with [`busy`]. So a reply always holds at
//! least one object, and never ends `OK` with none. A reply that ends in the
//! middle of an object ends with `CANCELLED`, never `OK`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::Stream;
use tonic::Status;

use crate::object::MAX_ENCODED_BYTES;
use crate::v1::ObjectPart;
use crate::{MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES};

/// A reply stops before the next object once it has sent this many encoded
/// bytes, so it holds less than this plus one object. The client asks again
/// for the rest, so one call stays short.
pub const REPLY_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES;

/// The byte budget of one process: the most object bytes that all its
/// replies together hold after a read and before their client takes them.
pub const IN_FLIGHT_BYTES: usize = 4 * MAX_OBJECT_BYTES;

/// A reply ends when its client takes no part for this long, so a client
/// that stops reading does not hold the budget. (A request timeout covers
/// only the start of a streamed reply.)
// SHORTCUT: a stalled reply ends, but HTTP/2 keeps the one part (at most
// 1 MiB) that it was sending until the connection closes, and the number of
// stalled replies has no bound. Upgrade (issue #42): a response body timeout
// that resets the stream.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a reply waits for room in the budget for its next object. For
/// the first object, the call then fails with [`busy`]. For a later one, the
/// reply ends `OK` and the client asks again. A tier waits longer than this
/// for each step of its upstream, so it never gives up on a server that is
/// only busy.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

// Any one object fits in the budget, even when a tier holds it both packed
// and unpacked, and the budget counts in u32 permits.
const _: () = assert!(MAX_ENCODED_BYTES + MAX_OBJECT_BYTES <= IN_FLIGHT_BYTES);
const _: () = assert!(IN_FLIGHT_BYTES <= u32::MAX as usize);

/// An object for a reply: its parts, or why it cannot be sent, with its share
/// of the budget.
pub type Found = (Result<Vec<ObjectPart>, Status>, OwnedSemaphorePermit);

/// A part on its way to the client, with its piece of the budget.
pub struct Sent {
    part: Result<ObjectPart, Status>,
    /// Freed when the client takes the part.
    _share: Option<OwnedSemaphorePermit>,
    /// Whether the part ends its object (or is an error).
    last: bool,
}

/// The `GetObjects` reply: the parts from the channel, minus the budget each
/// held. If the channel closes in the middle of an object, the reply ends
/// with `CANCELLED`.
#[derive(Debug)]
pub struct ObjectStream {
    rx: mpsc::Receiver<Sent>,
    /// The last part taken did not end its object.
    in_object: bool,
}

impl Stream for ObjectStream {
    type Item = Result<ObjectPart, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match this.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(sent)) => {
                this.in_object = !sent.last;
                Poll::Ready(Some(sent.part))
            }
            Poll::Ready(None) if this.in_object => {
                this.in_object = false;
                Poll::Ready(Some(Err(Status::cancelled(
                    "the reply was cut off in the middle of an object; ask again",
                ))))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

/// A new reply: the sender for [`send_objects`] and the stream for tonic.
#[must_use]
pub fn channel() -> (mpsc::Sender<Sent>, ObjectStream) {
    let (tx, rx) = mpsc::channel(1);
    let stream = ObjectStream {
        rx,
        in_object: false,
    };
    (tx, stream)
}

/// The share of the budget for `bytes` bytes: at least 1, at most
/// [`IN_FLIGHT_BYTES`], which the asserts above keep within u32.
fn share(bytes: usize) -> u32 {
    u32::try_from(bytes.clamp(1, IN_FLIGHT_BYTES)).unwrap_or(u32::MAX)
}

/// Takes the share of `budget` for `bytes` bytes. Waits until the share is
/// free, at most `wait`: the budget is held by objects that clients have not
/// taken yet. `None` if the wait ends, or the budget is closed.
pub async fn take(
    budget: &Arc<Semaphore>,
    bytes: usize,
    wait: Duration,
) -> Option<OwnedSemaphorePermit> {
    let acquire = Arc::clone(budget).acquire_many_owned(share(bytes));
    tokio::time::timeout(wait, acquire).await.ok()?.ok()
}

/// The status of a call whose first object found no room in the budget
/// within [`BUSY_TIMEOUT`]. The client asks again later.
#[must_use]
pub fn busy() -> Status {
    Status::resource_exhausted("busy: no memory for this reply; retry later")
}

/// Takes the share of `budget` for `bytes` bytes if it is free now.
#[must_use]
pub fn try_take(budget: &Arc<Semaphore>, bytes: usize) -> Option<OwnedSemaphorePermit> {
    Arc::clone(budget).try_acquire_many_owned(share(bytes)).ok()
}

/// Sends `first`, which the caller found before the call answered, then one
/// object per key, part by part, as the client takes them. `produce` finds an
/// object, takes its share of the budget with [`take`], and only then reads
/// the bytes, so a reply that waits for budget holds no object. Stops after
/// an error, once the reply has sent [`REPLY_BUDGET_BYTES`], when the client
/// has gone, when `produce` returns `None`, or when the client does not take
/// a part within `stall`. Finding an object has no time limit here: a cache
/// tier bounds its upstream fetches itself.
pub async fn send_objects<K, F>(
    first: Found,
    keys: impl IntoIterator<Item = K>,
    tx: mpsc::Sender<Sent>,
    stall: Duration,
    produce: impl Fn(K) -> F,
) where
    F: Future<Output = Option<Found>>,
{
    let mut keys = keys.into_iter();
    let mut next = Some(first);
    let mut sent = 0;
    while let Some((object, permit)) = next {
        let parts = match object {
            Ok(parts) => parts,
            Err(status) => {
                let error = Sent {
                    part: Err(status),
                    _share: Some(permit),
                    last: true,
                };
                tokio::time::timeout(stall, tx.send(error)).await.ok();
                return;
            }
        };
        let count = parts.len();
        let mut rest = Some(permit);
        for (i, part) in parts.into_iter().enumerate() {
            let len = part.data.len();
            let last = i + 1 == count;
            // Each part takes its piece of the share; the last one the rest.
            let piece = if last {
                rest.take()
            } else {
                rest.as_mut()
                    .and_then(|rest| rest.split(len.min(rest.num_permits() - 1)))
            };
            let part = Sent {
                part: Ok(part),
                _share: piece,
                last,
            };
            match tokio::time::timeout(stall, tx.send(part)).await {
                Ok(Ok(())) => sent += len,
                // The client is gone or stalled. In the middle of an object,
                // the stream then ends with `CANCELLED`.
                _ => return,
            }
        }
        if sent >= REPLY_BUDGET_BYTES {
            return;
        }
        let Some(key) = keys.next() else { return };
        next = produce(key).await;
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt as _;
    use tonic::Code;

    use super::*;
    use crate::object::{Header, chunks};
    use crate::v1::{Encoding, ObjectKind};

    /// The parts of a raw blob of `len` bytes.
    fn parts(len: usize) -> Vec<ObjectPart> {
        let header = Header {
            kind: ObjectKind::Blob,
            id: [1; 32],
            encoding: Encoding::Raw,
            encoded_size: len,
            size: len,
        };
        header.parts(chunks(vec![9; len]))
    }

    /// Waits until `done` holds, at most 10 s.
    async fn until(done: impl Fn() -> bool) {
        let started = tokio::time::Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(10), "timed out");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn each_part_frees_its_piece_of_the_share_when_taken() {
        let chunk = crate::CHUNK_BYTES;
        let len = 3 * chunk + 10;
        let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
        let permit = take(&budget, len, BUSY_TIMEOUT).await.unwrap();
        let (tx, mut stream) = channel();
        let first = (Ok(parts(len)), permit);
        let stall = Duration::from_secs(10);
        let reply = tokio::spawn(send_objects(first, [(); 0], tx, stall, |()| async { None }));
        let held = || IN_FLIGHT_BYTES - budget.available_permits();

        // The whole share is taken before the first part goes. Each part the
        // client takes frees exactly its own bytes.
        until(|| held() == len).await;
        for left in [len - chunk, len - 2 * chunk, len - 3 * chunk, 0] {
            stream.next().await.unwrap().unwrap();
            until(|| held() == left).await;
        }
        assert!(stream.next().await.is_none());
        reply.await.unwrap();
    }

    #[tokio::test]
    async fn a_client_that_stops_in_the_middle_of_an_object_gets_cancelled_and_frees_the_budget() {
        let len = 2 * crate::CHUNK_BYTES + 1;
        let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
        let permit = take(&budget, len, BUSY_TIMEOUT).await.unwrap();
        let (tx, mut stream) = channel();
        let stall = Duration::from_millis(50);
        let first = (Ok(parts(len)), permit);
        let reply = tokio::spawn(send_objects(first, [(); 0], tx, stall, |()| async { None }));

        // The client takes one part and stops. The second part waits in the
        // channel, the third stalls, and the reply ends.
        stream.next().await.unwrap().unwrap();
        reply.await.unwrap();
        let held = IN_FLIGHT_BYTES - budget.available_permits();
        assert_eq!(held, crate::CHUNK_BYTES, "only the part in the channel");
        stream.next().await.unwrap().unwrap();
        let status = stream.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::Cancelled, "{status:?}");
        assert!(stream.next().await.is_none());
        assert_eq!(budget.available_permits(), IN_FLIGHT_BYTES);
    }
}

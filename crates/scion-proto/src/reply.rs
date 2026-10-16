//! How a `GetObjects` reply streams objects. The server and every cache tier
//! follow the same rules, so they share this code.
//!
//! A reply hands the parts of its objects ([`crate::object`]) to its client
//! one at a time, through a one-slot channel. Before a reply reads an
//! object's bytes, it takes the object's share of a byte budget: all the
//! memory that the reply holds for the object. The reply keeps the object,
//! and its share, until it has handed the object's last part to the channel.
//! Each part that goes into the channel is a buffer of its own, so a part
//! that waits for a client that stopped reading never keeps the rest of its
//! object in memory.
//!
//! A reply sends the next object only while it has sent less than
//! [`REPLY_BUDGET_BYTES`]. It ends when its client takes no part for
//! [`STALL_TIMEOUT`], or when the budget has no room for the next object for
//! [`BUSY_TIMEOUT`]. A reply finds its first object before the call answers.
//! If the budget has no room for it, the call fails with [`busy`]. So a reply
//! always holds at least one object, and never ends `OK` with none. A reply
//! that ends in the middle of an object ends with `DEADLINE_EXCEEDED`, as
//! `ReadFile` does, never `OK`.
//!
//! Not counted in the budget: the parts that a reply leaves in its channel
//! and in HTTP/2 when its client stops reading ([`crate::listen`]).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::Stream;
use tonic::Status;

use crate::object::{Header, MAX_ENCODED_BYTES, UNPACK_BYTES};
use crate::v1::ObjectPart;
use crate::{Bytes, CHUNK_BYTES, MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES};

/// A reply stops before the next object once it has sent this many encoded
/// bytes, so it holds less than this plus one object. The client asks again
/// for the rest, so one call stays short.
pub const REPLY_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES;

/// The byte budget of one process: the most memory that all its replies
/// together hold for their objects.
pub const IN_FLIGHT_BYTES: usize = 4 * MAX_OBJECT_BYTES;

/// A reply ends when its client takes no part for this long, so a client
/// that stops reading does not hold the budget. (A request timeout covers
/// only the start of a streamed reply.)
pub const STALL_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a reply waits for room in the budget for its next object. For
/// the first object, the call then fails with [`busy`]. For a later one, the
/// reply ends `OK` and the client asks again. A tier waits longer than this
/// for each step of its upstream, so it never gives up on a server that is
/// only busy.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

// Any one object fits in the budget, even when a tier holds it packed and
// unpacked with a decoder, and the budget counts in u32 permits.
const _: () = assert!(MAX_ENCODED_BYTES + MAX_OBJECT_BYTES + UNPACK_BYTES <= IN_FLIGHT_BYTES);
const _: () = assert!(IN_FLIGHT_BYTES <= u32::MAX as usize);

/// The encoded bytes of an object for a reply.
#[derive(Debug)]
pub enum Body {
    /// Chunks of [`CHUNK_BYTES`] (the last shorter), each a buffer of its
    /// own, as [`crate::object::read_chunks`] makes them. Each goes out as a
    /// part as it is.
    Chunks(Vec<Bytes>),
    /// One buffer. Each part is copied out of it as it goes, so a part never
    /// keeps the whole buffer.
    Whole(Bytes),
}

/// An object for a reply.
#[derive(Debug)]
pub struct Outgoing {
    /// What its first part says.
    pub header: Header,
    /// Its encoded bytes.
    pub body: Body,
}

/// An object for a reply, or why it cannot be sent, with its share of the
/// budget.
pub type Found = (Result<Outgoing, Status>, OwnedSemaphorePermit);

/// A part on its way to the client.
pub struct Sent {
    part: Result<ObjectPart, Status>,
    /// Whether the part ends its object (or is an error).
    last: bool,
}

/// The `GetObjects` reply: the parts from the channel. If the channel closes
/// in the middle of an object, the reply ends with `DEADLINE_EXCEEDED`.
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
                Poll::Ready(Some(Err(Status::deadline_exceeded(
                    "the client took no part for the stall limit, in the middle of an object; ask again",
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

/// Takes the share of `budget` for `bytes` bytes if it is free now.
#[must_use]
pub fn try_take(budget: &Arc<Semaphore>, bytes: usize) -> Option<OwnedSemaphorePermit> {
    Arc::clone(budget).try_acquire_many_owned(share(bytes)).ok()
}

/// Gives back the part of `permit` above the share for `bytes` bytes, once
/// the work that needed more memory is done.
pub fn trim(permit: &mut OwnedSemaphorePermit, bytes: usize) {
    let keep = share(bytes) as usize;
    drop(permit.split(permit.num_permits().saturating_sub(keep)));
}

/// The status of a call whose first object found no room in the budget
/// within [`BUSY_TIMEOUT`]. The client asks again later.
#[must_use]
pub fn busy() -> Status {
    Status::resource_exhausted("busy: no memory for this reply; retry later")
}

/// The parts of an object, made one at a time as the reply sends them.
struct Parts {
    /// Until the first part is made.
    header: Option<Header>,
    rest: Rest,
}

/// The encoded bytes of an object that are not in a part yet.
enum Rest {
    Chunks(VecDeque<Bytes>),
    Whole { buffer: Bytes, offset: usize },
}

impl Parts {
    fn new(object: Outgoing) -> Self {
        let rest = match object.body {
            Body::Chunks(chunks) => Rest::Chunks(chunks.into()),
            Body::Whole(buffer) => Rest::Whole { buffer, offset: 0 },
        };
        Self {
            header: Some(object.header),
            rest,
        }
    }

    /// The next part, and whether it is the last. `None` after the last.
    fn next(&mut self) -> Option<(ObjectPart, bool)> {
        let header = self.header.take();
        let (data, last) = match &mut self.rest {
            Rest::Chunks(chunks) => match chunks.pop_front() {
                Some(data) => (data, chunks.is_empty()),
                None if header.is_some() => (Bytes::new(), true),
                None => return None,
            },
            Rest::Whole { buffer, offset } => {
                if *offset == buffer.len() && header.is_none() {
                    return None;
                }
                let end = buffer.len().min(*offset + CHUNK_BYTES);
                // A part that is the whole buffer goes as it is. Any other
                // is copied out, so it does not keep the whole buffer.
                let data = if *offset == 0 && end == buffer.len() {
                    buffer.clone()
                } else {
                    Bytes::copy_from_slice(&buffer[*offset..end])
                };
                *offset = end;
                (data, end == buffer.len())
            }
        };
        let part = match header {
            Some(header) => header.first_part(data),
            None => ObjectPart {
                data,
                ..ObjectPart::default()
            },
        };
        Some((part, last))
    }
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
        let mut parts = match object {
            Ok(object) => Parts::new(object),
            Err(status) => {
                let error = Sent {
                    part: Err(status),
                    last: true,
                };
                tokio::time::timeout(stall, tx.send(error)).await.ok();
                return;
            }
        };
        while let Some((part, last)) = parts.next() {
            let len = part.data.len();
            let part = Sent {
                part: Ok(part),
                last,
            };
            match tokio::time::timeout(stall, tx.send(part)).await {
                Ok(Ok(())) => sent += len,
                // The client is gone or stalled. In the middle of an object,
                // the stream then ends with `DEADLINE_EXCEEDED`.
                _ => return,
            }
        }
        // The object's bytes are gone, and so is its share.
        drop((parts, permit));
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
    use crate::object::read_chunks;
    use crate::v1::{Encoding, ObjectKind};

    /// A raw blob of `len` bytes, in one buffer or in chunks.
    fn blob(len: usize, whole: bool) -> Outgoing {
        let header = Header {
            kind: ObjectKind::Blob,
            id: [1; 32],
            encoding: Encoding::Raw,
            encoded_size: len,
            size: len,
        };
        let data = vec![9; len];
        let body = if whole {
            Body::Whole(Bytes::from(data))
        } else {
            Body::Chunks(read_chunks(&data[..], len).unwrap())
        };
        Outgoing { header, body }
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
    async fn an_object_keeps_its_share_until_its_last_part_is_handed_on() {
        let chunk = CHUNK_BYTES;
        let len = 3 * chunk + 10;
        for whole in [true, false] {
            let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
            let permit = take(&budget, len, BUSY_TIMEOUT).await.unwrap();
            let (tx, mut stream) = channel();
            let object = blob(len, whole);
            // Where the bytes of a whole buffer are.
            let buffer = match &object.body {
                Body::Whole(buffer) => buffer.as_ptr_range(),
                Body::Chunks(_) => std::ptr::null()..std::ptr::null(),
            };
            let first = (Ok(object), permit);
            let stall = Duration::from_secs(10);
            let reply = tokio::spawn(send_objects(first, [(); 0], tx, stall, |()| async { None }));
            let held = || IN_FLIGHT_BYTES - budget.available_permits();

            // Each part is a buffer of its own. The share stays until the
            // reply has handed on the last part: after the client took the
            // third.
            let mut got = Vec::new();
            for _ in 0..2 {
                got.push(stream.next().await.unwrap().unwrap());
                assert_eq!(held(), len, "whole: {whole}");
            }
            got.push(stream.next().await.unwrap().unwrap());
            until(|| held() == 0).await;
            got.push(stream.next().await.unwrap().unwrap());
            let lens: Vec<usize> = got.iter().map(|p| p.data.len()).collect();
            assert_eq!(lens, [chunk, chunk, chunk, 10]);
            // A part of a whole buffer is a copy, not a slice that keeps it.
            let inside = |part: &ObjectPart| buffer.contains(&part.data.as_ptr());
            assert!(!got.iter().any(inside), "whole: {whole}");
            assert_eq!(got[0].encoded_size, len as u64);
            assert!(got[1..].iter().all(|p| p.encoded_size == 0));
            assert!(stream.next().await.is_none());
            reply.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_client_that_stops_in_the_middle_of_an_object_gets_an_error_and_frees_the_budget() {
        let len = 2 * CHUNK_BYTES + 1;
        let budget = Arc::new(Semaphore::new(IN_FLIGHT_BYTES));
        let permit = take(&budget, len, BUSY_TIMEOUT).await.unwrap();
        let (tx, mut stream) = channel();
        let stall = Duration::from_millis(50);
        let first = (Ok(blob(len, true)), permit);
        let reply = tokio::spawn(send_objects(first, [(); 0], tx, stall, |()| async { None }));

        // The client takes one part and stops. The second part waits in the
        // channel, the third stalls, and the reply ends. Its object and its
        // share go; the part in the channel is a copy of 1 MiB.
        let first = stream.next().await.unwrap().unwrap();
        reply.await.unwrap();
        assert_eq!(budget.available_permits(), IN_FLIGHT_BYTES);
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.data.len(), CHUNK_BYTES);
        drop(first);
        let status = stream.next().await.unwrap().unwrap_err();
        assert_eq!(status.code(), Code::DeadlineExceeded, "{status:?}");
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn trim_gives_back_the_share_above_what_stays() {
        let budget = Arc::new(Semaphore::new(100));
        let mut permit = try_take(&budget, 60).unwrap();
        trim(&mut permit, 20);
        assert_eq!(budget.available_permits(), 80);
        trim(&mut permit, 0);
        assert_eq!(budget.available_permits(), 99);
    }
}

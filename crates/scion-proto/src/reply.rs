//! How a `GetObjects` reply streams objects. The server and every cache tier
//! follow the same rules, so they share this code.
//!
//! A reply hands objects to its client one at a time, through a one-slot
//! channel. Before a reply reads an object's bytes, it takes the object's
//! share of a byte budget. The object keeps that share until the client takes
//! it. A reply stops once it has sent [`REPLY_BUDGET_BYTES`], always after at
//! least one object. It ends when its client takes no object for
//! [`STALL_TIMEOUT`], or when the budget has no room for that long.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_stream::StreamExt as _;
use tokio_stream::adapters::Map;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::v1::Object;
use crate::{MAX_MESSAGE_BYTES, MAX_OBJECT_BYTES};

/// A reply stops before the next object once it has sent this many bytes.
/// The client asks again for the rest, so one call stays short.
pub const REPLY_BUDGET_BYTES: usize = MAX_MESSAGE_BYTES;

/// The byte budget of one process: the most object bytes that all its
/// replies together hold after a read and before their stream hands the
/// object on.
pub const IN_FLIGHT_BYTES: usize = 4 * MAX_OBJECT_BYTES;

/// A reply ends when its client takes no object for this long, so a client
/// that stops reading does not hold memory and budget. (A request timeout
/// covers only the start of a streamed reply.)
// SHORTCUT: a stalled reply ends, but HTTP/2 keeps the one encoded object it
// was sending until the connection closes. Upgrade (issue #42): a response
// body timeout that resets the stream.
pub const STALL_TIMEOUT: Duration = Duration::from_secs(120);

// Any one object fits in the budget, and the budget counts in u32 permits.
const _: () = assert!(MAX_OBJECT_BYTES <= IN_FLIGHT_BYTES);
const _: () = assert!(IN_FLIGHT_BYTES <= u32::MAX as usize);

/// An object on its way to the client, with the share of the byte budget
/// that it holds until the stream hands it on.
pub type Sent = (Result<Object, Status>, OwnedSemaphorePermit);

/// The `GetObjects` reply: the channel, minus the budget each object held.
pub type ObjectStream = Map<ReceiverStream<Sent>, fn(Sent) -> Result<Object, Status>>;

/// A new reply: the sender for [`send_objects`] and the stream for tonic.
pub fn channel() -> (mpsc::Sender<Sent>, ObjectStream) {
    let (tx, rx) = mpsc::channel(1);
    let release: fn(Sent) -> Result<Object, Status> = |(object, _permit)| object;
    (tx, ReceiverStream::new(rx).map(release))
}

/// The share of the budget for an object of `size` bytes: at least 1, at most
/// [`IN_FLIGHT_BYTES`], which the asserts above keep within u32.
fn share(size: usize) -> u32 {
    u32::try_from(size.clamp(1, IN_FLIGHT_BYTES)).unwrap_or(u32::MAX)
}

/// Takes the share of `budget` for an object of `size` bytes. Waits until the
/// share is free, at most `stall`: the budget is held by objects that clients
/// have not taken yet. `None` if the wait ends, or the budget is closed.
pub async fn take(
    budget: &Arc<Semaphore>,
    size: usize,
    stall: Duration,
) -> Option<OwnedSemaphorePermit> {
    let acquire = Arc::clone(budget).acquire_many_owned(share(size));
    tokio::time::timeout(stall, acquire).await.ok()?.ok()
}

/// Takes the share of `budget` for an object of `size` bytes if it is free
/// now.
#[must_use]
pub fn try_take(budget: &Arc<Semaphore>, size: usize) -> Option<OwnedSemaphorePermit> {
    Arc::clone(budget).try_acquire_many_owned(share(size)).ok()
}

/// Sends one object per key, as the client takes them. `produce` finds an
/// object, takes its share of the budget with [`take`], and only then reads
/// the bytes, so a reply that waits for budget holds no object. Stops after
/// an error, once the reply has sent [`REPLY_BUDGET_BYTES`], when the client
/// has gone, when `produce` returns `None`, or when the client does not take
/// an object within `stall`. Finding an object has no time limit here: a
/// cache tier bounds its upstream fetches itself.
pub async fn send_objects<K, F>(
    keys: impl IntoIterator<Item = K>,
    tx: mpsc::Sender<Sent>,
    stall: Duration,
    produce: impl Fn(K) -> F,
) where
    F: Future<Output = Option<Sent>>,
{
    let mut sent = 0;
    for key in keys {
        if sent >= REPLY_BUDGET_BYTES {
            break;
        }
        let Some((object, permit)) = produce(key).await else {
            break;
        };
        let len = object.as_ref().map_or(0, |o| o.data.len());
        let failed = object.is_err();
        match tokio::time::timeout(stall, tx.send((object, permit))).await {
            Ok(Ok(())) if !failed => sent += len,
            _ => break,
        }
    }
}

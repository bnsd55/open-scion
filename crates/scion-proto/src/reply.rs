//! How a `GetObjects` reply streams objects. The server and every cache tier
//! follow the same rules, so they share this code.
//!
//! A reply hands objects to its client one at a time, through a one-slot
//! channel. Each object holds a share of a byte budget, taken before its bytes
//! are read, until the client takes it. A reply stops once it has sent
//! [`REPLY_BUDGET_BYTES`], always after at least one object, and ends when its
//! client takes no object for [`STALL_TIMEOUT`].

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

/// Takes the share of `budget` for an object of `size` bytes: at least 1, at
/// most [`IN_FLIGHT_BYTES`]. Waits until the share is free. `None` if the
/// budget is closed.
pub async fn take(budget: &Arc<Semaphore>, size: usize) -> Option<OwnedSemaphorePermit> {
    // Clamped to the budget, which the asserts above keep within u32.
    let share = u32::try_from(size.clamp(1, IN_FLIGHT_BYTES)).unwrap_or(u32::MAX);
    Arc::clone(budget).acquire_many_owned(share).await.ok()
}

/// Sends one object per key, as the client takes them. `produce` finds an
/// object, takes its share of the budget with [`take`], and only then reads
/// the bytes, so a reply that waits for budget holds no object. Stops after
/// an error, once the reply has sent [`REPLY_BUDGET_BYTES`], when the client
/// has gone, when `produce` returns `None`, or when an object is not handed
/// on within `stall`.
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
        let handed_on = async {
            let (object, permit) = produce(key).await?;
            let len = object.as_ref().map_or(0, |o| o.data.len());
            let failed = object.is_err();
            tx.send((object, permit)).await.ok()?;
            Some((len, failed))
        };
        match tokio::time::timeout(stall, handed_on).await {
            Ok(Some((len, false))) => sent += len,
            _ => break,
        }
    }
}

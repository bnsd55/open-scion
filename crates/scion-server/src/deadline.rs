//! When a job that a call started gives up.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use scion_proto::runtime::deadline::Until;
use tonic::Request;

use crate::error::{Error, Result};

/// The limit of a job whose call gave no deadline of its own: the same
/// default as a request timeout.
pub(crate) const CALL_LIMIT: Duration = Duration::from_secs(600);

/// When a job gives up: at an instant, or once the call that asked for it
/// has gone. A request timeout or a client that cancels drops the call, but
/// not a job that is already running, so the long loops of a job check this.
#[derive(Clone, Debug)]
pub struct Deadline {
    at: Instant,
    gone: Arc<AtomicBool>,
}

impl Deadline {
    /// A deadline at `at`.
    #[must_use]
    pub fn new(at: Instant) -> Self {
        Self {
            at,
            gone: Arc::default(),
        }
    }

    /// The deadline of a job for `request`: at most `cap` from now, and
    /// before the caller's own deadline, less the margin of [`Until`], so
    /// the job stops and the call answers before the caller gives up.
    #[must_use]
    pub fn of_call<T>(request: &Request<T>, cap: Duration) -> Self {
        Self::new(Instant::now() + Until::of(request).wait(cap))
    }

    /// A guard for the call to hold. When it drops, the deadline has passed.
    pub fn guard(&self) -> CallGuard {
        CallGuard(Arc::clone(&self.gone))
    }

    /// The instant the deadline passes.
    #[must_use]
    pub fn at(&self) -> Instant {
        self.at
    }

    /// Fails with [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn check(&self) -> Result<()> {
        let why = if self.gone.load(Ordering::Relaxed) {
            "its call has gone"
        } else if Instant::now() >= self.at {
            "the call ran past its deadline"
        } else {
            return Ok(());
        };
        Err(Error::DeadlineExceeded(why.to_owned()))
    }
}

/// Held by a call. Dropping it, when the call ends or is dropped, ends the
/// call's [`Deadline`].
#[must_use = "the deadline ends when the guard drops"]
pub struct CallGuard(Arc<AtomicBool>);

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

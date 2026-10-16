//! When a job that a call started gives up.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::error::{Error, Result};

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

    /// A guard for the call to hold. When it drops, the deadline has passed.
    pub fn guard(&self) -> CallGuard {
        CallGuard(Arc::clone(&self.gone))
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

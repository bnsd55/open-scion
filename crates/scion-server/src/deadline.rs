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

    /// Fails with [`Error::Cancelled`] once the call has gone, or
    /// [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn check(&self) -> Result<()> {
        if self.gone.load(Ordering::Relaxed) {
            return Err(Error::Cancelled("its call has gone".to_owned()));
        }
        if Instant::now() >= self.at {
            return Err(Error::DeadlineExceeded(
                "the call ran past its deadline".to_owned(),
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};

    use super::*;

    #[test]
    fn a_check_after_its_call_went_answers_cancelled() {
        let deadline = Deadline::new(Instant::now() + Duration::from_secs(600));
        let guard = deadline.guard();
        drop(guard);
        let err = deadline.check().expect_err("the call has gone");
        assert!(
            matches!(&err, Error::Cancelled(why) if why == "its call has gone"),
            "{err:?}"
        );
        assert_eq!(Status::from(err).code(), Code::Cancelled);
    }

    #[test]
    fn a_check_past_the_deadline_with_its_call_alive_answers_deadline_exceeded() {
        let deadline = Deadline::new(Instant::now());
        let _guard = deadline.guard();
        let err = deadline.check().expect_err("the deadline has passed");
        assert!(
            matches!(&err, Error::DeadlineExceeded(why) if why == "the call ran past its deadline"),
            "{err:?}"
        );
        assert_eq!(Status::from(err).code(), Code::DeadlineExceeded);
    }
}

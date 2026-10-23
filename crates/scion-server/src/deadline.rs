//! When a job that a call started gives up.

// Every pub item of this module is re-exported by the test seam
// (`test_support` in lib.rs). unreachable_pub does not see through
// a re-export and fires on these items with and without the
// test-hooks feature, so the allow covers both sets.
#![allow(unreachable_pub)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use scion_proto::runtime::deadline::Until;
use tonic::Request;

use crate::error::{Error, Result};

/// The limit of a job whose call gave no deadline of its own: the same
/// default as a request timeout.
pub(crate) const CALL_LIMIT: Duration = Duration::from_secs(600);

#[cfg(test)]
thread_local! {
    /// Deadline checks on this thread, each a read of the clock. Tests use
    /// it to bound the clock reads of a query.
    pub(crate) static CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// When a job gives up: at an instant, or once the call that asked for it
/// has gone. A request timeout or a client that cancels drops the call, but
/// not a job that is already running, so the long loops of a job check this.
#[derive(Clone, Debug)]
pub struct Deadline {
    at: Instant,
    gone: Arc<AtomicBool>,
    /// Test-only: expires the deadline on the test's word, so a test
    /// decides the order of a job's steps by state, not by a clock that
    /// races a thread start ([#566]'s flake). Absent from production
    /// builds.
    #[cfg(test)]
    expired: Arc<AtomicBool>,
}

impl Deadline {
    /// A deadline at `at`.
    #[must_use]
    pub fn new(at: Instant) -> Self {
        Self {
            at,
            gone: Arc::default(),
            #[cfg(test)]
            expired: Arc::default(),
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

    /// Expires the deadline now, on the test's word: a test that must
    /// order a job's steps by state expires its deadline when the state
    /// says so, instead of arming a clock that can lose to a thread
    /// start. Test-only.
    #[cfg(test)]
    pub(crate) fn test_expire(&self) {
        self.expired.store(true, Ordering::Relaxed);
    }

    /// How long a wait may last: what is left until this. Zero once this
    /// has passed, never negative, never a panic.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }

    /// Fails with [`Error::Cancelled`] once the call has gone, or
    /// [`Error::DeadlineExceeded`] once the deadline has passed.
    pub(crate) fn check(&self) -> Result<()> {
        #[cfg(test)]
        CHECKS.set(CHECKS.get() + 1);
        #[cfg(test)]
        if self.expired.load(Ordering::Relaxed) {
            return Err(Error::DeadlineExceeded(
                "the test expired this deadline".to_owned(),
            ));
        }
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

/// The test-only delay of a guard's drop, read once per process from
/// `SCION_TEST_GUARD_DROP_MS`. The abort of a call ends its deadline when
/// the aborted task's future drops the guard, not when `abort` returns;
/// armed, the drop takes this long, so a counting loop can hold the
/// window between an abort and the job's next check open.
#[cfg(any(test, feature = "test-hooks"))]
fn test_guard_drop_delay() -> Duration {
    static DELAY: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *DELAY.get_or_init(|| {
        std::env::var("SCION_TEST_GUARD_DROP_MS")
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_default()
    })
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-hooks"))]
        {
            let delay = test_guard_drop_delay();
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
        }
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};

    use super::*;

    /// The one arithmetic a wait can get wrong: after expiry, remaining
    /// must be zero — never a panic, never a negative that wraps huge.
    #[test]
    fn remaining_is_zero_after_expiry_and_counts_down_before_it() {
        let past = Deadline::new(Instant::now().checked_sub(Duration::from_secs(10)).unwrap());
        assert_eq!(past.remaining(), Duration::ZERO);
        let future = Deadline::new(Instant::now() + Duration::from_secs(60));
        let left = future.remaining();
        assert!(
            left > Duration::ZERO && left <= Duration::from_secs(60),
            "{left:?}"
        );
    }

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

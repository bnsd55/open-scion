//! The memory that blame calls share.
//!
//! A blame call reads whole versions of a file and diffs them, so its memory
//! grows with the file: the two texts it holds, a row per line for the
//! answer, and the tables of one diff. [`Bytes`] is a pool of bytes that
//! calls take from, so that together they hold no more than the budget. A
//! call that cannot take its share waits, until the budget frees up or its
//! deadline passes. A file whose share is more than the whole budget is
//! refused: a wrong blame is worse than a refused one.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::error::{Error, Result};

/// The bytes of one file line that a blame call holds, counting the row of
/// the answer (an [`Option<ChangesetId>`] and a line number) and its share
/// of one diff's state: the line tables of [`gix_diff`][gix::diff] measure
/// 78 bytes per line, whatever the line length. Measured with a counting
/// allocator around the real diff; see the tests of `memory` use.
pub const BYTES_PER_LINE: usize = 128;

/// A pool of bytes that calls take for the memory they hold.
///
/// Cheap to clone: every handle shares one pool.
#[derive(Clone)]
pub struct Bytes(Arc<Pool>);

struct Pool {
    cap: usize,
    free: Mutex<usize>,
    woke: Condvar,
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bytes").field("cap", &self.0.cap).finish()
    }
}

impl Bytes {
    /// A pool of `cap` bytes, all free.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self(Arc::new(Pool {
            cap,
            free: Mutex::new(cap),
            woke: Condvar::new(),
        }))
    }

    /// The whole pool, in bytes.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.0.cap
    }

    /// Takes `n` bytes and returns what holds them: dropping it gives the
    /// bytes back. Waits for them to free up, until `until`; `None` waits
    /// for as long as it takes.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if `n` is more than the whole pool, or
    /// if `until` passes first; the error names the budget.
    pub fn take(&self, n: usize, until: Option<Instant>) -> Result<Held> {
        if n > self.0.cap {
            return Err(Error::ResourceExhausted(format!(
                "a blame of {n} bytes does not fit the budget of {} bytes",
                self.0.cap
            )));
        }
        let mut free = self.0.free.lock().expect("the budget lock is not poisoned");
        while *free < n {
            let Some(wait) = until.map(|at| at.saturating_duration_since(Instant::now())) else {
                free = self
                    .0
                    .woke
                    .wait(free)
                    .expect("the budget lock is not poisoned");
                continue;
            };
            let (locked, timeout) = self
                .0
                .woke
                .wait_timeout(free, wait)
                .expect("the budget lock is not poisoned");
            if timeout.timed_out() {
                return Err(Error::ResourceExhausted(format!(
                    "a blame of {n} bytes waited past its deadline for the budget of {} bytes",
                    self.0.cap
                )));
            }
            free = locked;
        }
        *free -= n;
        Ok(Held {
            pool: Arc::clone(&self.0),
            n,
        })
    }

    /// Takes `n` bytes without waiting.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if they are not free now or `n` is more
    /// than the whole pool; the error names the budget.
    pub fn try_take(&self, n: usize) -> Result<Held> {
        self.take(n, Some(Instant::now()))
    }
}

/// Holds bytes taken from a [`Bytes`] pool. Dropping it gives them back.
#[must_use = "the bytes stay taken until the Held drops"]
pub struct Held {
    pool: Arc<Pool>,
    n: usize,
}

impl Held {
    /// Gives the bytes back before the guard drops.
    pub fn release(this: Self) {
        drop(this);
    }

    /// Keeps at most `n` of the held bytes and gives the rest back at once,
    /// without waiting.
    pub fn keep(&mut self, n: usize) {
        if n < self.n {
            let rest = self.n - n;
            let mut free = self.pool.free.lock().expect("the budget lock is not poisoned");
            *free += rest;
            self.pool.woke.notify_all();
            self.n = n;
        }
    }
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Held").field("n", &self.n).finish()
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut free = self.pool.free.lock().expect("the budget lock is not poisoned");
        *free += self.n;
        self.pool.woke.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn take_and_give_round_trip() {
        let bytes = Bytes::new(1000);
        {
            let held = bytes.take(400, None).expect("fits");
            let again = bytes.take(600, None).expect("fits");
            assert!(bytes.try_take(1).is_err(), "the pool is spent");
            Held::release(held);
            assert!(bytes.try_take(1).is_ok(), "the bytes came back");
            drop(again);
            assert!(bytes.try_take(1000).is_ok(), "the whole pool came back");
        }
        assert!(bytes.try_take(1000).is_ok());
    }

    #[test]
    fn a_take_over_the_cap_is_refused_at_once() {
        let bytes = Bytes::new(100);
        let at = Instant::now();
        let err = bytes.take(101, Some(at)).expect_err("over the cap");
        assert!(matches!(err, Error::ResourceExhausted(_)), "{err:?}");
    }

    #[test]
    fn a_take_that_waits_times_out_at_its_deadline() {
        let bytes = Bytes::new(100);
        let held = bytes.take(100, None).expect("fits");
        let started = Instant::now();
        let err = bytes
            .take(1, Some(started + Duration::from_millis(10)))
            .expect_err("the pool is spent");
        assert!(matches!(err, Error::ResourceExhausted(_)), "{err:?}");
        assert!(started.elapsed() >= Duration::from_millis(10));
        drop(held);
    }
}

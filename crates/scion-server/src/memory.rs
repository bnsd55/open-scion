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

#[derive(Debug)]
struct Pool {
    cap: usize,
    free: Mutex<usize>,
    woke: Condvar,
    /// How many takes wait for bytes right now. Tests use it to see that a
    /// waiter is really blocked before they free bytes for it.
    #[cfg(test)]
    waiting: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bytes")
            .field("cap", &self.0.cap)
            .field("free", &self.0.free)
            .finish()
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
            #[cfg(test)]
            waiting: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    /// The whole pool, in bytes.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.0.cap
    }

    /// Takes `n` bytes and returns what holds them: dropping it gives the
    /// bytes back. Waits for them to free up, until `until`; `None` waits
    /// for as long as it takes. A call that already holds bytes must use
    /// [`Bytes::try_take`] instead: two callers that each hold part of the
    /// pool and wait for more can wait for each other until both their
    /// deadlines, with nobody able to finish.
    ///
    /// # Errors
    ///
    /// [`Error::ResourceExhausted`] if `n` is more than the whole pool, or
    /// if `until` passes first; the error names the budget.
    ///
    /// # Panics
    ///
    /// If the budget lock is poisoned, which only a panic inside it causes.
    pub fn take(&self, n: usize, until: Option<Instant>) -> Result<Held> {
        if n > self.0.cap {
            return Err(Error::ResourceExhausted(format!(
                "a blame of {n} bytes does not fit the budget of {} bytes",
                self.0.cap
            )));
        }
        let mut free = self.0.free.lock().expect("the budget lock is not poisoned");
        #[cfg(test)]
        let _waiting = if *free < n {
            self.0
                .waiting
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some(WaitingGuard(&self.0.waiting))
        } else {
            None
        };
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

/// Counts one take that waits for bytes, for as long as it waits.
#[cfg(test)]
struct WaitingGuard<'a>(&'a std::sync::atomic::AtomicUsize);

#[cfg(test)]
impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
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
    ///
    /// # Panics
    ///
    /// If the budget lock is poisoned, which only a panic inside it causes.
    pub fn keep(&mut self, n: usize) {
        if n < self.n {
            let rest = self.n - n;
            let mut free = self
                .pool
                .free
                .lock()
                .expect("the budget lock is not poisoned");
            *free += rest;
            self.pool.woke.notify_all();
            self.n = n;
        }
    }
}

impl std::fmt::Debug for Held {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Held")
            .field("n", &self.n)
            .field("pool", &self.pool)
            .finish()
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut free = self
            .pool
            .free
            .lock()
            .expect("the budget lock is not poisoned");
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

    #[test]
    fn keep_gives_the_rest_back_at_once() {
        let bytes = Bytes::new(100);
        let mut held = bytes.take(60, None).expect("fits");
        held.keep(10);
        assert_eq!(held.n, 10, "keep holds the kept part");
        // The other 50 are free again: a take of them succeeds at once.
        let rest = bytes.try_take(50).expect("keep freed the rest");
        drop((held, rest));
        assert!(bytes.try_take(100).is_ok(), "the whole pool came back");
    }

    #[test]
    fn keep_wakes_a_waiter_for_the_bytes_it_gives_back() {
        let bytes = Arc::new(Bytes::new(100));
        let mut held = bytes.take(60, None).expect("fits");
        let waiter = {
            let bytes = Arc::clone(&bytes);
            std::thread::spawn(move || {
                let until = Instant::now() + Duration::from_millis(500);
                bytes.take(50, Some(until))
            })
        };
        // The order must be certain: a keep that runs before the waiter
        // blocks would hand the bytes over without waking anything, and a
        // lost notify would go unseen.
        let blocked = (0..2000).any(|_| {
            if bytes.0.waiting.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
            false
        });
        assert!(blocked, "the waiter never blocked on the empty budget");
        held.keep(10);
        let got = waiter.join().unwrap().expect("keep woke the waiter");
        drop((held, got));
    }

    #[test]
    fn a_call_that_holds_bytes_never_waits_for_more() {
        let bytes = Bytes::new(100);
        let held = bytes.take(60, None).expect("fits");
        // The rest of the pool is 40: a take of more fails at once, though
        // a waiting take with no deadline would wait forever.
        let err = bytes.try_take(50).expect_err("the rest does not fit");
        assert!(matches!(err, Error::ResourceExhausted(_)), "{err:?}");
        // What is free now is taken at once.
        let rest = bytes.try_take(40).expect("fits");
        drop((held, rest));
    }
}

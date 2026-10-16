//! Shared helpers of the integration tests.
// Every test binary compiles this file and uses its own subset of it.
#![allow(dead_code)]

/// Keeps child processes out of the window where a test drops a store and
/// checks its lock released. A forked child shares the flock of every open
/// file description until it execs, so a child spawned while another test
/// drops a store can hold the dropped store's lock past that check. Every
/// test that spawns a child takes the read side across the spawn, and a
/// test that asserts a lock is released takes the write side across its
/// drop and reopen: a pending write side also stops new read sides, so the
/// spawners finish before the release window opens. (The same guard, for
/// the unit tests of the server, is `store::spawn_guard`.)
pub mod spawn_guard {
    use std::sync::RwLock;

    static SPAWNS: RwLock<()> = RwLock::new(());

    /// Holds the read side while `f` spawns its child: any number of tests
    /// may spawn at once.
    pub fn across_spawn<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.read().expect("the spawn guard is not poisoned");
        f()
    }

    /// Holds the write side while `f` drops a store and checks its lock is
    /// released: no child is in its fork-to-exec window, and no new spawn
    /// starts, until `f` is done.
    pub fn across_release<T>(f: impl FnOnce() -> T) -> T {
        let _held = SPAWNS.write().expect("the spawn guard is not poisoned");
        f()
    }

    /// Primes gix's one-time `git` installation lookup under the read
    /// side. The first `gix::open` in a process forks `git` outside our
    /// control (gix-path), and caches the result afterwards. Every test
    /// that reaches `gix` calls this first, so that fork waits out any
    /// release window like our own spawns do.
    pub fn prime_gix_lookup() {
        across_spawn(|| {
            let _ = gix::path::env::installation_config();
        });
    }

    /// Waits until the server lock of `dir` is free. Call it on the write
    /// side (inside [`across_release`]), after dropping the stores and
    /// before the reopen that must see the lock released.
    ///
    /// The poll takes the real condition — the lock is free — instead of
    /// a timing guess: it opens `<dir>/server.lock` and takes a
    /// non-blocking exclusive flock every 1 ms, up to 2 s, then fails
    /// with the holder message, so a real leak still fails. The delay it
    /// absorbs is the operating system's: macOS tears a reaped child's
    /// inherited `flock` down one tick after `waitpid`, and about 7% of
    /// immediate reopens after a Rust spawn and its wait fail with
    /// `EWOULDBLOCK` there, sub-ms; Linux shows 0 of 2,000 rounds (a
    /// standalone probe). (The same helper, for the unit tests of the
    /// server, is `store::spawn_guard::wait_for_lock_release`.)
    pub fn wait_for_lock_release(dir: &std::path::Path) {
        use std::time::{Duration, Instant};

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(dir.join("server.lock"))
                .unwrap_or_else(|e| panic!("cannot open the lock of {}: {e}", dir.display()));
            // rustix's flock takes the lock or fails with would-block:
            // `Ok` is free, `Err` is still held.
            let free =
                rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                    .is_ok();
            drop(file);
            if free {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the server lock of {} was never released within 2 s",
                dir.display()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

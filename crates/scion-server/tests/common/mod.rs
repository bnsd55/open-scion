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
}

//! The library of `scn`, the command-line client of Scion.
//!
//! `scn` is one of the five crates of [openscion]: the user-facing client
//! that checks out, mounts, inspects and commits against a server, a
//! cache, or a local daemon. The binary is the same crate's `scn` target;
//! this library holds its pieces, so the tests drive them and other
//! clients can reuse them.
//!
//! [openscion]: https://github.com/bnsd55/openscion
//!
//! - [`client`]: the gRPC client stubs, the deadline and the ticket
//!   handling of every call.
//! - [`cmd`]: one module per command (`cat`, `ls`, `log`, `commit`,
//!   `checkout`, `mount`, the query commands) and their `--json` output.
//! - `out`: the one stdout writer, and the quiet exit that a closed
//!   pipe (`scn blame f | head -1`) gets.
//! - `materialize`: writing trees to disk, the checkout and the mount
//!   overlay.
//! - `vfs`: reading trees through the API, with the sizes and states
//!   that `scn ls` shows.
//!
//! The library exists for the `scn` binary and this package's tests: it
//! is the binary's pieces, not an API to build on. The one test seam is
//! the `test_support` module, behind the `test-hooks` feature (default
//! off); it carries no stability promise.
//!
//! A `scn` process reaches a server, a `scion-cache`, or a
//! `scion-daemon` over its `--server` and `--objects` addresses; how the
//! objects travel is the [`scion-proto`](https://docs.rs/open-scion-proto)
//! contract.
// These items were crate-private behind the `scn` binary, and their docs
// hold no `# Errors` sections; relaxed until the docs are written out
// (https://github.com/bnsd55/openscion/issues/11).
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]

pub mod client;
pub mod cmd;
pub(crate) mod materialize;
pub(crate) mod out;
pub(crate) mod vfs;

/// The test seams, behind the test-hooks feature (default off). One
/// module, the only way in: the integration tests reach the pieces the
/// binary drives only from outside. Not API, and not covered by a
/// stability promise.
#[cfg(feature = "test-hooks")]
pub mod test_support {
    /// A client pre-set for tests: the object roots of a running server.
    pub mod client {
        pub use crate::client::{set_put_pause, test_objects};
    }
    /// The mount session file, so a test stages what
    /// `scn commit --from-mount` reads.
    pub mod cmd {
        /// The session records of the mount command.
        pub mod mount {
            pub use crate::cmd::mount::record::{MountRecord, write_sessions};
        }
    }
    /// The NFS filesystem the mount serves, so a test drives it
    /// in-process.
    pub mod vfs {
        pub use crate::vfs::{NFSFileSystem, Vfs};
    }
}

/// The gate the unit tests of this binary run their forks and their
/// serve-lock reads under: a fork copies the process's open lock files
/// to the child until the child execs, and macOS is slow to exec, so a
/// parallel test's fork can hold a probe's lock for ticks and flip its
/// answer (`an_unmount_does_not_signal_an_unrelated_process` flaked so
/// twice). **Every test in this binary that forks takes the gate**, and
/// so does every test that reads a serve lock.
#[cfg(test)]
pub(crate) mod fork_gate {
    use std::sync::{Mutex, MutexGuard, PoisonError};

    static SERIAL: Mutex<()> = Mutex::new(());

    pub(crate) fn serial() -> MutexGuard<'static, ()> {
        SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

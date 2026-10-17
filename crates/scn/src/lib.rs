//! The library of `scn`, the command-line client of Scion.
//!
//! `scn` is one of the five crates of [open-scion]: the user-facing client
//! that checks out, mounts, inspects and commits against a server, a
//! cache, or a local daemon. The binary is the same crate's `scn` target;
//! this library holds its pieces, so the tests drive them and other
//! clients can reuse them.
//!
//! [open-scion]: https://github.com/bnsd55/open-scion
//!
//! - [`client`]: the gRPC client stubs, the deadline and the ticket
//!   handling of every call.
//! - [`cmd`]: one module per command (`cat`, `ls`, `log`, `commit`,
//!   `checkout`, `mount`, the query commands) and their `--json` output.
//! - [`materialize`]: writing trees to disk, the checkout and the mount
//!   overlay.
//! - [`vfs`]: reading trees through the API, with the sizes and states
//!   that `scn ls` shows.
//!
//! A `scn` process reaches a server, a `scion-cache`, or a
//! `scion-daemon` over its `--server` and `--objects` addresses; how the
//! objects travel is the [`scion-proto`](https://docs.rs/open-scion-proto)
//! contract.
// These items were crate-private behind the `scn` binary, and their docs
// hold no `# Errors` sections; relaxed until the docs are written out
// (https://github.com/bnsd55/open-scion/issues/11).
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]

pub mod client;
pub mod cmd;
pub mod materialize;
pub mod vfs;

//! Scion server: durable storage, server-side commits and lands, and reads.
//!
//! This is the storage and serving crate of [open-scion]: the bottom of the
//! system above the object model ([`scion-core`]), holding the metadata and
//! blobs, doing commits, lands and reads, and serving the API of
//! [`scion-proto`](https://docs.rs/open-scion-proto) to the client
//! ([`scn`]) and the cache tiers
//! ([`scion-tier`](https://docs.rs/open-scion-tier)). One server runs per
//! data directory or per Postgres database — a second server on the same
//! database fails at open (see [`Backend`]) — and the metadata may live in
//! Postgres instead of the data directory; the blobs stay on local disk in
//! both. The operations guide, `docs/operations.md`, covers both shapes.
//!
//! [open-scion]: https://github.com/bnsd55/open-scion
//! [`scion-core`]: https://docs.rs/open-scion-core
//! [`scn`]: https://docs.rs/open-scion
//!
//! The layers, bottom up. The private ones carry no links: they are not
//! API, and their names here are orientation, not references.
//!
//! - `backend` and `meta`: one metadata trait over two backends: a data
//!   directory on this machine (`store`: SQLite for metadata and trees),
//!   and a Postgres database (`pg`) for the metadata instead. The blob
//!   files stay on local disk in both: one blob folder per server. One
//!   server runs per database: a second one is refused at open. What a
//!   user touches is [`Backend`].
//! - `derived`: one mechanism for every kind of derived data, with
//!   [`backfill`] and the [`Kind`] implementations.
//! - [`tree`]: apply changesets to trees and derive each changeset's root
//!   tree.
//! - [`graph`]: the commit-graph index, and the graph queries that read it.
//! - [`history`]: the path history of each changeset, for `Log` with a path
//!   and for lands on old bases.
//! - `read`, `blame`, the `write` module, `land`: the operations behind the
//!   RPCs. A long one stops at its call's deadline. `similar` finds
//!   renames for `Diff` and `Blame`.
//! - `views`: the view of the repo that a partial reader (folder read
//!   rules, [`access`]) reads through.
//! - [`quotas`]: what one principal may hold at once; [`tickets`]: the
//!   signed read tickets of object reads.
//! - [`fsck`]: reports the damage in a data directory. Read-only.
//! - [`gc`]: collects the blobs that nothing names any more.
//! - [`import`]: one-way import from a git repository.
//! - [`service`], `objects` and `auth`: the gRPC front end. [`access`]:
//!   which principal may read or write what.

//! ### What the facade is
//!
//! The API of this crate is the list below the modules: the types and
//! functions a server main and a client need. Everything else exists for
//! the two binaries that this workspace ships, and can change without a
//! release. Items marked `#[doc(hidden)]` are not API at all: they are
//! the test seams, behind the `test-hooks` feature (default off), which
//! exists so the integration tests can hold locks, move refs and inject
//! commit failures at exact moments. Nothing behind the feature, and
//! nothing `#[doc(hidden)]`, carries a stability promise: enabling it in
//! a build is at the user's own risk.

pub mod access;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod auth;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod auth;
pub mod backend;
pub use backend::Backend;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod blame;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod blame;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod db;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod db;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod deadline;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod deadline;
pub mod derived;
pub mod error;
pub mod fsck;
pub mod gc;
pub mod graph;
pub mod history;
pub mod import;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod land;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod land;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod memory;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod memory;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod meta;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod meta;
pub mod metrics;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod names;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod names;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod objects;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod objects;
mod pg;
pub mod quotas;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod read;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod read;
pub mod service;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod similar;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod similar;
pub mod store;
pub mod tickets;
pub mod tree;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod views;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod views;
// Internal: the binaries' implementation. Tests reach it only with the
// test-hooks feature on, which the crate's own dev-dependency sets.
#[cfg(any(test, feature = "test-hooks"))]
pub mod write;
#[cfg(not(any(test, feature = "test-hooks")))]
pub(crate) mod write;

pub use store::Store;

// The facade: the API of this crate, one list, kept here so it is one
// thing to read and one thing to keep honest. Every other name in the
// modules is an implementation detail of the two binaries, however
// public the module it sits in looks. The doc(hidden) items are not
// here, and never were.
pub use access::Access;
pub use derived::{Kind, backfill};
pub use error::{Error, Result};
pub use gc::DEFAULT_GRACE_SECS;
pub use graph::CommitGraph;
pub use history::PathHistory;
pub use import::{ImportOptions, ImportReport, import_git};
pub use metrics::Metrics;
pub use quotas::Quotas;
pub use service::{ServeOptions, serve};
pub use tickets::Keys;
pub use tree::RootTree;

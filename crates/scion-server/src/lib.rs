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
//! Modules from the bottom up:
//!
//! - [`backend`] and [`meta`]: one [`Meta`](meta::Meta) trait over two
//!   backends: a data directory on this machine ([`store`]: SQLite for
//!   metadata and trees), and a Postgres database (the `pg` module) for
//!   the metadata instead. The blob files stay on local disk in both: one
//!   blob folder per server. One server runs per database: a second one
//!   is refused at open.
//! - [`derived`]: one mechanism for every kind of derived data.
//! - [`tree`]: apply changesets to trees and derive each changeset's root tree.
//! - [`graph`]: the commit-graph index, and the graph queries that read it.
//! - [`history`]: the path history of each changeset, for `Log` with a path
//!   and for lands on old bases.
//! - [`read`], [`blame`], the `write` module, [`land`]: the operations behind the RPCs.
//!   A long one stops at its call's [`deadline`].
//!   [`similar`] finds renames for `Diff` and `Blame`.
//! - [`views`]: the view of the repo that a partial reader (folder read
//!   rules, [`access`]) reads through.
//! - [`quotas`]: what one principal may hold at once; [`tickets`]: the
//!   signed read tickets of object reads.
//! - [`fsck`]: reports the damage in a data directory. Read-only.
//! - [`gc`]: collects the blobs that nothing names any more.
//! - [`import`]: one-way import from a git repository.
//! - [`service`], [`objects`] and [`auth`]: the gRPC front end. [`access`]:
//!   which principal may read or write what.

pub mod access;
pub mod auth;
pub mod backend;
pub use backend::Backend;
pub mod blame;
pub mod db;
pub mod deadline;
pub mod derived;
pub mod error;
pub mod fsck;
pub mod gc;
pub mod graph;
pub mod history;
pub mod import;
pub mod land;
pub mod memory;
pub mod meta;
pub mod metrics;
pub mod names;
pub mod objects;
mod pg;
pub mod quotas;
pub mod read;
pub mod service;
pub mod similar;
pub mod store;
pub mod tickets;
pub mod tree;
pub mod views;
pub mod write;

pub use error::{Error, Result};
pub use store::Store;

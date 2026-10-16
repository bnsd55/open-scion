//! Scion server: durable storage, server-side commits and lands, and reads.
//!
//! Modules from the bottom up:
//!
//! - [`store`] and [`db`]: `SQLite` for metadata and trees, a folder for blobs.
//! - [`derived`]: one mechanism for every kind of derived data.
//! - [`tree`]: apply changesets to trees and derive each changeset's root tree.
//! - [`graph`]: the commit-graph index, and the graph queries that read it.
//! - [`read`], [`blame`], [`write`], [`land`]: the operations behind the RPCs.
//!   A long one stops at its call's [`deadline`].
//!   [`similar`] finds renames for `Diff` and `Blame`.
//! - [`import`]: one-way import from a git repository.
//! - [`service`], [`objects`] and [`auth`]: the gRPC front end. [`access`]:
//!   which principal may read or write what.

pub mod access;
pub mod auth;
pub mod blame;
pub mod db;
pub mod deadline;
pub mod derived;
pub mod error;
pub mod graph;
pub mod import;
pub mod land;
pub mod names;
pub mod objects;
pub mod read;
pub mod service;
pub mod similar;
pub mod store;
pub mod tree;
pub mod write;

pub use error::{Error, Result};
pub use store::Store;

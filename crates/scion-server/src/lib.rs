//! Scion server: durable storage, server-side commits and lands, and reads.
//!
//! Modules from the bottom up:
//!
//! - [`store`] and [`db`]: `SQLite` for metadata and trees, a folder for blobs.
//! - [`derived`]: one mechanism for every kind of derived data.
//! - [`tree`]: apply changesets to trees and derive each changeset's root tree.
//! - [`read`], [`blame`], [`write`], [`land`]: the operations behind the RPCs.
//! - [`import`]: one-way import from a git repository.
//! - [`service`], [`objects`] and [`auth`]: the gRPC front end.

pub mod auth;
pub mod blame;
pub mod db;
pub mod derived;
pub mod error;
pub mod import;
pub mod land;
pub mod names;
pub mod objects;
pub mod read;
pub mod service;
pub mod store;
pub mod tree;
pub mod write;

pub use error::{Error, Result};
pub use store::Store;

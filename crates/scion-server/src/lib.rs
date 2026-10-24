#![forbid(unsafe_code)]
//! Scion server: durable storage, server-side commits and lands, and reads.
//!
//! This is the storage and serving crate of [openscion]: the bottom of the
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
//! [openscion]: https://github.com/bnsd55/openscion
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
//! - `derived`: one mechanism for every kind of derived data, which
//!   `backfill_root_tree` and its two sibling functions drive.
//! - `tree`: apply changesets to trees and derive each changeset's root
//!   tree.
//! - `graph`: the commit-graph index, and the graph queries that read it.
//! - `history`: the path history of each changeset, for `Log` with a path
//!   and for lands on old bases.
//! - `read`, `blame`, the `write` module, `land`: the operations behind the
//!   RPCs. A long one stops at its call's deadline. `similar` finds
//!   renames for `Diff` and `Blame`.
//! - `views`: the view of the repo that a partial reader (folder read
//!   rules, `access`) reads through.
//! - `quotas`: what one principal may hold at once; `tickets`: the
//!   signed read tickets of object reads.
//! - [`fsck`]: reports the damage in a data directory. Read-only.
//! - [`gc`]: collects the blobs that nothing names any more.
//! - `import`: one-way import from a git repository.
//! - `service`, `objects` and `auth`: the gRPC front end. `access`:
//!   which principal may read or write what.

//! ### What the facade is
//!
//! The API of this crate is the list below the modules: the types and
//! functions a server main and a client need, plus the `fsck` and `gc`
//! operations a server main runs by hand. Everything else exists for the
//! two binaries that this workspace ships, and can change without a
//! release. The one test seam is `test_support`, behind the `test-hooks`
//! feature (default off): it exists so the integration tests and the fuzz
//! targets can hold locks, move refs and inject commit failures at exact
//! moments. Nothing behind the feature carries a stability promise:
//! enabling it in a build is at the user's own risk.

pub(crate) mod access;
pub(crate) mod audit;
pub(crate) mod auth;
pub(crate) mod backend;
pub use backend::Backend;
pub mod archive;
pub(crate) mod blame;
pub(crate) mod db;
pub(crate) mod deadline;
pub(crate) mod derived;
pub(crate) mod error;
// These two are API, not implementation: a server main runs them by hand,
// and docs/operations.md documents both. Everything else in the list is
// an implementation detail of the two binaries, however public it looks.
#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_support/docs.rs"]
pub(crate) mod docs_hooks;
pub mod fsck;
pub mod gc;
pub(crate) mod graph;
pub(crate) mod history;
pub(crate) mod import;
pub(crate) mod land;
pub(crate) mod memory;
pub(crate) mod meta;
pub(crate) mod metrics;
pub(crate) mod names;
pub(crate) mod objects;
mod pg;
pub(crate) mod quotas;
pub(crate) mod read;
pub(crate) mod reply;
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod seed;
pub(crate) mod service;
pub(crate) mod similar;
pub(crate) mod store;
pub(crate) mod tickets;
pub(crate) mod tree;
pub(crate) mod views;
pub(crate) mod write;

/// The test seams, behind the test-hooks feature (default off). One module,
/// the only way in: the integration tests and the fuzz targets reach the
/// implementation through these re-exports, and nothing else. Not API, and
/// not covered by a stability promise.
///
/// The submodules re-export the implementation modules' public items rather
/// than the modules themselves: the modules are pub(crate), and a pub
/// re-export of a pub(crate) module is refused (E0365), while a re-export
/// of the items inside a public submodule is fine.
#[cfg(feature = "test-hooks")]
pub mod test_support {
    /// The caller behind an authenticated RPC, for tests that name one.
    pub mod auth {
        pub use crate::auth::Caller;
    }
    /// The audit log: a real file, or the off handle, and the fault
    /// hook that makes the next appends fail.
    pub mod audit {
        #[allow(unreachable_pub)]
        pub use crate::audit::{Audit, Event};
    }
    /// The blame operation behind the `Blame` RPC, with its limits.
    pub mod blame {
        pub use crate::blame::*;
    }
    /// The archive section readers: any bytes in their files end in a
    /// verdict or a refusal, never a panic. Wrappers, because the readers
    /// are pub(crate) helpers of the public `verify` and `restore`.
    pub mod archive {
        use std::path::Path;

        type Changesets = Vec<(scion_core::ChangesetId, i64, Vec<u8>)>;

        /// Reads the changeset frames of `path`.
        ///
        /// # Errors
        ///
        /// Why the bytes are not the changesets' length-framed stream.
        #[allow(unreachable_pub)]
        pub fn read_changesets(path: &Path) -> crate::error::Result<Changesets> {
            crate::archive::read_changesets(path)
        }

        /// The transactions the restores of this process committed so
        /// far: a test that must see a restore commit many transactions
        /// reads it before and after.
        #[allow(unreachable_pub)]
        pub fn restore_tx_count() -> u64 {
            crate::archive::RESTORE_TXS.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Reads the blob members of `path`.
        ///
        /// # Errors
        ///
        /// Why a line of the file is not a blob id.
        #[allow(unreachable_pub)]
        pub fn read_members(path: &Path) -> crate::error::Result<Vec<scion_core::BlobId>> {
            crate::archive::read_members(path)
        }

        /// Reads the ref rows of `path`.
        ///
        /// # Errors
        ///
        /// Why the JSON is not an array of ref rows.
        #[allow(unreachable_pub)]
        pub fn read_refs(
            path: &Path,
        ) -> crate::error::Result<Vec<(String, scion_core::ChangesetId, i64)>> {
            crate::archive::read_refs(path)
        }

        /// The rows, as a count: the land rows name crate-private types,
        /// and the caller only needs the verdict.
        ///
        /// # Errors
        ///
        /// Why the JSON is not an array of land rows.
        #[allow(unreachable_pub)]
        pub fn read_lands(path: &Path) -> crate::error::Result<usize> {
            crate::archive::read_lands(path).map(|rows| rows.len())
        }

        /// The rows, as a count: a provenance row names a type of a
        /// private dependency, and the caller only needs the verdict.
        ///
        /// # Errors
        ///
        /// Why the JSON is not an array of provenance rows.
        #[allow(unreachable_pub)]
        pub fn read_provenance(path: &Path) -> crate::error::Result<usize> {
            crate::archive::read_provenance(path).map(|rows| rows.len())
        }

        /// The manifest, as its top-level fields' count: `serde_json` is
        /// not a dependency of the fuzz crate, so the value stays inside.
        ///
        /// # Errors
        ///
        /// Why the file is not a scion archive's manifest.
        #[allow(unreachable_pub)]
        pub fn read_manifest(archive: &Path) -> crate::error::Result<usize> {
            crate::archive::read_manifest(archive)
                .map(|value| value.as_object().map_or(0, serde_json::Map::len))
        }
    }
    /// The per-call deadline every long operation stops at.
    pub mod deadline {
        pub use crate::deadline::*;
    }
    /// The opaque backend, for the tests that build one from a store
    /// they prepared, or read the data directory's own store out of
    /// one. The backend itself carries no test methods: the facade
    /// type stays clean, and the seams live here.
    pub mod backend {
        /// Wraps a store the test prepared ([`Store::open`], or
        /// `open_with_walks` for a capped walk) into a backend, as the
        /// engine-agnostic constructors cannot: they open their own.
        /// The engine the store holds is the test's choice, not part
        /// of the API.
        #[must_use]
        pub fn from_store(store: crate::test_support::store::Store) -> crate::Backend {
            crate::Backend::sqlite(store)
        }

        /// The test-only reads of a backend: the data directory's own
        /// store, when the backend is one. [`None`] on Postgres, whose
        /// rows a test reads through the [`MetaTx`] queries.
        pub trait BackendExt {
            /// The store of a data-directory backend, for the tests
            /// that assert the storage's internals (its raw rows, its
            /// page caps).
            fn sqlite(&self) -> Option<&crate::test_support::store::Store>;
        }

        impl BackendExt for crate::Backend {
            fn sqlite(&self) -> Option<&crate::test_support::store::Store> {
                self.as_sqlite()
            }
        }

        /// The schema versions this build migrates to, so a test pins
        /// the migration story without naming a literal that the next
        /// migration turns stale.
        pub mod schema_version {
            /// The SQLite store's schema version ([`crate::store`]).
            pub const SQLITE: u32 = crate::store::SCHEMA_VERSION;
            /// The Postgres schema's version ([`crate::pg`]).
            pub const POSTGRES: u32 = crate::pg::PG_SCHEMA_VERSION;
        }
    }
    /// The error a changeset apply can end in, and the conversion the
    /// tests share with the crate, for the write tests.
    pub mod error {
        pub use crate::error::{ApplyError, from_sqlite};
    }
    /// The counters the import tests read between retries.
    pub mod import {
        pub use crate::import::{BUSY_TRIES, CHECKS, STRUCTURE_CAPS};
    }
    /// The land operation: its queue, requests and outcomes.
    pub mod land {
        pub use crate::land::*;
    }
    /// The blob pool the server hands out reads from.
    pub mod memory {
        pub use crate::memory::*;
    }
    /// The metadata traits the tests and the fuzz target call directly.
    pub mod meta {
        pub use crate::meta::*;
    }
    /// The read operations behind the read RPCs, and their limits.
    pub mod read {
        pub use crate::read::*;
    }
    /// The gRPC front end, so tests and fuzz targets serve on a port.
    pub mod service {
        pub use crate::service::*;
    }
    /// The on-disk store, so tests open and inspect one directly.
    pub mod store {
        pub use crate::store::*;
        /// The schema version this build ships, and the migration runner,
        /// for a test that makes a store of an older version.
        #[allow(unreachable_pub)]
        pub use crate::store::{SCHEMA_VERSION, migrate};
    }
    /// The guide's own commands, so a Proof's test runs the block under
    /// its runbook's heading instead of retyping it.
    pub mod docs {
        pub use crate::docs_hooks::*;
    }
    /// Tree apply and the per-changeset root tree.
    pub mod tree {
        pub use crate::tree::*;
    }
    /// The write path: changeset decode and apply, and its limits.
    pub mod write {
        pub use crate::write::*;
    }
    /// The bulk seed for deep file-less ladders, for the tests that
    /// measure the queries instead of the seeding.
    pub mod seed {
        pub use crate::seed::*;
    }
}

// The facade: the API of this crate, one list, kept here so it is one
// thing to read and one thing to keep honest. Every other name in the
// modules is an implementation detail of the two binaries, however
// public the module it sits in looks. The doc(hidden) items are not
// here, and never were.
pub use access::Access;
pub use derived::DeriveReport;
pub use error::{Error, Git, Result, Storage, StorageClass};
pub use gc::DEFAULT_GRACE_SECS;
pub use graph::{CommitGraph, backfill_commit_graph};
pub use history::{PathHistory, backfill_path_history};
#[cfg(any(test, feature = "test-hooks"))]
pub use import::set_import_per_commit_txs;
pub use import::{ImportOptions, ImportReport, import_git};
pub use quotas::Quotas;
pub use reply::REPLY_MEMORY_BUDGET;
pub use scion_proto::runtime::metrics::Metrics;
pub use service::{ServeOptions, serve};
// `ServeOptions` is public and holds the audit handle, so the handle is
// public too; unreachable_pub does not see through the re-export.
#[allow(unreachable_pub)]
pub use audit::{Audit, Event};
pub use tickets::Keys;
pub use tree::{RootTree, backfill_root_tree};

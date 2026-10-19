//! gRPC API for Scion, plus the rules that every process of the API shares.
//!
//! The supported surface is the protocol contract: the message and service
//! types generated from `proto/scion.proto` ([`v1`]), the wire constants
//! of this module, and the signed read tickets that make object IDs
//! unreadable without permission ([`ticket`]). A server mints a ticket
//! with [`ticket::sign`]; the receiver checks it with [`ticket::parse`]:
//!
//! ```
//! use scion_proto::ticket::{parse, sign};
//!
//! let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
//! let bytes = sign(&key, "monorepo", "alice", [9; 32], 1_700_000_000)
//!     .map_err(|e| e.to_string())?;
//! let keys = [ed25519_dalek::VerifyingKey::from(&key)];
//! let ticket = parse(&bytes, &keys, 1_700_000_100).map_err(|e| e.to_string())?;
//! assert_eq!(ticket.repo, "monorepo");
//! assert_eq!(ticket.principal, "alice");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Everything else the five crates share — bearer tokens and the access
//! checks ([`runtime::auth`]), checks on request fields
//! ([`runtime::check`]), how a caller's deadline passes down
//! ([`runtime::deadline`]), how one object travels in parts and
//! encodings ([`runtime::object`]), the operator metrics
//! ([`runtime::metrics`]), how a `GetObjects` reply streams objects
//! ([`runtime::reply`]), how many connections and streams a process
//! serves ([`runtime::listen`]), TLS for those connections
//! ([`runtime::tls`]), and request IDs ([`runtime::request_id`]) — is
//! runtime code in [`runtime`], which is not the published contract.
//!
//! The object bytes themselves live in
//! [`scion-core`](https://docs.rs/open-scion-core).

/// The runtime code that serves the contract: transport, TLS, the
/// listener, metrics, the reply loop, and the access rules our binaries
/// enforce.
///
/// # Unstable
///
/// This module is not the published contract of the crate. It moves and
/// changes with the binaries it serves, and an outside implementer codes
/// against [`v1`], the wire constants, and [`ticket`] instead. The crate
/// publishes it for the server, the tiers and the daemon of this
/// repository, which share it.
pub mod runtime;

pub mod ticket;

/// The buffer type of `ObjectPart.data`: parts of one object are slices of one
/// buffer.
pub use prost::bytes::Bytes;

/// Largest single gRPC message either side sends or accepts. A message of an
/// `Objects` reply is smaller (see [`MAX_PART_MESSAGE_BYTES`]).
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;

/// Chunk size for streamed file content (`PutBlob`, `ReadFile`), and the
/// most encoded bytes in one part of an object in a `GetObjects` reply.
pub const CHUNK_BYTES: usize = 1 << 20;

/// Largest object a server stores or sends, uncompressed: 100 MiB.
pub const MAX_OBJECT_BYTES: usize = 100 << 20;

/// Largest message of an `Objects` reply: one part of an object (at most
/// [`CHUNK_BYTES`]) plus its header.
pub const MAX_PART_MESSAGE_BYTES: usize = CHUNK_BYTES + 1024;

/// Most objects one `GetObjects` call may name.
pub const MAX_OBJECT_REFS: usize = 1000;

/// Version 1 of the API.
#[allow(missing_docs, clippy::pedantic)]
pub mod v1 {
    tonic::include_proto!("scion.v1");
}

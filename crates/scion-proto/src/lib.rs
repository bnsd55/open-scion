//! gRPC API for Scion, plus the rules that every process of the API shares.
//!
//! The message and service types are generated from `proto/scion.proto`
//! ([`v1`]); everything else is the shared behavior that the server, the
//! cache tier and the daemon agree on:
//! bearer tokens ([`auth`]), checks on request fields ([`check`]), how a
//! caller's deadline passes down ([`deadline`]), how one object travels in
//! parts and encodings ([`object`]), the operator metrics ([`metrics`]),
//! how a `GetObjects` reply streams objects ([`reply`]), how many
//! connections and streams a process serves ([`listen`]), TLS for those
//! connections ([`tls`]), and the signed read tickets that make object
//! IDs unreadable without permission ([`ticket`]).
//!
//! This crate is the contract the five crates share: the server serves
//! it, the tiers forward it, and a client talks it; the object bytes
//! themselves live in [`scion-core`](https://docs.rs/open-scion-core).
//!
//! A server mints a read ticket with [`ticket::sign`]; the receiver
//! checks it with [`ticket::parse`]:
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

pub mod auth;
pub mod check;
pub mod deadline;
pub mod listen;
pub mod metrics;
pub mod object;
pub mod reply;
pub mod request_id;
pub mod ticket;
pub mod tls;

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

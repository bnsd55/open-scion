//! gRPC API for Scion, generated from `proto/scion.proto`, plus the rules that
//! every server of the API shares: bearer tokens ([`auth`]), checks on request
//! fields ([`check`]), how a caller's deadline passes down ([`deadline`]), how
//! one object travels in parts and encodings ([`object`]), the operator
//! metrics ([`metrics`]), how a
//! `GetObjects` reply streams objects ([`reply`]), and how many connections
//! and streams a process serves ([`listen`]), and TLS for those connections
//! ([`tls`]).

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

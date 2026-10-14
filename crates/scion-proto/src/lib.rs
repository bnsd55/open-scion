//! gRPC API for Scion, generated from `proto/scion.proto`.

/// Largest single gRPC message either side sends or accepts, except a
/// `GetObjects` reply (see [`MAX_OBJECT_MESSAGE_BYTES`]).
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;

/// Chunk size for streamed file content (`PutBlob`, `ReadFile`).
pub const CHUNK_BYTES: usize = 1 << 20;

/// Largest object a server stores or sends, uncompressed: 100 MiB.
pub const MAX_OBJECT_BYTES: usize = 100 << 20;

/// Largest `GetObjects` reply message: one object plus its kind and ID.
pub const MAX_OBJECT_MESSAGE_BYTES: usize = MAX_OBJECT_BYTES + 1024;

/// Most objects one `GetObjects` call may name.
pub const MAX_OBJECT_REFS: usize = 1000;

/// Version 1 of the API.
#[allow(missing_docs, clippy::pedantic)]
pub mod v1 {
    tonic::include_proto!("scion.v1");
}

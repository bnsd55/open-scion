//! gRPC API for Scion, generated from `proto/scion.proto`.

/// Largest single gRPC message either side sends or accepts.
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;

/// Chunk size for streamed file content (`PutBlob`, `ReadFile`).
pub const CHUNK_BYTES: usize = 1 << 20;

/// Largest `GetObjects` reply message: one object of up to 100 MiB (the
/// server's object limit) plus its kind and ID.
pub const MAX_OBJECT_MESSAGE_BYTES: usize = (100 << 20) + 1024;

/// Most objects one `GetObjects` call may name.
pub const MAX_OBJECT_REFS: usize = 1000;

/// Version 1 of the API.
#[allow(missing_docs, clippy::pedantic)]
pub mod v1 {
    tonic::include_proto!("scion.v1");
}

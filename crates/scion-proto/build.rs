//! Generates the gRPC client and server from `proto/scion.proto`, and
//! the descriptor set the protocol-surface golden reads: the surface is
//! checked against what the compiler saw, not against a hand parse of
//! the text.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The proto file enters the crate through a link to the one at the
    // repository root, so the packaged crate carries it and builds alone.
    // Without this line, a changed proto keeps the old code.
    println!("cargo:rerun-if-changed=proto/scion.proto");
    // An object part's data is `Bytes`, so a buffer that a store keeps goes
    // into a reply without a copy.
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .bytes(".scion.v1.ObjectPart.data")
        // The wire surface, compiled: the protocol-surface golden decodes
        // this and holds every call, field number, label, type and enum
        // value the compiler saw (tests/protocol_surface.rs).
        .file_descriptor_set_path(out_dir.join("scion_descriptor.bin"))
        .compile_protos(&["proto/scion.proto"], &["proto"])?;
    Ok(())
}

//! Generates the gRPC client and server from `proto/scion.proto`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The proto file is outside this package, so cargo does not watch it
    // unless told to. Without this line, a changed proto keeps the old code.
    println!("cargo:rerun-if-changed=../../proto/scion.proto");
    // A part of an object is a slice of one buffer, never a copy.
    tonic_prost_build::configure()
        .bytes(".scion.v1.ObjectPart.data")
        .compile_protos(&["../../proto/scion.proto"], &["../../proto"])?;
    Ok(())
}

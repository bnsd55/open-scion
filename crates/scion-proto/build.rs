//! Generates the gRPC client and server from `proto/scion.proto`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The proto file is outside this package, so cargo does not watch it
    // unless told to. Without this line, a changed proto keeps the old code.
    println!("cargo:rerun-if-changed=../../proto/scion.proto");
    // An object part's data is `Bytes`, so a buffer that a store keeps goes
    // into a reply without a copy.
    tonic_prost_build::configure()
        .bytes(".scion.v1.ObjectPart.data")
        .compile_protos(&["../../proto/scion.proto"], &["../../proto"])?;
    Ok(())
}

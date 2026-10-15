//! Generates the gRPC client and server from `proto/scion.proto`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The proto file is outside this package, so cargo does not watch it
    // unless told to. Without this line, a changed proto keeps the old code.
    println!("cargo:rerun-if-changed=../../proto/scion.proto");
    tonic_prost_build::configure()
        .compile_protos(&["../../proto/scion.proto"], &["../../proto"])?;
    Ok(())
}

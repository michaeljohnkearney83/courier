// protox compiles the .proto files in pure Rust, so no protoc install is needed.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptors = protox::compile(["courier/v1/courier.proto"], ["../../proto"])?;
    tonic_build::configure().compile_fds(descriptors)?;
    println!("cargo:rerun-if-changed=../../proto");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = "proto/spooltail/v1/spooltail.proto";
    println!("cargo:rerun-if-changed={schema}");
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_client(false)
        .compile_with_config(config, &[schema], &["proto/spooltail/v1"])?;
    Ok(())
}

//! Allows compile *.proto files
//! see https://github.com/hyperium/tonic/blob/master/tonic-build/README.md

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Grpc output dir: {:?}", std::env::var("OUT_DIR").unwrap());

    let mut cfg = prost_build::Config::new();
    cfg.protoc_arg("--experimental_allow_proto3_optional");

    tonic_build::configure()
        .build_server(true)
        .compile_with_config(
            cfg,
            &[
                "../../protocol-grpc/protobuf/ope_btree.proto",
                "../../protocol-grpc/protobuf/ope_db_api.proto",
            ],
            &["../../protocol-grpc/protobuf/"],
        )?;
    Ok(())
}

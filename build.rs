//! Allows compile *.proto files
//! see https://github.com/hyperium/tonic/blob/master/tonic-build/README.md

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TEST {:?}", std::env::var("OUT_DIR").unwrap());

    tonic_build::configure().build_server(true).compile(
        &[
            "../protocol-grpc/protobuf/ope_btree.proto",
            "../protocol-grpc/protobuf/ope_db_api.proto",
        ],
        &["../protocol-grpc/protobuf/"],
    )?;
    Ok(())
}

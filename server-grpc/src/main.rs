use common::noop_hasher::NoOpHasher;
use server::ope_btree::OpeBTreeConf;
use tonic::transport::Server;
use std::error::Error;
use std::env;
use env_logger::Env;

// Grpc-based protocol implementation
mod grpc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let conf = OpeBTreeConf {
        arity: 4,
        alpha: 0.25,
    };
    let (rx, _tx) = tokio::sync::mpsc::channel(1);
    let db = grpc::new_in_memory_db::<NoOpHasher>(conf, rx);

    let addr = "[::1]:7777".parse().unwrap();

    log::info!("Start listening {}", addr);

    Server::builder()
        .add_service(grpc::rpc::db_rpc_server::DbRpcServer::new(db))
        .serve(addr)
        .await?;

    Ok(())

}

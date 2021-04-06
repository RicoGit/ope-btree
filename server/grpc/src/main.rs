use common::noop_hasher::NoOpHasher;
use env_logger::Env;
use server::ope_btree::OpeBTreeConf;
use std::error::Error;
use tonic::transport::Server;

// Grpc-based protocol implementation
mod grpc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let conf = OpeBTreeConf {
        arity: 4,
        alpha: 0.25,
    };
    let (rx, mut tx) = tokio::sync::mpsc::channel(100);
    let db = grpc::new_in_memory_db::<NoOpHasher>(conf, rx).await;

    let addr = "[::1]:7777".parse().unwrap();

    log::info!("Start listening {}", addr);

    tokio::spawn(async move {
        while let Some(update) = tx.recv().await {
            log::info!("Update channel: {:?}", update)
        }
    });

    Server::builder()
        .add_service(grpc::rpc::db_rpc_server::DbRpcServer::new(db))
        .serve(addr)
        .await?;

    Ok(())
}

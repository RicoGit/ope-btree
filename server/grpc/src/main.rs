use common::noop_hasher::NoOpHasher;
use env_logger::Env;
use server::ope_btree::OpeBTreeConf;
use server_grpc::grpc::rpc::db_rpc_server::DbRpcServer;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(StructOpt, Debug)]
#[structopt(name = "ope-db-server", about = "Encrypted database server")]
pub struct Config {
    /// Ope btree (index) property. Maximum size of nodes (maximum number children in branches), should be even.
    #[structopt(long, default_value = "4")]
    arity: u8,

    /// Ope btree (index) property. Minimum capacity factor of node. Should be between 0 and 0.5. 0.25 means that
    /// each node except root should always contains between 25% and 100% children.
    #[structopt(long, default_value = "0.25")]
    alpha: f32,

    /// Grpc server listening port
    #[structopt(long, default_value = "7777")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    let config: Config = dbg!(Config::from_args());

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let conf = OpeBTreeConf {
        arity: config.arity,
        alpha: config.alpha,
    };
    let (rx, mut tx) = tokio::sync::mpsc::channel(100);
    let db = server_grpc::grpc::new_in_memory_db::<NoOpHasher>(conf, rx).await;

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), config.port);

    log::info!("Start listening {}", addr);

    tokio::spawn(async move {
        while let Some(update) = tx.recv().await {
            log::info!("Update channel: {:?}", update)
        }
    });

    Server::builder()
        .add_service(DbRpcServer::new(db))
        .serve(addr)
        .await?;

    Ok(())
}

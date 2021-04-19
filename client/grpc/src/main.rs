//! Simple client just create Db and get and put few values (for debug purpose)

use anyhow::Context;
use bytes::Bytes;
use client::ope_btree::test::NoOpCrypt;
use client::ope_btree::OpeBTreeClient;
use client::ope_db::OpeDatabaseClient;
use client_grpc::grpc::rpc::db_rpc_client::DbRpcClient;
use client_grpc::grpc::GrpcDbRpc;
use client_grpc::state::ClientState;
use common::noop_hasher::NoOpHasher;
use common::Hash;
use env_logger::Env;
use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "ope-db-client", about = "Encrypted database CLI client")]
pub struct AppConfig {
    /// Server host and port, ex `http://localhost:7777`
    #[structopt(long)]
    host: String,

    /// Client's secret key for encryption keys and values.
    #[structopt(long, required_if("debug-mode", "false"))]
    encryption_key: Option<String>,

    /// Disables encryption and uses noop hasher if set to true
    #[structopt(long, default_value = "false")]
    debug_mode: String, // can't be bool, used in required_if for encrypted_key

    /// Where client state will be saved after ending the session.
    /// If empty state.json will be created in the same folder
    #[structopt(long, parse(from_os_str), default_value = "state.json")]
    state_path: PathBuf,

    /// Logging level
    #[structopt(long, default_value = "info")]
    log_lvl: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    let config: AppConfig = dbg!(AppConfig::from_args());

    env_logger::Builder::from_env(Env::default().default_filter_or(&config.log_lvl)).init();

    let state = ClientState::read_or_create(config.state_path.clone())?;
    log::info!("Client state: {:?}", &state);

    let client = DbRpcClient::connect(config.host.clone()).await?;
    let rpc = GrpcDbRpc::new(client);

    log::info!("Connected to {}", config.host);

    // Creates client for String Key and Val, dummy Hasher and without Encryption
    let index = OpeBTreeClient::<NoOpCrypt, NoOpHasher>::new(Hash::empty(), NoOpCrypt {}, ());
    let mut db_client = OpeDatabaseClient::<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>::new(
        index,
        NoOpCrypt {},
        rpc,
        AtomicUsize::new(42),
        Bytes::from("dataset_id"),
    );

    // todo finish from here
    // interact with user via CLI

    log::info!("Send GET k1");
    let k1 = "k1".to_string();
    let res = db_client.get(k1).await?;
    log::info!("Response: {:?}", res);
    assert_eq!(None, res);

    log::info!("Send PUT k1 v1");
    let k1 = "k1".to_string();
    let v1 = "v1".to_string();
    let res = db_client.put(k1, v1.clone()).await?;
    log::info!("Response: {:?}", res);
    assert_eq!(None, res);

    log::info!("Send GET k1");
    let k1 = "k1".to_string();
    let res = db_client.get(k1).await?;
    log::info!("Response: {:?}", res);
    assert_eq!(Some(v1), res);

    Ok(())
}

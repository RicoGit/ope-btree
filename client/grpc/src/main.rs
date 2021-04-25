//! Simple client just create Db and get and put few values (for debug purpose)

use crate::tui::Cmd;
use client::ope_btree::test::NoOpCrypt;
use client::ope_btree::OpeBTreeClient;
use client::ope_db::OpeDatabaseClient;
use client_grpc::config_store::ConfigStore;
use client_grpc::grpc::rpc::db_rpc_client::DbRpcClient;
use client_grpc::grpc::GrpcDbRpc;
use common::noop_hasher::NoOpHasher;
use common::Hash;
use dialoguer::Input;
use dialoguer::{theme::ColorfulTheme, Select};
use env_logger::Env;
use std::error::Error;
use std::path::PathBuf;
use structopt::StructOpt;

mod tui;

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
    #[structopt(long, default_value = "warn")]
    log_lvl: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    let config: AppConfig = dbg!(AppConfig::from_args());

    env_logger::Builder::from_env(Env::default().default_filter_or(&config.log_lvl)).init();

    let config_store = ConfigStore::read_or_create(config.state_path.clone());
    log::info!("Client state: {:?}", &config_store.config);

    let client = DbRpcClient::connect(config.host.clone()).await?;
    let rpc = GrpcDbRpc::new(client);

    log::info!("Connected to {}", config.host);

    // Creates client for String Key and Val, dummy Hasher and without Encryption
    let index = OpeBTreeClient::<NoOpCrypt, NoOpHasher>::new(Hash::empty(), NoOpCrypt {}, ());

    let mut db_client = OpeDatabaseClient::<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>::new(
        index,
        NoOpCrypt {},
        rpc,
        config_store.config.clone(),
    );

    let datasets = config_store
        .config
        .datasets
        .into_iter()
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();

    // choose dataset
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select dataset")
        .default(0)
        .items(&datasets)
        .interact()
        .unwrap();

    let selected_ds_id = &datasets[selection];

    if let Some(_) = db_client.dataset(selected_ds_id).await {
        log::info!("Current dataset is {} now", selected_ds_id);
    }

    loop {
        let cmd: tui::Cmd = Input::with_theme(&ColorfulTheme::default())
            .with_prompt(selected_ds_id)
            .interact()
            .unwrap();

        match cmd {
            Cmd::Get(key) => {
                let res = db_client.get(key).await?;
                println!("Response: {:?}", res);
            }
            Cmd::Put(key, val) => {
                let res = db_client.put(key, val).await?;
                // todo update client state and write it to file
                println!("Response: {:?}", res);
            }
            Cmd::New(_dataset_id) => {
                println!("Create new dataset is not implemented yet")
            }
            Cmd::Exit => {
                println!("Bye!");
                break;
            }
        }
    }

    Ok(())
}

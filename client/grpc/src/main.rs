//! Simple client just create Db and get and put few values (for debug purpose)

use crate::lib::GrpcDbRpc;
use bytes::Bytes;
use client::ope_btree::test::NoOpCrypt;
use client::ope_btree::OpeBTreeClient;
use client::ope_db::OpeDatabaseClient;
use common::noop_hasher::NoOpHasher;
use common::Hash;
use env_logger::Env;
use lib::rpc::db_rpc_client::DbRpcClient;
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::Request;

mod lib;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let addr = "http://[::1]:7777";
    let mut client = DbRpcClient::connect(addr).await?;
    let rpc = lib::GrpcDbRpc::new(client);

    log::info!("Connected to {}", addr);

    // Creates client for String Key and Val, dummy Hasher and without Encryption
    let index = OpeBTreeClient::<NoOpCrypt, NoOpHasher>::new(Hash::empty(), NoOpCrypt {}, ());
    let mut db_client = OpeDatabaseClient::<NoOpCrypt, NoOpCrypt, NoOpHasher, GrpcDbRpc>::new(
        index,
        NoOpCrypt {},
        rpc,
        AtomicUsize::new(42),
        Bytes::from("dataset_id"),
    );

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

    // log::info!("Send GET k1");
    // let k1 = "k1".to_string();
    // let res = db_client.get(k1).await?;
    // log::info!("Response: {:?}", res);
    // assert_eq!(Some(v1), res);

    Ok(())
}

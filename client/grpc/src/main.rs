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

mod lib;

// todo move it to separate integration test
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let addr = "http://[::1]:7777";
    let client = DbRpcClient::connect(addr).await?;
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

    log::info!("Send GET k1");
    let k1 = "k1".to_string();
    let res = db_client.get(k1).await?;
    log::info!("Response: {:?}", res);
    assert_eq!(Some(v1), res);

    log::info!("Send PUT k2 v2");
    let k2 = "k2".to_string();
    let v2 = "v2".to_string();
    let res = db_client.put(k2, v2.clone()).await?;
    log::info!("Response: {:?}", res);
    assert_eq!(None, res);

    log::info!("Send GET k2");
    let k2 = "k2".to_string();
    let res = db_client.get(k2).await?;
    log::info!("Response: {:?}", res);
    assert_eq!(Some(v2), res);

    let n = 20;
    for idx in 3..n {
        log::info!("Send PUT k{:?} v{:?}", idx, idx);
        let k = format!("k{:?}", idx);
        let v = format!("k{:?}", idx);
        let res = db_client.put(k, v.clone()).await?;
        log::info!("Response: {:?}", res);
        assert_eq!(None, res);
    }

    for idx in 3..n {
        log::info!("Send GET k{:?} v{:?}", idx, idx);
        let k = format!("k{:?}", idx);
        let v = format!("k{:?}", idx);
        let res = db_client.get(k).await?;
        log::info!("Response: {:?}", res);
        assert_eq!(Some(v), res);
    }

    // override values

    let n = 10;
    for idx in 3..n {
        log::info!("Send PUT k{:?} v{:?}", idx, idx);
        let k = format!("k{:?}", idx);
        let old_v = format!("k{:?}", idx);
        let new_v = format!("k{:?}!", idx);
        let res = db_client.put(k, new_v).await?;
        log::info!("Response: {:?}", res);
        assert_eq!(Some(old_v), res);
    }

    for idx in 3..n {
        log::info!("Send GET k{:?} v{:?}", idx, idx);
        let k = format!("k{:?}", idx);
        let v = format!("k{:?}!", idx);
        let res = db_client.get(k).await?;
        log::info!("Response: {:?}", res);
        assert_eq!(Some(v), res);
    }

    Ok(())
}

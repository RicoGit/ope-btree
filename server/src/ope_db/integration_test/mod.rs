//! Integration test for OpeDatabase and OpeDatabaseClient.
//! Complex test for index and database.

use crate::ope_btree::internal::node_store::BinaryNodeStore;
use crate::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen};
use crate::ope_db::{DatasetChanged, OpeDatabase};
use bytes::Bytes;
use client::ope_btree::test::NoOpCrypt;
use client::ope_btree::OpeBTreeClient;
use client::ope_db::OpeDatabaseClient;
use common::gen::NumGen;

use common::noop_hasher::NoOpHasher;
use common::Hash;

use kvstore_inmemory::hashmap_store::HashMapKVStore;
use protocol::btree::{PutCallback, SearchCallback};
use protocol::database::OpeDatabaseRpc;
use protocol::RpcFuture;
use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc::{channel, Sender};

#[tokio::test]
#[ignore]
async fn get_from_empty_db_test() {
    // get from empty db

    let (tx, _rx) = channel::<DatasetChanged>(1);
    let db = create_server(tx).await;
    let client = create_client(db);

    let result = client.get("k1".to_string()).await;
    assert_eq!(result.unwrap(), None)
}

struct TestDatabaseRpc {
    db: RefCell<OpeDatabase<BinStore<Vec<u8>>, BinStore<Bytes>, NoOpHasher>>,
}

impl TestDatabaseRpc {
    fn new(db: OpeDatabase<BinStore<Vec<u8>>, BinStore<Bytes>, NoOpHasher>) -> Self {
        TestDatabaseRpc {
            db: RefCell::new(db),
        }
    }
}

impl OpeDatabaseRpc for TestDatabaseRpc {
    fn get<'f, Cb: SearchCallback>(
        &self,
        _dataset_id: Bytes,
        _version: usize,
        _search_callback: Cb,
    ) -> RpcFuture<'f, Option<Bytes>> {
        // self.db
        //     .borrow_mut()
        //     .get(search_callback)
        //     .map_err(|err| ProtocolError::RpcErr {
        //         msg: err.to_string(),
        //     })
        //     .boxed()
        todo!()
    }

    fn put<'f, Cb: PutCallback>(
        &self,
        _dataset_id: Bytes,
        _version: usize,
        _put_callback: Cb,
        _encrypted_value: Bytes,
    ) -> RpcFuture<'f, Option<Bytes>> {
        // self.db
        //     .borrow_mut()
        //     .put(put_callback, version, encrypted_value)
        //     .map_err(|err| ProtocolError::RpcErr {
        //         msg: err.to_string(),
        //     })
        //     .boxed()
        todo!()
    }
}

type BinStore<B> = HashMapKVStore<B, B>;

async fn create_server(
    sender: Sender<DatasetChanged>,
) -> OpeDatabase<BinStore<Vec<u8>>, BinStore<Bytes>, NoOpHasher> {
    let conf = OpeBTreeConf {
        arity: 4,
        alpha: 0.25,
    };
    let node_store = BinaryNodeStore::new(HashMapKVStore::new(), NumGen(0));
    let value_store = HashMapKVStore::new();
    let index = OpeBTree::new(conf, node_store, ValRefGen(100));
    let mut db = OpeDatabase::new(index, value_store, sender);
    db.init().await.unwrap();
    db
}

fn create_client(
    db: OpeDatabase<BinStore<Vec<u8>>, BinStore<Bytes>, NoOpHasher>,
) -> OpeDatabaseClient<NoOpCrypt, NoOpCrypt, NoOpHasher, TestDatabaseRpc> {
    let crypt = NoOpCrypt {};
    let index: OpeBTreeClient<NoOpCrypt, NoOpHasher> =
        OpeBTreeClient::<NoOpCrypt, NoOpHasher>::new(Hash::empty(), crypt.clone(), ());
    let rpc = TestDatabaseRpc::new(db);
    let dataset_id = "test_dataset".into();

    OpeDatabaseClient::new(index, crypt, rpc, AtomicUsize::new(0), dataset_id)
}

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
use futures::{FutureExt, TryFutureExt};
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use protocol::btree::{BtreeCallback, ClientPutDetails, PutCallback, SearchCallback, SearchResult};
use protocol::database::OpeDatabaseRpc;
use protocol::{ProtocolError, RpcFuture};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;

#[tokio::test]
async fn get_from_empty_db_test() {
    // get from empty db

    let (tx, _rx) = channel::<DatasetChanged>(1);
    let db = create_server(tx).await;
    let client = create_client(db);

    let result = client.get("k1".to_string()).await;
    assert_eq!(result.unwrap(), None)
}

#[tokio::test]
async fn put_to_empty_db_test() {
    // get from empty db

    let (tx, _rx) = channel::<DatasetChanged>(1);
    let db = create_server(tx).await;
    let client = create_client(db);

    let result = client.put("k1".to_string(), "v1".to_string()).await;
    assert_eq!(result.unwrap(), None)
}

struct TestDatabaseRpc {
    db: Arc<Mutex<OpeDatabase<BinStore<Vec<u8>>, BinStore<Bytes>, NoOpHasher>>>,
}

impl TestDatabaseRpc {
    fn new(db: OpeDatabase<BinStore<Vec<u8>>, BinStore<Bytes>, NoOpHasher>) -> Self {
        TestDatabaseRpc {
            db: Arc::new(Mutex::new(db)),
        }
    }
}

impl OpeDatabaseRpc for TestDatabaseRpc {
    fn get<'f, Cb: SearchCallback + 'f>(
        &self,
        _dataset_id: Bytes,
        _version: usize,
        search_callback: Cb,
    ) -> RpcFuture<'f, Option<Bytes>> {
        let db = self.db.clone();
        let cb = TestCb::search(Box::new(search_callback));

        async move {
            let mut lock = db.lock().await;
            lock.get(cb)
                .map_err(|err| ProtocolError::RpcErr {
                    msg: err.to_string(),
                })
                .await
        }
        .boxed()
    }

    fn put<'f, Cb: PutCallback + 'f>(
        &self,
        _dataset_id: Bytes,
        version: usize,
        put_callback: Cb,
        encrypted_value: Bytes,
    ) -> RpcFuture<'f, Option<Bytes>> {
        let db = self.db.clone();
        let cb = TestCb::put(Box::new(put_callback));

        async move {
            let mut lock = db.lock().await;
            lock.put(cb, version, encrypted_value)
                .map_err(|err| ProtocolError::RpcErr {
                    msg: err.to_string(),
                })
                .await
        }
        .boxed()
    }
}

struct TestCb<Cb> {
    cb: *mut Cb,
}

unsafe impl<Cb> Send for TestCb<Cb> {}

unsafe impl<Cb> Sync for TestCb<Cb> {}

impl<Cb> Clone for TestCb<Cb> {
    fn clone(&self) -> Self {
        TestCb { cb: self.cb }
    }
}

impl<Cb: SearchCallback> TestCb<Cb> {
    fn search(cb: Box<Cb>) -> Self {
        let cb: *mut Cb = Box::into_raw(cb);
        TestCb { cb }
    }
}

impl<Cb: PutCallback> TestCb<Cb> {
    fn put(cb: Box<Cb>) -> Self {
        let cb: *mut Cb = Box::into_raw(cb);
        TestCb { cb }
    }
}

impl<Cb: BtreeCallback> BtreeCallback for TestCb<Cb> {
    fn next_child_idx<'f>(
        &mut self,
        keys: Vec<Bytes>,
        children_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize> {
        unsafe {
            self.cb
                .as_mut()
                .unwrap()
                .next_child_idx(keys, children_hashes)
        }
    }
}

impl<Cb: SearchCallback> SearchCallback for TestCb<Cb> {
    fn submit_leaf<'f>(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, SearchResult> {
        unsafe { self.cb.as_mut().unwrap().submit_leaf(keys, values_hashes) }
    }
}

impl<Cb: PutCallback> PutCallback for TestCb<Cb> {
    fn put_details<'f>(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, ClientPutDetails> {
        unsafe { self.cb.as_mut().unwrap().put_details(keys, values_hashes) }
    }

    fn verify_changes<'f>(
        &mut self,
        server_merkle_root: Bytes,
        was_splitting: bool,
    ) -> RpcFuture<'f, Bytes> {
        unsafe {
            self.cb
                .as_mut()
                .unwrap()
                .verify_changes(server_merkle_root, was_splitting)
        }
    }

    fn changes_stored<'f>(&self) -> RpcFuture<'f, ()> {
        unsafe { self.cb.as_ref().unwrap().changes_stored() }
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

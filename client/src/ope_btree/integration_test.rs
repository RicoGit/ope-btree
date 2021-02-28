//! Integration test for OpeBTreeClient and OpeBTree server.

use super::*;
use bytes::{BufMut, Bytes, BytesMut};
use common::gen::NumGen;
use common::noop_hasher::NoOpHasher;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use log::LevelFilter;
use protocol::{BtreeCallback, ClientPutDetails, PutCallback, RpcFuture};
use server::ope_btree::command::Cmd;
use server::ope_btree::internal::node_store::BinaryNodeStore;
use server::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen, ValueRef};
use std::sync::Mutex;

#[derive(Clone, Debug)]
struct NoOpKeyCrypt {}

impl Encryptor for NoOpKeyCrypt {
    type PlainData = String;

    fn encrypt(&self, data: Self::PlainData) -> crate::crypto::Result<Bytes> {
        Ok(Bytes::from(data))
    }
}

impl Decryptor for NoOpKeyCrypt {
    type PlainData = String;

    fn decrypt(&self, encrypted_data: &[u8]) -> crate::crypto::Result<Self::PlainData> {
        Ok(String::from_utf8_lossy(encrypted_data).into())
    }
}

#[tokio::test]
async fn get_from_empty_tree_test() {
    // get from empty tree

    let tree = create_server().await;
    let client = create_client();

    let search_state = client.init_get("k1".to_string()).await;
    let cmd = Cmd::new(search_state);

    let result = tree.get(cmd).await;
    assert_eq!(result.unwrap(), None)
}

#[tokio::test]
async fn put_one_test() {
    // put one value
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    let search_state = client.init_get("k1".to_string()).await;
    let cmd = Cmd::new(search_state);
    assert_eq!(tree.get(cmd).await.unwrap(), None);

    let put_state = client.init_put(k("k1"), h("v1"), 0).await;
    let cmd = Cmd::new(PutStateWrapper::new(put_state));
    assert_eq!(tree.put(cmd).await.unwrap(), (vr(101), Bytes::new()))
}

#[tokio::test]
async fn put_one_and_get_it_back_test() {
    // put one value and get it back
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    let search_state = client.init_get("k1".to_string()).await;
    let cmd = Cmd::new(search_state);
    assert_eq!(tree.get(cmd).await.unwrap(), None);

    let put_state = client.init_put(k("k1"), h("v1"), 0).await;
    let cmd = Cmd::new(PutStateWrapper::new(put_state));
    assert_eq!(tree.put(cmd).await.unwrap(), (vr(101), Bytes::new()));

    dbg!(&client.state.read().await.m_root);

    let search_state = client.init_get("k1".to_string()).await;
    let cmd = Cmd::new(search_state);
    assert_eq!(tree.get(cmd).await.unwrap(), Some(vr(101)));
}

#[tokio::test]
async fn fill_root_node_test() {
    // put 4 and get them back (tree depth 1)

    init_logger();
    let mut tree = create_server().await;
    let client = create_client();

    // fill root node with 4 keys
    put(4, &mut tree, &client).await;

    // get all keys back
    get(4, &mut tree, &client).await;
}

#[tokio::test]
async fn rebalancing_root_node_test() {
    // put 10 and get them back (tree depth 2)
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    // fill root node with 4 keys
    put(10, &mut tree, &client).await;

    // get all keys back
    get(10, &mut tree, &client).await;
}

async fn put(
    n: usize,
    tree: &mut OpeBTree<HashMapKVStore<Vec<u8>, Vec<u8>>, NoOpHasher>,
    client: &OpeBTreeClient<NoOpKeyCrypt, NoOpHasher>,
) {
    for idx in 1..n + 1 {
        let put_state = client.init_put(key(idx), hash(idx), idx).await;
        let cmd = Cmd::new(PutStateWrapper::new(put_state));
        assert_eq!(tree.put(cmd).await.unwrap(), (vr(100 + idx), Bytes::new()));
    }
}

async fn get(
    n: usize,
    tree: &mut OpeBTree<HashMapKVStore<Vec<u8>, Vec<u8>>, NoOpHasher>,
    client: &OpeBTreeClient<NoOpKeyCrypt, NoOpHasher>,
) {
    for idx in 1..n {
        let get_state = client.init_get(key(idx)).await;
        let cmd = Cmd::new(get_state);
        assert_eq!(tree.get(cmd).await.unwrap(), Some(vr(100 + idx)));
    }
}

fn create_client() -> OpeBTreeClient<NoOpKeyCrypt, NoOpHasher> {
    let crypt = NoOpKeyCrypt {};
    let client: OpeBTreeClient<NoOpKeyCrypt, NoOpHasher> =
        OpeBTreeClient::<NoOpKeyCrypt, NoOpHasher>::new(Hash::empty(), crypt, ());
    client
}

async fn create_server() -> OpeBTree<HashMapKVStore<Vec<u8>, Vec<u8>>, NoOpHasher> {
    let conf = OpeBTreeConf {
        arity: 4,
        alpha: 0.25,
    };
    let kv_store = HashMapKVStore::new();
    let node_store = BinaryNodeStore::new(kv_store, NumGen(0));
    let mut tree = OpeBTree::new(conf, node_store, ValRefGen(100));
    tree.init().await.unwrap();
    tree
}

fn h(str: &str) -> Hash {
    Hash::build::<NoOpHasher, _>(str.as_bytes())
}

fn hash(idx: usize) -> Hash {
    Hash::build::<NoOpHasher, _>(format!("h{}", idx).as_bytes())
}

fn k(str: &str) -> String {
    str.to_string()
}

fn key(idx: usize) -> String {
    format!("k{}", idx)
}

fn vr(idx: usize) -> ValueRef {
    let mut buf = BytesMut::new();
    buf.put_u64(idx as u64);
    ValueRef(buf.freeze())
}

fn init_logger() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(LevelFilter::Info)
        .try_init();
}

/// Wraps PutState and used for testing, contains simple blocking Mutex for protect PutState.
/// Allows use non-clonable PutState directly in Btree that requires clone.
#[derive(Clone, Debug)]
struct PutStateWrapper<'a> {
    state: Arc<Mutex<PutState<'a, String, NoOpHasher, NoOpKeyCrypt>>>,
}

impl<'a> PutStateWrapper<'a> {
    fn new(put_state: PutState<'a, String, NoOpHasher, NoOpKeyCrypt>) -> Self {
        PutStateWrapper {
            state: Arc::new(Mutex::new(put_state)),
        }
    }
}

impl<'a> BtreeCallback for PutStateWrapper<'a> {
    fn next_child_idx<'f>(
        &mut self,
        keys: Vec<Bytes>,
        children_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize> {
        self.state
            .lock()
            .unwrap()
            .next_child_idx(keys, children_hashes)
    }
}

impl<'a> PutCallback for PutStateWrapper<'a> {
    fn put_details<'f>(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, ClientPutDetails> {
        self.state.lock().unwrap().put_details(keys, values_hashes)
    }

    fn verify_changes<'f>(
        &mut self,
        server_merkle_root: Bytes,
        was_splitting: bool,
    ) -> RpcFuture<'f, Bytes> {
        self.state
            .lock()
            .unwrap()
            .verify_changes(server_merkle_root, was_splitting)
    }

    fn changes_stored<'f>(&self) -> RpcFuture<'f, ()> {
        self.state.lock().unwrap().changes_stored()
    }
}

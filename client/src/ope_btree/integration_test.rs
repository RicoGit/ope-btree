//! Integration test for OpeBTreeClient and OpeBTree server.

use super::*;
use bytes::{BufMut, Bytes, BytesMut};
use common::gen::NumGen;
use common::noop_hasher::NoOpHasher;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use log::LevelFilter;
use server::ope_btree::command::Cmd;
use server::ope_btree::internal::node_store::BinaryNodeStore;
use server::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen, ValueRef};

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
    // put one value and get it back
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(LevelFilter::Debug)
        .try_init();

    let mut tree = create_server().await;
    let client = create_client();

    let search_state = client.init_get("k1".to_string()).await;
    let cmd = Cmd::new(search_state);
    assert_eq!(tree.get(cmd).await.unwrap(), None);

    let put_state = client.init_put(k("k1"), h("v1"), 0).await;
    let cmd = Cmd::new(put_state);
    assert_eq!(tree.put(cmd).await.unwrap(), (vr(101), Bytes::new()))
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

fn k(str: &str) -> String {
    str.to_string()
}

fn vr(idx: usize) -> ValueRef {
    let mut buf = BytesMut::new();
    buf.put_u64(idx as u64);
    ValueRef(buf.freeze())
}

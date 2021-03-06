//! Integration test for OpeBTree and OpeBTreeClient.
//! We test only index here!

use super::*;
use crate::ope_btree::command::Cmd;
use crate::ope_btree::internal::node_store::BinaryNodeStore;
use crate::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen, ValueRef};
use bytes::{BufMut, Bytes, BytesMut};

use client::ope_btree::test::*;
use client::ope_btree::*;
use common::gen::NumGen;
use common::noop_hasher::NoOpHasher;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use log::LevelFilter;

type BinStore = HashMapKVStore<Vec<u8>, Vec<u8>>;

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

    let search_state = client.init_get("k1".to_string()).await;
    let cmd = Cmd::new(search_state);
    assert_eq!(tree.get(cmd).await.unwrap(), Some(vr(101)));
}

#[tokio::test]
async fn one_depth_tree_test() {
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
async fn two_depth_tree_test() {
    // put 10 and get them back (tree depth 2)
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    put(10, &mut tree, &client).await;
    get(10, &mut tree, &client).await;
}

#[tokio::test]
async fn three_depth_tree_test() {
    // put 20 and get them back (tree depth 3)
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    put(20, &mut tree, &client).await;
    get(20, &mut tree, &client).await;
}

#[tokio::test]
async fn five_depth_tree_test() {
    // put 200 and get them back (tree depth 5)
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    put(200, &mut tree, &client).await;
    get(200, &mut tree, &client).await;
}

#[tokio::test]
async fn five_depth_tree_reverse_test() {
    // put 200 in reverse order and get them back (tree depth 5)
    init_logger();

    let mut tree = create_server().await;
    let client = create_client();

    reverse_put(200, &mut tree, &client).await;
    reverse_get(200, &mut tree, &client).await;
}

async fn put(
    n: usize,
    tree: &mut OpeBTree<BinStore, NoOpHasher>,
    client: &OpeBTreeClient<NoOpCrypt, NoOpHasher>,
) {
    for idx in 1..n + 1 {
        let put_state = client.init_put(key(idx), hash(idx), idx).await;
        let cmd = Cmd::new(PutStateWrapper::new(put_state));
        assert_eq!(tree.put(cmd).await.unwrap(), (vr(100 + idx), Bytes::new()));
    }
}

async fn reverse_put(
    n: usize,
    tree: &mut OpeBTree<BinStore, NoOpHasher>,
    client: &OpeBTreeClient<NoOpCrypt, NoOpHasher>,
) {
    for idx in (1..n + 1).rev() {
        let put_state = client.init_put(key(idx), hash(idx), idx).await;
        let cmd = Cmd::new(PutStateWrapper::new(put_state));
        assert_eq!(
            tree.put(cmd).await.unwrap(),
            (vr(100 + (n - idx + 1)), Bytes::new())
        );
    }
}

async fn get(
    n: usize,
    tree: &mut OpeBTree<BinStore, NoOpHasher>,
    client: &OpeBTreeClient<NoOpCrypt, NoOpHasher>,
) {
    for idx in 1..n {
        let get_state = client.init_get(key(idx)).await;
        let cmd = Cmd::new(get_state);
        assert_eq!(tree.get(cmd).await.unwrap(), Some(vr(100 + idx)));
    }
}

async fn reverse_get(
    n: usize,
    tree: &mut OpeBTree<BinStore, NoOpHasher>,
    client: &OpeBTreeClient<NoOpCrypt, NoOpHasher>,
) {
    for idx in (1..n + 1).rev() {
        let get_state = client.init_get(key(idx)).await;
        let cmd = Cmd::new(get_state);
        assert_eq!(tree.get(cmd).await.unwrap(), Some(vr(100 + (n - idx + 1))));
    }
}

fn create_client() -> OpeBTreeClient<NoOpCrypt, NoOpHasher> {
    let crypt = NoOpCrypt {};
    let client: OpeBTreeClient<NoOpCrypt, NoOpHasher> =
        OpeBTreeClient::<NoOpCrypt, NoOpHasher>::new(Hash::empty(), crypt, ());
    client
}

async fn create_server() -> OpeBTree<BinStore, NoOpHasher> {
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

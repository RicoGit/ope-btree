//! BTree persistence store.
//! todo add docs later

use common::gen::Generator;
use kvstore_api::kvstore::*;
use kvstore_binary::BinKVStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeStoreError {
    #[error("KVStore Error")]
    KVStoreErr {
        #[from]
        source: kvstore_api::kvstore::KVStoreError,
    },
}

type Result<V> = std::result::Result<V, NodeStoreError>;

#[derive(Debug)]
pub struct BinaryNodeStore<Id, Node, Store, IdGen>
where
    Id: Send,
    Node: Send,
    Store: KVStore<Vec<u8>, Vec<u8>>,
    IdGen: Generator<Item = Id>,
{
    store: BinKVStore<Id, Node, Store>,
    id_generator: IdGen,
}

impl<Id, Node, Store, IdGen> BinaryNodeStore<Id, Node, Store, IdGen>
where
    Id: Serialize + DeserializeOwned + Send,
    Node: Serialize + DeserializeOwned + Send,
    Store: KVStore<Vec<u8>, Vec<u8>>,
    IdGen: Generator<Item = Id>,
{
    pub fn new(store: Store, id_generator: IdGen) -> Self {
        BinaryNodeStore {
            store: BinKVStore::new(store),
            id_generator,
        }
    }

    pub fn next_id(&mut self) -> Id {
        self.id_generator.next()
    }

    pub async fn get(&self, node_id: Id) -> Result<Option<Node>> {
        let node = self.store.get(node_id).await?;
        Ok(node)
    }

    pub async fn set(&mut self, node_id: Id, node: Node) -> Result<()> {
        self.store.set(node_id, node).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ope_btree::internal::node::tests as node_test;
    use crate::ope_btree::internal::node::Node;
    use common::gen::NumGen;
    use kvstore_inmemory::hashmap_store::HashMapKVStore;
    use std::sync::Arc;

    type BinStore = HashMapKVStore<Vec<u8>, Vec<u8>>;

    #[test]
    fn sync_dyn_test() {
        let _store: Arc<dyn Send + Sync> = Arc::new(create(0));
    }

    #[tokio::test]
    async fn get_from_empty_store() {
        let store = create(0);

        let result = store.get(0);
        assert_eq!(None, result.await.unwrap());
    }

    #[tokio::test]
    async fn put_into_empty_store() {
        let mut store = create(0);

        let id = store.next_id();
        let leaf: Node = Node::Leaf(node_test::create_leaf());
        assert_eq!((), store.set(id, leaf.clone()).await.unwrap());
        assert_eq!(Some(leaf), store.get(id).await.unwrap());
        assert_eq!(store.next_id(), 2);
    }

    #[tokio::test]
    async fn functional_test() {
        let mut store = create(100);

        let id1 = store.next_id();
        let node1 = Node::Leaf(node_test::create_leaf());
        assert_eq!(None, store.get(id1).await.unwrap());
        assert_eq!((), store.set(id1, node1.clone()).await.unwrap());
        assert_eq!(Some(node1.clone()), store.get(id1).await.unwrap());

        assert_eq!((), store.set(id1, node1.clone()).await.unwrap());
        assert_eq!(Some(node1.clone()), store.get(id1).await.unwrap());

        let id2 = store.next_id();
        let node2 = Node::Branch(node_test::create_branch());
        assert_eq!((), store.set(id1, node2.clone()).await.unwrap());
        assert_eq!(Some(node2.clone()), store.get(id1).await.unwrap());

        let id3 = store.next_id();
        let node3 = Node::Leaf(node_test::create_leaf());
        assert_eq!((), store.set(id3, node3.clone()).await.unwrap());

        let id4 = store.next_id();
        let node4 = Node::Branch(node_test::create_branch());
        assert_eq!((), store.set(id4, node4.clone()).await.unwrap());

        let id5 = store.next_id();
        let node5 = Node::Leaf(node_test::create_leaf());
        assert_eq!((), store.set(id5, node5.clone()).await.unwrap());
        assert_eq!(Some(node4.clone()), store.get(id4).await.unwrap());

        assert_eq!(vec![id1, id2, id3, id4, id5], vec![101, 102, 103, 104, 105])
    }

    // todo add negative cases

    fn create(idx: usize) -> BinaryNodeStore<usize, Node, BinStore, NumGen> {
        let store = HashMapKVStore::new();
        BinaryNodeStore::new(store, NumGen(idx))
    }
}

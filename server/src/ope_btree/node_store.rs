//! BTree persistence store.
//! todo add docs later

use futures::{future, StreamExt, TryFutureExt};
use rmp_serde::decode;
use rmp_serde::encode;
use rmps::{Deserializer, Serializer};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Add;

use futures::future::BoxFuture;
use kvstore_api::kvstore::*;
use kvstore_api::*;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeStoreError {
    #[error("Store Error: {msg:?}")]
    StoreErr {
        // todo rename?
        msg: String,
    },
    #[error("Serialization Error")]
    SerializeErr {
        #[from]
        source: rmp_serde::encode::Error,
    },
    #[error("Deserialization Error")]
    DeserializeErr {
        #[from]
        source: rmp_serde::decode::Error,
    },
    #[error("KVStore Error")]
    KVStoreErr {
        #[from]
        source: kvstore_api::kvstore::KVStoreError,
    },
}

type Result<V> = std::result::Result<V, NodeStoreError>;

type Task<'s, V> = BoxFuture<'s, Result<V>>;

/// BTree persistence store API.
pub trait NodeStore<Id, Node>: Send + Sync
// todo remove trait form NodeStore?
where
    Id: Send,
    Node: Serialize + Send,
{
    /// Returns next surrogate Id for storing new node.
    fn next_id(&mut self) -> Id; // todo maybe wrap to Future or/and Result?

    /// Gets stored node for specified id.
    fn get<'a: 's, 's>(&'s self, node_id: &Id) -> Task<'s, Option<Node>>;

    /// Stores the specified node with the specified id.
    /// Rewrite existing value if it's present.
    fn put<'a: 's, 's>(&'s mut self, node_id: Id, node: Node) -> Task<'a, ()>;

    // todo remove
}

pub struct BinaryNodeStore<Id, Node, Store>
where
    Id: Send,
    Node: Send,
    Store: KVStore<Vec<u8>, Vec<u8>>, // todo Bytes?
{
    store: Store,
    // todo Result? Is Error is possible here ? todo static dispatch?
    id_generator: Box<dyn FnMut() -> Id + Send + Sync>,
    _phantom: PhantomData<Node>,
}

impl<Id, Node, Store> BinaryNodeStore<Id, Node, Store>
where
    Id: Send,
    Node: Send,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    pub fn new(store: Store, id_generator: Box<dyn FnMut() -> Id + Send + Sync>) -> Self {
        BinaryNodeStore {
            store,
            id_generator,
            _phantom: PhantomData::<Node>,
        }
    }
}

/// Stores tree nodes in the binary key-value store.
impl<Id, Node, Store> NodeStore<Id, Node> for BinaryNodeStore<Id, Node, Store>
where
    Id: Serialize + DeserializeOwned + Send + Sync,
    Node: Serialize + DeserializeOwned + Send + Sync + 'static,
    Store: KVStore<Vec<u8>, Vec<u8>> + Send + Sync,
{
    fn next_id(&mut self) -> Id {
        (self.id_generator)()
    }

    // todo problem with lifetimes right here, need to switch to async\await

    fn get<'a: 's, 's>(&'s self, node_id: &Id) -> Task<'s, Option<Node>> {
        let node_id = to_byte(&node_id).unwrap();
        let bytes: StoreTask<Option<Vec<u8>>> = self.store.get(node_id);

        let node = bytes.map_err(Into::into).and_then(|node_bytes| {
            let res = node_bytes.map(|raw| from_byte::<Node>(&raw)).transpose();
            async { res }
        });

        Box::pin(node)
    }

    fn put<'a: 's, 's>(&'s mut self, node_id: Id, node: Node) -> Task<'a, ()> {
        // todo do lazy (wrap all in future) Arc  might help!
        let result = to_byte(&node_id)
            .and_then(|id| to_byte(node).map(|n| (id, n)))
            .map(|(id, node)| {
                self.store
                    .set(id, node)
                    .map_err::<NodeStoreError, _>(Into::into)
            });

        match result {
            Err(err) => Box::pin(future::err(err)),
            Ok(_) => Box::pin(future::ok(())),
        }
    }
}

fn to_byte<T: Serialize>(obj: T) -> Result<Vec<u8>> {
    encode::to_vec(&obj).map_err(Into::into)
}

fn from_byte<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    decode::from_slice::<T>(&(bytes)).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ope_btree::node::tests as node_test;
    use crate::ope_btree::node::Node;
    use std::sync::Arc;

    #[test]
    fn sync_dyn_test() {
        let _store: Arc<dyn Send + Sync> = Arc::new(create(0));
    }

    #[tokio::test]
    async fn get_from_empty_store() {
        let store = create(0);

        let result = store.get(&0);
        assert_eq!(None, result.await.unwrap());
    }

    #[tokio::test]
    async fn put_into_empty_store() {
        let mut store = create(0);

        let id = store.next_id();
        let leaf: Node = Node::Leaf(node_test::create_leaf());
        assert_eq!((), store.put(id, leaf.clone()).await.unwrap());
        assert_eq!(Some(leaf), store.get(&id).await.unwrap());
    }

    #[tokio::test]
    async fn functional_test() {
        let mut store = create(100);

        let id1 = store.next_id();
        let node1 = Node::Leaf(node_test::create_leaf());
        assert_eq!(None, store.get(&id1).await.unwrap());
        assert_eq!((), store.put(id1, node1.clone()).await.unwrap());
        assert_eq!(Some(node1.clone()), store.get(&id1).await.unwrap());
        assert_eq!((), store.put(id1, node1.clone()).await.unwrap());
        assert_eq!(Some(node1.clone()), store.get(&id1).await.unwrap());
        let id2 = store.next_id();
        let node2 = Node::Branch(node_test::create_branch());
        assert_eq!((), store.put(id1, node2.clone()).await.unwrap());
        assert_eq!(Some(node2.clone()), store.get(&id1).await.unwrap());
        let id3 = store.next_id();
        let node3 = Node::Leaf(node_test::create_leaf());
        assert_eq!((), store.put(id3, node3.clone()).await.unwrap());
        let id4 = store.next_id();
        let node4 = Node::Branch(node_test::create_branch());
        assert_eq!((), store.put(id4, node4.clone()).await.unwrap());
        let id5 = store.next_id();
        let node5 = Node::Leaf(node_test::create_leaf());
        assert_eq!((), store.put(id5, node5.clone()).await.unwrap());
        assert_eq!(Some(node4.clone()), store.get(&id4).await.unwrap());
        assert_eq!(vec![id1, id2, id3, id4, id5], vec![101, 102, 103, 104, 105])
    }

    // todo add negative cases

    fn create(mut idx: u64) -> impl NodeStore<u64, Node> {
        let store = HashMapKVStore::new();
        BinaryNodeStore::new(
            store,
            Box::new(move || {
                idx += 1;
                idx
            }),
        )
    }
}

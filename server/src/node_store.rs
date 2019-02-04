//! BTree persistence store.
//! todo add docs later

use async_kvstore::hashmap_store::HashMapStore;
use async_kvstore::KVStore;
use errors::*;
use futures::future;
use futures::future::FutureResult;
use futures::Future;
use rmp_serde::decode;
use rmp_serde::encode;
use rmps::{Deserializer, Serializer};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Add;

mod errors {
    use futures::Future;
    use std::error;
    error_chain! {

        errors {
            StoreError(msg:  String) {
                display("Node Store Error: {:?}", msg)
            }
        }

        foreign_links {
            SerializationError(::rmp_serde::encode::Error);
            DeserializationxError(::rmp_serde::decode::Error);
        }

        links {
            KVStoreError(async_kvstore::errors::Error, async_kvstore::errors::ErrorKind);
        }

    }
}

type SFuture<'s, V> = Box<dyn Future<Item = V, Error = errors::Error> + Send + 's>;

/// BTree persistence store API.
pub trait NodeStore<Id, Node>
where
    Node: Serialize + Send,
{
    /// Returns next surrogate Id for storing new node.
    fn next_id(&mut self) -> Id; // todo maybe wrap to Future or/and Result?

    /// Gets stored node for specified id.
    fn get(&self, node_id: &Id) -> SFuture<Option<Node>>;

    /// Stores the specified node with the specified id.
    /// Rewrite existing value if it's present.
    fn put(&mut self, node_id: Id, node: Node) -> SFuture<()>;
}

struct BinaryNodeStore<Id, Node> {
    pub store: Box<KVStore<Vec<u8>, Vec<u8>>>,
    id_generator: Box<FnMut() -> Id>, // todo Result? Is Error is possible here ?
    _phantom: PhantomData<Node>,      // needed for keeping Node type
}

impl<Id, Node: Send> BinaryNodeStore<Id, Node> {
    fn new(store: Box<KVStore<Vec<u8>, Vec<u8>>>, id_generator: Box<FnMut() -> Id>) -> Self {
        BinaryNodeStore {
            store,
            id_generator,
            _phantom: PhantomData::<Node>,
        }
    }
}

/// Stores tree nodes in the binary key-value store.
impl<Id, Node: Send> NodeStore<Id, Node> for BinaryNodeStore<Id, Node>
where
    Id: Serialize + DeserializeOwned,
    Node: Serialize + DeserializeOwned,
{
    fn next_id(&mut self) -> Id {
        (self.id_generator)()
    }

    fn get(&self, node_id: &Id) -> SFuture<Option<Node>> {
        // todo consider chaining error with better error explanation

        match to_byte(&node_id) {
            Err(err) => Box::new(future::failed(err)),
            Ok(id) => {
                let res = self
                    .store
                    .get(&id)
                    .map_err(Into::into)
                    .and_then(|node_bytes_opt| match node_bytes_opt {
                        None => future::ok(None),
                        Some(node_raw) => future::result(from_byte(node_raw).map(Some)),
                    });

                Box::new(res)
            }
        }
    }

    fn put(&mut self, node_id: Id, node: Node) -> SFuture<()> {
        // todo do lazy (wrap all in future) Arc  might help!
        let result = to_byte(&node_id)
            .and_then(|id| to_byte(node).map(|n| (id, n)))
            .map(|(id, node)| self.store.put(id, node).map_err(Into::into));

        match result {
            Err(err) => Box::new(future::failed(err)),
            Ok(res) => Box::new(res),
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
    use crate::btree::Node;
    use async_kvstore::hashmap_store::HashMapStore;
    use futures::prelude::*;
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    use crate::node_store::BinaryNodeStore;
    use crate::node_store::NodeStore;
    use rmp_serde::decode::ReadReader;

    fn create(mut idx: u64) -> BinaryNodeStore<u64, Node> {
        let store = Box::new(HashMapStore::new());
        BinaryNodeStore::new(
            store,
            Box::new(move || {
                idx += 1;
                idx
            }),
        )
    }

    #[test]
    fn get_from_empty_store() {
        let store = create(0);

        let result = store.get(&0);
        assert_eq!(None, result.wait().unwrap());
    }

    #[test]
    fn put_into_empty_store() {
        let mut store = create(0);

        let id = store.next_id();
        let node = Node::new(10);
        assert_eq!((), store.put(id, node.clone()).wait().unwrap());
        assert_eq!(Some(node), store.get(&id).wait().unwrap());
    }

    #[test]
    fn functional_test() {
        let mut store = create(100);

        let id1 = store.next_id();
        let node1 = Node::new(10);
        assert_eq!(None, store.get(&id1).wait().unwrap());
        assert_eq!((), store.put(id1, node1.clone()).wait().unwrap());
        assert_eq!(Some(node1.clone()), store.get(&id1).wait().unwrap());
        assert_eq!((), store.put(id1, node1.clone()).wait().unwrap());
        assert_eq!(Some(node1.clone()), store.get(&id1).wait().unwrap());
        let id2 = store.next_id();
        let node2 = Node::new(20);
        assert_eq!((), store.put(id1, node2.clone()).wait().unwrap());
        assert_eq!(Some(node2.clone()), store.get(&id1).wait().unwrap());
        let id3 = store.next_id();
        let node3 = Node::new(30);
        assert_eq!((), store.put(id3, node3.clone()).wait().unwrap());
        let id4 = store.next_id();
        let node4 = Node::new(40);
        assert_eq!((), store.put(id4, node4.clone()).wait().unwrap());
        let id5 = store.next_id();
        let node5 = Node::new(50);
        assert_eq!((), store.put(id5, node5.clone()).wait().unwrap());
        assert_eq!(Some(node4.clone()), store.get(&id4).wait().unwrap());
        assert_eq!(vec![id1, id2, id3, id4, id5], vec![101, 102, 103, 104, 105])
    }

    // todo add negative cases
}
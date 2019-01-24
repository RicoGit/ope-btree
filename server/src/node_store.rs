//! BTree persistence store.

use async_kvstore::hashmap_store::HashMapStore;
use async_kvstore::KVStore;
use futures::Future;
use serde::Deserialize;
use serde::Serialize;
use std::ops::Add;

type StoreFuture<V> = Box<Future<Item = V, Error = NodeStoreError>>;

pub trait NodeStore<Id, Node>
where
    Node: Serialize,
{
    /// Returns next surrogate Id for storing new node.
    fn next_id(&mut self) -> Id; // todo maybe wrap to Future or/and Result?

    /// Gets stored node for specified id.
    fn get(&self, node_id: Id) -> StoreFuture<Node>;

    /// Stores the specified node with the specified id.
    /// Rewrite existing value if it's present.
    fn put(&mut self, node_id: Id, node: Node) -> StoreFuture<()>;
}

pub enum NodeStoreError {
    SerializationError(),
    StoreError(async_kvstore::StoreError),
}

struct BinaryNodeStore<Id> {
    store: Box<KVStore<[u8], [u8]>>, // todo [u8] => Vec<u8> ?
    id_generator: fn() -> Id,        // todo Result? Is Error is possible here ?
}

impl<Id, Node> NodeStore<Id, Node> for BinaryNodeStore<Id>
where
    Node: Serialize,
{
    fn next_id(&mut self) -> Id {
        (self.id_generator)()
    }

    fn get(&self, node_id: Id) -> Box<Future<Item = Node, Error = NodeStoreError>> {
        unimplemented!()
    }

    fn put(&mut self, node_id: Id, node: Node) -> Box<Future<Item = (), Error = NodeStoreError>> {
        unimplemented!()
    }
}

impl<Id> BinaryNodeStore<Id> {
    fn new(store: Box<KVStore<[u8], [u8]>>, id_generator: fn() -> Id) -> Self {
        BinaryNodeStore {
            store,
            id_generator,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::btree::Node;
    use rmps::{Deserializer, Serializer};
    use serde::Deserialize;
    use serde::Serialize;

    #[test]
    fn test_codec() {
        let node = Node { size: 13 };

        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(node, Deserialize::deserialize(&mut de).unwrap());
    }

}

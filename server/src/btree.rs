//! BTree implementation.

use crate::node_store::NodeStore;
use async_kvstore::KVStore;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;

//
// BTree
//

struct OpeBTree<K, V, NS>
where
    NS: NodeStore<usize, V>,
    K: Serialize,
    V: Serialize,
{
    // todo remove
    ph1: PhantomData<K>,
    // todo remove
    ph2: PhantomData<V>,
    node_store: NS,
}

impl<K, V, NS> OpeBTree<K, V, NS>
where
    NS: NodeStore<usize, V>,
    K: Serialize,
    V: Serialize,
{
    fn new() -> Self {
        unimplemented!()
    }
}

//
// Node
//

#[derive(Debug, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub size: usize,
    // todo fill and remove pub
}

#[cfg(test)]
mod tests {

    use crate::btree::Node;
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    #[test]
    fn node_serde_test() {
        let node = Node { size: 13 };

        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(node, Deserialize::deserialize(&mut de).unwrap());
    }

}

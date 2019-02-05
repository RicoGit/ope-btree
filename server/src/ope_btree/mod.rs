//! BTree implementation.
//! todo more documentation when btree will be ready

// module with persistent level for any tree
pub mod node_store;

use self::node_store::BinaryNodeStore;
use self::node_store::NodeStore;
use async_kvstore::KVStore;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

//
// BTree
//

/// Configuration for OpeBtree.
pub struct OpeBTreeConf {
    /// Maximum size of nodes (maximum number children in branches)
    arity: u8,
    /// Minimum capacity factor of node. Should be between 0 and 0.5. 0.25 means that
    /// each node except root should always contains between 25% and 100% children.
    alpha: f32,
}

/// This class implements a search tree, which allows to run queries over encrypted
/// data. This code based on research paper: '''Popa R.A.,Li F.H., Zeldovich N.
/// 'An ideal-security protocol for order-preserving encoding.' 2013 '''
///
/// In its essence this tree is a hybrid of [B+Tree](https://en.wikipedia.org/wiki/B%2B_tree)
/// and [MerkleTree](https://en.wikipedia.org/wiki/Merkle_tree) data structures.
///
/// This ''B+tree'' is an N-ary tree with a number of children per node ranging
/// between ''MinDegree'' and ''MaxDegree''. A tree consists of a root, internal
/// branch nodes and leaves. The root may be either a leaf or a node with two or
///  more children. Copies of the some keys are stored in the internal nodes
/// (for efficient searching); keys and records are stored in leaves. Tree is
/// kept balanced by requiring that all leaf nodes are at the same depth.
///  This depth will increase slowly as elements are added to the tree. Depth
/// increases only when the root is being splitted.
///
///  Note that the tree provides only algorithms (i.e., functions) to search,
/// insert and delete elements. Tree nodes are actually stored externally using
/// the [`NodeStore`] to make the tree maximally pluggable and seamlessly switch
/// between in memory, on disk, or maybe, over network storages. Key comparison
/// operations in the tree are also pluggable and are provided by the [`BTreeCommand`]
/// implementations, which helps to impose an order over for example encrypted nodes data.
///
/// [`NodeStore`]: ../node_store/trait.NodeStore.html
/// [`BTreeCommand`]: ../command/trait.BTreeCommand.html
pub struct OpeBTree {
    node_store: Box<dyn NodeStore<usize, Node>>,
    config: OpeBTreeConf,
}

impl OpeBTree {
    fn new(config: OpeBTreeConf, node_store: Box<dyn NodeStore<usize, Node>>) -> Self {
        OpeBTree { config, node_store }
    }
}

//
// Node
//

#[derive(Debug, PartialOrd, PartialEq, Serialize, Deserialize, Clone)]
pub struct Node {
    pub size: usize,
    // todo fill and remove pub
}

impl Node {
    pub fn new(size: usize) -> Self {
        Node { size }
    }
}

#[cfg(test)]
mod tests {
    use crate::ope_btree::Node;
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

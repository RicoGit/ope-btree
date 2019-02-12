//! BTree implementation.
//! todo more documentation when btree will be ready

pub mod commands;
pub mod node;
pub mod node_store;

use self::node::Node;
use self::node::TreeNode;
use self::node_store::BinaryNodeStore;
use self::node_store::NodeStore;
use crate::ope_btree::commands::BTreeCmd;
use crate::ope_btree::commands::SearchCmd;
use async_kvstore::KVStore;
use bytes::Bytes;
use common::Key;
use futures::Future;
use futures_locks::RwLock;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Mutex;

//
// BTree
//

type GetFuture<'a> = Box<dyn Future<Item = ValueRef, Error = errors::Error> + Send + 'a>;

/// Configuration for OpeBtree.
pub struct OpeBTreeConf {
    /// Maximum size of nodes (maximum number children in branches)
    pub arity: u8,
    /// Minimum capacity factor of node. Should be between 0 and 0.5. 0.25 means that
    /// each node except root should always contains between 25% and 100% children.
    pub alpha: f32,
}

pub mod errors {
    error_chain! {}
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
pub struct OpeBTree<NS>
where
    NS: NodeStore<usize, Node>,
{
    node_store: RwLock<NS>,
    config: OpeBTreeConf,
    // todo should contain valRefProvider: () ⇒ ValueRef
}

impl<NS> OpeBTree<NS>
where
    NS: NodeStore<usize, Node>,
{
    pub fn new(config: OpeBTreeConf, node_store: NS) -> Self {
        OpeBTree {
            config,
            node_store: RwLock::new(node_store),
        }
    }

    pub fn get<'a, SCmd>(&self, cmd: SCmd) -> GetFuture<'a>
    where
        SCmd: SearchCmd + BTreeCmd,
    {
        unimplemented!()
    }

    // todo put, remove, traverse
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct ValueRef(pub Bytes);

impl From<ValueRef> for Bytes {
    fn from(val: ValueRef) -> Self {
        val.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Hash;
    use async_kvstore::hashmap_store::HashMapStore;

    fn create_node_store(mut idx: usize) -> impl NodeStore<usize, Node> {
        BinaryNodeStore::new(
            HashMapStore::new(),
            Box::new(move || {
                idx += 1;
                idx
            }),
        )
    }

    fn create_tree<NS: NodeStore<usize, Node>>(store: NS) -> OpeBTree<NS> {
        OpeBTree::new(
            OpeBTreeConf {
                arity: 8,
                alpha: 0.25_f32,
            },
            store,
        )
    }

    #[test]
    fn new_tree_test() {
        let node_store = create_node_store(0);
        let _tree = create_tree(node_store);
        // todo
        // tree.get()
    }
}

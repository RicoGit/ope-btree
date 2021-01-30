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
use async_kvstore::boxed::KVStore;
use bytes::Bytes;
use common::Key;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Mutex;
use thiserror::Error;
use tokio::sync::RwLock;

type Result<V> = std::result::Result<V, BTreeErr>;

type Task<'a, V> = BoxFuture<'a, Result<V>>;

/// BTree errors
#[derive(Error, Debug)]
pub enum BTreeErr {
    #[error("Node Store Error")]
    NodeStoreError {
        #[from]
        source: node_store::NodeStoreError,
    },

    #[error("Unexpected Error: {msg:?}")]
    Other { msg: String },
}

/// Tree root id is constant, it always points to root node in node_store.
static ROOT_ID: usize = 0;

/// Configuration for OpeBtree.
pub struct OpeBTreeConf {
    /// Maximum size of nodes (maximum number children in branches)
    pub arity: u8,
    /// Minimum capacity factor of node. Should be between 0 and 0.5. 0.25 means that
    /// each node except root should always contains between 25% and 100% children.
    pub alpha: f32,
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
    NS: NodeStore<usize, Node> + 'static,
{
    node_store: RwLock<NS>,
    config: OpeBTreeConf,
    // todo should contain valRefProvider: () ⇒ ValueRef
}

impl<NS> OpeBTree<NS>
where
    NS: NodeStore<usize, Node> + 'static,
{
    pub fn new(config: OpeBTreeConf, node_store: NS) -> Self {
        OpeBTree {
            config,
            node_store: RwLock::new(node_store),
        }
    }

    pub fn get<'a, SCmd>(&self, cmd: SCmd) -> Task<'a, ValueRef>
    where
        SCmd: SearchCmd + BTreeCmd,
    {
        // get root
        // cmd.sent_leaf
        //

        unimplemented!()
    }

    fn get_root(&self) -> Task<Node> {
        //        let root = self.node_store.read()
        //            .map_err(|_| "Can't get read access to node_store".into())
        //            .and_then(|store| {
        //                store.get(&ROOT_ID)
        //                    .from_err::<errors::Error>()
        //            }).wait();
        //
        //        match root {
        //            Ok(Some(node)) => node,
        //            Ok(None) => {
        //
        //                // create root and safe it to node_store
        //            }
        //
        //        }
        //        // todo root should be somewhere created either when Tree created or here
        //        Box::new(root)

        unimplemented!()
    }

    // todo refactor and continue
    pub(self) fn load_node<'a: 's, 's>(&'s self, node_id: usize) -> Task<'s, Option<Node>> {
        let lock = self.node_store.read();
        // .map_err(|_| "Can't get read access to node_store".into())
        // .then(move |store| store.get(&node_id).from_err::<BTreeErr>());
        let res = lock.then(move |store| async move { Ok(store.get(&node_id).await?) });
        Box::pin(res)
    }

    pub(self) fn save_node<'a: 's, 's>(&'s mut self, node_id: usize, node: Node) -> Task<'s, ()> {
        let result = self
            .node_store
            .write()
            // .map_err(|_| "Can't get write access to node_store".into())
            .then(move |mut store| async move { Ok(store.put(node_id, node).await?) });
        Box::pin(result)
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
    use async_kvstore::boxed::HashMapKVStorage;

    fn create_node_store(mut idx: usize) -> impl NodeStore<usize, Node> {
        BinaryNodeStore::new(
            HashMapKVStorage::new(),
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

    #[tokio::test]
    async fn load_save_node_test() {
        let node_store = create_node_store(0);
        let mut tree = create_tree(node_store);

        let leaf1 = tree.load_node(1).await;
        assert_eq!(None, leaf1.unwrap());

        let put = tree.save_node(1, Node::empty_leaf()).await;
        assert_eq!((), put.unwrap());

        let leaf2 = tree.load_node(1).await;

        assert_eq!(Some(Node::empty_leaf()), leaf2.unwrap())

        // todo
        // tree.get()
    }
}

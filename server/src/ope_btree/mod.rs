//! BTree implementation.
//! todo more documentation when btree will be ready

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Mutex;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use kvstore_api::kvstore::KVStore;
use kvstore_binary::BinKVStore;
use log::debug;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

use common::gen::NumGen;
use common::Key;

use crate::ope_btree::commands::BTreeCmd;
use crate::ope_btree::commands::SearchCmd;
use crate::ope_btree::core::node::Node;
use crate::ope_btree::core::node_store::{BinaryNodeStore, NodeStoreError};

pub mod commands;
pub mod core;

type Result<V> = std::result::Result<V, BTreeErr>;

/// BTree errors
#[derive(Error, Debug)]
pub enum BTreeErr {
    #[error("Node Store Error")]
    NodeStoreError {
        #[from]
        source: NodeStoreError,
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
pub struct OpeBTree<Store>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    node_store: RwLock<BinaryNodeStore<usize, Node, Store, NumGen>>,
    config: OpeBTreeConf,
    // todo should contain valRefProvider: () ⇒ ValueRef
}

impl<Store> OpeBTree<Store>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    pub fn new(
        config: OpeBTreeConf,
        node_store: BinaryNodeStore<usize, Node, Store, NumGen>,
    ) -> Self {
        let node_store = RwLock::new(node_store);

        OpeBTree { config, node_store }
    }

    pub async fn get<GetCmd>(&self, cmd: GetCmd) -> Result<ValueRef>
    where
        GetCmd: SearchCmd + BTreeCmd,
    {
        let root = self.get_root().await?;

        // get root
        // cmd.sent_leaf
        //

        unimplemented!()
    }

    /// Gets or creates root node
    async fn get_root(&self) -> Result<Node> {
        let node_store = self.node_store.read().await;

        let root = if let Some(root) = node_store.get(ROOT_ID).await? {
            root
        } else {
            let mut node_store = self.node_store.write().await;
            let new_root = Node::empty_branch();
            node_store.set(ROOT_ID, new_root.clone()).await?;
            new_root
        };
        Ok(root)
    }

    pub(self) async fn read_node(&self, node_id: usize) -> Result<Option<Node>> {
        let lock = self.node_store.read().await;
        let node = lock.get(node_id).await?;
        Ok(node)
    }

    pub(self) async fn write_node(&mut self, node_id: usize, node: Node) -> Result<()> {
        let mut lock = self.node_store.write().await;
        lock.set(node_id, node).await?;
        Ok(())
    }

    // /// Saves all changed nodes to tree store. Apply put_task to old tree state for getting new tree state.
    // ///
    // /// @param putTask Pool of changed nodes
    // fn commit_new_state(put_task: PutTask) -> Result<()> {
    //     debug!("commit_new_state for nodes={:?}", put_task);
    //     // todo start transaction
    //
    //     // Task
    //     // .gatherUnordered(putTask.nodesToSave.map { case NodeWithId(id, node) ⇒ saveNode(id, node) })
    //     // .foreachL(_ ⇒ if (putTask.increaseDepth) this.depth.increment())
    //
    //     // todo end transaction
    //     todo!()
    // }

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
    use kvstore_inmemory::hashmap_store::HashMapKVStore;

    fn create_node_store(
        idx: usize,
    ) -> BinaryNodeStore<usize, Node, HashMapKVStore<Vec<u8>, Vec<u8>>, NumGen> {
        let store = HashMapKVStore::new();
        BinaryNodeStore::new(store, NumGen(idx))
    }

    fn create_tree<Store: KVStore<Vec<u8>, Vec<u8>> + Send>(
        store: BinaryNodeStore<usize, Node, Store, NumGen>,
    ) -> OpeBTree<Store> {
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

        let leaf1 = tree.read_node(1).await;
        assert_eq!(None, leaf1.unwrap());

        let put = tree.write_node(1, Node::empty_leaf()).await;
        assert_eq!((), put.unwrap());

        let leaf2 = tree.read_node(1).await;

        assert_eq!(Some(Node::empty_leaf()), leaf2.unwrap())

        // todo
        // tree.get()
    }
}

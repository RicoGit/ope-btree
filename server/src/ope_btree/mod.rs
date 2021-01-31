//! BTree implementation.
//! todo more documentation when btree will be ready

use async_recursion::async_recursion;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use kvstore_api::kvstore::KVStore;
use kvstore_binary::BinKVStore;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

use common::gen::NumGen;
use common::Key;

use crate::ope_btree::commands::SearchCmd;
use crate::ope_btree::commands::{BTreeCmd, CmdError};
use crate::ope_btree::internal::node::{BranchNode, LeafNode, Node, NodeWithId};
use crate::ope_btree::internal::node_store::{BinaryNodeStore, NodeStoreError};

pub mod commands;
pub mod internal;

type Result<V> = std::result::Result<V, BTreeErr>;

/// BTree errors
#[derive(Error, Debug)]
pub enum BTreeErr {
    #[error("Node Store Error")]
    NodeStoreErr {
        #[from]
        source: NodeStoreError,
    },
    #[error("Command Error")]
    CmdErr {
        #[from]
        source: CmdError,
    },
    #[error("Illegal State Error: {msg:?}")]
    IllegalStateErr { msg: String },
    #[error("Unexpected Error: {msg:?}")]
    Other { msg: String },
}

impl BTreeErr {
    fn illegal_state(msg: &str) -> BTreeErr {
        BTreeErr::IllegalStateErr {
            msg: msg.to_string(),
        }
    }
}

/// Tree root id is constant, it always points to root node in node_store.
static ROOT_ID: usize = 0;

pub type NodeId = usize;

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
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
    depth: AtomicUsize,
    config: OpeBTreeConf,
    // todo should contain valRefProvider: () ⇒ ValueRef
}

// todo move outside

#[derive(Debug, Clone)]
struct GetFlow<GetCmd, Store>
where
    GetCmd: SearchCmd + BTreeCmd,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    cmd: GetCmd,
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
}

impl<GetCmd, Store> GetFlow<GetCmd, Store>
where
    GetCmd: SearchCmd + BTreeCmd,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    fn new(cmd: GetCmd, store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>) -> Self {
        GetFlow {
            cmd,
            node_store: store,
        }
    }

    async fn get_for_node(&self, node: Node) -> Result<Option<ValueRef>> {
        if node.is_empty() {
            Ok(None) // This is the terminal action, nothing to find in empty tree
        } else {
            match node {
                Node::Leaf(leaf) => self.get_for_leaf(leaf).await,
                Node::Branch(branch) => self.get_for_branch(branch).await,
            }
        }
    }

    async fn get_for_leaf(&self, leaf: LeafNode) -> Result<Option<ValueRef>> {
        log::debug!("Get for leaf={:?}", &leaf);

        let response = self.cmd.submit_leaf(Some(&leaf)).await?;

        // get value ref from leaf by searched index
        match response {
            Some(Ok(idx)) => {
                let value_ref = leaf.values_refs.get(idx).cloned();
                Ok(value_ref)
            }
            _ => Ok(None),
        }
    }

    /// Function is recursive and required Boxed Future
    // #[async_recursion]
    // async fn get_for_branch(&self, branch: BranchNode) -> Result<Option<ValueRef>> {
    //     log::debug!("Get for branch={:?}", &branch);
    //     let (_, child) = self.search_child(branch).await?;
    //     self.get_for_node(child).await
    // }

    // fn get_for_branch(&self, branch: BranchNode) -> BoxFuture<'static, Result<Option<ValueRef>>> {
    //
    //     async move {
    //         log::debug!("Get for branch={:?}", &branch);
    //         self.search_child(branch)
    //             .and_then(move |(_, child)| self.get_for_node(child))
    //             .await
    //     }
    //         .boxed()
    // }

    // todo finish, this function isn't compiled GetFlow should be Send
    fn get_for_branch(&self, branch: BranchNode) -> BoxFuture<'static, Result<Option<ValueRef>>> {
        // async move {
        //     log::debug!("Get for branch={:?}", &branch);
        //
        //     let (_, child) = self.search_child(branch).await?;
        //     let node = self.get_for_node(child).await?;
        //     Ok(node)
        // }
        // .boxed()
        todo!()
    }

    /// ## Method makes remote call!
    ///
    /// Searches and returns next child node of tree.
    /// First of all we call remote client for getting index of child.
    /// After that we gets child ''nodeId'' by this index. By ''nodeId'' we fetch ''child node'' from store.
    /// `branch` Branch node for searching
    /// `cmd` A command for BTree execution (it's a 'bridge' for communicate with BTree client)
    /// Returns index of searched child and the child
    async fn search_child(&self, branch: BranchNode) -> Result<(usize, Node)> {
        // cmd
        // .nextChildIndex(branch)
        // .flatMap(searchedIdx ⇒ {
        // val childId = branch.childsReferences(searchedIdx)
        // store.get(childId).map(searchedIdx → _)
        // })
        todo!()
    }
}

impl<Store> OpeBTree<Store>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    pub fn new(
        config: OpeBTreeConf,
        node_store: BinaryNodeStore<NodeId, Node, Store, NumGen>,
    ) -> Self {
        let node_store = Arc::new(RwLock::new(node_store));
        let depth = AtomicUsize::new(0);
        OpeBTree {
            config,
            depth,
            node_store,
        }
    }

    /// BTree initialization (creates root node if needed)
    pub async fn init(&mut self) -> Result<()> {
        let node_store = self.node_store.read().await;

        if node_store.get(ROOT_ID).await?.is_none() {
            log::debug!("Root node not found - create new one");

            std::mem::drop(node_store);
            let new_root = Node::empty_branch();
            let nodes = vec![NodeWithId {
                id: ROOT_ID,
                node: new_root,
            }];
            let task = PutTask::new(nodes, true, false);
            self.commit_new_state(task).await?;
        }

        Ok(())
    }

    //
    // Get
    //

    pub async fn get<GetCmd>(&self, cmd: GetCmd) -> Result<Option<ValueRef>>
    where
        GetCmd: SearchCmd + BTreeCmd,
    {
        log::debug!("Get starts");

        let root = self.get_root().await?;
        let flow = GetFlow::new(cmd, self.node_store.clone());
        flow.get_for_node(root).await
    }

    /// Gets or creates root node
    async fn get_root(&self) -> Result<Node> {
        let node = self.read_node(ROOT_ID).await?;
        node.ok_or_else(|| BTreeErr::illegal_state("Root not isn't exists"))
    }

    pub(self) async fn read_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        log::debug!("Read node: id={:?}", node_id);

        let lock = self.node_store.read().await;
        let node = lock.get(node_id).await?;
        Ok(node)
    }

    /// Saves specified node to tree store
    pub(self) async fn write_node(&mut self, node_id: NodeId, node: Node) -> Result<()> {
        log::debug!("Write node: id={:?} node={:?}", node_id, &node);

        let mut lock = self.node_store.write().await;
        lock.set(node_id, node).await?;
        Ok(())
    }

    /// Saves all changed nodes to tree store. Apply `task` to old tree state for getting new tree state.
    async fn commit_new_state(&mut self, task: PutTask) -> Result<()> {
        log::debug!("commit_new_state for nodes={:?}", task);

        // todo start transaction

        if task.increase_depth {
            self.depth.fetch_add(1, Ordering::Relaxed);
        }
        let mut store = self.node_store.write().await;

        for NodeWithId { id, node } in task.nodes_to_save {
            store.set(id, node).await?;
        }
        // todo end transaction

        Ok(())
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

/// Task for persisting. Contains updated node after inserting new value and rebalancing the tree.
#[derive(Debug, Clone, Default)]
struct PutTask {
    /// Pool of changed nodes that should be persisted to tree store
    nodes_to_save: Vec<NodeWithId<NodeId, Node>>,
    /// If root node was split than tree depth should be increased.
    /// If true - tree depth will be increased in physical state, if false - depth won't changed.
    /// Note that each put operation might increase root depth only by one.
    increase_depth: bool,
    /// Indicator of the fact that during putting there was a rebalancing
    was_splitting: bool,
}

impl PutTask {
    fn from(nodes_to_save: Vec<NodeWithId<NodeId, Node>>) -> Self {
        PutTask {
            nodes_to_save,
            increase_depth: false,
            was_splitting: false,
        }
    }

    fn new(
        nodes_to_save: Vec<NodeWithId<NodeId, Node>>,
        increase_depth: bool,
        was_splitting: bool,
    ) -> Self {
        PutTask {
            nodes_to_save,
            increase_depth,
            was_splitting,
        }
    }
}

#[cfg(test)]
mod tests {
    use kvstore_inmemory::hashmap_store::HashMapKVStore;
    use super::*;

    fn create_node_store(
        idx: NodeId,
    ) -> BinaryNodeStore<NodeId, Node, HashMapKVStore<Vec<u8>, Vec<u8>>, NumGen> {
        let store = HashMapKVStore::new();
        BinaryNodeStore::new(store, NumGen(idx))
    }

    fn create_tree<Store: KVStore<Vec<u8>, Vec<u8>> + Send>(
        store: BinaryNodeStore<NodeId, Node, Store, NumGen>,
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

//! BTree implementation.
//! todo more documentation when btree will be ready

use std::convert::TryInto;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use kvstore_api::kvstore::KVStore;
use kvstore_binary::BinKVStore;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

use common::gen::{Generator, NumGen};
use common::merkle::{MerkleError, MerklePath, NodeProof};
use common::misc::ToBytes;
use common::{Digest, Hash, Key};
use protocol::{
    BtreeCallback, ClientPutDetails, ProtocolError, PutCallbacks, SearchCallback, SearchResult,
};

use crate::ope_btree::commands::{Cmd, CmdError};
use crate::ope_btree::internal::node::{BranchNode, LeafNode, Node, NodeWithId};
use crate::ope_btree::internal::node_store::{BinaryNodeStore, NodeStoreError};
use crate::ope_btree::internal::tree_path::TreePath;

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
    #[error("Merkle Error")]
    MerkleErr {
        #[from]
        source: MerkleError,
    },
    #[error("Protocol Error")]
    ProtocolErr {
        #[from]
        source: ProtocolError,
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

    fn node_not_found(ixd: NodeId, details: &str) -> BTreeErr {
        BTreeErr::illegal_state(&format!(
            "Node not found for specified idx={} ({})",
            ixd, details
        ))
    }
}

/// Tree root id is constant, it always points to root node in node_store.
static ROOT_ID: usize = 0;

type Trail = TreePath<NodeId>;

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
/// the [`BinaryNodeStore`] to make the tree maximally pluggable and seamlessly switch
/// between in memory, on disk, or maybe, over network storages. Key comparison
/// operations in the tree are also pluggable and are provided by the [`Cmd`]
/// implementations, which helps to impose an order over for example encrypted nodes data.
///
/// [`BinaryNodeStore`]: internal/node_store/struct.BinaryNodeStore.html
/// [`Cmd`]: commands/struct.Cmd.html
pub struct OpeBTree<Store, D>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    /// Store that keeping BTree nodes
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
    /// Dept of Btree
    depth: AtomicUsize,
    /// Btree configurations
    config: OpeBTreeConf,
    /// Generates ValueRef for BTree
    val_ref_gen: Arc<Mutex<ValRefGen>>,

    phantom_data: PhantomData<D>,
}

struct FoundChild {
    /// Node id for found child
    child_id: NodeId,
    // Found child node
    child_node: Node,
    /// Index sended by client
    found_idx: usize,
}

/// Encapsulates all logic by traversing tree for Get operation.
#[derive(Debug, Clone)]
struct GetFlow<Cb, Store>
where
    Cb: SearchCallback,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    cmd: Cmd<Cb>,
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
}

impl<Cb, Store> GetFlow<Cb, Store>
where
    Cb: SearchCallback,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    fn new(cmd: Cmd<Cb>, store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>) -> Self {
        GetFlow {
            cmd,
            node_store: store,
        }
    }

    /// Traverses tree and finds returns ether None or leaf, client makes decision.
    /// It's hard to write recursion here, because it required BoxFuture with Send + Sync + 'static,
    async fn get_for_node(self, node: Node) -> Result<Option<ValueRef>> {
        let mut current_node = node;
        loop {
            if current_node.is_empty() {
                // This is the terminal action, nothing to find in empty tree
                return Ok(None);
            } else {
                match current_node {
                    Node::Leaf(leaf) => return self.get_for_leaf(leaf).await,
                    Node::Branch(branch) => {
                        log::debug!("GetFlow: Get for branch={:?}", &branch);
                        let res = self.search_child(branch).await?;
                        current_node = res.child_node;
                    }
                }
            }
        }
    }

    async fn get_for_leaf(self, leaf: LeafNode) -> Result<Option<ValueRef>> {
        log::debug!("GetFlow: Get for leaf={:?}", &leaf);

        let response = self.cmd.submit_leaf(leaf.clone()).await?;

        match response {
            Ok(idx) => {
                // if client returns index, fetch value for this index and send it to client
                let value_ref = leaf.values_refs.get(idx).cloned();
                Ok(value_ref)
            }
            _ => Ok(None),
        }
    }

    /// Searches and returns next child node of tree.
    /// First of all we call remote client for getting index of child.
    /// After that we gets child ''nodeId'' by this index. By ''nodeId'' we fetch ''child node'' from store.
    ///
    /// `branch` Branch node for searching
    ///
    /// Returns index of searched child and the child
    async fn search_child(&self, branch: BranchNode) -> Result<FoundChild> {
        let found_idx = self.cmd.next_child_idx(branch.clone()).await?;
        let child_id = *branch.children_refs.get(found_idx).ok_or_else(|| {
            BTreeErr::node_not_found(found_idx, "search_child: Invalid node idx from client")
        })?;

        let child_node = self.read_node(child_id).await?.ok_or_else(|| {
            BTreeErr::node_not_found(found_idx, "search_child: Can't find in node storage")
        })?;

        Ok(FoundChild {
            child_id,
            child_node,
            found_idx,
        })
    }

    async fn read_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        log::debug!("GetFlow: Read node: id={:?}", node_id);
        let lock = self.node_store.read().await;
        let node = lock.get(node_id).await?;
        Ok(node)
    }
}

/// Encapsulates all logic for putting into tree
#[derive(Debug, Clone)]
struct PutFlow<Cb, Store, D>
where
    Cb: PutCallbacks,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    cmd: Cmd<Cb>,
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
    val_ref_gen: Arc<Mutex<ValRefGen>>,
    phantom_data: PhantomData<D>,
}

impl<Cb, Store, D: Digest> PutFlow<Cb, Store, D>
where
    Cb: SearchCallback + PutCallbacks + Clone, // todo separate BTreeFlow for BTreeCallback
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    fn new(
        cmd: Cmd<Cb>,
        node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
        val_ref_gen: Arc<Mutex<ValRefGen>>,
    ) -> Self {
        PutFlow {
            cmd,
            node_store,
            val_ref_gen,
            phantom_data: PhantomData::default(),
        }
    }

    /// Finds and fetches next child, makes step down the tree and updates trail.
    ///
    /// `node_id` Id of walk-through branch node
    /// `node`   Walk-through node
    async fn put_for_node(self, node_id: NodeId, node: Node) -> Result<(PutTask, ValueRef)> {
        let mut trail = Trail::empty();
        let mut current_node_id = node_id;
        let mut current_node = node;
        let get_flow = GetFlow::new(self.cmd.clone(), self.node_store.clone());

        loop {
            match current_node {
                Node::Leaf(leaf) => return self.put_for_leaf(node_id, leaf, trail).await,
                Node::Branch(branch) => {
                    log::debug!("PutFlow: Put for branch={:?}", &branch);

                    let res = get_flow.search_child(branch.clone()).await?;
                    trail.push(current_node_id, branch, res.found_idx);

                    current_node_id = res.child_id;
                    current_node = res.child_node;
                }
            }
        }
    }

    /// Puts new ''key'' and ''value'' to this leaf.
    /// Also makes all tree transformation (rebalancing, persisting to store).
    /// This is the terminal method.
    /// Returns plan of updating Btree and value reference
    async fn put_for_leaf(
        self,
        leaf_id: NodeId,
        leaf: LeafNode,
        trail: Trail,
    ) -> Result<(PutTask, ValueRef)> {
        log::debug!("Put to leaf={:?}, id={:?}", &leaf, leaf_id);

        let put_details = self.cmd.put_details(leaf.clone()).await?;
        let (updated_leaf, val_ref) = self.update_leaf(leaf, put_details.clone()).await?;

        // makes all transformations over the copy of tree
        let (new_state_proof, put_task) =
            self.logical_put(leaf_id, updated_leaf, put_details.idx().clone(), trail);

        // after all the logical operations, we need to send the merkle path to the client for verification
        self.cmd
            .verify_changes::<D>(new_state_proof, put_task.was_splitting)
            .await?;

        Ok((put_task, val_ref))
    }

    /// Puts new ''key'' and ''value'' to this leaf.
    ///  * if search key was found - rewrites key and value
    ///  * if key wasn't found - inserts new key and value
    async fn update_leaf(
        &self,
        leaf: LeafNode,
        put_detail: ClientPutDetails,
    ) -> Result<(LeafNode, ValueRef)> {
        log::debug!("Update leaf={:?}, put_details={:?}", &leaf, &put_detail);

        let ClientPutDetails {
            key,
            val_hash,
            search_result,
        } = put_detail;

        let res = match search_result {
            SearchResult::Ok(idx_of_update) => {
                // key was founded in this Leaf, update leaf with new value
                let old_value_ref =
                    leaf.values_refs
                        .get(idx_of_update)
                        .cloned()
                        .ok_or_else(|| {
                            BTreeErr::node_not_found(
                                idx_of_update,
                                "update_leaf: Invalid node idx from client",
                            )
                        })?;
                let updated_leaf = leaf.update::<D>(
                    key.into(),
                    old_value_ref.clone(),
                    val_hash.into(),
                    idx_of_update,
                );
                (updated_leaf, old_value_ref)
            }
            SearchResult::Err(idx_of_insert) => {
                // key wasn't found in this Leaf, insert new value to the leaf
                let new_val_ref = self.val_ref_gen.lock().await.next();
                let updated_leaf = leaf.insert::<D>(
                    key.into(),
                    new_val_ref.clone(),
                    val_hash.into(),
                    idx_of_insert,
                );
                (updated_leaf, new_val_ref)
            }
        };
        Ok(res)
    }

    /// This method does all mutation operations over the tree in memory without changing tree state
    /// and composes merkle path for new tree state. It inserts new value to leaf,
    /// and does tree rebalancing if it needed.
    /// All changes makeover copies of the visited nodes and actually don't change the tree.
    ///
    /// `leaf_id` Id of leaf that was updated
    /// `new_leaf` Leaf that was updated with new key and value
    /// `found_val_idx` Insertion index of a new value
    /// `trail` The path traversed from the root to a leaf with all visited tree nodes.
    ///
    /// Returns tuple with [`MerklePath`] for tree after updating and [`PutTask`] for persisting changes
    ///
    /// [`MerklePath`]: common/merkle/struct.MerklePath.html
    /// [`PutTask`]: struct.PutTask.html
    fn logical_put(
        &self,
        leaf_id: NodeId,
        new_leaf: LeafNode,
        found_val_idx: usize,
        trail: Trail,
    ) -> (MerklePath, PutTask) {
        log::debug!(
            "Logical put for leaf_id={}, leaf={:?}, trail={:?}",
            leaf_id,
            new_leaf,
            trail
        );

        todo!()
    }
}

impl<Store, D: Digest> OpeBTree<Store, D>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    pub fn new(
        config: OpeBTreeConf,
        node_store: BinaryNodeStore<NodeId, Node, Store, NumGen>,
        val_ref_gen: ValRefGen,
    ) -> Self {
        let node_store = Arc::new(RwLock::new(node_store));
        let depth = AtomicUsize::new(0);
        OpeBTree {
            config,
            depth,
            node_store,
            val_ref_gen: Arc::new(Mutex::new(val_ref_gen)),
            phantom_data: PhantomData::default(),
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

    pub async fn get<Cb>(&self, cmd: Cmd<Cb>) -> Result<Option<ValueRef>>
    where
        Cb: SearchCallback,
    {
        log::debug!("Get starts");

        let root = self.get_root().await?;
        let flow = GetFlow::new(cmd, self.node_store.clone());
        flow.get_for_node(root).await
    }

    //
    // Put
    //

    pub async fn put<Cb>(&mut self, cmd: Cmd<Cb>) -> Result<ValueRef>
    where
        Cb: PutCallbacks + SearchCallback + Clone,
    {
        log::debug!("Put starts");

        let root = self.get_root().await?;

        if root.is_empty() {
            // if root is empty don't need to finding slot for putting
            log::debug!("Root node is empty, put in Root node");
            // send to client empty details, client answers with put details
            let put_details = cmd.cb.put_details(vec![], vec![]).await?;
            let value_ref = self.val_ref_gen.lock().await.next();
            let new_leaf = LeafNode::create::<D>(
                put_details.key.into(),
                value_ref.clone(),
                put_details.val_hash.into(),
            )?;

            // send the merkle path to the client for verification
            let leaf_proof =
                NodeProof::try_new(Hash::empty(), new_leaf.kv_hashes.clone(), Some(0))?;
            let merkle_root = MerklePath::new(leaf_proof).calc_merkle_root::<D>(None);
            let _signed_root = cmd.cb.verify_changes(merkle_root.bytes(), false).await?;

            // Safe changes
            let task = PutTask::new(
                vec![NodeWithId::new(ROOT_ID, Node::Leaf(new_leaf))],
                true,
                false,
            );
            self.commit_new_state(task).await?;
            Ok(value_ref)
        } else {
            let flow =
                PutFlow::<_, _, D>::new(cmd, self.node_store.clone(), self.val_ref_gen.clone());
            let (put_task, value_ref) = flow.put_for_node(ROOT_ID, root).await?;

            // persists changes
            self.commit_new_state(put_task).await?;
            Ok(value_ref)
        }
    }

    /// Generates new btree node reference
    async fn next_node_ref(&self) -> NodeId {
        let mut store = self.node_store.write().await;
        store.next_id()
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

impl ValueRef {
    /// Returns empty ValueRef.
    pub fn empty() -> Self {
        ValueRef(Bytes::new())
    }

    /// Turns str to ValueRef (for test purpose)
    pub fn from_str(str: &'static str) -> ValueRef {
        ValueRef(Bytes::from(str))
    }
}

impl From<ValueRef> for Bytes {
    fn from(val: ValueRef) -> Self {
        val.0
    }
}

/// Generates value references
#[derive(Debug, Clone)]
pub struct ValRefGen(pub usize);

impl Generator for ValRefGen {
    type Item = ValueRef;

    fn next(&mut self) -> Self::Item {
        self.0 += 1;
        ValueRef(Bytes::copy_from_slice(&self.0.to_be_bytes()[..]))
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

    use common::noop_hasher::NoOpHasher;
    use protocol::{ClientPutDetails, SearchResult};

    use crate::ope_btree::commands::tests::TestCallback;
    use crate::ope_btree::commands::Cmd;
    use crate::ope_btree::BTreeErr::IllegalStateErr;

    use super::*;

    fn create_node_store(
        idx: NodeId,
    ) -> BinaryNodeStore<NodeId, Node, HashMapKVStore<Vec<u8>, Vec<u8>>, NumGen> {
        let store = HashMapKVStore::new();
        BinaryNodeStore::new(store, NumGen(idx))
    }

    fn create_tree<Store: KVStore<Vec<u8>, Vec<u8>> + Send>(
        store: BinaryNodeStore<NodeId, Node, Store, NumGen>,
    ) -> OpeBTree<Store, NoOpHasher> {
        OpeBTree::new(
            OpeBTreeConf {
                arity: 4,
                alpha: 0.25_f32,
            },
            store,
            ValRefGen(0),
        )
    }

    async fn empty_tree() -> OpeBTree<HashMapKVStore<Vec<u8>, Vec<u8>>, NoOpHasher> {
        let node_store = create_node_store(0);
        let mut tree = create_tree(node_store);
        tree.init().await.unwrap();
        tree
    }

    #[tokio::test]
    #[should_panic]
    async fn forgot_run_init_test() {
        // forgot run init before get
        let tree = create_tree(create_node_store(0));
        tree.get(Cmd::new(TestCallback::empty())).await.unwrap();
    }

    #[tokio::test]
    async fn get_flow_empty_test() {
        // Get from empty tree return None
        let tree = empty_tree().await;

        let cmd = Cmd::new(TestCallback::empty());
        let res1 = tree.get(Cmd::new(TestCallback::empty())).await;
        assert_eq!(res1.unwrap(), None);

        let cmd = Cmd::new(TestCallback::empty());
        let res2 = tree.get(cmd).await;
        assert_eq!(res2.unwrap(), None);
    }

    #[tokio::test]
    async fn put_one_and_get_into_empty_tree_test() {
        // Put item into empty tree and get it back
        let mut tree = empty_tree().await;

        // get from empty tree
        let cmd1 = Cmd::new(TestCallback::empty());
        let res1 = tree.get(cmd1).await.unwrap();
        assert_eq!(res1, None);

        // put to empty tree
        let put_details_vec = vec![ClientPutDetails::new(
            "Key".into(),
            "ValHash".into(),
            SearchResult::Err(0),
        )];
        let verify_changes_vec = vec!["SignedRoot".into()];
        let cmd2 = Cmd::new(TestCallback::new(
            vec![],
            vec![],
            put_details_vec,
            verify_changes_vec,
        ));
        let res2 = tree.put(cmd2).await.unwrap();

        // get value stored before
        let submit_leaf_vec = vec![SearchResult::Ok(0)];
        let cmd3 = Cmd::new(TestCallback::new(vec![], submit_leaf_vec, vec![], vec![]));
        let res3 = tree.get(cmd3).await.unwrap();
        assert_eq!(res3, Some(res2)); // val_ref is the same as we got from put

        // get value non-stored before
        let cmd4 = Cmd::new(TestCallback::for_get(vec![], vec![SearchResult::Err(1)]));
        let res4 = tree.get(cmd4).await.unwrap();
        assert_eq!(res4, None);
    }

    #[tokio::test]
    async fn load_save_node_test() {
        // Test private methods 'read_node' and 'write_node'
        let node_store = create_node_store(0);
        let mut tree = create_tree(node_store);

        let leaf1 = tree.read_node(1).await;
        assert_eq!(leaf1.unwrap(), None);

        let put = tree.write_node(1, Node::empty_leaf()).await;
        assert_eq!(put.unwrap(), ());

        let leaf2 = tree.read_node(1).await;
        assert_eq!(leaf2.unwrap(), Some(Node::empty_leaf()))
    }
}

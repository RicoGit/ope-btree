//! BTree implementation.
//! todo more documentation when btree will be ready

use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::ope_btree::command::{Cmd, CmdError};
use crate::ope_btree::internal::node::{LeafNode, Node, NodeWithId};
use crate::ope_btree::internal::node_store::{BinaryNodeStore, NodeStoreError};
use crate::ope_btree::internal::tree_path::TreePath;
use bytes::Bytes;
use common::gen::{Generator, NumGen};
use common::merkle::{MerkleError, MerklePath, NodeProof};
use common::{Digest, Hash};
use flow::get_flow::GetFlow;
use flow::put_flow::{PutFlow, PutTask};
use kvstore_api::kvstore::KVStore;
use protocol::{BtreeCallback, ProtocolError, PutCallbacks, SearchCallback};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

pub mod command;
pub mod flow;
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

    fn node_not_found(idx: NodeId, details: &str) -> BTreeErr {
        BTreeErr::illegal_state(&format!(
            "Node not found for specified idx={} ({})",
            idx, details
        ))
    }
}

/// Tree root id is constant, it always points to root node in node_store.
static ROOT_ID: usize = 0;

type Trail = TreePath<NodeId>;

pub type NodeId = usize;

/// Configuration for OpeBtree.
pub struct OpeBTreeConf {
    /// Maximum size of nodes (maximum number children in branches), should be even.
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
    /// Max number of keys in node
    max_degree: usize,
    /// Min number of keys in node
    min_degree: usize,

    phantom_data: PhantomData<D>,
}

impl<Store, D> OpeBTree<Store, D>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
    D: Digest + 'static,
{
    pub fn new(
        config: OpeBTreeConf,
        node_store: BinaryNodeStore<NodeId, Node, Store, NumGen>,
        val_ref_gen: ValRefGen,
    ) -> Self {
        assert_eq!(config.arity % 2, 0, "Arity should be even");
        let node_store = Arc::new(RwLock::new(node_store));
        let depth = AtomicUsize::new(0);
        let max_degree = config.arity as usize;
        let min_degree = (config.alpha * config.arity as f32) as usize;
        OpeBTree {
            config,
            depth,
            node_store,
            val_ref_gen: Arc::new(Mutex::new(val_ref_gen)),
            max_degree,
            min_degree,
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
            self.commit_new_state(task, &Bytes::new()).await?;
        }

        Ok(())
    }

    /// === GET ===
    ///
    /// We are looking for a specified key in this B+Tree.
    /// Starting from the root, we are looking for some leaf which needed to the
    /// BTree client. We using `search_callback` for communication with client.
    /// While visiting each node we figure out which internal pointer we should follow.
    /// Get have O(log<sub>arity</sub>n) algorithmic complexity.
    ///
    /// `search_callback` A command for BTree execution (it's a 'bridge' for
    /// communicate with BTree client)
    ///   Returns reference to value that corresponds search Key, or None if Key
    /// was not found in tree
    pub async fn get<Cb>(&self, cmd: Cmd<Cb>) -> Result<Option<ValueRef>>
    where
        Cb: SearchCallback,
    {
        log::debug!("Get starts");

        let root = self.get_root().await?;
        let flow = GetFlow::new(cmd, self.node_store.clone());
        flow.get_for_node(root).await
    }

    /// === PUT ===
    ///
    /// Starting from the root, we are looking for some place for putting key
    /// and value in this B+Tree. We're using `put_callback` for communication
    /// with client. At each node, we figure out which internal pointer we
    /// should follow. When we go down the tree we put each visited node to
    /// 'Trail'(see [`TreePath`]) in the same order. Trail is just a array of
    /// all visited nodes from root to leaf. When we found slot for insertion
    /// we do all tree transformation in logical copy of sector of tree;
    /// actually 'Trail' is this copy - copy of visited nodes that will be
    /// changed after insert. We insert new 'key' and 'value' and split leaf
    /// if leaf is full. We also split leaf parent if parent is filled and so on
    /// to the root of the BTree. Also after changing leaf we should re-calculate
    /// merkle root and update checksums of all visited nodes. Absolutely all
    /// tree's transformations are performed on copies and don't change the tree.
    /// When all transformation in logical state ended we commit changes (see method
    /// 'commit_new_state').
    /// Put have O(log,,arity,,n) algorithmic complexity.
    ///
    /// `cmd` A command for BTree execution (it's a 'bridge' for
    /// communicate with BTree client)
    ///
    /// Returns reference to value that corresponds search Key. In update case
    /// will be returned old reference, in insert case will be created new
    /// reference to value
    pub async fn put<Cb>(&mut self, mut cmd: Cmd<Cb>) -> Result<(ValueRef, Bytes)>
    where
        Cb: PutCallbacks + BtreeCallback + Clone,
    {
        log::debug!("Put starts");

        let root = self.get_root().await?;

        // todo verify changes in simple case
        if root.is_empty() {
            // if root is empty don't need to finding slot for putting
            log::debug!("Root node is empty, put in Root node");
            // send to client empty details, client answers with put details
            let put_details = cmd.cb.put_details(vec![], vec![]).await?;
            let value_ref = self.val_ref_gen.lock().await.next();
            let new_leaf = LeafNode::new::<D>(
                put_details.key.into(),
                value_ref.clone(),
                put_details.val_hash.into(),
            );

            // send the merkle path to the client for verification
            let leaf_proof = NodeProof::new(Hash::empty(), new_leaf.kv_hashes.clone(), None);
            let state_signed_by_client = cmd
                .verify_changes::<D>(MerklePath::new(leaf_proof), false)
                .await?;

            // Safe changes
            let task = PutTask::new(
                vec![NodeWithId::new(ROOT_ID, Node::Leaf(new_leaf))],
                true,
                false,
            );
            self.commit_new_state(task, &state_signed_by_client).await?;
            Ok((value_ref, state_signed_by_client))
        } else {
            let flow = PutFlow::<_, _, D>::new(
                cmd,
                self.node_store.clone(),
                self.val_ref_gen.clone(),
                self.max_degree,
                self.min_degree,
            );
            let (put_task, value_ref, state_signed_by_client) =
                flow.put_for_node(ROOT_ID, root).await?;

            // persists changes
            self.commit_new_state(put_task, &state_signed_by_client)
                .await?;
            Ok((value_ref, state_signed_by_client))
        }
    }

    /// Gets or creates root node
    pub async fn get_root(&self) -> Result<Node> {
        let node = self.read_node(ROOT_ID).await?;
        node.ok_or_else(|| BTreeErr::illegal_state("Root not isn't exists"))
    }

    /// Generates new btree node reference
    async fn next_node_ref(&self) -> NodeId {
        let mut store = self.node_store.write().await;
        store.next_id()
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
    async fn commit_new_state(
        &mut self,
        task: PutTask,
        _state_signed_by_client: &Bytes,
    ) -> Result<()> {
        // todo before commit state we have to check client's signature

        log::debug!("commit_new_state for nodes={:?}", task);

        // todo start transaction

        if task.increase_depth {
            self.depth.fetch_add(1, Ordering::Relaxed);
        }
        let mut store = self.node_store.write().await;

        for NodeWithId { id, node } in task.nodes_to_save {
            log::trace!("Store: {}={:?}", id, node);
            store.set(id, node).await?;
        }
        // todo end transaction

        Ok(())
    }

    // todo put, remove, traverse
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct ValueRef(pub Bytes);

#[allow(clippy::should_implement_trait)]
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

#[cfg(test)]
mod tests {
    use kvstore_inmemory::hashmap_store::HashMapKVStore;

    use common::noop_hasher::NoOpHasher;
    use protocol::{ClientPutDetails, SearchResult};

    use crate::ope_btree::command::tests::TestCallback;
    use crate::ope_btree::command::Cmd;

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
            SearchResult(Err(0)),
        )];
        let verify_changes_vec = vec!["SignedRoot".into()];
        let cmd2 = Cmd::new(TestCallback::new(
            vec![],
            vec![],
            put_details_vec,
            verify_changes_vec,
        ));
        let (res2, signature) = tree.put(cmd2).await.unwrap();
        assert_eq!(signature, Bytes::from("SignedRoot"));

        // get value stored before
        let submit_leaf_vec = vec![SearchResult(Ok(0))];
        let cmd3 = Cmd::new(TestCallback::new(vec![], submit_leaf_vec, vec![], vec![]));
        let res3 = tree.get(cmd3).await.unwrap();
        assert_eq!(res3, Some(res2)); // val_ref is the same as we got from put

        // get value non-stored before
        let cmd4 = Cmd::new(TestCallback::for_get(vec![], vec![SearchResult(Err(1))]));
        let res4 = tree.get(cmd4).await.unwrap();
        assert_eq!(res4, None);
    }

    #[tokio::test]
    async fn put_many_and_get_them_back_test() {
        let _ = env_logger::builder().is_test(true).try_init();

        // Put items into empty tree and get them back
        let mut tree = empty_tree().await;

        // get from empty tree
        assert_eq!(
            tree.get(Cmd::new(TestCallback::empty())).await.unwrap(),
            None
        );

        create_3_depth_tree(&mut tree).await;

        // get K1
        let submit_leaf_vec = vec![SearchResult(Ok(0))];
        let cmd1 = Cmd::new(TestCallback::new(
            vec![0, 0, 0],
            submit_leaf_vec,
            vec![],
            vec![],
        ));
        let res1 = tree.get(cmd1).await.unwrap();
        assert_eq!(res1, Some(ValueRef::from_str("\0\0\0\0\0\0\0\x12"))); // val_ref is the same as we got from put
    }

    async fn create_3_depth_tree(
        tree: &mut OpeBTree<HashMapKVStore<Vec<u8>, Vec<u8>>, NoOpHasher>,
    ) {
        let number = 19;
        // ok - number = 18 2 depth tree:
        // [[K4][K7][K10][K13][K1V1K2V2K3V3K4V4][K5V5K6V6K7V7][K8V8K9V9K10V10][K11V11K12V12K13V13][K14V14K15V15K16V16]]]
        // ok - number = 19 3 depth tree:
        // [[K6][[K3][K6][K1V1K2V2K3V3][K4V4K5V5K6V6]][[K9][K12][K15][K7V7K8V8K9V9][K10V10K11V11K12V12][K13V13K14V14K15V15][K16V16K17V17K18V18]]]]

        let search_result_vec: Vec<usize> = vec![0; number];
        for idx in (1..number).rev() {
            let cmd2 = Cmd::new(TestCallback::new(
                vec![0, 0, 0],
                vec![],
                vec![ClientPutDetails::new(
                    format!("K{}", idx).into(),
                    format!("V{}", idx).into(),
                    SearchResult(Err(search_result_vec[idx - 1])),
                )],
                vec!["*".into()],
            ));

            tree.put(cmd2).await.unwrap();
        }
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

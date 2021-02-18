use crate::ope_btree::command::Cmd;
use crate::ope_btree::flow::get_flow::GetFlow;
use crate::ope_btree::internal::node::{LeafNode, Node, NodeWithId};
use crate::ope_btree::internal::node_store::BinaryNodeStore;
use crate::ope_btree::{BTreeErr, NodeId, Result, Trail, ValRefGen, ValueRef};
use common::gen::{Generator, NumGen};
use common::merkle::MerklePath;
use common::Digest;
use kvstore_api::kvstore::KVStore;
use protocol::{ClientPutDetails, PutCallbacks, SearchResult};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Encapsulates all logic for putting into tree
#[derive(Debug, Clone)]
pub struct PutFlow<Cb, Store, D>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    cmd: Cmd<Cb>,
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
    val_ref_gen: Arc<Mutex<ValRefGen>>,
    phantom_data: PhantomData<D>,
}

/// Implementation PutFlow for PutCallbacks
impl<Cb, Store, D: Digest> PutFlow<Cb, Store, D>
where
    Cb: PutCallbacks + Clone,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    pub fn new(
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
    pub async fn put_for_node(self, node_id: NodeId, node: Node) -> Result<(PutTask, ValueRef)> {
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
    pub async fn put_for_leaf(
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
    pub async fn update_leaf(
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
    pub fn logical_put(
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

/// Task for persisting. Contains updated node after inserting new value and rebalancing the tree.
#[derive(Debug, Clone, Default)]
pub struct PutTask {
    /// Pool of changed nodes that should be persisted to tree store
    pub nodes_to_save: Vec<NodeWithId<NodeId, Node>>,
    /// If root node was split than tree depth should be increased.
    /// If true - tree depth will be increased in physical state, if false - depth won't changed.
    /// Note that each put operation might increase root depth only by one.
    pub increase_depth: bool,
    /// Indicator of the fact that during putting there was a rebalancing
    pub was_splitting: bool,
}

impl PutTask {
    pub fn from(nodes_to_save: Vec<NodeWithId<NodeId, Node>>) -> Self {
        PutTask {
            nodes_to_save,
            increase_depth: false,
            was_splitting: false,
        }
    }

    pub fn new(
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

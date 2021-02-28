use std::marker::PhantomData;
use std::sync::Arc;

use kvstore_api::kvstore::KVStore;
use tokio::sync::{Mutex, RwLock};

use common::gen::{Generator, NumGen};
use common::merkle::MerklePath;
use common::{Digest, Hash};
use protocol::{ClientPutDetails, PutCallback, SearchResult};

use crate::ope_btree::command::Cmd;
use crate::ope_btree::flow::get_flow::GetFlow;
use crate::ope_btree::internal::node::{
    BranchNode, ChildRef, LeafNode, Node, NodeWithId, TreeNode,
};
use crate::ope_btree::internal::node_store::BinaryNodeStore;
use crate::ope_btree::internal::tree_path::PathElem;
use crate::ope_btree::{BTreeErr, NodeId, Result, Trail, ValRefGen, ValueRef, ROOT_ID};
use bytes::Bytes;

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
    max_degree: usize,
    min_degree: usize,
}

/// Implementation PutFlow for PutCallbacks
impl<Cb, Store, D> PutFlow<Cb, Store, D>
where
    Cb: PutCallback + Clone,
    Store: KVStore<Vec<u8>, Vec<u8>>,
    D: Digest + 'static,
{
    pub fn new(
        cmd: Cmd<Cb>,
        node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
        val_ref_gen: Arc<Mutex<ValRefGen>>,
        max_degree: usize,
        min_degree: usize,
    ) -> Self {
        PutFlow {
            cmd,
            node_store,
            val_ref_gen,
            phantom_data: PhantomData::default(),
            max_degree,
            min_degree,
        }
    }

    /// Finds and fetches next child, makes step down the tree and updates trail.
    ///
    /// `node_id` Id of walk-through branch node
    /// `node`   Walk-through node
    ///
    /// Returns plan of updating Btree, value reference, and sign for new state tree
    pub async fn put_for_node(
        self,
        node_id: NodeId,
        node: Node,
    ) -> Result<(PutTask, ValueRef, Bytes)> {
        let mut trail = Trail::empty();
        let mut current_node_id = node_id;
        let mut current_node = node;
        let mut get_flow = GetFlow::new(self.cmd.clone(), self.node_store.clone());

        loop {
            match current_node {
                Node::Leaf(leaf) => {
                    return self.put_for_leaf(current_node_id, leaf, trail).await;
                }
                Node::Branch(branch) => {
                    log::debug!(
                        "PutFlow: Put for branch_id={}, branch {:?}",
                        current_node_id,
                        &branch
                    );

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
    /// Returns plan of updating Btree, value reference, and sign for new state tree
    pub async fn put_for_leaf(
        mut self,
        leaf_id: NodeId,
        leaf: LeafNode,
        trail: Trail,
    ) -> Result<(PutTask, ValueRef, Bytes)> {
        log::debug!("Put for leaf_id={:?}, leaf={:?}", leaf_id, &leaf);

        let put_details = self.cmd.put_details(leaf.clone()).await?;
        let (updated_leaf, val_ref) = self.update_leaf(leaf, put_details.clone()).await?;

        // makes all transformations over the copy of tree
        let (new_state_proof, put_task) = self
            .logical_put(
                leaf_id,
                updated_leaf,
                put_details.search_result.idx(),
                trail,
            )
            .await;

        // after all the logical operations, we need to send the merkle path to the client for verification
        let signed_state = self
            .cmd
            .verify_changes::<D>(new_state_proof, put_task.was_splitting)
            .await?;

        Ok((put_task, val_ref, signed_state))
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
            SearchResult(Ok(idx_of_update)) => {
                // key was founded in this Leaf, update leaf with new value
                let old_value_ref = leaf.val_refs.get(idx_of_update).cloned().ok_or_else(|| {
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
            SearchResult(Err(idx_of_insert)) => {
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

    //
    // Logical put
    //

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
    pub async fn logical_put(
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

        let mut ctx: PutCtx = self.create_leaf_ctx(leaf_id, new_leaf, found_val_idx).await;

        for branch in trail.branches.into_iter().rev() {
            ctx = self.create_tree_path_ctx(ctx, branch).await;
        }

        (ctx.new_state_proof, ctx.put_task)
    }

    /// Using for folding all visited branches of 'trail' in reverse order (root comes last)
    ///
    /// If branch isn't overflowed
    ///  * updates branch checksum into parent node and put branch and it's parent to ''nodesToSave'' into [[PutTask]].
    ///
    /// If it's overflowed
    ///  * splits branch into two, adds left branch to parent as a new child and updates right branch checksum into parent node.
    ///  * if parent isn't exist create new parent with 2 new children.
    ///  * put all updated and new nodes into ''nodesToSave'' into [[PutTask]]
    async fn create_tree_path_ctx(
        &self,
        mut ctx: PutCtx,
        visited_node: PathElem<NodeId>,
    ) -> PutCtx {
        let updated = ctx.update_parent.update_parent::<D>(visited_node); // todo double @check

        let PathElem {
            branch_id,
            branch,
            next_child_idx,
        } = updated;

        if branch.has_overflow(self.max_degree) {
            log::info!(
                "Do split for branch_id={}, branch={:?}, next_child_idx={}",
                branch_id,
                branch,
                next_child_idx
            );

            let is_root = branch_id == ROOT_ID;
            let (left, right) = branch.split::<D>();

            let left_id = self.next_node_ref().await;
            // RootId is always linked with root node and will not changed, store right node with new id if split root
            let right_id = if is_root {
                self.next_node_ref().await
            } else {
                branch_id
            };

            let is_insert_to_left = next_child_idx < left.size;
            let (affected_branch, affected_branch_idx) = if is_insert_to_left {
                (left.clone(), next_child_idx)
            } else {
                (right.clone(), next_child_idx - left.size)
            };

            ctx.new_state_proof
                .push_parent(affected_branch.to_proof::<D>(Some(affected_branch_idx)));

            if is_root {
                // there was no parent, root node was splitting
                log::trace!("Logical put with rebalancing for root branch");

                let pop_up_key = left.keys.iter().last().cloned().expect("Can't be empty");
                let new_parent = BranchNode::new::<D>(
                    pop_up_key,
                    ChildRef::new(left_id, left.hash.clone()),
                    ChildRef::new(right_id, right.hash.clone()),
                );
                let affected_parent_idx = if is_insert_to_left { 0 } else { 1 };

                ctx.new_state_proof
                    .push_parent(new_parent.to_proof::<D>(Some(affected_parent_idx)));

                let mut node_to_save = ctx.put_task.nodes_to_save;
                node_to_save.extend(vec![
                    NodeWithId::new(left_id, Node::Branch(left)),
                    NodeWithId::new(right_id, Node::Branch(right)),
                    NodeWithId::new(ROOT_ID, Node::Branch(new_parent)),
                ]);
                PutCtx {
                    new_state_proof: ctx.new_state_proof,
                    update_parent: UpdateParent::Nothing,
                    put_task: PutTask::new(node_to_save, true, true),
                }
            } else {
                log::trace!("Logical put with rebalancing for regular branch");

                // some regular branch was splitting
                let left_with_id = NodeWithId::new(left_id, Node::Branch(left));
                let right_with_id = NodeWithId::new(right_id, Node::Branch(right));
                let mut node_to_save = ctx.put_task.nodes_to_save;
                node_to_save.extend(vec![left_with_id.clone(), right_with_id.clone()]);
                PutCtx {
                    new_state_proof: ctx.new_state_proof,
                    update_parent: UpdateParent::AfterChildSplitting {
                        left: left_with_id,
                        right: right_with_id,
                        is_insert_to_left,
                    },
                    put_task: PutTask::new(node_to_save, false, true),
                }
            }
        } else {
            log::trace!("Simple logical put of branch={:?}", &branch);

            let child_hash = branch.hash.clone();
            ctx.new_state_proof
                .push_parent(branch.to_proof::<D>(Some(next_child_idx)));
            ctx.put_task
                .nodes_to_save
                .push(NodeWithId::new(branch_id, Node::Branch(branch)));
            PutCtx {
                new_state_proof: ctx.new_state_proof,
                update_parent: UpdateParent::AfterChildChanging { child_hash },
                put_task: ctx.put_task,
            }
        }
    }

    /// If leaf isn't overflowed
    /// * updates leaf checksum into parent node and put leaf and it's parent to ''nodesToSave'' into [[PutTask]].
    ///
    /// If it's overflowed
    /// * splits leaf into two, adds left leaf to parent as new child and update right leaf checksum into parent node.
    /// * if parent isn't exist create new parent with 2 new children.
    /// * puts all updated and new nodes to ''nodesToSave'' into [[PutTask]]
    ///
    /// `leaf_id`  Id of leaf that was updated
    /// `leaf` Leaf that was updated with new key and value
    /// `searched_value_idx` Insertion index of a new value
    ///
    async fn create_leaf_ctx(
        &self,
        leaf_id: NodeId,
        leaf: LeafNode,
        searched_value_idx: usize,
    ) -> PutCtx {
        if leaf.has_overflow(self.max_degree) {
            log::info!("Do split for leaf_id={}, leaf={:?}", leaf_id, leaf);

            let is_root = leaf_id == ROOT_ID;
            // get ids for new nodes, right node should be with new id, because each leaf points to right sibling
            let right_id = self.next_node_ref().await;
            // RootId is always linked with root node and will not changed, store left node with new id if split root
            let left_id = if is_root {
                self.next_node_ref().await
            } else {
                leaf_id
            };

            let (left, right) = leaf.split::<D>(right_id);

            let is_insert_to_left = searched_value_idx < left.size;
            let (affected_leaf, _affected_leaf_idx) = if is_insert_to_left {
                (left.clone(), searched_value_idx)
            } else {
                (right.clone(), searched_value_idx - left.size)
            };

            let mut merkle_path = MerklePath::new(affected_leaf.to_proof(None));

            if is_root {
                log::trace!("Logical put with rebalancing for root leaf");

                // there is no parent, root leaf was split
                let pop_up_key = left.keys.iter().last().cloned().expect("Can't be empty");
                let new_parent = BranchNode::new::<D>(
                    pop_up_key,
                    ChildRef::new(left_id, left.hash.clone()),
                    ChildRef::new(right_id, right.hash.clone()),
                );
                let affected_parent_idx = if is_insert_to_left { 0 } else { 1 };

                merkle_path.push_parent(new_parent.to_proof::<D>(Some(affected_parent_idx)));

                PutCtx {
                    new_state_proof: merkle_path,
                    update_parent: UpdateParent::Nothing,
                    put_task: PutTask::new(
                        vec![
                            NodeWithId::new(left_id, Node::Leaf(left)),
                            NodeWithId::new(right_id, Node::Leaf(right)),
                            NodeWithId::new(ROOT_ID, Node::Branch(new_parent)),
                        ],
                        true,
                        true,
                    ),
                }
            } else {
                log::trace!("Logical put with rebalancing for regular leaf");

                // some regular leaf was split
                let left_with_id = NodeWithId::new(left_id, Node::Leaf(left));
                let right_with_id = NodeWithId::new(right_id, Node::Leaf(right));
                PutCtx {
                    new_state_proof: merkle_path,
                    update_parent: UpdateParent::AfterChildSplitting {
                        left: left_with_id.clone(),
                        right: right_with_id.clone(),
                        is_insert_to_left,
                    },
                    put_task: PutTask::new(vec![left_with_id, right_with_id], false, true),
                }
            }
        } else {
            log::trace!("Simple logical put of leaf={:?}", &leaf);
            PutCtx {
                new_state_proof: MerklePath::new(leaf.to_proof(None)),
                update_parent: UpdateParent::AfterChildChanging {
                    child_hash: leaf.hash.clone(),
                },
                put_task: PutTask::new(
                    vec![NodeWithId::new(leaf_id, Node::Leaf(leaf))],
                    false,
                    false,
                ),
            }
        }
    }

    /// Generates new btree node reference
    async fn next_node_ref(&self) -> NodeId {
        let mut store = self.node_store.write().await;
        store.next_id()
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

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum UpdateParent {
    Nothing,
    AfterChildChanging {
        child_hash: Hash,
    },
    AfterChildSplitting {
        left: NodeWithId<NodeId, Node>,
        right: NodeWithId<NodeId, Node>,
        is_insert_to_left: bool,
    },
}

impl UpdateParent {
    /// Function-mutator that will be applied to parent of current node
    fn update_parent<D: Digest>(self, mut visited_node: PathElem<NodeId>) -> PathElem<NodeId> {
        match self {
            UpdateParent::Nothing => visited_node,
            UpdateParent::AfterChildChanging { child_hash } => {
                // Updates child's checksum into parent node
                visited_node.updated_after_child_changing::<D>(child_hash);
                visited_node
            }
            UpdateParent::AfterChildSplitting {
                left,
                right,
                is_insert_to_left,
            } => {
                // Returns function that makes two changes into the parent node:
                //  * It inserts left node as new child before right node.
                //  * It updates checksum and id of changed right node.

                let PathElem {
                    branch_id,
                    mut branch,
                    next_child_idx,
                } = visited_node;

                let pop_up_key = left.node.keys().last().unwrap().clone();
                log::trace!(
                    "Add child to parent node: with key={:?}, child={:?}, idx={}",
                    pop_up_key,
                    left,
                    next_child_idx
                );

                // update parent node with new left node. Parent already contains right node as a child (but with stale id and checksum).
                // updating right node checksum and id is needed, checksum and id of right node were changed after splitting

                branch.insert_child::<D>(
                    pop_up_key,
                    ChildRef::new(left.id, left.node.hash()),
                    next_child_idx,
                );
                branch.update_child_ref::<D>(
                    ChildRef::new(right.id, right.node.hash()),
                    next_child_idx + 1,
                );

                let idx_of_updated_child = if is_insert_to_left {
                    next_child_idx
                } else {
                    next_child_idx + 1
                };

                PathElem {
                    branch_id,
                    branch,
                    next_child_idx: idx_of_updated_child,
                }
            }
        }
    }
}

/// Just a state for each recursive operation of ''logicalPut''.
#[derive(Debug)]
struct PutCtx {
    /// Merkle path of made changes
    pub new_state_proof: MerklePath,
    /// Describes changes should be done over parent node
    pub update_parent: UpdateParent,
    /// What actually should be done, when we will commit changed
    pub put_task: PutTask,
}

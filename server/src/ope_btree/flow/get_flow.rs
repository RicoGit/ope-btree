use crate::ope_btree::command::Cmd;
use crate::ope_btree::internal::node::{BranchNode, LeafNode, Node};
use crate::ope_btree::internal::node_store::BinaryNodeStore;
use crate::ope_btree::{BTreeErr, NodeId, Result, ValueRef};
use common::gen::NumGen;
use kvstore_api::kvstore::KVStore;
use protocol::btree::{BtreeCallback, SearchCallback, SearchResult};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct FoundChild {
    /// Node id for found child
    pub child_id: NodeId,
    // Found child node
    pub child_node: Node,
    /// Index sended by client
    pub found_idx: usize,
}

/// Encapsulates all logic by traversing tree for Get operation.
#[derive(Debug, Clone)]
pub struct GetFlow<Cb, Store>
where
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    cmd: Cmd<Cb>,
    node_store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
}

/// Implementation GetFlow for BtreeCallback
impl<Cb, Store> GetFlow<Cb, Store>
where
    Cb: BtreeCallback,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    pub fn new(
        cmd: Cmd<Cb>,
        store: Arc<RwLock<BinaryNodeStore<NodeId, Node, Store, NumGen>>>,
    ) -> Self {
        GetFlow {
            cmd,
            node_store: store,
        }
    }

    /// Searches and returns next child node of tree.
    /// First of all we call remote client for getting index of child.
    /// After that we gets child ''nodeId'' by this index. By ''nodeId'' we fetch ''child node'' from store.
    ///
    /// `branch` Branch node for searching
    ///
    /// Returns index of searched child and the child
    pub async fn search_child(&mut self, branch: BranchNode) -> Result<FoundChild> {
        let found_clild_idx = self.cmd.next_child_idx(branch.clone()).await?;
        let node_id = *branch.children_refs.get(found_clild_idx).ok_or_else(|| {
            BTreeErr::node_not_found(
                found_clild_idx,
                "search_child: Invalid node idx from client",
            )
        })?;

        let child_node = self.read_node(node_id).await?.ok_or_else(|| {
            BTreeErr::node_not_found(found_clild_idx, "search_child: Can't find in node storage")
        })?;

        Ok(FoundChild {
            child_id: node_id,
            child_node,
            found_idx: found_clild_idx,
        })
    }

    pub async fn read_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        log::debug!("GetFlow: Read node: id={:?}", node_id);
        let lock = self.node_store.read().await;
        let node = lock.get(node_id).await?;
        Ok(node)
    }

    pub fn get_cmd(self) -> Cmd<Cb> {
        self.cmd
    }
}

/// Implementation GetFlow for SearchCallback
impl<Cb, Store> GetFlow<Cb, Store>
where
    Cb: SearchCallback,
    Store: KVStore<Vec<u8>, Vec<u8>>,
{
    /// Traverses tree and finds returns ether None or leaf, client makes decision.
    /// It's hard to write recursion here, because it required BoxFuture with Send + Sync + 'static,
    pub async fn get_for_node(mut self, node: Node) -> Result<Option<ValueRef>> {
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

    pub async fn get_for_leaf(mut self, leaf: LeafNode) -> Result<Option<ValueRef>> {
        log::debug!("GetFlow: Get for leaf={:?}", &leaf);

        let response = self.cmd.submit_leaf(leaf.clone()).await?;

        match response {
            SearchResult(Ok(idx)) => {
                // if client returns index, fetch value for this index and send it to client
                let value_ref = leaf.val_refs.get(idx).cloned();
                Ok(value_ref)
            }
            _ => Ok(None),
        }
    }
}

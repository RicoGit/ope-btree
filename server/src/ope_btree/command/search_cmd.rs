use crate::ope_btree::command::Cmd;
use crate::ope_btree::command::Result;
use crate::ope_btree::internal::node::BranchNode;
use crate::ope_btree::internal::node::LeafNode;
use common::misc::ToVecBytes;
use protocol::btree::{BtreeCallback, SearchCallback, SearchResult};

impl<Cb: BtreeCallback> Cmd<Cb> {
    /// Returns a next child index to makes the next step down the tree.
    /// The BTree client searches for required key in the keys of this branch
    /// and returns index.
    ///
    /// # Arguments
    ///
    /// * `branch` -  A branch node of OpeBTree for searching.
    ///
    pub async fn next_child_idx(&mut self, branch: BranchNode) -> Result<usize> {
        let BranchNode {
            keys,
            children_hashes,
            ..
        } = branch;

        let res = self
            .cb
            .next_child_idx(keys.into_bytes(), children_hashes.into_bytes())
            .await?;

        Ok(res)
    }
}

/// A command for a searching some value in OpeBTree (by a client search key).
/// Search key is always stored at the client. Server will never know search key.
impl<Cb: SearchCallback> Cmd<Cb> {
    /// A tree sends required leaf with all keys and hashes of values to the client.
    /// If a tree hasn't any leafs - sends None. Client returns a result of searching
    /// client's key among keys of the current leaf or None if leaf was None.
    ///
    /// # Arguments
    ///
    /// * `leaf` -  A leaf node of OpeBTree for searching.
    ///
    pub async fn submit_leaf(&mut self, leaf: LeafNode) -> Result<SearchResult> {
        let LeafNode {
            keys,
            val_hashes: values_hashes,
            ..
        } = leaf;

        let res = self
            .cb
            .submit_leaf(keys.into_bytes(), values_hashes.into_bytes())
            .await?;
        return Ok(res);
    }
}

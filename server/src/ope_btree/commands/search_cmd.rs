use crate::ope_btree::commands::Cmd;
use crate::ope_btree::commands::CmdError;
use crate::ope_btree::commands::Result;
use crate::ope_btree::internal::node::BranchNode;
use crate::ope_btree::internal::node::CloneAsBytes;
use crate::ope_btree::internal::node::LeafNode;

use bytes::Bytes;
use futures::future::TryFuture;
use futures::TryFutureExt;
use protocol::BtreeCallback;
use protocol::SearchCallback;
use protocol::SearchResult;

impl<Cb: BtreeCallback> Cmd<Cb> {
    /// Returns a next child index to makes the next step down the tree.
    /// The BTree client searches for required key in the keys of this branch
    /// and returns index.
    ///
    /// # Arguments
    ///
    /// * `branch` -  A branch node of OpeBTree for searching.
    ///
    pub async fn next_child_idx(&self, branch: BranchNode) -> Result<usize> {
        let BranchNode {
            keys,
            children_hashes,
            ..
        } = branch;

        let res = self
            .cb
            .next_child_idx(
                keys.clone_as_bytes(),            // todo clone is redundant
                children_hashes.clone_as_bytes(), // todo clone is redundant
            )
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
    pub async fn submit_leaf(&self, leaf: Option<LeafNode>) -> Result<Option<SearchResult>> {
        let future = if let Some(LeafNode {
            keys,
            values_hashes,
            ..
        }) = leaf
        {
            // todo clone is redundant
            let res = self
                .cb
                .submit_leaf(keys.clone_as_bytes(), values_hashes.clone_as_bytes())
                .await?;
            return Ok(res);
        } else {
            let res = self.cb.leaf_not_found().await?;
            return Ok(res);
        };
    }
}

use crate::ope_btree::commands::CmdFuture;
use crate::ope_btree::commands::SearchCmd;
use crate::ope_btree::commands::{BTreeCmd, CmdError};
use crate::ope_btree::node::BranchNode;
use crate::ope_btree::node::CloneAsBytes;
use crate::ope_btree::node::LeafNode;

use bytes::Bytes;
use futures::future::TryFuture;
use futures::TryFutureExt;
use protocol::BtreeCallback;
use protocol::SearchCallback;
use protocol::SearchResult;

/// Implements `BTreeCmd` for each type with `BtreeCallback` functionality.
impl<Cb> BTreeCmd for Cb
where
    Cb: BtreeCallback,
{
    fn next_child_idx<'f>(&self, branch: &'f BranchNode) -> CmdFuture<'f, usize> {
        Box::pin(
            self.next_child_idx(
                branch.keys.clone_as_bytes(),
                branch.children_hashes.clone_as_bytes(),
            )
            .map_err(Into::into),
        )
    }
}

/// Implements `SearchCmd` for each type with `SearchCallback` functionality.
impl<Cb: BTreeCmd> SearchCmd for Cb
where
    Cb: SearchCallback,
{
    fn submit_leaf<'f>(&self, leaf: Option<&'f LeafNode>) -> CmdFuture<'f, Option<SearchResult>> {
        let future = if let Some(LeafNode {
            keys,
            values_hashes,
            ..
        }) = leaf
        {
            self.submit_leaf(keys.clone_as_bytes(), values_hashes.clone_as_bytes())
        } else {
            self.leaf_not_found()
        };

        Box::pin(future.map_err(Into::into))
    }
}

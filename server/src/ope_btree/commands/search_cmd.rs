use bytes::Bytes;
use futures::future::TryFuture;
use futures::TryFutureExt;

use protocol::BtreeCallback;
use protocol::SearchCallback;
use protocol::SearchResult;

use crate::ope_btree::commands::Cmd;
use crate::ope_btree::commands::CmdError;
use crate::ope_btree::commands::Result;
use crate::ope_btree::internal::node::AsBytes;
use crate::ope_btree::internal::node::BranchNode;
use crate::ope_btree::internal::node::LeafNode;

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
    pub async fn submit_leaf(&self, leaf: LeafNode) -> Result<SearchResult> {
        let LeafNode {
            keys,
            values_hashes,
            ..
        } = leaf;

        let res = self
            .cb
            .submit_leaf(keys.into_bytes(), values_hashes.into_bytes())
            .await?;
        return Ok(res);
    }
}

#[cfg(test)]
pub mod tests {
    use std::cell::Cell;

    use futures::FutureExt;

    use protocol::RpcFuture;

    use super::*;

    pub struct TestCallback {
        next_child_idx_vec: Cell<Vec<usize>>,
        submit_leaf_vec: Cell<Vec<SearchResult>>,
    }

    impl TestCallback {
        pub fn new(next_child_idx_vec: Vec<usize>, submit_leaf_vec: Vec<SearchResult>) -> Self {
            TestCallback {
                next_child_idx_vec: Cell::new(next_child_idx_vec),
                submit_leaf_vec: Cell::new(submit_leaf_vec),
            }
        }

        pub fn empty() -> Self {
            TestCallback::new(vec![], vec![])
        }
    }

    impl BtreeCallback for TestCallback {
        fn next_child_idx<'f>(
            &self,
            _keys: Vec<Bytes>,
            _children_hashes: Vec<Bytes>,
        ) -> RpcFuture<'f, usize> {
            let mut vec = self.next_child_idx_vec.take();
            let res = vec
                .pop()
                .expect("TestCallback.next_child_idx: index should be appeared");
            self.next_child_idx_vec.replace(vec);
            async move { Ok(res) }.boxed()
        }
    }

    impl SearchCallback for TestCallback {
        fn submit_leaf<'f>(
            &self,
            _keys: Vec<Bytes>,
            _values_hashes: Vec<Bytes>,
        ) -> RpcFuture<'f, SearchResult> {
            let mut vec = self.submit_leaf_vec.take();
            let res = vec
                .pop()
                .expect("TestCallback.submit_leaf: SearchResult should be appeared");
            self.submit_leaf_vec.replace(vec);
            async move { Ok(res) }.boxed()
        }
    }
}

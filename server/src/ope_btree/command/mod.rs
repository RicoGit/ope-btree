//! Commands that OpeBtree can perform.

pub mod put_cmd;
pub mod search_cmd;

use super::internal::node::BranchNode;
use crate::ope_btree::internal::node::LeafNode;
use common::merkle::MerklePath;
use protocol::{BtreeCallback, ClientPutDetails, SearchCallback, SearchResult};
use thiserror::Error;

use futures::future::BoxFuture;
use std::future::Future;

#[derive(Error, Debug)]
#[error("Command Error")]
pub struct CmdError {
    #[from]
    source: protocol::ProtocolError,
}

pub type Result<V> = std::result::Result<V, CmdError>;

/// Structs for any OpeBTree commands.
#[derive(Debug, Clone)]
pub struct Cmd<Cb> {
    pub cb: Cb,
}

impl<Cb> Cmd<Cb> {
    pub fn new(cb: Cb) -> Self {
        Cmd { cb }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::FutureExt;
    use protocol::{PutCallbacks, RpcFuture};
    use std::cell::RefCell;

    /// Stub Callback for testing
    #[derive(Clone, Debug)]
    pub struct TestCallback {
        next_child_idx_vec: RefCell<Vec<usize>>,
        submit_leaf_vec: RefCell<Vec<SearchResult>>,
        put_details_vec: RefCell<Vec<ClientPutDetails>>,
        verify_changes_vec: RefCell<Vec<Bytes>>,
    }

    impl TestCallback {
        pub fn new(
            next_child_idx_vec: Vec<usize>,
            submit_leaf_vec: Vec<SearchResult>,
            put_details_vec: Vec<ClientPutDetails>,
            verify_changes_vec: Vec<Bytes>,
        ) -> Self {
            TestCallback {
                next_child_idx_vec: RefCell::new(next_child_idx_vec),
                submit_leaf_vec: RefCell::new(submit_leaf_vec),
                put_details_vec: RefCell::new(put_details_vec),
                verify_changes_vec: RefCell::new(verify_changes_vec),
            }
        }

        pub fn empty() -> Self {
            TestCallback::new(vec![], vec![], vec![], vec![])
        }

        pub fn for_get(next_child_idx_vec: Vec<usize>, submit_leaf_vec: Vec<SearchResult>) -> Self {
            TestCallback::new(next_child_idx_vec, submit_leaf_vec, vec![], vec![])
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

    impl PutCallbacks for TestCallback {
        fn put_details<'f>(
            &self,
            keys: Vec<Bytes>,
            values_hashes: Vec<Bytes>,
        ) -> RpcFuture<'f, ClientPutDetails> {
            let mut vec = self.put_details_vec.take();
            let res = vec
                .pop()
                .expect("TestCallback.put_details: SearchResult should be appeared");
            self.put_details_vec.replace(vec);
            async move { Ok(res) }.boxed()
        }

        fn verify_changes<'f>(
            &self,
            server_merkle_root: Bytes,
            was_splitting: bool,
        ) -> RpcFuture<'f, Bytes> {
            let mut vec = self.verify_changes_vec.take();
            let res = vec
                .pop()
                .expect("TestCallback.verify_changes: client signed root should be appeared");
            self.verify_changes_vec.replace(vec);
            async move { Ok(res) }.boxed()
        }

        fn changes_stored<'f>(&self) -> RpcFuture<'f, ()> {
            async {
                log::info!("TestCallback.changes_stored: All changes stored");
                Ok(())
            }
            .boxed()
        }
    }
}

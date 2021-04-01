//! Commands that OpeBtree can perform.

pub mod put_cmd;
pub mod search_cmd;
use thiserror::Error;

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
    use bytes::Bytes;
    use futures::FutureExt;
    use protocol::btree::*;
    use protocol::RpcFuture;

    /// Stub Callback for testing
    #[derive(Clone, Debug)]
    pub struct TestCallback {
        next_child_idx_vec: Vec<usize>,
        submit_leaf_vec: Vec<SearchResult>,
        put_details_vec: Vec<ClientPutDetails>,
        verify_changes_vec: Vec<Bytes>,
    }

    impl TestCallback {
        pub fn new(
            next_child_idx_vec: Vec<usize>,
            submit_leaf_vec: Vec<SearchResult>,
            put_details_vec: Vec<ClientPutDetails>,
            verify_changes_vec: Vec<Bytes>,
        ) -> Self {
            TestCallback {
                next_child_idx_vec,
                submit_leaf_vec,
                put_details_vec,
                verify_changes_vec,
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
        fn next_child_idx(
            &mut self,
            _keys: Vec<Bytes>,
            _children_hashes: Vec<Bytes>,
        ) -> RpcFuture<usize> {
            let res = self
                .next_child_idx_vec
                .pop()
                .expect("TestCallback.next_child_idx: index should be appeared");
            async move { Ok(res) }.boxed()
        }
    }

    impl SearchCallback for TestCallback {
        fn submit_leaf(
            &mut self,
            _keys: Vec<Bytes>,
            _values_hashes: Vec<Bytes>,
        ) -> RpcFuture<SearchResult> {
            let res = self
                .submit_leaf_vec
                .pop()
                .expect("TestCallback.submit_leaf: SearchResult should be appeared");
            async move { Ok(res) }.boxed()
        }
    }

    impl PutCallback for TestCallback {
        fn put_details<'f>(
            &mut self,
            _keys: Vec<Bytes>,
            _values_hashes: Vec<Bytes>,
        ) -> RpcFuture<'f, ClientPutDetails> {
            let res = self
                .put_details_vec
                .pop()
                .expect("TestCallback.put_details: SearchResult should be appeared");
            async move { Ok(res) }.boxed()
        }

        fn verify_changes<'f>(
            &mut self,
            _server_merkle_root: Bytes,
            _was_splitting: bool,
        ) -> RpcFuture<'f, Bytes> {
            let res = self
                .verify_changes_vec
                .pop()
                .expect("TestCallback.verify_changes: client signed root should be appeared");
            async move { Ok(res) }.boxed()
        }

        fn changes_stored<'f>(&mut self) -> RpcFuture<'f, ()> {
            async {
                log::trace!("TestCallback.changes_stored: All changes stored");
                Ok(())
            }
            .boxed()
        }
    }
}

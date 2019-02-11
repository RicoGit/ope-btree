//! Server side encrypted (OPE) Key-Value database. Uses Ope-Btree as index and KVStore
//! as backend for persisting data.

use crate::ope_btree::OpeBTree;
use async_kvstore::KVStore;
use bytes::Bytes;
use futures::Future;

mod errors {
    error_chain! {}
}

struct OpeDatabase<'db> {
    /// Ope Btree index.
    index: OpeBTree<'db>,
    /// Blob storage for persisting encrypted values.
    store: Box<dyn KVStore<Bytes, Bytes>>,
}

impl<'db> OpeDatabase<'db> {
    fn new() -> Self {
        unimplemented!()
    }

    // todo get, put, remove, traverse
    //    fn get(search_callback: impl SearchCallback) -> Future<Item=Option<Bytes>, Error = errors:Error>>
}

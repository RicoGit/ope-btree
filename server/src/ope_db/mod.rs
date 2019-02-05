//! Server side encrypted (OPE) Key-Value database. Uses Ope-Btree as index and KVStore
//! as backend for persisting data.

use crate::ope_btree::OpeBTree;
use crate::ope_db::callbacks::SearchCallback;
use async_kvstore::KVStore;
use bytes::Bytes;
use futures::Future;

mod errors {
    error_chain! {}
}

struct OpeDatabase {
    /// Ope Btree index.
    index: OpeBTree,
    /// Blob storage for persisting encrypted values.
    store: Box<dyn KVStore<Bytes, Bytes>>,
}

impl OpeDatabase {
    fn new() -> Self {
        unimplemented!()
    }

    // todo get, put, remove, traverse
    //    fn get(search_callback: impl SearchCallback) -> Future<Item=Option<Bytes>, Error = errors:Error>>
}

// todo move to protocol
mod callbacks {

    // todo  finish
    pub trait SearchCallback {}

}

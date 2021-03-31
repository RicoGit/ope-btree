//! Remotely-accessible interface to value storage.
//! All parts of storage(btree index, value storage) use this Rpc.

use super::*;
use crate::btree::{PutCallback, SearchCallback};
use bytes::Bytes;

pub trait OpeDatabaseRpc {
    /// Initiates 'Get' operation in remote OpeBTree.
    ///
    /// `dataset_id` Dataset Id. Dataset is one key-value collection.
    /// `version`   Dataset version expected to the client
    /// `search_callback` Wrapper for all callback needed for ''Get'' operation to the BTree
    ///
    /// Returns found value, None if nothing was found.
    fn get<'cb, 's: 'cb, Cb: 'cb + SearchCallback + Send>(
        &'s mut self,
        dataset_id: Bytes,
        version: usize,
        search_callback: Cb,
    ) -> RpcFuture<'cb, Option<Bytes>>;
    // todo consider remove 'cb at all

    /// Initiates 'Put' operation in remote OpeBTree.
    ///
    /// `dataset_id` Dataset Id. Dataset is one key-value collection.
    /// `version` Dataset version expected to the client
    /// `put_callback` Wrapper for all callback needed for 'Put' operation to the BTree.
    /// `encrypted_value` Encrypted value.
    ///
    /// Returns old value if old value was overridden, None otherwise.
    fn put<'cb, 's: 'cb, Cb: 'cb + PutCallback + Send>(
        &'s self,
        dataset_id: Bytes,
        version: usize,
        put_callback: Cb,
        encrypted_value: Bytes,
    ) -> RpcFuture<'cb, Option<Bytes>>;

    // todo add callback for remove operation
}

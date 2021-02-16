//! OpeBtree Rpc protocol.

#![allow(dead_code)] // todo remove later

use std::result;

use bytes::Bytes;
use futures::future::BoxFuture;
use thiserror::Error;

/// Protocol errors
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Rpc Error: {msg:?}")]
    RpcError { msg: String },
    // todo io error ?
}

type Result<V> = result::Result<V, ProtocolError>;

/// Future that carries result of Rpc call.
pub type RpcFuture<'f, V> = BoxFuture<'f, Result<V>>;

/// A result of searching some key in an array of keys.
///
/// If the value is found then [`Result::Ok`] is returned, containing the index
/// of the matching element. If the value is not found then [`Result::Err`] is
/// returned, containing the index where a matching element could be inserted
/// while maintaining sorted order.
pub type SearchResult = result::Result<usize, usize>;

/// Base parent for all callback wrappers needed for any BTree's operation.
pub trait BtreeCallback: Send {
    /// Server asks next child node index (position).
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys of current branch for searching position
    /// * `children_hashes` - All children's hashes of current branch
    ///
    /// # Return
    ///
    /// Next child node position.
    ///
    fn next_child_idx<'f>(
        &self,
        keys: Vec<Bytes>,
        children_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize>;
}

/// Wrapper for all callbacks needed for BTree's search operation.
/// Each callback corresponds to an operation needed Btree for traversing and
/// getting index.
pub trait SearchCallback: BtreeCallback {
    /// Server sends found leaf details.
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys of current branch for searching position
    /// * `values_hashes` - Hashes of values for a current leaf
    ///
    /// # Return
    ///
    /// A result of searching client key in a current branch keys.
    /// Returns Err(idx) if key was found, Ok(idx) otherwise
    ///
    fn submit_leaf<'f>(
        &self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, SearchResult>;
}

/// A structure for holding all client details needed for inserting a key and a
/// value to the OpeBTree.
pub struct ClientPutDetails {
    /// The key that will be placed to the BTree
    pub key: Bytes,
    /// Hash of value that will be placed to the BTree
    pub val_hash: Bytes,
    /// A result of searching client's key among keys of the leaf in which the
    /// new key and value will be inserted. Contains an index for inserting.
    pub search_result: SearchResult,
}

/// Wrapper for all callbacks needed for BTree's ''Put'' operation.
/// Each callback corresponds to operation needed btree for traversing
/// and inserting value.
pub trait PutCallbacks: BtreeCallback {
    /// Server sends founded leaf details.
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys of current branch for searching position
    /// * `values_hashes` - Hashes of values for current leaf
    ///
    /// # Return
    ///
    /// Details from client needed for inserting a key and a value to the BTree.
    ///
    fn put_details<'f>(
        &self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, ClientPutDetails>;

    /// Server sends a new merkle root to a client for approve made changes.
    ///
    /// # Arguments
    ///
    /// * `server_merkle_root` - New merkle root after inserting key/value
    /// * `values_hashes` - 'True' if server performed tree rebalancing, 'False' otherwise
    ///
    /// # Return
    ///
    /// Returns signed by client new merkle root as bytes.
    ///
    fn verify_changes<'f>(
        &self,
        server_merkle_root: Bytes,
        was_splitting: bool,
    ) -> RpcFuture<'f, Bytes>;

    /// Server confirms that all changes was persisted.
    fn changes_stored<'f>(&self) -> RpcFuture<'f, ()>;
}

// todo add callback for remove operation

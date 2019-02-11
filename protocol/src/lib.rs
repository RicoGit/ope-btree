//! OpeBtree Rpc protocol.

#![allow(dead_code)]

use bytes::Bytes;
use futures::Future;
use std::result;

#[macro_use]
extern crate error_chain;

/// A result of searching some key in an array of keys.
///
/// If the value is found then [`Result::Ok`] is returned, containing the index
/// of the matching element. If the value is not found then [`Result::Err`] is
/// returned, containing the index where a matching element could be inserted
/// while maintaining sorted order.
pub type SearchResult = result::Result<usize, usize>;

/// Future that carries result of Rpc call.
type RpcFuture<'f, V> = Box<dyn Future<Item = V, Error = errors::Error> + Send + 'f>;

/// Base parent for all callback wrappers needed for any BTree's operation.
pub trait BtreeCallback {
    /// Server asks next child node index(position).
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys of current branch for searching position
    /// * `child_hashes` - All children's hashes of current branch
    ///
    /// # Return
    ///
    /// Next child node position.
    ///
    fn next_child_idx<'f>(
        &self,
        keys: Vec<Bytes>,
        child_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize>;
}

/// Wrapper for all callbacks needed for BTree's search operation.
/// Each callback corresponds to an operation needed btree for traversing and
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
    ///
    fn submit_leaf<'f>(
        &self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, Option<SearchResult>>;

    /// Server sends that leaf wasn't found.
    fn leaf_not_found<'f>(&self) -> RpcFuture<'f, Option<SearchResult>>;
}

/// Wrapper for all callbacks needed for BTree's ''Put'' operation.
/// Each callback corresponds to operation needed btree for traversing
/// and inserting value.
pub trait PutCallbacks: BtreeCallback {
    /// Server sends founded leaf details.
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys of current branch for searching an position
    /// * `values_hashes` - Hashes of values for current leaf
    ///
    /// # Return
    ///
    /// Details from client needed for inserting a key and a value to the BTree.
    ///
    fn put_details<'f>(
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
    fn verify_changes<'f>(server_merkle_root: Bytes, was_splitting: bool) -> RpcFuture<'f, Bytes>;

    /// Server confirms that all changes was persisted.
    fn changes_stored<'f>() -> RpcFuture<'f, ()>;
}

/// A structure for holding all client details needed for inserting a key and a
/// value to the OpeBTree.
pub struct ClientPutDetails {
    /// The key that will be placed to the BTree
    key: Bytes,
    /// Hash of value that will be placed to the BTree
    val_hash: Bytes,
    /// A result of searching client's key among keys of the leaf in which the
    /// new key and value will be inserted. Contains an index for inserting.
    search_result: SearchResult,
}

// todo add callback for remove operation

pub mod errors {
    error_chain! {
        errors {
            RpcError(msg:  String) {
                display("Rpc Error: {:?}", msg)
            }
        }
    }
}

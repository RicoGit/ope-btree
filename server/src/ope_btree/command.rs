//! Commands that OpeBtree can perform.

use super::node::BranchNode;
use crate::ope_btree::node::LeafNode;
use common::merkle::MerklePath;
use errors::*;
use futures::Future;
use protocol::{ClientPutDetails, SearchResult};

/// Future that carries the Command result.
type CmdFuture<'f, V> = Box<dyn Future<Item = V, Error = errors::Error> + Send + 'f>;

/// Root functionality for all OpeBTree commands.
trait BTreeCommand {
    /// Returns a next child index to makes the next step down the tree.
    /// The BTree client searches for required key in the keys of this branch
    /// and returns index.
    ///
    /// # Arguments
    ///
    /// * `branch` -  A branch node of OpeBTree for searching.
    ///
    fn next_child_idx<'f>(branch: BranchNode) -> CmdFuture<'f, usize>;
}

/// A command for a searching some value in OpeBTree (by a client search key).
/// Search key is always stored at the client.
trait SearchCommand: BTreeCommand {
    /// A tree sends required leaf with all keys and hashes of values to the client.
    /// If a tree hasn't any leafs - sends None. Client returns a result of searching
    /// client's key among keys of the current leaf.
    ///
    /// # Arguments
    ///
    /// * `leaf` -  A leaf node of OpeBTree for searching.
    ///
    fn submit_leaf<'f>(leaf: Option<LeafNode>) -> CmdFuture<'f, SearchResult>;
}

/// Command for inserting key and value to the OpeBTree.
trait PutCommand: BTreeCommand {
    /// Returns all details needed for inserting a key-value to the OpeBTree.
    ///
    /// # Arguments
    ///
    /// * `leaf` - Values for calculating current node hash on the client side
    ///             and find an index to insert.
    fn put_details<'f>(leaf: Option<LeafNode>) -> CmdFuture<'f, ClientPutDetails>;

    /// Server sends a merkle path to the client after inserting a key-value
    /// pair into the tree.
    ///
    /// # Arguments
    ///
    /// * `merkle_path` - A tree path traveled in the OpeBTree from the root to a leaf
    /// * `was_splitting` - An indicator of the fact that during inserting there
    ///                     was a tree rebalancing
    fn verify_changes<'f>(merkle_path: MerklePath, was_splitting: bool) -> CmdFuture<'f, ()>;
}

mod errors {
    error_chain! {}
}

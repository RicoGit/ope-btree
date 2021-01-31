//! Commands that OpeBtree can perform.

pub mod search_cmd;

use super::core::node::BranchNode;
use crate::ope_btree::core::node::LeafNode;
use common::merkle::MerklePath;
use protocol::{ClientPutDetails, SearchResult};
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

/// Future that carries the Command result.
pub type CmdFuture<'f, V> = BoxFuture<'f, Result<V>>;

/// Root functionality for all OpeBTree commands.
pub trait BTreeCmd {
    /// Returns a next child index to makes the next step down the tree.
    /// The BTree client searches for required key in the keys of this branch
    /// and returns index.
    ///
    /// # Arguments
    ///
    /// * `branch` -  A branch node of OpeBTree for searching.
    ///
    fn next_child_idx<'f>(&self, branch: &'f BranchNode) -> CmdFuture<'f, usize>;
}

/// A command for a searching some value in OpeBTree (by a client search key).
/// Search key is always stored at the client. Server will never know search key.
pub trait SearchCmd: BTreeCmd {
    /// A tree sends required leaf with all keys and hashes of values to the client.
    /// If a tree hasn't any leafs - sends None. Client returns a result of searching
    /// client's key among keys of the current leaf or None if leaf was None.
    ///
    /// # Arguments
    ///
    /// * `leaf` -  A leaf node of OpeBTree for searching.
    ///
    fn submit_leaf<'f>(&self, leaf: Option<&'f LeafNode>) -> CmdFuture<'f, Option<SearchResult>>;
}

/// Command for inserting key and value to the OpeBTree.
pub trait PutCmd: BTreeCmd {
    /// Returns all details needed for inserting a key-value to the OpeBTree.
    ///
    /// # Arguments
    ///
    /// * `leaf` - Values for calculating current node hash on the client side
    ///             and find an index to insert.
    fn put_details<'f>(&self, leaf: Option<LeafNode>) -> CmdFuture<'f, ClientPutDetails>;

    /// Server sends a merkle path to the client after inserting a key-value
    /// pair into the tree.
    ///
    /// # Arguments
    ///
    /// * `merkle_path` - A tree path traveled in the OpeBTree from the root to a leaf
    /// * `was_splitting` - An indicator of the fact that during inserting there
    ///                     was a tree rebalancing
    fn verify_changes<'f>(&self, merkle_path: MerklePath, was_splitting: bool)
        -> CmdFuture<'f, ()>;
}

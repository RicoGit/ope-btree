use crate::ope_btree::commands::{Cmd, Result};
use crate::ope_btree::internal::node::LeafNode;
use common::merkle::MerklePath;
use protocol::{ClientPutDetails, PutCallbacks};

/// Command for inserting key and value to the OpeBTree.

impl<Cb: PutCallbacks> Cmd<Cb> {
    /// Returns all details needed for inserting a key-value to the OpeBTree.
    ///
    /// # Arguments
    ///
    /// * `leaf` - Values for calculating current node hash on the client side
    ///             and find an index to insert.
    async fn put_details(&self, leaf: Option<LeafNode>) -> Result<ClientPutDetails> {
        todo!()
    }

    /// Server sends a merkle path to the client after inserting a key-value
    /// pair into the tree.
    ///
    /// # Arguments
    ///
    /// * `merkle_path` - A tree path traveled in the OpeBTree from the root to a leaf
    /// * `was_splitting` - An indicator of the fact that during inserting there
    ///                     was a tree rebalancing
    pub async fn verify_changes(&self, merkle_path: MerklePath, was_splitting: bool) -> Result<()> {
        todo!()
    }
}

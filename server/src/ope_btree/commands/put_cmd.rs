use crate::ope_btree::commands::{Cmd, Result};
use crate::ope_btree::internal::node::{AsBytes, LeafNode};
use common::merkle::MerklePath;
use common::misc::ToBytes;
use common::Digest;
use protocol::{ClientPutDetails, PutCallbacks};

/// Command for inserting key and value to the OpeBTree.

impl<Cb: PutCallbacks> Cmd<Cb> {
    /// Returns all details needed for inserting a key-value to the OpeBTree.
    ///
    /// # Arguments
    ///
    /// * `leaf` - Values for calculating current node hash on the client side
    ///             and find an index to insert.
    pub async fn put_details(&self, leaf: LeafNode) -> Result<ClientPutDetails> {
        let LeafNode {
            keys,
            values_hashes,
            ..
        } = leaf;
        let res = self
            .cb
            .put_details(keys.into_bytes(), values_hashes.into_bytes())
            .await?;
        Ok(res)
    }

    /// Server sends a merkle path to the client after inserting a key-value
    /// pair into the tree.
    ///
    /// # Arguments
    ///
    /// * `merkle_path` - A tree path traveled in the OpeBTree from the root to a leaf
    /// * `was_splitting` - An indicator of the fact that during inserting there
    ///                     was a tree rebalancing
    pub async fn verify_changes<D: Digest>(
        &self,
        merkle_path: MerklePath,
        was_splitting: bool,
    ) -> Result<()> {
        let merkle_root = merkle_path.calc_merkle_root::<D>(None).bytes();
        let res = self.cb.verify_changes(merkle_root, was_splitting).await?;
        self.cb.changes_stored().await?;
        Ok(())
    }
}

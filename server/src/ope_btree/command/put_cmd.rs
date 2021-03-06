use crate::ope_btree::command::{Cmd, Result};
use crate::ope_btree::internal::node::LeafNode;
use bytes::Bytes;
use common::merkle::MerklePath;
use common::misc::{ToBytes, ToVecBytes};
use common::Digest;
use protocol::btree::{ClientPutDetails, PutCallback};

/// Command for inserting key and value to the OpeBTree.
impl<Cb: PutCallback> Cmd<Cb> {
    /// Returns all details needed for inserting a key-value to the OpeBTree.
    ///
    /// # Arguments
    ///
    /// * `leaf` - Values for calculating current node hash on the client side
    ///             and find an index to insert.
    pub async fn put_details(&mut self, leaf: LeafNode) -> Result<ClientPutDetails> {
        let res = self
            .cb
            .put_details(leaf.keys.into_bytes(), leaf.val_hashes.into_bytes())
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
    /// Returns signed by client new Btree state
    pub async fn verify_changes<D: Digest>(
        &mut self,
        merkle_path: MerklePath,
        was_splitting: bool,
    ) -> Result<Bytes> {
        let merkle_root = merkle_path.calc_merkle_root::<D>(None).bytes();
        let state_signed_by_client = self.cb.verify_changes(merkle_root, was_splitting).await?;
        self.cb.changes_stored().await?;
        Ok(state_signed_by_client)
    }
}

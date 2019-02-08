use super::Hash;

/// Contains all information needed for recalculating hash of some OpeTree node.
///
/// To reduce the size and improve the usability of a merkle proof for the OpeBTree
/// node proof was divided into 2 parts:
///
/// - the first part is `state_hash` - it's a hash of node state that does not
///      affect the substitution.
///
/// - the second part is `children_hashes` - it's a sequence of hashes for the
///     substitution.
///
/// See `LeafNode::to_proof()` and `BranchNode::to_proof()` for more understanding.
///
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct NodeProof {
    /// A hash of node inner state, this hash does not contain hashes of a
    /// child's nodes.
    state_hash: Hash,
    /// An array of child's hashes, which participates in the substitution.
    ///
    /// For leafs `children_hashes` is a sequence with hashes of each 'key-value'
    /// pair of this leaf and `substitution_idx` is an index for inserting hash
    /// of 'key-value' pair.
    ///
    /// For branches `children_hashes` is a sequence with hashes for each children
    /// of this branch and `substitution_idx` is an index for inserting hash
    /// of child.
    children_hashes: Vec<Hash>,
    /// An index for a substitution some child hash to `children_hashes`
    substitution_idx: usize,
}

impl NodeProof {
    /// Calculates a merkle root hash for the current merkle proof and the
    /// substituted value.
    ///
    /// # Arguments
    ///
    /// * `hash_for_substitution` - Child's hash for substitution to ''children_hashes''
    ///
    pub fn to_merkle_root(&self, _hash_for_substitution: Option<Hash>) -> Hash {
        unimplemented!("NodeProof::to_merkle_root()");
        // todo implement
        // todo default Hash and Hasher isn't suitable for us because it returns u64,
        // and does not allow configuring hash concatenation,
        // and use &self instead self that force to use clone()
    }
}

/// A Merkle path traversed from the root to a leaf. The head of this path corresponds
/// to the root of the Merkle tree.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct MerklePath(
    /// Ordered sequence of `NodeProof` starts with the root node ends with a leaf.
    pub Vec<NodeProof>,
);

impl MerklePath {
    /// Adds new `proof` to the end of the merkle path.
    pub fn add(&mut self, proof: NodeProof) {
        self.0.push(proof);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {
        // todo test for this
    }

}

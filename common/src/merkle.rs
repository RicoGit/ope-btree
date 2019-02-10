use super::Hash;
use sha3::Digest;

mod errors {
    error_chain! {}
}

use errors::*;

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
    /// Validates params and create `NodeProof`.
    pub fn new(
        state_hash: Hash,
        children_hashes: Vec<Hash>,
        substitution_idx: usize,
    ) -> Result<Self> {
        if substitution_idx >= children_hashes.len() {
            bail!(
                "Substitution index have to be less than number of children \
                 hashes {}, but actually it is {}",
                children_hashes.len(),
                substitution_idx
            )
        }

        Ok(NodeProof {
            state_hash,
            children_hashes,
            substitution_idx,
        })
    }

    /// Calculates a merkle root hash for the current merkle proof and the
    /// substituted value.
    ///
    /// # Arguments
    ///
    /// * `hash_for_substitution` - Child's hash for substitution to ''children_hashes''
    ///
    pub fn into_merkle_root<D: Digest>(self, hash_for_substitution: Option<Hash>) -> Hash {
        let mut hasher = D::new();
        let state = match hash_for_substitution {
            None => {
                // if a hash for substitution isn't defined just calculate node hash
                let NodeProof {
                    mut state_hash,
                    children_hashes,
                    ..
                } = self;

                state_hash.concat_all(children_hashes);
                state_hash
            }
            Some(hash) => {
                // if hash is defined - substitute it to 'hash_for_substitution'
                // and calculate node hash
                let NodeProof {
                    mut state_hash,
                    mut children_hashes,
                    substitution_idx,
                } = self;

                children_hashes.insert(substitution_idx, hash);
                state_hash.concat_all(children_hashes);
                state_hash
            }
        };

        hasher.input(state);
        hasher.result().into()
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
    use crate::merkle::MerklePath;
    use crate::merkle::NodeProof;
    use crate::misc::ToBytes;
    use crate::Hash;
    use bytes::Bytes;
    use sha3::Digest;
    use sha3::Sha3_256;

    #[test]
    fn node_proof_new_err_test() {
        let proof = NodeProof::new(Hash::empty(), Vec::new(), 10);
        assert!(proof.is_err())
    }

    #[test]
    fn node_proof_new_ok_test() {
        let proof = NodeProof::new(Hash::empty(), vec![Hash::empty()], 0);
        let expected = NodeProof {
            state_hash: Hash::empty(),
            children_hashes: vec![Hash::empty()],
            substitution_idx: 0,
        };
        assert_eq!(expected, proof.unwrap())
    }

    #[test]
    fn without_substitution_node_proof_to_merkle_root_test() {
        let proof = node_proof("state");
        let result = proof.clone().into_merkle_root::<Sha3_256>(None);

        let NodeProof {
            mut state_hash,
            children_hashes,
            ..
        } = proof;

        let expected_hash = {
            state_hash.concat_all(children_hashes);
            get_hash::<Sha3_256>(state_hash.bytes().as_ref())
        };
        assert_eq!(expected_hash, result)
    }

    #[test]
    fn with_substitution_node_proof_to_merkle_root_test() {
        let proof = node_proof("state");
        let result = proof
            .clone()
            .into_merkle_root::<Sha3_256>(Some(hash("new_hash")));

        let NodeProof {
            mut state_hash,
            mut children_hashes,
            substitution_idx,
        } = proof;

        let expected_hash = {
            children_hashes.insert(substitution_idx, hash("new_hash"));
            state_hash.concat_all(children_hashes);
            get_hash::<Sha3_256>(state_hash.bytes().as_ref())
        };

        assert_eq!(expected_hash, result)
    }

    fn get_hash<D: Digest>(slice: &[u8]) -> Hash {
        Hash::from(D::digest(slice))
    }

    #[test]
    fn merkle_path_add_test() {
        let proof1 = node_proof("proof1");
        let proof2 = node_proof("proof2");
        let mut m_path = MerklePath(vec![]);
        m_path.add(proof1.clone());
        assert_eq!(MerklePath(vec![proof1.clone()]), m_path);
        m_path.add(proof2.clone());
        assert_eq!(MerklePath(vec![proof1.clone(), proof2.clone()]), m_path);
    }

    fn node_proof(state: &str) -> NodeProof {
        NodeProof {
            state_hash: Hash(Bytes::from(state)),
            children_hashes: vec![hash("child1"), hash("child2"), hash("child3")],
            substitution_idx: 1,
        }
    }

    fn hash(str: &str) -> Hash {
        Hash(Bytes::from(str))
    }
}

use crate::misc::ToBytes;
use crate::{Hash, Key};
use digest::Digest;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Merkle errors
#[derive(Error, Debug)]
pub enum MerkleError {
    #[error("Index Error: {msg:?}")]
    IndexErr { msg: String },
}

pub type Result<V> = std::result::Result<V, MerkleError>;

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
    /// child's nodes. For leaf is always empty.
    state_hash: Hash,
    /// An array of child's hashes, which participates in the substitution.
    ///
    /// For leaf's `children_hashes` is a sequence with hashes of each 'key-value'
    /// pair of this leaf and `substitution_idx` is an index for inserting hash
    /// of 'key-value' pair.
    ///
    /// For branches `children_hashes` is a sequence with hashes for each children
    /// of this branch and `substitution_idx` is an index for inserting hash
    /// of child.
    children_hashes: Vec<Hash>,
    /// An index for a substitution some child hash to `children_hashes`
    substitution_idx: Option<usize>,
}

impl NodeProof {
    /// Validates params and create `NodeProof`.
    /// substitution_idx=None is special values that skip first assert
    pub fn new(
        state_hash: Hash,
        children_hashes: Vec<Hash>,
        substitution_idx: Option<usize>,
    ) -> Self {
        assert!(substitution_idx.is_none() || substitution_idx.unwrap() < children_hashes.len(),
            "Substitution index have to be less than number of children hashes {}, but actually it is {:?}",
            children_hashes.len(),
            substitution_idx
        );

        NodeProof {
            state_hash,
            children_hashes,
            substitution_idx,
        }
    }

    pub fn new_branch_proof<D: Digest>(
        keys: Vec<Key>,
        children_hashes: Vec<Hash>,
        substitution_idx: Option<usize>,
    ) -> Self {
        NodeProof::new(
            NodeProof::calc_keys_hash::<D>(keys),
            children_hashes,
            substitution_idx,
        )
    }

    pub fn new_leaf_proof<D: Digest>(
        keys: &[Key],
        children_hashes: &[Hash],
        substitution_idx: Option<usize>,
    ) -> Self {
        let kv_hashes = NodeProof::calc_kv_hashes::<D>(keys, children_hashes);
        NodeProof::new(Hash::empty(), kv_hashes, substitution_idx)
    }

    pub fn calc_keys_hash<D: Digest>(keys: Vec<Key>) -> Hash {
        let mut hash = Hash::empty();
        hash.concat_all(keys.into_iter().map(|key| Hash::build::<D, _>(key.bytes())));
        hash
    }

    /// Returns array of checksums for each key-value pair
    pub fn calc_kv_hashes<D: Digest>(keys: &[Key], values: &[Hash]) -> Vec<Hash> {
        keys.iter()
            .cloned()
            .zip(values.iter().cloned())
            .map(|(k, v)| {
                let mut key: Hash = Hash::build::<D, _>(k);
                key.concat(v);
                Hash::build::<D, _>(key)
            })
            .collect()
    }

    /// Calculates a checksum (hash) for the current node proof and the substituted value.
    ///
    /// # Arguments
    ///
    /// * `hash_for_substitution` - Child's hash for substitution to ''children_hashes''
    ///
    pub fn calc_checksum<D: Digest>(self, hash_for_substitution: Option<Hash>) -> Hash {
        let state = match hash_for_substitution {
            None => {
                assert!(self.substitution_idx.is_none(), "Illegal state: substitution idx is define, but hash for substitution is not defined.");
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

                let substitution_idx =
                    substitution_idx.expect("substitution_idx should be defined");
                let _old = std::mem::replace(&mut children_hashes[substitution_idx], hash);
                state_hash.concat_all(children_hashes);
                state_hash
            }
        };

        if state.is_empty() {
            state
        } else {
            Hash::build::<D, _>(state)
        }
    }

    /// Updates substitution index
    pub fn set_idx(&mut self, substitution_idx: usize) {
        let _ = self.substitution_idx.replace(substitution_idx);
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
    pub fn new(proof: NodeProof) -> Self {
        MerklePath(vec![proof])
    }

    pub fn empty() -> Self {
        MerklePath(vec![])
    }

    /// Adds new `proof` to the head of the merkle path (adds new parent proof).
    pub fn push_parent(&mut self, proof: NodeProof) {
        self.0.insert(0, proof);
    }

    /// Adds new `proof` to the end of the merkle path.
    pub fn push(&mut self, proof: NodeProof) {
        self.0.push(proof);
    }

    /// Returns child's hash for substitution index from last proof
    pub fn last_proof_children_hash(&self) -> Option<&Hash> {
        let proof = self.0.last()?;
        let idx = proof.substitution_idx?;
        proof.children_hashes.get(idx)
    }

    /// If last proof exists, adds to it child_hash on substitution position
    pub fn insert_child_hash_to_last_proof(&mut self, child_hash: Hash, idx: usize) {
        if let Some(last_proof) = self.0.last_mut() {
            last_proof.children_hashes.insert(idx, child_hash);
        }
    }

    /// Calculates new merkle root from merkle path. Folds merkle path from the right to the left and
    /// calculate merkle tree root. Inserts ''substituted_checksum'' into element in last position in merkle path.
    /// Substitution into the last element occurs at the substitution idx of this element.
    pub fn calc_merkle_root<D: Digest>(self, substituted_checksum: Option<Hash>) -> Hash {
        let folded_path = self
            .0
            .into_iter()
            .rfold(substituted_checksum.clone(), |prev_hash, node_proof| {
                Some(node_proof.calc_checksum::<D>(prev_hash))
            });

        folded_path.unwrap_or_else(|| {
            substituted_checksum
                // case when one proof in merkle path: hash checksum is mandatory
                .map(|cs| Hash::build::<D, _>(cs))
                .unwrap_or_default()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::merkle::{MerklePath, NodeProof};
    use crate::misc::ToBytes;
    use crate::noop_hasher::NoOpHasher;
    use crate::Hash;
    use bytes::BytesMut;
    use sha3::Sha3_256;

    #[test]
    #[should_panic]
    fn node_proof_new_err_test() {
        // create invalid NodeProof
        NodeProof::new(Hash::empty(), Vec::new(), Some(10));
    }

    #[test]
    fn node_proof_new_ok_test() {
        // create valid NodeProof
        let proof = NodeProof::new(Hash::empty(), vec![Hash::empty()], Some(0));
        let expected = NodeProof {
            state_hash: Hash::empty(),
            children_hashes: vec![Hash::empty()],
            substitution_idx: Some(0),
        };
        assert_eq!(expected, proof)
    }

    #[test]
    fn node_proof_checksum_noop_hasher_test() {
        // Calculate checksum of nodeProof with substitution (noop hasher)
        let proof = node_proof("State", Some(1));
        let result = proof
            .clone()
            .calc_checksum::<NoOpHasher>(Some(Hash::from_string("#new#")));

        assert_eq!(result.to_string(), "Hash[512, [StateChild1#new#Child3]]")
    }

    #[test]
    fn node_proof_checksum_noop_hasher_without_substitution_test() {
        // Calculate checksum of nodeProof without substitution (noop hasher)
        let proof = node_proof("State", None);
        let result = proof.clone().calc_checksum::<NoOpHasher>(None);

        assert_eq!(result.to_string(), "Hash[512, [StateChild1Child2Child3]]")
    }

    #[test]
    fn node_proof_to_checksum_without_substitution_test() {
        // Calculate checksum of nodeProof without substitution
        let proof = node_proof("state", None);
        let result = proof.clone().calc_checksum::<Sha3_256>(None);

        let NodeProof {
            mut state_hash,
            children_hashes,
            ..
        } = proof;

        let expected_hash = {
            state_hash.concat_all(children_hashes);
            Hash::build::<Sha3_256, _>(state_hash.bytes().as_ref())
        };
        assert_eq!(expected_hash, result)
    }

    #[test]
    fn node_proof_to_checksum_with_substitution_test() {
        // Calculate checksum of nodeProof with substitution
        let proof = node_proof("state", Some(1));
        let result = proof
            .clone()
            .calc_checksum::<Sha3_256>(Some(Hash::from_string("#new#")));

        let NodeProof {
            mut state_hash,
            mut children_hashes,
            substitution_idx,
        } = proof;

        let expected_hash = {
            let old = std::mem::replace(
                &mut children_hashes[substitution_idx.unwrap()],
                Hash::from_string("#new#"),
            );
            assert_eq!(old, Hash::from_string("Child2"));
            state_hash.concat_all(children_hashes);
            Hash::build::<Sha3_256, _>(state_hash.bytes().as_ref())
        };

        assert_eq!(expected_hash, result)
    }

    #[test]
    fn merkle_path_push_parent_test() {
        // Adds new NodeProof to MerklePath
        let proof1 = node_proof("proof1", None);
        let proof2 = node_proof("proof2", None);
        let mut m_path = MerklePath::empty();
        m_path.push_parent(proof1.clone());
        assert_eq!(MerklePath(vec![proof1.clone()]), m_path);
        m_path.push_parent(proof2.clone());
        assert_eq!(MerklePath(vec![proof2.clone(), proof1.clone()]), m_path);
    }

    #[test]
    fn merkle_path_push_test() {
        // Adds new NodeProof to MerklePath
        let proof1 = node_proof("proof1", None);
        let proof2 = node_proof("proof2", None);
        let mut m_path = MerklePath::empty();
        m_path.push(proof1.clone());
        assert_eq!(MerklePath(vec![proof1.clone()]), m_path);
        m_path.push(proof2.clone());
        assert_eq!(MerklePath(vec![proof1.clone(), proof2.clone()]), m_path);
    }

    #[test]
    fn merkle_path_to_root_noop_hasher_test() {
        // Calculate merkle root hash of merkle path with substitution (noop hasher)
        let m_path = merkle_path(
            vec!["Proof1", "Proof2", "Proof3"],
            vec![Some(1), Some(1), Some(1)],
        );
        let result = m_path.calc_merkle_root::<NoOpHasher>(Some(Hash::from_string("#new#")));
        assert_eq!(
            result.to_string(),
            "Hash[512, [Proof1Child1[Proof2Child1[Proof3Child1#new#Child3]Child3]Child3]]"
                .to_string()
        )
    }

    #[test]
    fn merkle_path_to_root_without_substitution_noop_hasher_test() {
        // Calculate merkle root hash of merkle path without substitution
        let m_path = merkle_path(
            vec!["Proof1", "Proof2", "Proof3"],
            vec![Some(1), Some(1), None],
        );
        let result = m_path.calc_merkle_root::<NoOpHasher>(None);
        assert_eq!(
            result.to_string(),
            "Hash[512, [Proof1Child1[Proof2Child1[Proof3Child1Child2Child3]Child3]Child3]]"
                .to_string()
        )
    }

    fn node_proof(state: &str, idx: Option<usize>) -> NodeProof {
        NodeProof {
            state_hash: Hash(BytesMut::from(state)),
            children_hashes: vec![
                Hash::from_string("Child1"),
                Hash::from_string("Child2"),
                Hash::from_string("Child3"),
            ],
            substitution_idx: idx,
        }
    }

    fn merkle_path(proofs: Vec<&str>, indexes: Vec<Option<usize>>) -> MerklePath {
        let mut m_path = MerklePath::empty();
        for (proof, idx) in proofs.into_iter().zip(indexes) {
            m_path.push(node_proof(proof, idx));
        }
        m_path
    }
}

use std::marker::PhantomData;

use common::merkle::{MerklePath, NodeProof};

use common::{Digest, Hash, Key};
use protocol::ClientPutDetails;

/// Arbiter for checking correctness of Btree server's responses.
pub struct BTreeVerifier<D> {
    _digest: PhantomData<D>,
}

impl<D: Digest> BTreeVerifier<D> {
    pub fn new() -> Self {
        BTreeVerifier {
            _digest: PhantomData::default(),
        }
    }

    /// Verifies that server made correct tree modification.
    /// Returns Some(newRoot) if server pass verifying, None otherwise.
    /// Client can update merkle root if this method returns true.
    pub fn new_merkle_root(
        &self,
        client_m_path: MerklePath,
        put_details: ClientPutDetails,
        server_m_root: Hash,
        was_split: bool,
    ) -> Option<Hash> {
        let new_m_root = if was_split {
            self.calc_m_root_after_rebalancing(client_m_path, put_details, server_m_root.clone())
        } else {
            self.calc_expected_m_root_after_simple_put(client_m_path, put_details)
        };

        if new_m_root == server_m_root {
            log::debug!("Server merkle root is correct");
            Some(new_m_root)
        } else {
            log::warn!(
                "Server made wrong changes! New client merkle root={:?} != server merkle root={:?}",
                new_m_root,
                server_m_root
            );
            None
        }
    }

    /// Checks 'server's proof' correctness. Calculates proof checksums and compares it with expected checksum.
    /// `server_proof` A [[NodeProof]] of branch/leaf for verify from server
    /// `m_root` The merkle root of server tree (provides by client)
    /// `m_path` The merkle path passed from tree root at this moment (provides by client)
    fn check_proof(&self, server_proof: NodeProof, m_root: Hash, m_path: MerklePath) -> bool {
        let server_hash = server_proof.calc_checksum::<D>(None);
        let client_hash = self.expected_checksum(m_root, m_path);

        let verifying_result = server_hash == client_hash;
        if !verifying_result {
            log::warn!(
                "Merkle proof verifying failed! client's={}, server's={}",
                client_hash,
                server_hash
            );
        }
        verifying_result
    }

    /// Builds merkle root for verify server's made changes without rebalancing
    fn calc_expected_m_root_after_simple_put(
        &self,
        mut client_m_path: MerklePath,
        put_details: ClientPutDetails,
    ) -> Hash {
        let mut encrypted_key = Hash::from(put_details.key);
        let val_checksum = Hash::from(put_details.val_hash);
        encrypted_key.concat(val_checksum); // todo hash it before concat?
        let kv_hash = Hash::build::<D, _>(encrypted_key);

        match put_details.search_result {
            Err(_) => client_m_path.calc_merkle_root::<D>(Some(kv_hash)),
            Ok(_) => {
                if client_m_path.is_empty() {
                    client_m_path.add(NodeProof::new(Hash::empty(), vec![kv_hash], Some(0)));
                    client_m_path.calc_merkle_root::<D>(None)
                } else {
                    client_m_path.calc_merkle_root::<D>(Some(kv_hash))
                }
            }
        }
    }

    /// Builds merkle root for verify server's made changes after rebalancing
    fn calc_m_root_after_rebalancing(
        &self,
        _client_m_path: MerklePath,
        _put_details: ClientPutDetails,
        server_m_root: Hash,
    ) -> Hash {
        // todo implement, not it returns server merkle root as is without checking
        server_m_root
    }

    /// Returns [`NodeProof`] for leaf details from server.
    /// `keys` Keys of leaf for verify
    /// `values_checksums` Checksums of leaf values for verify
    fn get_leaf_proof(&self, keys: Vec<Key>, values_checksums: Vec<Hash>) -> NodeProof {
        NodeProof::new_proof::<D>(keys, values_checksums, None)
    }

    /// Returns [[NodeProof]] for branch details from server.
    /// `keys` Keys of branch for verify
    /// `children_checksums` Childs checksum of branch for verify
    /// `substitution_idx` Next child index.
    fn get_branch_proof(
        &self,
        keys: Vec<Key>,
        children_checksums: Vec<Hash>,
        substitution_idx: usize,
    ) -> NodeProof {
        NodeProof::new_proof::<D>(keys, children_checksums, Some(substitution_idx))
    }

    /// Returns expected checksum of next branch that should be returned from server.
    /// We don't nee to check all merkle path, there is enough to check only next proof.
    /// `m_root` The merkle root of server tree
    /// `m_path` The merkle path already passed from tree root
    fn expected_checksum(&self, m_root: Hash, m_path: MerklePath) -> Hash {
        m_path.last_proof_children_hash().cloned().unwrap_or(m_root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::noop_hasher::NoOpHasher;

    #[test]
    fn check_proof_false_test() {
        let _ = env_logger::builder().is_test(true).try_init();

        let verifier = BTreeVerifier::<NoOpHasher>::new();
        let children_hashes = vec![h("h1"), h("h2"), h("h3")];

        // wrong root (correct is Hash[512, [[h1][h2][h3]]])
        let res = verifier.check_proof(
            NodeProof::new(Hash::empty(), children_hashes.clone(), None),
            h("m_root"),
            MerklePath::empty(),
        );
        assert!(!res);

        // wrong state_hash (correct is Hash[512, [[k1k2][h1][h2][h3]]])
        let res = verifier.check_proof(
            NodeProof::new(h("k1k2"), children_hashes.clone(), None),
            h("m_root"),
            MerklePath::empty(),
        );
        assert!(!res);

        // substitution index doesnt make any effect (correct is Hash[512, [[k1k2][h1][h2][h3]]])
        let res = verifier.check_proof(
            NodeProof::new(h("k1k2"), children_hashes.clone(), Some(1)),
            h("m_root"),
            MerklePath::empty(),
        );
        assert!(!res);

        // if client merkle path is not empty, compare client and server (correct is Hash[512, [h2])
        let proof = NodeProof::new(h("k1k2"), children_hashes.clone(), Some(1));
        let res = verifier.check_proof(proof.clone(), h("m_root"), MerklePath::new(proof));
        assert!(!res);
    }

    #[test]
    fn check_proof_true_test() {
        let _ = env_logger::builder().is_test(true).try_init();

        let verifier = BTreeVerifier::<NoOpHasher>::new();
        let children_hashes = vec![h("h1"), h("h2"), h("h3")];

        // client's path is empty, state hash is empty
        let res = verifier.check_proof(
            NodeProof::new(Hash::empty(), children_hashes.clone(), None),
            h("[h1][h2][h3]"),
            MerklePath::empty(),
        );
        assert!(res);

        // client's path is empty, state hash exists
        let res = verifier.check_proof(
            NodeProof::new(h("k1k2"), children_hashes.clone(), None),
            h("[k1k2][h1][h2][h3]"),
            MerklePath::empty(),
        );
        assert!(res);

        // client's path is not empty, state hash exists
        let proof = NodeProof::new(h("k1k2"), children_hashes.clone(), Some(1));
        let res = verifier.check_proof(
            NodeProof::new(Hash::empty(), vec![Hash::from_str("h2")], None),
            h("[k1k2][h1][h2][h3]"),
            MerklePath::new(proof),
        );
        assert!(res);
    }

    fn k(idx: usize) -> String {
        format!("k{}", idx)
    }
    fn h(str: &str) -> Hash {
        Hash::build::<NoOpHasher, _>(str.as_bytes())
    }
}
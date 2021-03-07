use std::marker::PhantomData;

use common::merkle::{MerklePath, NodeProof};

use common::{Hash, Key};
use protocol::btree::{ClientPutDetails, SearchResult};

/// Arbiter for checking correctness of Btree server's responses.
#[derive(Clone, Debug)]
pub struct BTreeVerifier<Digest> {
    _digest: PhantomData<Digest>,
}

impl<Digest: common::Digest> BTreeVerifier<Digest> {
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

    /// Returns [[NodeProof]] for branch details from server.
    /// `keys` Keys of branch for verify
    /// `children_checksums` Childs checksum of branch for verify
    /// `substitution_idx` Next child index.
    pub fn get_branch_proof(
        &self,
        keys: Vec<Key>,
        children_checksums: Vec<Hash>,
        substitution_idx: Option<usize>,
    ) -> NodeProof {
        NodeProof::new_branch_proof::<Digest>(keys, children_checksums, substitution_idx)
    }

    /// Returns [`NodeProof`] for leaf details from server.
    /// `keys` Keys of leaf for verify
    /// `values_checksums` Checksums of leaf values for verify
    pub fn get_leaf_proof(&self, keys: Vec<Key>, values_checksums: Vec<Hash>) -> NodeProof {
        NodeProof::new_leaf_proof::<Digest>(&keys, &values_checksums, None)
    }

    /// Checks 'server's proof' correctness. Calculates proof checksums and compares it with expected checksum.
    /// `server_proof` A [[NodeProof]] of branch/leaf for verify from server
    /// `m_root` The merkle root of server tree (provides by client)
    /// `m_path` The merkle path passed from tree root at this moment (provides by client)
    pub fn check_proof(&self, server_proof: NodeProof, m_root: Hash, m_path: &MerklePath) -> bool {
        log::debug!(
            "check_proof server_proof={:?}, m_root={:?}, m_path={:?}",
            server_proof,
            m_root,
            m_path,
        );
        let server_hash = MerklePath::new(server_proof).calc_merkle_root::<Digest>(None);
        let client_hash = self.expected_checksum(m_root, m_path.clone());

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
        let mut encrypted_key = Hash::build::<Digest, _>(put_details.key);
        let val_checksum = Hash::from(put_details.val_hash);
        encrypted_key.concat(val_checksum);
        let kv_hash = Hash::build::<Digest, _>(encrypted_key);

        match put_details.search_result {
            SearchResult(Err(idx)) => {
                assert!(
                    !client_m_path.is_empty(),
                    "Client m_path should have at least one proof at this moment"
                );
                // we make fake insert: add hash to proof by idx from put_details and build root
                client_m_path.insert_child_hash_to_last_proof(kv_hash.clone(), idx);
                client_m_path.calc_merkle_root::<Digest>(Some(kv_hash)) // todo consider None instead
            }
            SearchResult(Ok(_)) => {
                // replace old value with new one
                client_m_path.calc_merkle_root::<Digest>(Some(kv_hash))
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

    /// Returns expected checksum of next branch that should be returned from server.
    /// We don't nee to check all merkle path, there is enough to check only next proof.
    /// `m_root` The merkle root of server tree
    /// `m_path` The merkle path already passed from tree root
    fn expected_checksum(&self, m_root: Hash, m_path: MerklePath) -> Hash {
        m_path.last_proof_children_hash().cloned().unwrap_or(m_root)
    }
}

impl<Digest: common::Digest> Default for BTreeVerifier<Digest> {
    fn default() -> Self {
        BTreeVerifier::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::misc::ToBytes;
    use common::noop_hasher::NoOpHasher;
    use log::LevelFilter;
    use protocol::btree::SearchResult;

    #[test]
    #[should_panic]
    fn check_proof_illegal_state_test() {
        // case when substitution index define and substitution hash isn't defined
        let verifier = BTreeVerifier::<NoOpHasher>::new();
        let children_hashes = vec![h("h1"), h("h2"), h("h3")];

        // substitution index doesnt make any effect (correct is Hash[512, [[k1k2][h1][h2][h3]]])
        let _res = verifier.check_proof(
            NodeProof::new(h("[[k1][k2]]"), children_hashes.clone(), Some(1)),
            h("m_root"),
            &MerklePath::empty(),
        );
    }

    #[test]
    fn check_proof_false_test() {
        init_logger();
        let verifier = BTreeVerifier::<NoOpHasher>::new();
        let children_hashes = vec![h("h1"), h("h2"), h("h3")];

        // wrong root (correct is Hash[512, [[h1][h2][h3]]])
        let res = verifier.check_proof(
            NodeProof::new(Hash::empty(), children_hashes.clone(), None),
            h("m_root"),
            &MerklePath::empty(),
        );
        assert!(!res);

        // wrong state_hash (correct is Hash[512, [[k1k2][h1][h2][h3]]])
        let res = verifier.check_proof(
            NodeProof::new(h("k1k2"), children_hashes.clone(), None),
            h("m_root"),
            &MerklePath::empty(),
        );
        assert!(!res);

        // if client merkle path is not empty, compare client and server (correct is Hash[512, [h2])
        let proof = NodeProof::new(h("k1k2"), children_hashes.clone(), None);
        let res = verifier.check_proof(proof.clone(), h("m_root"), &MerklePath::new(proof));
        assert!(!res);
    }

    #[test]
    fn check_proof_true_test() {
        init_logger();

        let verifier = BTreeVerifier::<NoOpHasher>::new();
        let children_hashes = vec![h("h1"), h("h2"), h("h3")];

        // client's path is empty, state hash is empty
        let res = verifier.check_proof(
            NodeProof::new(Hash::empty(), children_hashes.clone(), None),
            h("[h1][h2][h3]"),
            &MerklePath::empty(),
        );
        assert!(res);

        // client's path is empty, state hash exists
        let res = verifier.check_proof(
            NodeProof::new(h("k1k2"), children_hashes.clone(), None),
            h("[k1k2][h1][h2][h3]"),
            &MerklePath::empty(),
        );
        assert!(res);

        // client's path is not empty, state hash exists
        let proof = NodeProof::new(h("[k1][k2]"), children_hashes.clone(), Some(1));
        let res = verifier.check_proof(
            NodeProof::new(Hash::empty(), vec![Hash::from_str("h2")], None),
            h("[[k1][k2]][h1][h2][h3]"),
            &MerklePath::new(proof),
        );
        assert!(res);
    }

    #[test]
    fn simple_put_new_merkle_root_empty_tree_test() {
        // simple put putting into empty tree
        let verifier = BTreeVerifier::<NoOpHasher>::new();

        let client_path = MerklePath::empty();
        let put_details = put_det("k1", "h1", SearchResult(Ok(0)));
        let server_root = h("[k1][h1]");

        let res = verifier.new_merkle_root(client_path, put_details, server_root.clone(), false);
        assert_eq!(res, Some(server_root));
    }

    #[test]
    fn simple_put_new_merkle_root_insert_new_value_test() {
        init_logger();
        let verifier = BTreeVerifier::<NoOpHasher>::new();

        // insert new value
        let client_path =
            MerklePath::new(NodeProof::new(Hash::empty(), vec![h("[k2][h2]")], Some(0)));
        let put_details = put_det("k1", "h1", SearchResult(Err(0)));
        let server_root = h("[[k1][h1]][[k2][h2]]");

        let res = verifier.new_merkle_root(client_path, put_details, server_root.clone(), false);
        assert_eq!(res, Some(server_root));
    }

    #[test]
    fn simple_put_new_merkle_root_rewrite_value_test() {
        // rewrite old value with new value
        let verifier = BTreeVerifier::<NoOpHasher>::new();

        let client_path =
            MerklePath::new(NodeProof::new(Hash::empty(), vec![h("[k2][h2]")], Some(0)));
        let put_details = put_det("k2", "h#", SearchResult(Ok(0)));
        let server_root = h("[[k2][h#]]");

        let res = verifier.new_merkle_root(client_path, put_details, server_root.clone(), false);
        assert_eq!(res, Some(server_root));
    }

    #[test]
    fn simple_put_new_merkle_root_rewrite_2_lvl_value_test() {
        // rewrite old value with new value in second tree lvl
        init_logger();
        let verifier = BTreeVerifier::<NoOpHasher>::new();

        let root_children_hashes = vec![h("[[k1][h1]][[k2][h2]]"), h("[[k4][h4]][[k5][h5]]")];
        let root_proof = verifier.get_branch_proof(vec![k("k2")], root_children_hashes, Some(0));
        let leaf_proof = NodeProof::new(Hash::empty(), vec![h("[k1][h1]"), h("[k2][h2]")], Some(1));
        let client_path = MerklePath(vec![root_proof, leaf_proof]);
        let put_details = put_det("k2", "h#", SearchResult(Ok(0)));
        let server_root = h("[k2][[[k1][h1]][[k2][h#]]][[[k4][h4]][[k5][h5]]]");

        let res = verifier.new_merkle_root(client_path, put_details, server_root.clone(), false);
        assert_eq!(res, Some(server_root));
    }

    #[test]
    fn simple_put_new_merkle_root_insert_2_lvl_value_test() {
        // insert new value in second tree lvl
        let verifier = BTreeVerifier::<NoOpHasher>::new();

        let root_children_hashes = vec![h("[[k1][h1]][[k2][h2]]"), h("[[k4][h4]][[k5][h5]]")];
        let root_proof = verifier.get_branch_proof(vec![k("k2")], root_children_hashes, Some(1));
        let leaf_proof = NodeProof::new(Hash::empty(), vec![h("[k4][h4]"), h("[k5][h5]")], Some(0));
        let client_path = MerklePath(vec![root_proof, leaf_proof]);
        let put_details = put_det("k3", "h#", SearchResult(Err(0)));
        let server_root = h("[k2][[[k1][h1]][[k2][h2]]][[[k3][h#]][[k4][h4]][[k5][h5]]]");

        let res = verifier.new_merkle_root(client_path, put_details, server_root.clone(), false);
        assert_eq!(res, Some(server_root));
    }
    #[test]
    fn simple_put_new_merkle_root_fail_check_test() {
        // wrong root
        let verifier = BTreeVerifier::<NoOpHasher>::new();

        let client_path = MerklePath::empty();
        let put_details = put_det("k1", "h1", SearchResult(Ok(0)));
        let server_root = h("wrong server root");

        let res = verifier.new_merkle_root(client_path, put_details, server_root.clone(), false);
        assert_eq!(res, None);
    }

    #[test]
    fn rebalancing_new_merkle_root_empty_tree_test() {
        // todo add more test for verify after rebalancing
    }

    fn put_det(key: &str, val: &str, sr: SearchResult) -> ClientPutDetails {
        ClientPutDetails::new(k(key).bytes(), h(val).bytes(), sr)
    }
    fn k(str: &str) -> Key {
        Key::from_str(str)
    }
    fn h(str: &str) -> Hash {
        Hash::build::<NoOpHasher, _>(str.as_bytes())
    }

    fn init_logger() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Debug)
            .try_init();
    }
}

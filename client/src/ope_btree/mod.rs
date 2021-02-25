#![allow(dead_code)] // todo remove later

use common::merkle::MerklePath;
use common::Hash;

use crate::crypto::cypher_search::CipherSearch;

use crate::crypto::{Decryptor, Encryptor};
use crate::ope_btree::btree_verifier::BTreeVerifier;
use crate::ope_btree::errors::ClientBTreeError;
use crate::ope_btree::put_state::PutState;
use crate::ope_btree::search_state::SearchState;
use protocol::SearchResult;
use std::fmt::Debug;

pub mod btree_verifier;
pub mod errors;
mod put_state;
mod search_state;

pub type Result<V> = std::result::Result<V, ClientBTreeError>;

/// Client to calls for a remote MerkleBTree.
///  Note that this version is single-thread for Put operation, and multi-thread for Get operation.
///
/// `m_root` Current merkle root. For new dataset should be None
/// `key_crypt` Encrypting/decrypting provider for Key
/// `verifier` Arbiter for checking correctness of Btree server responses.
/// `signer` Algorithm to produce signatures. Used for sealing current state by data owner
pub struct OpeBTreeClient<Crypt, Digest> {
    m_root: Hash,
    key_crypt: Crypt,
    verifier: BTreeVerifier<Digest>,
    searcher: CipherSearch<Crypt>,
    signer: (), // todo implement
}

impl<Key, Crypt, Digest> OpeBTreeClient<Crypt, Digest>
where
    Key: Ord + Clone + Debug,
    Crypt: Encryptor<PlainData = Key> + Decryptor<PlainData = Key> + Clone,
    Digest: common::Digest + Clone,
{
    pub fn new(m_root: Hash, key_crypt: Crypt, signer: ()) -> Self {
        let searcher = CipherSearch::new(key_crypt.clone());
        let verifier = BTreeVerifier::new();

        OpeBTreeClient {
            m_root,
            key_crypt,
            verifier,
            searcher,
            signer,
        }
    }

    ///
    /// Returns callbacks for finding 'value' by specified 'key' in remote OpeBTree.
    ///
    pub async fn init_get(&self, key: Key) -> SearchState<Key, Digest, Crypt> {
        log::debug!("init_get starts for key={:?}", key);
        self.build_search_state(key)
    }

    /// Returns callbacks for saving encrypted 'key' and 'value' into remote OpeBTree.
    ///
    /// `key` Plain text key
    /// `value_checksum` Checksum of encrypted value to be store
    /// `version`  Dataset version expected to the client
    ///
    pub async fn init_put(
        &self,
        key: Key,
        value_checksum: Hash,
        version: usize,
    ) -> PutState<Key, Digest, Crypt> {
        log::debug!(
            "init_put starts put for key={:?}, value={:?}, version={:?}",
            key,
            value_checksum,
            version
        );
        self.build_put_state(key, value_checksum, version)
    }

    fn build_searcher(&self) -> Searcher<Digest, Crypt> {
        Searcher {
            verifier: self.verifier.clone(),
            cipher_search: self.searcher.clone(),
        }
    }

    fn build_search_state(&self, key: Key) -> SearchState<Key, Digest, Crypt> {
        SearchState::new(key, self.m_root.clone(), self.build_searcher())
    }

    fn build_put_state(
        &self,
        key: Key,
        value_checksum: Hash,
        version: usize,
    ) -> PutState<Key, Digest, Crypt> {
        let searcher = self.build_searcher();
        PutState::new(
            key,
            value_checksum,
            self.m_root.clone(),
            MerklePath::empty(),
            version,
            searcher,
            self.key_crypt.clone(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct Searcher<Digest, Decryptor> {
    pub verifier: BTreeVerifier<Digest>,
    cipher_search: CipherSearch<Decryptor>,
}

impl<Key, Digest, Dec> Searcher<Digest, Dec>
where
    Key: Ord + Debug,
    Digest: common::Digest + Clone,
    Dec: Decryptor<PlainData = Key>,
{
    /// Verifies merkle proof for server response, after that search index of next child of branch.
    ///
    /// `key` Plain text key (by client)
    /// `m_root` Merkle root for current request (by client)
    /// `m_path` Merkle path for current request (by client)
    /// `keys` Encrypted keys for deciding next tree child (from server)
    /// `children_hashes` Checksums of current branch children, for merkle proof checking (from server)
    ///
    /// Returns a tuple with updated merkle path and searched next tree child index
    pub fn search_in_branch(
        &self,
        key: Key,
        m_root: Hash,
        mut m_path: MerklePath,
        keys: Vec<common::Key>,
        children_hashes: Vec<Hash>,
    ) -> Result<(MerklePath, usize)> {
        let mut node_proof =
            self.verifier
                .get_branch_proof(keys.clone(), children_hashes.clone(), None);
        let valid_proof =
            self.verifier
                .check_proof(node_proof.clone(), m_root.clone(), m_path.clone());

        if valid_proof {
            let search_result = self.cipher_search.binary_search(&keys, key)?;
            let insertion_point = search_result.idx();
            node_proof.set_idx(insertion_point);
            m_path.push_head(node_proof);
            Ok((m_path, insertion_point))
        } else {
            Err(ClientBTreeError::wrong_proof(
                key,
                keys.clone(),
                children_hashes,
                m_root,
            ))
        }
    }

    /// Verifies merkle proof for server response, after that search index of key in leaf.
    ///
    /// `key` Plain text key (by client)
    /// `m_root` Merkle root for current request (by client)
    /// `m_path` Merkle path for current request (by client)
    /// `keys` Encrypted keys for deciding next tree child (from server)
    /// `children_hashes` Checksums of current leaf's values, for merkle proof checking (from server)
    ///
    /// Returns a tuple with updated merkle path and SearchResult
    pub fn search_in_leaf(
        &self,
        key: Key,
        m_root: Hash,
        mut m_path: MerklePath,
        keys: Vec<common::Key>,
        children_hashes: Vec<Hash>,
    ) -> Result<(MerklePath, SearchResult)> {
        let mut leaf_proof = self
            .verifier
            .get_leaf_proof(keys.clone(), children_hashes.clone());

        let valid_proof =
            self.verifier
                .check_proof(leaf_proof.clone(), m_root.clone(), m_path.clone());

        if valid_proof {
            let search_result = self.cipher_search.binary_search(&keys, key)?;
            leaf_proof.set_idx(search_result.idx());
            m_path.push_head(leaf_proof);
            Ok((m_path, search_result))
        } else {
            Err(ClientBTreeError::wrong_proof(
                key,
                keys.clone(),
                children_hashes,
                m_root,
            ))
        }
    }
}

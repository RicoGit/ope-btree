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
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod btree_verifier;
pub mod errors;
#[cfg(test)]
mod integration_test;
mod put_state;
mod search_state;

pub type Result<V> = std::result::Result<V, ClientBTreeError>;

/// Client to calls for a remote MerkleBTree.
///  Note that this version is single-thread for Put operation, and multi-thread for Get operation.
pub struct OpeBTreeClient<Crypt, Digest> {
    /// Encrypting/decrypting provider for Key
    key_crypt: Crypt,
    /// Arbiter for checking correctness of Btree server responses.
    verifier: BTreeVerifier<Digest>,
    /// Search over encrypted data algorithm
    searcher: CipherSearch<Crypt>,
    /// Algorithm to produce signatures. Used for sealing current state by data owner
    signer: (), // todo implement
    /// Client's state, actually current merkle root. For new dataset should be None.
    state: Arc<RwLock<State>>,
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
            key_crypt,
            verifier,
            searcher,
            signer,
            state: State::new(m_root),
        }
    }

    ///
    /// Returns callbacks for finding 'value' by specified 'key' in remote OpeBTree.
    ///
    pub async fn init_get(&self, key: Key) -> SearchState<'_, Key, Digest, Crypt> {
        log::debug!("init_get starts for key={:?}", key);
        self.build_search_state(key).await
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
    ) -> PutState<'_, Key, Digest, Crypt> {
        log::debug!(
            "init_put starts put for key={:?}, value={:?}, version={:?}",
            key,
            value_checksum,
            version
        );
        self.build_put_state(key, value_checksum, version).await
    }

    fn build_searcher(&self) -> Searcher<Digest, Crypt> {
        Searcher {
            verifier: self.verifier.clone(),
            cipher_search: self.searcher.clone(),
        }
    }

    async fn build_search_state(&self, key: Key) -> SearchState<'_, Key, Digest, Crypt> {
        let guard = self.state.read().await;
        SearchState::new(key, guard, self.build_searcher())
    }

    async fn build_put_state(
        &self,
        key: Key,
        value_checksum: Hash,
        version: usize,
    ) -> PutState<'_, Key, Digest, Crypt> {
        let searcher = self.build_searcher();
        let guard = self.state.write().await;

        PutState::new(
            key,
            value_checksum,
            MerklePath::empty(),
            version,
            searcher,
            self.key_crypt.clone(),
            guard,
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
        m_root: &Hash,
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
            m_path.push(node_proof);
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
        m_root: &Hash,
        mut m_path: MerklePath, // todo make &mut anf remove from return value
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
            m_path.push(leaf_proof);
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

#[derive(Clone, Debug)]
pub struct State {
    m_root: Hash,
}

impl State {
    pub fn new(m_root: Hash) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(State { m_root }))
    }

    pub fn empty() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(State {
            m_root: Hash::empty(),
        }))
    }
}

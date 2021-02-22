#![allow(dead_code)] // todo remove later

use common::merkle::MerklePath;
use common::{Digest, Hash};

use crate::crypto::cypher_search::CipherSearch;

use crate::crypto::Decryptor;
use crate::ope_btree::btree_verifier::BTreeVerifier;
use crate::ope_btree::errors::ClientBTreeError;
use std::fmt::Debug;

pub mod btree_verifier;
pub mod errors;
mod search_state;

pub type Result<V> = std::result::Result<V, ClientBTreeError>;

// todo OpeBTreeClient

pub struct Searcher<Digest, Decryptor> {
    verifier: BTreeVerifier<Digest>,
    cipher_search: CipherSearch<Decryptor>,
}

impl<Key, D, DD> Searcher<D, DD>
where
    Key: Ord + Debug,
    D: Digest,
    DD: Decryptor<PlainData = Key>,
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
    async fn search(
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
}

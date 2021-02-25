use std::fmt::{Debug, Formatter};

use bytes::Bytes;

use crate::crypto::Decryptor;

use crate::ope_btree::Searcher;
use common::merkle::MerklePath;
use common::Hash;
use futures::future::FutureExt;
use protocol::{BtreeCallback, RpcFuture, SearchCallback, SearchResult};

// todo make State enum, enum State { Search (...), Put {...} } ?

/// State for each search ('Get', 'Range', 'Delete') request to remote BTree.
/// One 'SearchState' corresponds to one series of round trip requests.
pub struct SearchState<Key, Digest, Decryptor> {
    /// The search plain text 'key'. Constant for round trip session
    key: Key,
    /// Copy of client merkle root at the beginning of the request. Constant for round trip session
    m_root: Hash,
    /// Client's merkle path.Client during the traversing creates own version of merkle path
    m_path: MerklePath,
    /// Provides search over encrypted data
    searcher: Searcher<Digest, Decryptor>,
}

impl<Key, Digest: Clone, Dec> SearchState<Key, Digest, Dec> {
    pub fn new(key: Key, m_root: Hash, searcher: Searcher<Digest, Dec>) -> Self {
        SearchState {
            key,
            m_root,
            m_path: MerklePath::empty(),
            searcher,
        }
    }
}

impl<Key, Digest, Dec> BtreeCallback for SearchState<Key, Digest, Dec>
where
    Key: Ord + Debug + Clone + Send,
    Digest: common::Digest + Clone,
    Dec: Decryptor<PlainData = Key>,
{
    /// Case when server asks next child
    fn next_child_idx<'f>(
        &mut self,
        keys: Vec<Bytes>,
        children_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize> {
        log::debug!(
            "next_child_idx starts for {:?}, keys={:?}, children_hashes={:?}",
            self,
            keys,
            children_hashes
        );

        let result = self
            .searcher
            .search_in_branch(
                // let (updated_m_path, idx) = self.searcher.search(
                self.key.clone(),
                self.m_root.clone(),
                self.m_path.clone(),
                keys.into_iter().map(common::Key::from).collect(),
                children_hashes.into_iter().map(Hash::from).collect(),
            )
            .map(|(m_path, idx)| {
                self.m_path = m_path;
                idx
            })
            .map_err(Into::into);

        async move { result }.boxed()
    }
}

impl<Key, Digest, Dec> SearchCallback for SearchState<Key, Digest, Dec>
where
    Key: Ord + Debug + Clone + Send,
    Digest: common::Digest + Clone,
    Dec: Decryptor<PlainData = Key>,
{
    /// Case when server returns founded leaf, this leaf either contains key,
    /// or new key may be inserted in this leaf
    fn submit_leaf<'f>(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, SearchResult> {
        log::debug!(
            "submit_leaf starts for {:?} keys={:?}, value_hashes:{:?}",
            self,
            keys,
            values_hashes
        );

        let result = self
            .searcher
            .search_in_leaf(
                self.key.clone(),
                self.m_root.clone(),
                self.m_path.clone(),
                keys.into_iter().map(common::Key::from).collect(),
                values_hashes.into_iter().map(Hash::from).collect(),
            )
            .map(|(m_path, search_res)| {
                self.m_path = m_path;
                search_res
            })
            .map_err(Into::into);

        async move { result }.boxed()
    }
}

impl<K: Debug, D, Dec> Debug for SearchState<K, D, Dec> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SearchState(key:{:?}, m_root:{:?}, m_path:{:?})",
            self.key, self.m_root, self.m_path
        )
    }
}

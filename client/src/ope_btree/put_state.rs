use crate::crypto::{Decryptor, Encryptor};
use crate::ope_btree::Searcher;
use bytes::Bytes;
use common::merkle::MerklePath;
use common::misc::ToBytes;
use common::{Digest, Hash};
use futures::FutureExt;
use protocol::{BtreeCallback, ClientPutDetails, PutCallbacks, RpcFuture};
use std::fmt::{Debug, Formatter};

/// State for each 'Put' request to remote BTree. One 'PutState' corresponds
/// to one series of round trip requests
pub struct PutState<Key, Digest, Decryptor, Encryptor> {
    /// The search plain text 'key'
    key: Key,
    /// Checksum of encrypted value to be store
    value_checksum: Hash,
    /// Copy of client merkle root at the beginning of the request
    m_root: Hash,
    /// Client's merkle path.Client during the traversing creates own version of merkle path
    m_path: MerklePath,
    /// Dataset version expected to the client
    version: usize,
    /// Provides search over encrypted data
    searcher: Searcher<Digest, Decryptor>,
    /// Encrypts keys
    encryptor: Encryptor,
}

impl<Key: Debug + Clone, D, Dec, Enc> PutState<Key, D, Dec, Enc> {
    fn new(
        key: Key,
        value_checksum: Hash,
        version: usize,
        m_root: Hash,
        m_path: MerklePath,
        searcher: Searcher<D, Dec>,
        encryptor: Enc,
    ) -> Self {
        PutState {
            key,
            value_checksum,
            m_path,
            m_root,
            version,
            searcher,
            encryptor,
        }
    }
}

impl<Key, D, Dec, Enc> BtreeCallback for PutState<Key, D, Dec, Enc>
where
    Key: Ord + Debug + Clone + Send,
    D: Digest,
    Dec: Decryptor<PlainData = Key>,
    Enc: Encryptor<PlainData = Key>,
{
    /// Case when server asks next child
    fn next_child_idx<'f>(
        &mut self,
        keys: Vec<Bytes>,
        children_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize> {
        log::debug!(
            "next_child_idx starts for {:?}, keys={:?}, children_hashes={:?}",
            &self,
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

impl<Key, D, Dec, Enc> PutCallbacks for PutState<Key, D, Dec, Enc>
where
    Key: Ord + Debug + Clone + Send,
    D: Digest,
    Dec: Decryptor<PlainData = Key>,
    Enc: Encryptor<PlainData = Key>,
{
    /// Case when server returns founded leaf
    fn put_details<'f>(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, ClientPutDetails> {
        log::debug!(
            "put_details starts for {:?}, keys:{:?}, values_hashes:{:?}",
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
            .and_then(|(mut m_path, search_res)| {
                // update idx in last proof
                m_path
                    .0
                    .last_mut()
                    .map(|proof| proof.set_idx(search_res.idx()));
                // update m_path
                self.m_path = m_path;
                self.encryptor
                    .encrypt(self.key.clone())
                    .map(|encrypted_key| {
                        ClientPutDetails::new(
                            encrypted_key,
                            self.value_checksum.clone().bytes(),
                            search_res,
                        )
                    })
                    .map_err(Into::into)
            })
            .map_err(Into::into);

        async move { result }.boxed()
    }

    fn verify_changes<'f>(
        &self,
        _server_merkle_root: Bytes,
        _was_splitting: bool,
    ) -> RpcFuture<'f, Bytes> {
        unimplemented!()
    }

    fn changes_stored<'f>(&self) -> RpcFuture<'f, ()> {
        unimplemented!()
    }
}

impl<K: Debug, D, Dec, Enc> Debug for PutState<K, D, Dec, Enc> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PutState(key:{:?}, hash:{:?}, m_root:{:?}, m_path:{:?}, version:{:?})",
            self.key, self.value_checksum, self.m_root, self.m_path, self.version
        )
    }
}

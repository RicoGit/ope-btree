use crate::crypto::{Decryptor, Encryptor};
use crate::ope_btree::errors::BTreeClientError;
use crate::ope_btree::{Searcher, State};
use bytes::Bytes;
use common::merkle::MerklePath;
use common::misc::ToBytes;
use common::Hash;
use futures::FutureExt;
use protocol::btree::{BtreeCallback, ClientPutDetails, PutCallback};
use protocol::RpcFuture;
use std::fmt::{Debug, Formatter};
use tokio::sync::RwLockWriteGuard;

/// State for each 'Put' request to remote BTree. One 'PutState' corresponds
/// to one series of round trip requests
pub struct PutState<'a, Key, Digest, Crypt> {
    /// The search plain text 'key'
    key: Key,
    /// Checksum of encrypted value to be store
    value_checksum: Hash,
    /// Client's merkle path.Client during the traversing creates own version of merkle path
    m_path: MerklePath,
    /// Dataset version expected to the client
    version: usize,
    /// All details needed for putting key and value to BTree
    put_details: Option<ClientPutDetails>,

    /// Provides search over encrypted data
    searcher: Searcher<Digest, Crypt>,
    /// Encrypts keys
    encryptor: Crypt,

    /// Write guard for current client state, while write guards are kept, other operations can't be start.
    /// Contains client's merkle root at the beginning of the request. Constant for round trip session.
    /// After it will be dropped OpeBTree client will be able to make next request.
    state: RwLockWriteGuard<'a, State>,
}

impl<'a, Key, Digest, Crypt> PutState<'a, Key, Digest, Crypt> {
    pub fn new(
        key: Key,
        value_checksum: Hash,
        m_path: MerklePath,
        version: usize,
        searcher: Searcher<Digest, Crypt>,
        encryptor: Crypt,
        state: RwLockWriteGuard<'a, State>,
    ) -> Self {
        PutState {
            key,
            value_checksum,
            m_path,
            version,
            put_details: None, // will filled on put_details step
            searcher,
            encryptor,
            state,
        }
    }

    pub fn m_root(&self) -> &Hash {
        &self.state.m_root
    }

    /// Safes new merkle root on the client
    fn update_m_root(&mut self, new_m_root: Hash) {
        self.state.m_root = new_m_root;
    }
}

impl<'a, Key, Digest, Crypt> BtreeCallback for PutState<'a, Key, Digest, Crypt>
where
    Key: Ord + Debug + Clone + Send,
    Digest: common::Digest,
    Crypt: Decryptor<PlainData = Key> + Encryptor<PlainData = Key>,
{
    /// Case when server asks next child
    fn next_child_idx(
        &mut self,
        keys: Vec<Bytes>,
        children_checksums: Vec<Bytes>,
    ) -> RpcFuture<usize> {
        log::debug!(
            "next_child_idx starts for {:?}, keys={:?}, children_hashes={:?}",
            &self,
            keys,
            children_checksums
        );

        let result = self
            .searcher
            .search_in_branch(
                self.key.clone(),
                self.m_root().clone(),
                &mut self.m_path,
                keys.into_iter().map(common::Key::from).collect(),
                children_checksums.into_iter().map(Hash::from).collect(),
            )
            .map_err(Into::into);

        async move { result }.boxed()
    }
}

impl<'a, Key, Digest, Crypt> PutCallback for PutState<'a, Key, Digest, Crypt>
where
    Key: Ord + Debug + Clone + Send,
    Digest: common::Digest,
    Crypt: Decryptor<PlainData = Key> + Encryptor<PlainData = Key>,
{
    /// Case when server returns founded leaf
    fn put_details(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<ClientPutDetails> {
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
                self.m_root().clone(),
                &mut self.m_path,
                keys.into_iter().map(common::Key::from).collect(),
                values_hashes.into_iter().map(Hash::from).collect(),
            )
            .and_then(|search_res| {
                // update idx in last proof if exists

                if let Some(proof) = self.m_path.0.last_mut() {
                    proof.set_idx(search_res.idx())
                }

                self.encryptor
                    .encrypt(self.key.clone())
                    .map(|encrypted_key| {
                        let details = ClientPutDetails::new(
                            encrypted_key,
                            self.value_checksum.clone().bytes(),
                            search_res,
                        );
                        self.put_details.replace(details.clone());
                        details
                    })
                    .map_err(Into::into)
            })
            .map_err(Into::into);

        async move { result }.boxed()
    }

    /// Case when server asks verify made changes
    fn verify_changes<'f>(
        &mut self,
        server_merkle_root: Bytes,
        was_split: bool,
    ) -> RpcFuture<'f, Bytes> {
        log::debug!(
            "verify_changes starts for {:?}, server_merkle_root:{:?}, was_split:{:?}",
            self,
            server_merkle_root,
            was_split
        );

        if let Some(put_details) = self.put_details.clone() {
            if let Some(new_m_root) = self.searcher.verifier.new_merkle_root(
                self.m_path.clone(),
                put_details,
                Hash::from(server_merkle_root.clone()),
                was_split,
            ) {
                // all is fine, send Confirm to server

                // todo here client should signs version with new merkle root
                // todo it looks like not responsibility of PutState, it should be moved outside,
                // todo for example as callback like 'make_signature_made_changes' or smth like that
                let signed_state = Bytes::new();

                // safe new merkle root on the client
                self.update_m_root(new_m_root);

                async move { Ok(signed_state) }.boxed()
            } else {
                // server was failed verification
                let error = BTreeClientError::wrong_put_proof(&self, &server_merkle_root).into();
                async move { Err(error) }.boxed()
            }
        } else {
            // illegal state from prev step
            let error = BTreeClientError::illegal_state(&self.key, self.m_root()).into();
            async move { Err(error) }.boxed()
        }
    }

    /// Case when server confirmed changes persisted
    fn changes_stored<'f>(&mut self) -> RpcFuture<'f, ()> {
        // change global client state with new merkle root
        log::debug!("changes_stored starts for state={:?}", self);
        async { Ok(()) }.boxed()
    }
}

impl<K: Debug, Digest, Crypt> Debug for PutState<'_, K, Digest, Crypt> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PutState(key:{:?}, hash:{:?}, m_root:{:?}, m_path:{:?}, version:{:?})",
            self.key, self.value_checksum, self.state, self.m_path, self.version
        )
    }
}

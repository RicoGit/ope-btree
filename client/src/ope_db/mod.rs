use crate::crypto::{Decryptor, Encryptor};
use crate::ope_btree::OpeBTreeClient;
use crate::ope_db::errors::DatabaseClientError;
use bytes::Bytes;
use common::Hash;
use protocol::database::OpeDatabaseRpc;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};

mod errors;

pub type Result<V> = std::result::Result<V, DatabaseClientError>;

/// Client that provides data access to Encrypted database.
pub struct OpeDatabaseClient<KeyCrypt, ValCrypt, Digest, Rpc> {
    index: OpeBTreeClient<KeyCrypt, Digest>,
    val_crypt: ValCrypt,
    storage: Rpc,
    version: AtomicUsize,
    dataset_id: Bytes,
}

impl<Key, Val, KeyCrypt, ValCrypt, Digest, Rpc> OpeDatabaseClient<KeyCrypt, ValCrypt, Digest, Rpc>
where
    Key: Ord + Send + Clone + Debug,
    Val: Clone + Debug,
    KeyCrypt: Encryptor<PlainData = Key> + Decryptor<PlainData = Key> + Clone + Send,
    ValCrypt: Encryptor<PlainData = Val> + Decryptor<PlainData = Val>,
    Digest: common::Digest + Clone + Send,
    Rpc: OpeDatabaseRpc,
{
    pub fn new(
        index: OpeBTreeClient<KeyCrypt, Digest>,
        val_crypt: ValCrypt,
        storage: Rpc,
        version: AtomicUsize,
        dataset_id: Bytes,
    ) -> Self {
        OpeDatabaseClient {
            index,
            val_crypt,
            storage,
            version,
            dataset_id,
        }
    }

    /// Gets stored value for specified key.
    ///
    /// `key` The key retrieve the value.
    ///
    /// Returns found value, None if nothing was found.
    pub async fn get(&mut self, key: Key) -> Result<Option<Val>> {
        let version = self.version.load(Ordering::SeqCst);
        let callback = self.index.init_get(key).await;
        let response = self
            .storage
            .get(self.dataset_id.clone(), version, callback)
            .await?;
        Ok(self.decrypt(response)?)
    }

    /// Puts key value pair (K, V). Update existing value if it's present.
    ///
    /// `key` The specified key to be inserted
    /// `value` The value associated with the specified key
    ///
    /// Returns old value if old value was overridden, None otherwise.
    pub async fn put(&mut self, key: Key, val: Val) -> Result<Option<Val>> {
        let version = self.version.load(Ordering::SeqCst);
        let enc_value = self.val_crypt.encrypt(val)?;
        let value_hash = Hash::build::<Digest, _>(enc_value.clone());
        let callback = self.index.init_put(key, value_hash, version).await;

        let response = self
            .storage
            .put(self.dataset_id.clone(), version, callback, enc_value)
            .await?;

        self.version.fetch_add(1, Ordering::SeqCst);

        Ok(self.decrypt(response)?)
    }

    fn decrypt(&self, response: Option<Bytes>) -> Result<Option<Val>> {
        let res = response
            .map(|bytes| self.val_crypt.decrypt(bytes.as_ref()))
            .transpose()?;
        Ok(res)
    }
}

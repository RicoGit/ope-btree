use crate::crypto::{Decryptor, Encryptor};
use crate::ope_btree::OpeBTreeClient;
use crate::ope_db::config::ClientConfig;
use crate::ope_db::errors::DatabaseClientError;
use bytes::Bytes;
use common::Hash;
use protocol::database::OpeDatabaseRpc;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod config;
mod errors;

pub type Result<V> = std::result::Result<V, DatabaseClientError>;

/// Client that provides data access to Encrypted database.
pub struct OpeDatabaseClient<KeyCrypt, ValCrypt, Digest, Rpc> {
    index: OpeBTreeClient<KeyCrypt, Digest>,
    val_crypt: ValCrypt,
    /// Remote server API
    storage: Rpc,
    /// Current client config
    config: ClientConfig,
    /// What actually dataset is querying right now
    current_dataset: Arc<RwLock<Option<DatasetState>>>, // todo sync m_root between this state and index state
    /// Signs a state on behalf of the client (approved by client state)
    signer: (), // todo impl later
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
        config: ClientConfig,
    ) -> Self {
        OpeDatabaseClient {
            index,
            val_crypt,
            storage,
            config,
            current_dataset: Arc::new(RwLock::new(None)),
            signer: (),
        }
    }

    /// Switches to specified dataset for querying.
    pub async fn dataset(&mut self, dataset_id: &str) -> Option<()> {
        let current_dataset = DatasetState::read_from_config(dataset_id, &self.config)?;
        self.current_dataset.write().await.replace(current_dataset);
        Some(())
    }

    pub fn new_dataset(&mut self, _name: String) -> Result<()> {
        todo!()
    }

    pub fn delete_dataset(&mut self, _name: &str) -> () {
        todo!()
    }

    // todo methods use and new for managing datasets ????

    /// Gets stored value for specified key.
    ///
    /// `key` The key retrieve the value.
    ///
    /// Returns found value, None if nothing was found.
    pub async fn get(&mut self, key: Key) -> Result<Option<Val>> {
        let dataset = self.current_ds().await;
        let callback = self.index.init_get(key).await;
        let response = self
            .storage
            .get(dataset.id, dataset.version, callback)
            .await?;
        Ok(self.decrypt(response)?)
    }

    /// Puts key value pair (K, V). Updates existing value if it's present.
    ///
    /// `key` The specified key to be inserted
    /// `value` The value associated with the specified key
    ///
    /// Returns old value if old value was overridden, None otherwise.
    pub async fn put(&mut self, key: Key, val: Val) -> Result<Option<Val>> {
        let dataset = self.current_ds().await;
        let enc_value = self.val_crypt.encrypt(val)?;
        let value_hash = Hash::build::<Digest, _>(enc_value.clone());
        let callback = self.index.init_put(key, value_hash, dataset.version).await;

        let response = self
            .storage
            .put(dataset.id, dataset.version, callback, enc_value)
            .await?;

        // todo update version and m_root (todo handle errors)
        let _ = self.increment_version().await;

        Ok(self.decrypt(response)?)
    }

    /// Finds and removes key value pair (K, V) for the corresponded key.
    ///
    /// `key` The specified key to be removed
    ///
    /// Returns old value if it was removed, None otherwise.
    pub async fn remove(&mut self, key: Key) -> Result<Option<Val>> {
        unimplemented!("Remove is not ready yet {:?}", key)
    }

    fn decrypt(&self, response: Option<Bytes>) -> Result<Option<Val>> {
        let res = response
            .map(|bytes| self.val_crypt.decrypt(bytes.as_ref()))
            .transpose()?;
        Ok(res)
    }

    /// Reads current dataset, if not define sets default one.
    async fn current_ds(&mut self) -> DatasetState {
        let lock = self.current_dataset.read().await;
        match lock.clone() {
            Some(ds) => ds,
            None => {
                std::mem::drop(lock);
                let default_ds = ClientConfig::default_ds_name();
                self.dataset(&default_ds).await;
                DatasetState::read_from_config(&default_ds, &self.config)
                    .expect("Can't read config for default dataset")
            }
        }
    }

    async fn increment_version(&mut self) -> Result<()> {
        let mut cur_ds = self.current_dataset.write().await;
        cur_ds.as_mut().map(|ds| ds.version += 1);
        Ok(())
    }
}

/// State that Client should track for verifying server actions
#[derive(Clone, Debug)]
struct DatasetState {
    id: Bytes,
    m_root: Hash,
    version: usize,
}

impl DatasetState {
    fn new(id: Bytes, m_root: Hash, version: usize) -> Self {
        DatasetState {
            id,
            m_root,
            version,
        }
    }

    fn read_from_config(dataset_id: &str, config: &ClientConfig) -> Option<DatasetState> {
        let (id, cfg) = config.datasets.get_key_value(dataset_id)?;

        let m_root = Hash::from_hex_str(&cfg.merkle_root)
            .map_err(|_err| {
                log::warn!(
                    "Can't decode merkle root ({:?}) from HEX for {:?}",
                    cfg.merkle_root,
                    id
                )
            })
            .ok()?;

        Some(DatasetState::new(
            Bytes::from(dataset_id.to_string()),
            m_root,
            cfg.version,
        ))
    }
}

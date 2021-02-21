//! Server side encrypted (OPE) Key-Value database. Uses Ope-Btree as index and KVStore
//! as backend for persisting data.

use crate::ope_btree::command::Cmd;
use crate::ope_btree::{BTreeErr, OpeBTree};
use bytes::Bytes;
use common::Digest;
use kvstore_api::kvstore::KVStore;
use kvstore_api::kvstore::*;
use protocol::{BtreeCallback, PutCallbacks, SearchCallback};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("BTree Error")]
    BTreeErr {
        #[from]
        source: BTreeErr,
    },
    #[error("Value Storage Error")]
    ValStoreErr {
        #[from]
        source: KVStoreError,
    },
}

pub type Result<V> = std::result::Result<V, DbError>;

pub struct OpeDatabase<NS, VS, D>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
{
    /// Ope Btree index.
    index: RwLock<OpeBTree<NS, D>>,
    /// Blob storage for persisting encrypted values.
    value_store: Arc<RwLock<VS>>,
}

impl<NS, VS, D> OpeDatabase<NS, VS, D>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
    D: Digest + 'static,
{
    fn new(index: OpeBTree<NS, D>, store: VS) -> Self {
        OpeDatabase {
            index: RwLock::new(index),
            value_store: Arc::new(RwLock::new(store)),
        }
    }

    /// Database initialization
    async fn init(&mut self) -> Result<()> {
        let mut idx = self.index.write().await;
        idx.init().await?;
        Ok(())
    }

    /// Initiates 'Get' operation in MerkleBTree.
    /// Returns found value, None if nothing was found.
    ///
    /// `search_callback` - Wrapper for all callback needed for 'Get' operation to the BTree
    ///
    pub async fn get<'a, Scb>(&mut self, search_callback: Scb) -> Result<Option<Bytes>>
    where
        Scb: SearchCallback + 'a,
    {
        let index_lock = self.index.read().await;

        if let Some(val_ref) = index_lock.get(Cmd::new(search_callback)).await? {
            let val_store = self.value_store.read().await;
            let value = val_store.get(val_ref.0).await?;
            Ok(value)
        } else {
            Ok(None)
        }
    }

    /// Initiates 'Put' operation in MerkleBTree.
    /// Returns old value if old value was overridden, None otherwise.
    ///
    /// `put_callbacks` Wrapper for all callback needed for 'Put' operation to the BTree.
    /// `version` Dataset version expected to the client
    /// `encrypted_value` Encrypted value
    pub async fn put<'a, Scb>(
        &mut self,
        put_callback: Scb,
        _version: usize,
        encrypted_value: Bytes,
    ) -> Result<Option<Bytes>>
    where
        Scb: PutCallbacks + BtreeCallback + Clone + 'a,
    {
        // todo start transaction

        // find place into index and get value reference
        let mut index_lock = self.index.write().await;
        let (val_ref, _state_signed_by_client) = index_lock.put(Cmd::new(put_callback)).await?;

        // safe new value to value store
        let mut val_store = self.value_store.write().await;
        let old_value = val_store.set(val_ref.0, encrypted_value).await?;

        // todo increment expected client version by one and save somewhere client's signature + merkle root + version

        // let root = index_lock.get_root().await?;
        // let state_signed_by_client = state_signed_by_client
        // let version = version + 1;

        Ok(old_value)

        // todo end transaction, revert all changes if error appears
    }

    // todo remove, traverse
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ope_btree::internal::node::Node;
    use crate::ope_btree::internal::node_store::BinaryNodeStore;
    use crate::ope_btree::{OpeBTreeConf, ValRefGen};
    use common::gen::NumGen;
    use common::noop_hasher::NoOpHasher;
    use kvstore_inmemory::hashmap_store::HashMapKVStore;

    #[tokio::test]
    async fn new_test() {
        let index = create_tree(create_node_store(0));
        let mut db = OpeDatabase::new(index, HashMapKVStore::new());
        assert!(db.init().await.is_ok())
    }

    #[tokio::test]
    async fn get_test() {
        let _db = create_db().await;
        // todo test later
    }

    // todo write more test cases

    fn create_node_store(
        idx: usize,
    ) -> BinaryNodeStore<usize, Node, HashMapKVStore<Vec<u8>, Vec<u8>>, NumGen> {
        BinaryNodeStore::new(HashMapKVStore::new(), NumGen(idx))
    }

    fn create_tree<Store: KVStore<Vec<u8>, Vec<u8>>>(
        node_store: BinaryNodeStore<usize, Node, Store, NumGen>,
    ) -> OpeBTree<Store, NoOpHasher> {
        OpeBTree::new(
            OpeBTreeConf {
                arity: 4,
                alpha: 0.25_f32,
            },
            node_store,
            ValRefGen(0),
        )
    }

    async fn create_db(
    ) -> OpeDatabase<HashMapKVStore<Vec<u8>, Vec<u8>>, HashMapKVStore<Bytes, Bytes>, NoOpHasher>
    {
        let index = create_tree(create_node_store(0));
        let mut db = OpeDatabase::new(index, HashMapKVStore::new());
        assert!(db.init().await.is_ok());
        db
    }
}

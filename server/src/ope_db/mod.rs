//! Server side encrypted (OPE) Key-Value database. Uses Ope-Btree as index and KVStore
//! as backend for persisting data.

use crate::ope_btree::command::Cmd;
use crate::ope_btree::internal::node::Node;
use crate::ope_btree::internal::node_store::BinaryNodeStore;
use crate::ope_btree::{BTreeErr, OpeBTree, ValueRef};
use bytes::Bytes;
use common::misc::ToBytes;
use common::Digest;
use futures::{Future, TryStreamExt};
use kvstore_api::kvstore::KVStore;
use kvstore_api::kvstore::*;
use protocol::SearchCallback;
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

impl<NS, VS, D: Digest> OpeDatabase<NS, VS, D>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
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

    /// Initiates ''Get'' operation in remote MerkleBTree. Returns found value,
    /// None if nothing was found.
    ///
    /// # Arguments
    ///
    /// * `search_callback` - Wrapper for all callback needed for ''Get''
    /// operation to the BTree
    ///
    pub async fn get<'a, Scb: SearchCallback + 'a>(
        &mut self,
        search_callback: Scb,
    ) -> Result<Option<Bytes>> {
        let index_lock = self.index.read().await;

        if let Some(val_ref) = index_lock.get(Cmd::new(search_callback)).await? {
            let val_store = self.value_store.read().await;
            let value = val_store.get(val_ref.0).await?;
            Ok(value)
        } else {
            Ok(None)
        }
    }

    // todo get, put, remove, traverse
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let db = create_db().await;
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
                arity: 8,
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

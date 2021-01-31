//! Server side encrypted (OPE) Key-Value database. Uses Ope-Btree as index and KVStore
//! as backend for persisting data.

use crate::ope_btree::node::Node;
use crate::ope_btree::node_store::BinaryNodeStore;
use crate::ope_btree::{BTreeErr, OpeBTree, ValueRef};
use bytes::Bytes;
use common::misc::ToBytes;
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

pub struct OpeDatabase<NS, VS>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
{
    /// Ope Btree index.
    index: RwLock<OpeBTree<NS>>,
    /// Blob storage for persisting encrypted values.
    value_store: Arc<RwLock<VS>>,
}

impl<NS, VS> OpeDatabase<NS, VS>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
{
    fn new(index: OpeBTree<NS>, store: VS) -> Self {
        OpeDatabase {
            index: RwLock::new(index),
            value_store: Arc::new(RwLock::new(store)),
        }
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
        let val_ref: ValueRef = index_lock.get(search_callback).await?;
        let val_store = self.value_store.read().await;
        let value = val_store.get(val_ref.0).await?;
        Ok(value)
    }

    // todo get, put, remove, traverse
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ope_btree::OpeBTreeConf;
    use kvstore_inmemory::hashmap_store::HashMapKVStore;

    #[test]
    fn new_test() {
        let index = create_tree(create_node_store(0));
        let _db = OpeDatabase::new(index, HashMapKVStore::new());
    }

    #[test]
    fn get_test() {
        let db = create_db();
        // todo test later
    }

    // todo write more test cases

    fn create_node_store(
        mut idx: usize,
    ) -> BinaryNodeStore<usize, Node, HashMapKVStore<Vec<u8>, Vec<u8>>> {
        BinaryNodeStore::new(
            HashMapKVStore::new(),
            Box::new(move || {
                idx += 1;
                idx
            }),
        )
    }

    fn create_tree<Store: KVStore<Vec<u8>, Vec<u8>>>(
        node_store: BinaryNodeStore<usize, Node, Store>,
    ) -> OpeBTree<Store> {
        OpeBTree::new(
            OpeBTreeConf {
                arity: 8,
                alpha: 0.25_f32,
            },
            node_store,
        )
    }

    fn create_db() -> OpeDatabase<HashMapKVStore<Vec<u8>, Vec<u8>>, HashMapKVStore<Bytes, Bytes>> {
        let index = create_tree(create_node_store(0));
        OpeDatabase::new(index, HashMapKVStore::new())
    }
}

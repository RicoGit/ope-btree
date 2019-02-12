//! Server side encrypted (OPE) Key-Value database. Uses Ope-Btree as index and KVStore
//! as backend for persisting data.

use crate::ope_btree::OpeBTree;
use async_kvstore::KVStore;
use bytes::Bytes;
use common::misc::ToBytes;
use futures::Future;
use futures_locks::RwLock;
use protocol::SearchCallback;
use std::sync::Arc;

mod errors {
    error_chain! {
        links {
            BTreeError(crate::ope_btree::errors::Error, crate::ope_btree::errors::ErrorKind);
            BinaryStoreError(async_kvstore::errors::Error, async_kvstore::errors::ErrorKind);
        }
    }
}

use self::errors::*;
use crate::ope_btree::node::Node;
use crate::ope_btree::node_store::BinaryNodeStore;
use crate::ope_btree::node_store::NodeStore;

pub struct OpeDatabase<NS, VS>
where
    NS: NodeStore<usize, Node>,
    VS: KVStore<Bytes, Bytes>,
{
    /// Ope Btree index.
    index: RwLock<OpeBTree<NS>>,
    /// Blob storage for persisting encrypted values.
    value_store: RwLock<VS>,
}

type GetFuture<'a> = Box<dyn Future<Item = Option<Bytes>, Error = errors::Error> + Send + 'a>;

impl<NS, VS> OpeDatabase<NS, VS>
where
    NS: NodeStore<usize, Node> + 'static,
    VS: KVStore<Bytes, Bytes> + 'static,
{
    fn new(index: OpeBTree<NS>, store: VS) -> Self {
        OpeDatabase {
            index: RwLock::new(index),
            value_store: RwLock::new(store),
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
    pub fn get<'a, Scb: SearchCallback + 'a>(&mut self, search_callback: Scb) -> GetFuture<'a> {
        let value_store = self.value_store.clone();
        let result = self
            .index
            .read()
            .map_err(|_| "Can't get read access to OpeBtree index".into())
            .and_then(move |tree| {
                tree.get(search_callback)
                    .from_err::<Error>()
                    .and_then(move |val_ref| {
                        value_store
                            .read()
                            .map_err(|_| "Can't get read access to value bin store".into())
                            .and_then(|store| {
                                store
                                    .get(&val_ref.bytes())
                                    .map(|value| value.map(|val| val.to_owned()))
                                    .from_err::<Error>()
                            })
                    })
            });

        Box::new(result)
    }

    // todo get, put, remove, traverse
}

#[cfg(test)]
mod tests {
    use super::OpeDatabase;
    use crate::ope_btree::node::Node;
    use crate::ope_btree::node_store::BinaryNodeStore;
    use crate::ope_btree::node_store::NodeStore;
    use crate::ope_btree::OpeBTree;
    use crate::ope_btree::OpeBTreeConf;
    use async_kvstore::hashmap_store::HashMapStore;
    use bytes::Bytes;

    #[test]
    fn new_test() {
        let index = create_tree(create_node_store(0));
        let _db = OpeDatabase::new(index, HashMapStore::new());
    }

    #[test]
    fn get_test() {
        let db = create_db();
        // todo test
        //        db.get()
    }

    // todo write more test cases

    fn create_node_store(mut idx: usize) -> impl NodeStore<usize, Node> {
        BinaryNodeStore::new(
            HashMapStore::new(),
            Box::new(move || {
                idx += 1;
                idx
            }),
        )
    }

    fn create_tree<NS: NodeStore<usize, Node>>(store: NS) -> OpeBTree<NS> {
        OpeBTree::new(
            OpeBTreeConf {
                arity: 8,
                alpha: 0.25_f32,
            },
            store,
        )
    }

    fn create_db() -> OpeDatabase<impl NodeStore<usize, Node>, HashMapStore<Bytes, Bytes>> {
        let index = create_tree(create_node_store(0));
        OpeDatabase::new(index, HashMapStore::new())
    }
}

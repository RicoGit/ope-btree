pub mod rpc {
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");
}

use kvstore_api::kvstore::KVStore;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use prost::bytes::Bytes;
use rpc::db_rpc_server::{DbRpc, DbRpcServer};
use server::ope_btree::internal::node_store::BinaryNodeStore;
use server::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen};
use server::ope_db::{DatasetChanged, OpeDatabase};
use server::Digest;
use std::pin::Pin;
use tokio::sync::mpsc::Sender;
use tonic::codegen::Stream;

pub struct DbRpcImpl<NS, VS, D>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
{
    db: OpeDatabase<NS, VS, D>,
}

pub fn new_in_memory_db<D: Digest + 'static>(
    conf: OpeBTreeConf,
    update_channel: Sender<DatasetChanged>,
) -> DbRpcImpl<HashMapKVStore<Vec<u8>, Vec<u8>>, HashMapKVStore<Bytes, Bytes>, D> {
    let db = server::ope_db::new_in_memory_db(conf, update_channel);
    DbRpcImpl { db }
}

#[tonic::async_trait]
impl<NS, VS, D> DbRpc for DbRpcImpl<NS, VS, D>
// todo Send SYnc static )
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
    D: Digest + 'static,
{
    type GetStream = Pin<
        Box<dyn Stream<Item = Result<rpc::GetCallback, tonic::Status>> + Send + Sync + 'static>,
    >;

    async fn get(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::PutCallbackReply>>,
    ) -> Result<tonic::Response<Self::GetStream>, tonic::Status> {
        unimplemented!()
    }

    type PutStream = Pin<
        Box<dyn Stream<Item = Result<rpc::PutCallback, tonic::Status>> + Send + Sync + 'static>,
    >;

    async fn put(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::PutCallbackReply>>,
    ) -> Result<tonic::Response<Self::PutStream>, tonic::Status> {
        unimplemented!()
    }
}

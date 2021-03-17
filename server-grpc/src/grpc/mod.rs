pub mod rpc {
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");
}
pub mod errors;

use futures::StreamExt;
use kvstore_api::kvstore::KVStore;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use prost::bytes::Bytes;
use rpc::db_rpc_server::{DbRpc, DbRpcServer};
use rpc::get_callback_reply::Reply;
use rpc::{DbInfo, Error, ReplyNextChildIndex, ReplySubmitLeaf};
use server::ope_btree::internal::node_store::BinaryNodeStore;
use server::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen};
use server::ope_db::{DatasetChanged, OpeDatabase};
use server::Digest;
use std::pin::Pin;
use tokio::sync::mpsc::Sender;
use tonic::codegen::Stream;
use tonic::Status;

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
where
    NS: KVStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static,
    VS: KVStore<Bytes, Bytes> + Send + Sync + 'static,
    D: Digest + Send + Sync + 'static,
{
    type GetStream =
        Pin<Box<dyn Stream<Item = Result<rpc::GetCallback, Status>> + Send + Sync + 'static>>;

    async fn get(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::GetCallbackReply>>,
    ) -> Result<tonic::Response<Self::GetStream>, Status> {
        let (result_in, result_out) = tokio::sync::oneshot::channel();

        let mut input_stream = request.into_inner();

        // spawn background processing
        tokio::spawn(async move {
            // let (get_stream_in, get_stream_out) = tokio::sync::mpsc::unbounded_channel();

            while let Some(client_reply) = input_stream.next().await {
                if let Err(status) = client_reply {
                    log::warn!("Client's reply error: {:?}", status);
                    result_in.send(Err(status));
                    break;
                }

                match client_reply.unwrap().reply {
                    Some(Reply::DbInfo(DbInfo { id, version })) => {
                        // todo: do nothing, many dd is not supported yet
                        log::info!("Client ask db: {:?}, version: {:?}", id, version);
                    }
                    Some(Reply::NextChildIdx(ReplyNextChildIndex { index })) => {
                        // todo finish, implement search_callback
                        // self.db.get()
                        todo!()
                    }
                    Some(Reply::SubmitLeaf(ReplySubmitLeaf { search_result })) => todo!(),
                    Some(Reply::ServerError(err)) => {
                        todo!()
                    }
                    None => {
                        let msg = "Empty reply from client";
                        log::warn!("{}", msg);
                        result_in.send(Err(Status::invalid_argument(msg.to_string())));
                        break;
                    }
                }
            }
        });

        // return result with stream of server responses
        result_out
            .await
            .map_err(|_| Status::internal("Result channel error"))?
    }

    type PutStream =
        Pin<Box<dyn Stream<Item = Result<rpc::PutCallback, Status>> + Send + Sync + 'static>>;

    async fn put(
        &self,
        request: tonic::Request<tonic::Streaming<rpc::PutCallbackReply>>,
    ) -> Result<tonic::Response<Self::PutStream>, tonic::Status> {
        unimplemented!()
    }
}

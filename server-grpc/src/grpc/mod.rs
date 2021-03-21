pub mod rpc {
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");
}
pub mod errors;

use futures::StreamExt;
use kvstore_api::kvstore::KVStore;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use prost::bytes::Bytes;
use protocol::btree::{BtreeCallback, SearchCallback, SearchResult};
use protocol::RpcFuture;
use rpc::db_rpc_server::DbRpc;
use rpc::get_callback_reply::Reply;
use rpc::DbInfo;

use server::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen};
use server::ope_db::{DatasetChanged, DbError, OpeDatabase};
use server::Digest;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::Stream;
use tonic::{Request, Response, Status, Streaming};

pub struct DbRpcImpl<NS, VS, D>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
{
    db: Arc<OpeDatabase<NS, VS, D>>,
}

pub fn new_in_memory_db<D: Digest + 'static>(
    conf: OpeBTreeConf,
    update_channel: Sender<DatasetChanged>,
) -> DbRpcImpl<HashMapKVStore<Vec<u8>, Vec<u8>>, HashMapKVStore<Bytes, Bytes>, D> {
    let db = server::ope_db::new_in_memory_db(conf, update_channel);
    DbRpcImpl { db: Arc::new(db) }
}

// struct GetRoundTrip<NS, VS, D>
// where
//     NS: KVStore<Vec<u8>, Vec<u8>>,
//     VS: KVStore<Bytes, Bytes>,
// {
//     client_replies: Streaming<rpc::GetCallbackReply>,
//     db: Arc<OpeDatabase<NS, VS, D>>,
// }
//
// impl<NS, VS, D> GetRoundTrip<NS, VS, D>
// where
//     NS: KVStore<Vec<u8>, Vec<u8>> + Send + Sync + 'static,
//     VS: KVStore<Bytes, Bytes> + Send + Sync + 'static,
//     D: Digest + Send + Sync + 'static,
// {
//
//     fn new(db: Arc<OpeDatabase<NS, VS, D>>, client_replies: Streaming<rpc::GetCallbackReply>) -> Self {
//       GetRoundTrip { client_replies, db }
//     }
//
//     async fn get(mut self) -> Result<Response<<DbRpcImpl::<NS, VS, D> as DbRpc>::GetStream>, Status> {
//         let (result_in, result_out) = tokio::sync::oneshot::channel();
//
//         // spawn background processing
//         tokio::spawn(async move {
//             // channel with size 1 allows make sure that one request corresponds to single response
//             let (get_stream_in, get_stream_out) = tokio::sync::mpsc::channel(1);
//
//             // todo add timeout for replies
//             while let Some(client_reply) = self.client_replies.next().await {
//                 if let Err(status) = client_reply {
//                     log::warn!("Client's reply error: {:?}", status);
//                     result_in.send(Err(status));
//                     break;
//                 }
//
//                 match client_reply.unwrap().reply {
//                     Some(Reply::DbInfo(DbInfo { id, version })) => {
//                         // todo: do nothing, many db is not supported yet
//                         log::info!("Client ask db: {:?}, version: {:?}", id, version);
//                         // this is a final step returned search result
//
//                         // tokio::spawn(async )
//                         let result = self.db.get(todo!()).await;
//                         // here we need stream not a response
//                         get_stream_in.send(result.map_err(db_err_to_status));
//                     }
//                     Some(reply @ Reply::NextChildIdx(ReplyNextChildIndex { index })) => {
//                         log::info!("Client reply with nextChildIndex: {:?}", index);
//                         // todo can I call callback methods instead of using channel?
//                         // get_stream_in.send(reply).await; // send client result as is
//                     }
//                     Some(Reply::SubmitLeaf(ReplySubmitLeaf { search_result })) => todo!(),
//                     Some(Reply::ServerError(err)) => {
//                         todo!()
//                     }
//                     None => {
//                         let msg = "Empty reply from client";
//                         log::warn!("{}", msg);
//                         result_in.send(Err(Status::invalid_argument(msg.to_string())));
//                         break;
//                     }
//                 }
//             }
//         });
//
//         // return result with stream of server responses
//         result_out
//             .await
//             .map_err(|_| Status::internal("Result channel error"))?
//
//     }
// }

fn db_err_to_status(_err: DbError) -> Status {
    todo!()
}

// todo
struct SearchCb {
    server_requests: Sender<Result<rpc::GetCallback, Status>>,
    client_replies: Streaming<rpc::GetCallbackReply>,
}

impl BtreeCallback for SearchCb {
    fn next_child_idx<'f>(
        &mut self,
        _keys: Vec<Bytes>,
        _children_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, usize> {
        unimplemented!()
    }
}

impl SearchCallback for SearchCb {
    fn submit_leaf<'f>(
        &mut self,
        _keys: Vec<Bytes>,
        _values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, SearchResult> {
        unimplemented!()
    }
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
        request: Request<Streaming<rpc::GetCallbackReply>>,
    ) -> Result<Response<Self::GetStream>, Status> {
        let (result_in, result_out) = tokio::sync::oneshot::channel();

        // wait first message with DnInfo
        let mut client_replies = request.into_inner();

        match client_replies.next().await {
            Some(client_reply) => {
                if let Err(status) = client_reply {
                    log::warn!("Client's reply error: {:?}", status);
                    result_in.send(Err(status));
                } else {
                    match client_reply.unwrap().reply {
                        Some(Reply::DbInfo(DbInfo { id: _, version: _ })) => {
                            // get specified Database with required version
                            // todo implement, only one db is supported now

                            // open stream for server requests
                            let (server_requests_in, server_requests_out) =
                                tokio::sync::mpsc::channel(1);

                            // starting a client-server round trip
                            let db = self.db.clone();
                            tokio::spawn(async move {
                                // todo result_in should be put into cb
                                let callback = SearchCb {
                                    server_requests: server_requests_in,
                                    client_replies,
                                };
                                // todo we ignore search result here, it should be sent to client via get_callback::Callback::value
                                db.get(callback).await;
                                // todo send result to stream
                            });

                            // return server request stream
                            let stream: Self::GetStream =
                                Box::pin(ReceiverStream::new(server_requests_out));
                            result_in.send(Ok(Response::new(stream)));
                        }
                        _ => {
                            let msg = "Empty reply from client";
                            log::warn!("{}", msg);
                            result_in.send(Err(Status::invalid_argument(msg.to_string())));
                        }
                    }
                }
            }
            None => {
                todo!()
            }
        }

        // return result with stream of server responses
        result_out
            .await
            .map_err(|_| Status::internal("Result channel error"))?
        // GetRoundTrip::new(Arc::new(self.db), request.into_inner()).get()
    }

    type PutStream =
        Pin<Box<dyn Stream<Item = Result<rpc::PutCallback, Status>> + Send + Sync + 'static>>;

    async fn put(
        &self,
        _request: tonic::Request<tonic::Streaming<rpc::PutCallbackReply>>,
    ) -> Result<tonic::Response<Self::PutStream>, tonic::Status> {
        unimplemented!()
    }
}

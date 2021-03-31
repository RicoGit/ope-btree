pub mod rpc {
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");
}
pub mod errors;

use kvstore_api::kvstore::KVStore;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use prost::bytes::Bytes;
use protocol::btree::{BtreeCallback, SearchCallback, SearchResult};
use protocol::RpcFuture;
use rpc::db_rpc_server::DbRpc;
use rpc::get_callback_reply::Reply;
use rpc::DbInfo;

use common::misc::{FromVecBytes, ToBytes, ToVecBytes};
use futures::FutureExt;
use log::*;
use server::ope_btree::BTreeErr::ProtocolErr;
use server::ope_btree::{OpeBTree, OpeBTreeConf, ValRefGen};
use server::ope_db::{DatasetChanged, DbError, OpeDatabase};
use server::Digest;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::codegen::Stream;
use tonic::{Request, Response, Status, Streaming};

pub struct DbRpcImpl<NS, VS, D>
where
    NS: KVStore<Vec<u8>, Vec<u8>>,
    VS: KVStore<Bytes, Bytes>,
{
    db: Arc<OpeDatabase<NS, VS, D>>,
}

pub async fn new_in_memory_db<D: Digest + 'static>(
    conf: OpeBTreeConf,
    update_channel: Sender<DatasetChanged>,
) -> DbRpcImpl<HashMapKVStore<Vec<u8>, Vec<u8>>, HashMapKVStore<Bytes, Bytes>, D> {
    let db = server::ope_db::new_in_memory_db(conf, update_channel);
    db.init().await.expect("Db initialization is failed");
    DbRpcImpl { db: Arc::new(db) }
}

struct SearchCb {
    /// Server sends requests to this channel
    server_requests: Sender<Result<rpc::GetCallback, Status>>,
    /// Server receives client responses from this stream
    client_replies: Streaming<rpc::GetCallbackReply>,
}

impl BtreeCallback for SearchCb {
    fn next_child_idx(
        &mut self,
        keys: Vec<Bytes>,
        children_checksums: Vec<Bytes>,
    ) -> RpcFuture<usize> {
        log::debug!(
            "BtreeCallback::next_child_idx({:?},{:?})",
            keys,
            children_checksums
        );

        // create server request
        let request = rpc::GetCallback {
            callback: Some(rpc::get_callback::Callback::NextChildIdx(
                rpc::AskNextChildIndex {
                    keys: keys.into_byte_vec(),
                    children_checksums: children_checksums.into_byte_vec(),
                },
            )),
        };

        let server_requests = self.server_requests.clone();

        let future = async move {
            // send request to client
            if let Err(err) = server_requests.send(Ok(request)).await {
                return Err(errors::send_err_to_protocol_err(err));
            }

            // wait response from client
            let response = self.client_replies.try_next().await;
            if let Ok(Some(rpc::GetCallbackReply {
                reply: Some(Reply::NextChildIdx(rpc::ReplyNextChildIndex { index })),
            })) = response
            {
                Ok(index as usize)
            } else {
                let err = response.err();
                log::warn!("Unexpected client's reply: {:?}", &err);
                Err(errors::opt_status_to_protocol_err(err))
            }
        };

        future.boxed()
    }
}

impl SearchCallback for SearchCb {
    fn submit_leaf<'f>(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<'f, SearchResult> {
        log::debug!(
            "SearchCallback::submit_leaf({:?},{:?})",
            keys,
            values_hashes
        );
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
        log::info!("Server received GET request");

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
                        Some(Reply::DbInfo(DbInfo { id, version })) => {
                            log::debug!("Server received DbInfo({:?},{:?})", id, version);

                            // get specified Database with required version
                            // todo implement, only one db is supported now

                            // open stream for server requests
                            let (server_requests_in, server_requests_out) =
                                tokio::sync::mpsc::channel(1);

                            // starting a client-server round trip
                            let db = self.db.clone();
                            tokio::spawn(async move {
                                let callback = SearchCb {
                                    server_requests: server_requests_in.clone(),
                                    client_replies,
                                };

                                let search_result = match db.get(callback).await {
                                    Ok(value) => {
                                        log::info!("OpeDatabase respond with {:?}", value);
                                        rpc::get_callback::Callback::Value(rpc::GetValue {
                                            value: value.map(|bytes| bytes.to_vec()),
                                        })
                                    }
                                    Err(db_err) => {
                                        log::warn!("OpeDatabase respond with ERROR: {:?}", db_err);
                                        errors::db_err_to_grpc_err(db_err)
                                    }
                                };

                                server_requests_in
                                    .send(Ok(rpc::GetCallback {
                                        callback: Some(search_result),
                                    }))
                                    .await;
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
                log::warn!("Client reply is None");
                todo!()
            }
        };

        // return result with stream of server responses
        result_out
            .await
            .map_err(|_| Status::internal("Result channel error"))?
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

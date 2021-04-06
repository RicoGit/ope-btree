pub mod errors;
pub mod rpc;

use kvstore_api::kvstore::KVStore;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use prost::bytes::Bytes;
use protocol::btree::{BtreeCallback, ClientPutDetails, PutCallback, SearchCallback, SearchResult};
use protocol::RpcFuture;
use rpc::db_rpc_server::DbRpc;
use rpc::{DbInfo, PutValue};

use crate::grpc::rpc::get::value_msg;
use common::misc::{FromVecBytes, ToBytes, ToVecBytes};
use futures::FutureExt;
use log::*;
use rpc::get_callback_reply::Reply as GetReply;
use rpc::put_callback_reply::Reply as PutReply;
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

mod get_cb;
mod put_cb;

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

        // waiting first message with DnInfo
        let mut client_replies = request.into_inner();
        match client_replies.next().await {
            Some(client_reply) => {
                if let Err(status) = client_reply {
                    log::warn!("Client's reply error: {:?}", status);
                    result_in.send(Err(status));
                } else {
                    match client_reply.unwrap().reply {
                        Some(GetReply::DbInfo(DbInfo { id, version })) => {
                            log::debug!("Server receive DbInfo({:?},{:?})", id, version);

                            // get specified Database with required version - todo implement, only one db is supported now

                            log::debug!("Open stream for server requests");
                            let (server_requests_in, server_requests_out) =
                                tokio::sync::mpsc::channel(1);

                            log::debug!("Starting a client-server round trip");
                            let db = self.db.clone();
                            tokio::spawn(async move {
                                let callback = get_cb::GetCbImpl {
                                    server_requests: server_requests_in.clone(),
                                    client_replies,
                                };

                                let search_result = match db.get(callback).await {
                                    Ok(value) => {
                                        log::info!("OpeDatabase found value: {:?}", value);
                                        rpc::get::value_msg(value)
                                    }
                                    Err(db_err) => {
                                        log::warn!("OpeDatabase respond with ERROR: {:?}", db_err);
                                        rpc::get::server_error_msg(db_err)
                                    }
                                };

                                server_requests_in.send(Ok(search_result)).await;
                            });

                            log::debug!("Return server request stream");
                            let stream: Self::GetStream =
                                Box::pin(ReceiverStream::new(server_requests_out));

                            result_in.send(Ok(Response::new(stream)));
                        }

                        //
                        // unexpected client msg
                        //
                        unexpected => {
                            let msg = format!(
                                "Wrong message order from client, expected DbInfo, actually: {:?}",
                                unexpected
                            );
                            log::warn!("{}", msg);
                            result_in.send(Err(Status::invalid_argument(msg.to_string())));
                        }
                    }
                }
            }

            //
            // receive end of stream
            //
            None => {
                log::warn!("Receive end of stream: client close GET round trip");
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
        request: tonic::Request<tonic::Streaming<rpc::PutCallbackReply>>,
    ) -> Result<tonic::Response<Self::PutStream>, tonic::Status> {
        log::info!("Server received PUT request");

        let (result_in, result_out) = tokio::sync::oneshot::channel();

        // waiting first message with DnInfo
        let mut client_replies = request.into_inner();

        // receive first msg: PutReply::DbInfo
        match client_replies.try_next().await {
            Ok(Some(rpc::PutCallbackReply {
                reply: Some(PutReply::DbInfo(DbInfo { id, version })),
            })) => {
                log::debug!("Server receive DbInfo({:?},{:?})", id.bytes(), version);

                // get specified Database with required version - todo implement, only one db is supported now

                // receive second msg: PutReply::Value
                match client_replies.try_next().await {
                    Ok(Some(rpc::PutCallbackReply {
                        reply: Some(PutReply::Value(rpc::PutValue { value })),
                    })) => {
                        log::debug!("Server receive PutValue({:?})", value);

                        log::debug!("Open stream for server requests");
                        let (server_requests_in, server_requests_out) =
                            tokio::sync::mpsc::channel(1);

                        log::debug!("Starting a client-server round trip in parallel");
                        let db = self.db.clone();
                        tokio::spawn(async move {
                            let callback =
                                put_cb::PutCbImpl::new(server_requests_in.clone(), client_replies);

                            let search_result =
                                match db.put(callback, version as usize, value.bytes()).await {
                                    Ok(value) => {
                                        log::info!("OpeDatabase found value: {:?}", value);
                                        rpc::put::value_msg(value)
                                    }
                                    Err(db_err) => {
                                        log::warn!("OpeDatabase respond with ERROR: {:?}", db_err);
                                        rpc::put::server_error_msg(db_err)
                                    }
                                };

                            server_requests_in.send(Ok(search_result)).await;
                        });

                        log::debug!("Return server request stream");
                        let stream: Self::PutStream =
                            Box::pin(ReceiverStream::new(server_requests_out));

                        result_in.send(Ok(Response::new(stream)));
                    }
                    Ok(unexpected) => {
                        let status = errors::unexpected_msg_status("PutValue", unexpected);
                        result_in.send(Err(status));
                    }
                    Err(status) => {
                        log::warn!("Client's reply error: {:?}", status);
                        result_in.send(Err(status));
                    }
                }
            }
            Ok(unexpected) => {
                let status = errors::unexpected_msg_status("DbInfo", unexpected);
                result_in.send(Err(status));
            }
            Err(status) => {
                log::warn!("Client's reply error: {:?}", status);
                result_in.send(Err(status));
            }
        }

        // return result with stream of server responses
        result_out
            .await
            .map_err(|_| Status::internal("Result channel error"))?
    }
}

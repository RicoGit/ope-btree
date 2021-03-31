use crate::lib::errors::status_to_protocol_err;
use crate::lib::rpc::db_rpc_client::DbRpcClient;
use client::ope_db::OpeDatabaseClient;
use common::misc::ToBytes;
use common::misc::ToVecBytes;
use futures::FutureExt;
use futures::{StreamExt, TryStreamExt};
use prost::bytes::Bytes;
use protocol::btree::{PutCallback, SearchCallback};
use protocol::database::OpeDatabaseRpc;
use protocol::{ProtocolError, RpcFuture};
use std::fmt::Debug;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub mod errors;

pub mod rpc {
    use bytes::Bytes;
    use protocol::btree::SearchResult;
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");

    pub fn db_info_msg(dataset_id: Bytes, version: usize) -> GetCallbackReply {
        GetCallbackReply {
            reply: Some(get_callback_reply::Reply::DbInfo(DbInfo {
                id: dataset_id.to_vec(),
                version: version as i64,
            })),
        }
    }

    pub fn next_child_idx_msg(idx: usize) -> GetCallbackReply {
        GetCallbackReply {
            reply: Some(get_callback_reply::Reply::NextChildIdx(
                ReplyNextChildIndex { index: idx as u32 },
            )),
        }
    }

    pub fn submit_leaf_msg(search_result: SearchResult) -> GetCallbackReply {
        let result = match search_result.0 {
            Err(idx) => reply_submit_leaf::SearchResult::InsertionPoint(idx as i32),
            Ok(idx) => reply_submit_leaf::SearchResult::Found(idx as i32),
        };

        GetCallbackReply {
            reply: Some(get_callback_reply::Reply::SubmitLeaf(ReplySubmitLeaf {
                search_result: Some(result),
            })),
        }
    }
}

pub struct GrpcDbRpc {
    client: DbRpcClient<tonic::transport::Channel>,
}

impl GrpcDbRpc {
    pub fn new(client: DbRpcClient<tonic::transport::Channel>) -> Self {
        GrpcDbRpc { client }
    }

    async fn get<Cb: SearchCallback>(
        &mut self,
        dataset_id: Bytes,
        version: usize,
        mut search_callback: Cb,
    ) -> Result<Option<Bytes>, ProtocolError> {
        // client's replies
        let (client_replies, client_replies_out) = tokio::sync::mpsc::channel(1);

        log::debug!("Send DbInfo message to server");
        client_replies
            .send(rpc::db_info_msg(dataset_id, version))
            .await
            .map_err(errors::send_err_to_protocol_err)?;

        log::debug!("Get server's requests stream");
        let mut server_requests = self
            .client
            .get(Request::new(ReceiverStream::new(client_replies_out))) // see server DbRpcImpl::get
            .await
            .map_err(errors::status_to_protocol_err)?
            .into_inner();

        log::debug!("Start receiving server requests");
        let mut result = None;
        while let request = server_requests
            .try_next()
            .await
            .map_err(errors::status_to_protocol_err)?
        {
            match request {
                //
                // receive GetValue
                //
                Some(rpc::GetCallback {
                    callback: Some(rpc::get_callback::Callback::Value(rpc::GetValue { value })),
                }) => {
                    let search_result = value.map(|vec| vec.bytes());
                    log::info!("Receive found value: {:?}", search_result);
                    result = search_result;
                }
                //
                // receive NextChildIdx
                //
                Some(rpc::GetCallback {
                    callback:
                        Some(rpc::get_callback::Callback::NextChildIdx(rpc::AskNextChildIndex {
                            keys,
                            children_checksums,
                        })),
                }) => {
                    log::debug!("Receive NextChildIdx");
                    let idx = search_callback
                        .next_child_idx(keys.into_bytes(), children_checksums.into_bytes())
                        .await?;

                    log::debug!("Send ReplyNextChildIndex message to server: idx={:?}", idx);
                    client_replies
                        .send(rpc::next_child_idx_msg(idx))
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }
                //
                // receive SubmitLeaf
                //
                Some(rpc::GetCallback {
                    callback:
                        Some(rpc::get_callback::Callback::SubmitLeaf(rpc::AskSubmitLeaf {
                            keys,
                            values_checksums,
                        })),
                }) => {
                    log::info!("Receive SubmitLeaf result");

                    let search_result = search_callback
                        .submit_leaf(keys.into_bytes(), values_checksums.into_bytes())
                        .await?;

                    log::debug!(
                        "Send ReplyNextChildIndex message to server: search_result={:?}",
                        search_result
                    );
                    client_replies
                        .send(rpc::submit_leaf_msg(search_result))
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }
                //
                // receive ServerError
                //

                // todo

                //
                // receive end of stream
                //
                None => {
                    log::debug!("Receive end of stream: server close GET round trip");
                    break;
                }
                //
                // receive unknown server request
                //
                other => {
                    log::warn!("Invalid server request: {:?}", other)
                }
            }
        }

        Ok(result)
    }
}

impl OpeDatabaseRpc for GrpcDbRpc {
    fn get<'cb, 's: 'cb, Cb: 'cb + SearchCallback + Send>(
        &'s mut self,
        dataset_id: Bytes,
        version: usize,
        search_callback: Cb,
    ) -> RpcFuture<'cb, Option<Bytes>> {
        log::debug!("OpeDatabaseRpc::get({:?},{:?})", dataset_id, version);
        let fut = self.get(dataset_id, version, search_callback);
        fut.boxed::<'cb>()
    }

    fn put<'f, Cb: PutCallback + 'f>(
        &self,
        dataset_id: Bytes,
        version: usize,
        put_callback: Cb,
        encrypted_value: Bytes,
    ) -> RpcFuture<'f, Option<Bytes>> {
        unimplemented!()
    }
}

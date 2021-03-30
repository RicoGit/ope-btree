use crate::lib::errors::status_to_protocol_err;
use crate::lib::rpc::db_rpc_client::DbRpcClient;
use client::ope_db::OpeDatabaseClient;
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
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");
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

        log::debug!("Send init Get message to server");
        let db_info_msg = rpc::GetCallbackReply {
            reply: Some(rpc::get_callback_reply::Reply::DbInfo(rpc::DbInfo {
                id: dataset_id.to_vec(),
                version: version as i64,
            })),
        };

        client_replies
            .send(db_info_msg)
            .await
            .map_err(errors::send_err_to_protocol_err)?;

        // get server's requests stream
        let mut server_requests = self
            .client
            .get(Request::new(ReceiverStream::new(client_replies_out)))  // see server DbRpcImpl::get
            .await
            .map_err(errors::status_to_protocol_err)?
            .into_inner();

        // receive server requests
        let mut result = None;
        while let request = server_requests
            .try_next()
            .await
            .map_err(errors::status_to_protocol_err)?
        {
            match request {
                Some(rpc::GetCallback {
                    callback:
                        Some(rpc::get_callback::Callback::NextChildIdx(rpc::AskNextChildIndex {
                            keys,
                            children_checksums,
                        })),
                }) => {
                    let idx = search_callback
                        .next_child_idx(keys.into_bytes(), children_checksums.into_bytes())
                        .await?;

                    let next_child_msg = rpc::GetCallbackReply {
                        reply: Some(rpc::get_callback_reply::Reply::NextChildIdx(
                            rpc::ReplyNextChildIndex { index: idx as u32 },
                        )),
                    };

                    // send init Get message to server
                    client_replies
                        .send(next_child_msg)
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }
                // todo impl all cases later
                other => {
                    // todo Some(GetCallback { callback: None }) comes when Db return None, should we describe it more clearly?
                    log::warn!("Invalid server request: {:?}", other) // todo impl Debug for GetCallback
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

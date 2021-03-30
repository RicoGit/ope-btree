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

        // server's requests
        let mut server_requests = self
            .client
            .get(Request::new(ReceiverStream::new(client_replies_out)))
            .await
            .map_err(errors::status_to_protocol_err)?
            .into_inner();

        let db_info_msg = rpc::GetCallbackReply {
            reply: Some(rpc::get_callback_reply::Reply::DbInfo(rpc::DbInfo {
                id: dataset_id.to_vec(),
                version: version as i64,
            })),
        };

        // send init Get message to server
        client_replies
            .send(db_info_msg)
            .await
            .map_err(errors::send_err_to_protocol_err)?;

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
                    log::warn!("Invalid server request: todo print message") // todo impl Debug for GetCallback
                }
            }
        }

        Ok(result)
    }

    // fn get2<'cb, 's: 'cb, Cb: 'cb + SearchCallback + Send>(
    //     &'s mut self,
    //     dataset_id: Bytes,
    //     version: usize,
    //     mut search_callback: Cb,
    // ) -> RpcFuture<'cb, Option<Bytes>> {
    //     let fut = self.get(dataset_id, version, search_callback);
    //     fut.boxed::<'cb>()
    // }
}

impl OpeDatabaseRpc for GrpcDbRpc {
    fn get<'cb, 's: 'cb, Cb: 'cb + SearchCallback + Send>(
        &'s mut self,
        dataset_id: Bytes,
        version: usize,
        search_callback: Cb,
    ) -> RpcFuture<'cb, Option<Bytes>> {
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

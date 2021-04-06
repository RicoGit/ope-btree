use crate::lib::errors::status_to_protocol_err;
use crate::lib::rpc::db_rpc_client::DbRpcClient;
use client::ope_btree::errors::BTreeClientError;
use client::ope_btree::errors::BTreeClientError::ProtocolErr;
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
pub mod rpc;

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
            .send(rpc::get::db_info_msg(dataset_id, version))
            .await
            .map_err(errors::send_err_to_protocol_err)?;

        log::debug!("Get server's requests stream");
        let mut server_requests = self
            .client
            .get(Request::new(ReceiverStream::new(client_replies_out))) // see server DbRpcImpl::get
            .await
            .map_err(errors::status_to_protocol_err)?
            .into_inner();

        let mut result = Ok(None);

        log::debug!("Start receiving server requests");
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
                    result = Ok(search_result);
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
                        .send(rpc::get::next_child_idx_msg(idx))
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
                        .send(rpc::get::submit_leaf_msg(search_result))
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }

                //
                // receive ServerError
                //
                Some(rpc::GetCallback {
                    callback:
                        Some(rpc::get_callback::Callback::ServerError(rpc::Error { code, description })),
                }) => {
                    result = Err(errors::protocol_error(code, description));
                    log::debug!("Receive ServerError: {:?}", result);
                    break;
                }

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

        result
    }

    async fn put<Cb: PutCallback>(
        &mut self,
        dataset_id: Bytes,
        version: usize,
        mut put_callback: Cb,
        encrypted_value: Bytes,
    ) -> Result<Option<Bytes>, ProtocolError> {
        // client's replies
        let (client_replies, client_replies_out) = tokio::sync::mpsc::channel(2);

        log::debug!("Send DbInfo message to server as first msg");
        client_replies
            .send(rpc::put::db_info_msg(dataset_id, version))
            .await
            .map_err(errors::send_err_to_protocol_err)?;

        log::debug!("Send encrypted message PutValue to server as second msg");
        client_replies
            .send(rpc::put::value_msg(encrypted_value))
            .await
            .map_err(errors::send_err_to_protocol_err)?;

        log::debug!("Get server's requests stream");
        let mut server_requests = self
            .client
            .put(Request::new(ReceiverStream::new(client_replies_out))) // see server DbRpcImpl::get
            .await
            .map_err(errors::status_to_protocol_err)?
            .into_inner();

        let mut result = Ok(None);

        log::debug!("Start receiving server requests");
        while let request = server_requests
            .try_next()
            .await
            .map_err(errors::status_to_protocol_err)?
        {
            match request {
                //
                // receive Value
                //
                Some(rpc::PutCallback {
                    callback: Some(rpc::put_callback::Callback::Value(rpc::PreviousValue { value })),
                }) => {
                    let search_result = value.map(|vec| vec.bytes());
                    log::info!("Receive previous value: {:?}", search_result);
                    result = Ok(search_result);
                }

                //
                // receive NextChildIdx
                //
                Some(rpc::PutCallback {
                    callback:
                        Some(rpc::put_callback::Callback::NextChildIdx(rpc::AskNextChildIndex {
                            keys,
                            children_checksums,
                        })),
                }) => {
                    log::debug!("Receive NextChildIdx");
                    let idx = put_callback
                        .next_child_idx(keys.into_bytes(), children_checksums.into_bytes())
                        .await?;

                    log::debug!("Send ReplyNextChildIndex message to server: idx={:?}", idx);
                    client_replies
                        .send(rpc::put::next_child_idx_msg(idx))
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }

                //
                // receive PutDetails
                //
                Some(rpc::PutCallback {
                    callback:
                        Some(rpc::put_callback::Callback::PutDetails(rpc::AskPutDetails {
                            keys,
                            values_checksums,
                        })),
                }) => {
                    log::debug!("Receive PutDetails");
                    let details = put_callback
                        .put_details(keys.into_bytes(), values_checksums.into_bytes())
                        .await?;

                    log::debug!(
                        "Send ReplyPutDetails message to server: put_details={:?}",
                        details
                    );
                    client_replies
                        .send(rpc::put::put_details_msg(details))
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }

                //
                // receive VerifyChanges
                //
                Some(rpc::PutCallback {
                    callback:
                        Some(rpc::put_callback::Callback::VerifyChanges(rpc::AskVerifyChanges {
                            server_merkle_root,
                            was_split,
                        })),
                }) => {
                    log::debug!("Receive VerifyChanges");
                    let m_root = put_callback
                        .verify_changes(server_merkle_root.bytes(), was_split)
                        .await?;

                    log::debug!(
                        "Send ReplyVerifyChanges message to server: m_root={:?}",
                        m_root
                    );
                    client_replies
                        .send(rpc::put::verify_changes_msg(m_root))
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }

                //
                // receive ChangesStored
                //
                Some(rpc::PutCallback {
                    callback:
                        Some(rpc::put_callback::Callback::ChangesStored(rpc::AskChangesStored {})),
                }) => {
                    log::debug!("Receive ChangesStored");
                    put_callback.changes_stored().await?;

                    log::debug!("Send ReplyChangesStored message to server");
                    client_replies
                        .send(rpc::put::changes_stored_msg())
                        .await
                        .map_err(errors::send_err_to_protocol_err)?;
                }

                //
                // receive ServerError
                //
                Some(rpc::PutCallback {
                    callback:
                        Some(rpc::put_callback::Callback::ServerError(rpc::Error { code, description })),
                }) => {
                    result = Err(errors::protocol_error(code, description));
                    log::debug!("Receive ServerError: {:?}", result);
                    break;
                }

                //
                // receive end of stream
                //
                None => {
                    log::debug!("Receive end of stream: server close PUT round trip");
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

        result
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

    fn put<'cb, 's: 'cb, Cb: 'cb + PutCallback + Send>(
        &'s mut self,
        dataset_id: Bytes,
        version: usize,
        put_callback: Cb,
        encrypted_value: Bytes,
    ) -> RpcFuture<'cb, Option<Bytes>> {
        log::debug!("OpeDatabaseRpc::put({:?},{:?})", dataset_id, version);
        let fut = self.put(dataset_id, version, put_callback, encrypted_value);
        fut.boxed::<'cb>()
    }
}

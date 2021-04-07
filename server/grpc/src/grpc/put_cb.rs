use crate::grpc::{errors, rpc};
use common::misc::ToBytes;
use futures::FutureExt;
use prost::bytes::Bytes;
use protocol::btree::{BtreeCallback, ClientPutDetails, PutCallback};
use protocol::{ProtocolError, RpcFuture};
use rpc::put_callback_reply::Reply as PutReply;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

#[derive(Clone)]
pub struct PutCbImpl {
    /// Server sends requests to this channel
    pub server_requests: Sender<Result<rpc::PutCallback, Status>>,
    /// Server receives client responses from this stream
    pub client_replies: Arc<Mutex<Streaming<rpc::PutCallbackReply>>>,
}

impl PutCbImpl {
    pub fn new(
        server_requests: Sender<Result<rpc::PutCallback, Status>>,
        client_replies: Streaming<rpc::PutCallbackReply>,
    ) -> Self {
        PutCbImpl {
            server_requests,
            client_replies: Arc::new(Mutex::new(client_replies)),
        }
    }

    pub async fn try_next(&self) -> Result<Option<rpc::PutCallbackReply>, ProtocolError> {
        let mut lock = self.client_replies.lock().await;
        let res = lock.try_next().await;
        res.map_err(|status| {
            log::warn!("Client sends error: {:?}", &status);
            errors::opt_status_to_protocol_err(Some(status))
        })
    }
}

impl BtreeCallback for PutCbImpl {
    fn next_child_idx(
        &mut self,
        keys: Vec<Bytes>,
        children_checksums: Vec<Bytes>,
    ) -> RpcFuture<usize> {
        log::debug!(
            "PutCbImpl::next_child_idx({:?},{:?})",
            keys,
            children_checksums
        );

        let server_requests = self.server_requests.clone();

        let future = async move {
            log::debug!("Send AskNextChildIndex message to client");
            server_requests
                .send(Ok(rpc::put::next_child_idx_msg(keys, children_checksums)))
                .await
                .map_err(errors::send_err_to_protocol_err)?;

            // wait response from client
            let response = self.try_next().await?;

            if let Some(rpc::PutCallbackReply {
                reply: Some(PutReply::NextChildIdx(rpc::ReplyNextChildIndex { index })),
            }) = response
            {
                log::debug!("Receive ReplyNextChildIndex({:?})", index);
                Ok(index as usize)
            } else {
                Err(errors::unexpected_msg_err("ReplyNextChildIndex", response))
            }
        };

        future.boxed()
    }
}

impl PutCallback for PutCbImpl {
    fn put_details(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<ClientPutDetails> {
        log::debug!("PutCbImpl::put_details({:?},{:?})", keys, values_hashes);

        let server_requests = self.server_requests.clone();

        let future = async move {
            log::debug!("Send AskPutDetails message to client");
            server_requests
                .send(Ok(rpc::put::put_details_msg(keys, values_hashes)))
                .await
                .map_err(errors::send_err_to_protocol_err)?;

            // wait response from client
            let response = self.try_next().await?;

            if let Some(rpc::PutCallbackReply {
                reply:
                    Some(PutReply::PutDetails(rpc::ReplyPutDetails {
                        key,
                        checksum,
                        search_result,
                    })),
            }) = response
            {
                log::debug!(
                    "Receive ReplyPutDetails({:?},{:?},{:?})",
                    key,
                    checksum,
                    search_result
                );

                Ok(ClientPutDetails::new(
                    key.bytes(),
                    checksum.bytes(),
                    rpc::put::search_result(search_result),
                ))
            } else {
                Err(errors::unexpected_msg_err("ReplyPutDetails", response))
            }
        };

        future.boxed()
    }

    fn verify_changes(
        &mut self,
        server_merkle_root: Bytes,
        was_splitting: bool,
    ) -> RpcFuture<Bytes> {
        log::debug!(
            "PutCbImpl::verify_changes(root={:?},was_splitting{:?})",
            server_merkle_root,
            was_splitting
        );

        let server_requests = self.server_requests.clone();

        let future = async move {
            log::debug!("Send AskVerifyChanges message to client");
            server_requests
                .send(Ok(rpc::put::verify_changes_msg(
                    server_merkle_root,
                    was_splitting,
                )))
                .await
                .map_err(errors::send_err_to_protocol_err)?;

            // wait response from client
            let response = self.try_next().await?;

            if let Some(rpc::PutCallbackReply {
                reply: Some(PutReply::VerifyChanges(rpc::ReplyVerifyChanges { signature })),
            }) = response
            {
                log::debug!("Receive ReplyVerifyChanges({:?})", signature);
                Ok(signature.bytes())
            } else {
                Err(errors::unexpected_msg_err("ReplyVerifyChanges", response))
            }
        };

        future.boxed()
    }

    fn changes_stored(&mut self) -> RpcFuture<()> {
        log::debug!("PutCbImpl::changes_stored)",);

        let server_requests = self.server_requests.clone();

        let future = async move {
            log::debug!("Send AskVerifyChanges message to client");
            server_requests
                .send(Ok(rpc::put::changes_stored_msg()))
                .await
                .map_err(errors::send_err_to_protocol_err)?;

            // wait response from client
            let response = self.try_next().await?;

            if let Some(rpc::PutCallbackReply {
                reply: Some(PutReply::ChangesStored(rpc::ReplyChangesStored {})),
            }) = response
            {
                log::debug!("Receive ReplyVerifyChanges()");
                Ok(())
            } else {
                Err(errors::unexpected_msg_err("ReplyVerifyChanges", response))
            }
        };

        future.boxed()
    }
}

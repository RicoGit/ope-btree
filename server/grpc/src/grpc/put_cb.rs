use kvstore_api::kvstore::KVStore;
use kvstore_inmemory::hashmap_store::HashMapKVStore;
use prost::bytes::Bytes;
use protocol::btree::{BtreeCallback, ClientPutDetails, PutCallback, SearchCallback, SearchResult};
use protocol::RpcFuture;
use rpc::db_rpc_server::DbRpc;
use rpc::{DbInfo, PutValue};

use crate::grpc::rpc::get::value_msg;
use crate::grpc::{errors, rpc};
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

// #[derive(Clone)]
pub struct PutCbImpl {
    /// Server sends requests to this channel
    pub server_requests: Sender<Result<rpc::PutCallback, Status>>,
    /// Server receives client responses from this stream
    pub client_replies: Streaming<rpc::PutCallbackReply>,
}

// todo remove later, do impl Clone for Cmd instead
impl Clone for PutCbImpl {
    fn clone(&self) -> Self {
        unimplemented!("Impl Clone for Cmd")
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
            let response = self.client_replies.try_next().await;

            if let Ok(Some(rpc::PutCallbackReply {
                reply: Some(PutReply::NextChildIdx(rpc::ReplyNextChildIndex { index })),
            })) = response
            {
                log::debug!("Receive ReplyNextChildIndex({:?})", index);

                Ok(index as usize)
            } else {
                log::warn!("Expected ReplyNextChildIndex, actually: {:?}", &response);
                Err(errors::opt_status_to_protocol_err(response.err()))
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
            let response = self.client_replies.try_next().await;

            if let Ok(Some(rpc::PutCallbackReply {
                reply:
                    Some(PutReply::PutDetails(rpc::ReplyPutDetails {
                        key,
                        checksum,
                        search_result,
                    })),
            })) = response
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
                log::warn!("Expected ReplyPutDetails, actually: {:?}", &response);
                Err(errors::opt_status_to_protocol_err(response.err()))
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
            let response = self.client_replies.try_next().await;

            if let Ok(Some(rpc::PutCallbackReply {
                reply: Some(PutReply::VerifyChanges(rpc::ReplyVerifyChanges { signature })),
            })) = response
            {
                log::debug!("Receive ReplyVerifyChanges({:?})", signature);
                Ok(signature.bytes())
            } else {
                log::warn!("Expected ReplyVerifyChanges, actually: {:?}", &response);
                Err(errors::opt_status_to_protocol_err(response.err()))
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
            let response = self.client_replies.try_next().await;

            if let Ok(Some(rpc::PutCallbackReply {
                reply: Some(PutReply::ChangesStored(rpc::ReplyChangesStored {})),
            })) = response
            {
                log::debug!("Receive ReplyVerifyChanges()");
                Ok(())
            } else {
                log::warn!("Expected ReplyVerifyChanges, actually: {:?}", &response);
                Err(errors::opt_status_to_protocol_err(response.err()))
            }
        };

        future.boxed()
    }
}

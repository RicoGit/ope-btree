use crate::grpc::{errors, rpc};

use futures::FutureExt;
use prost::bytes::Bytes;
use protocol::btree::{BtreeCallback, SearchCallback, SearchResult};
use protocol::RpcFuture;
use rpc::get_callback_reply::Reply as GetReply;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

pub struct GetCbImpl {
    /// Server sends requests to this channel
    pub server_requests: Sender<Result<rpc::GetCallback, Status>>,
    /// Server receives client responses from this stream
    pub client_replies: Streaming<rpc::GetCallbackReply>,
}

impl BtreeCallback for GetCbImpl {
    fn next_child_idx(
        &mut self,
        keys: Vec<Bytes>,
        children_checksums: Vec<Bytes>,
    ) -> RpcFuture<usize> {
        log::debug!(
            "GetCbImpl::next_child_idx({:?},{:?})",
            keys,
            children_checksums
        );

        let server_requests = self.server_requests.clone();

        let future = async move {
            log::debug!("Send AskNextChildIndex message to client");
            server_requests
                .send(Ok(rpc::get::next_child_idx_msg(keys, children_checksums)))
                .await
                .map_err(errors::send_err_to_protocol_err)?;

            // wait response from client
            let response = self.client_replies.try_next().await;

            if let Ok(Some(rpc::GetCallbackReply {
                reply: Some(GetReply::NextChildIdx(rpc::ReplyNextChildIndex { index })),
            })) = response
            {
                log::debug!("Receive NextChildIdx({:?})", index);

                Ok(index as usize)
            } else {
                log::warn!("Expected ReplyNextChildIndex, actually: {:?}", &response);
                Err(errors::opt_status_to_protocol_err(response.err()))
            }
        };

        future.boxed()
    }
}

impl SearchCallback for GetCbImpl {
    fn submit_leaf(
        &mut self,
        keys: Vec<Bytes>,
        values_hashes: Vec<Bytes>,
    ) -> RpcFuture<SearchResult> {
        log::debug!("GetCbImpl::submit_leaf({:?},{:?})", keys, values_hashes);

        let server_requests = self.server_requests.clone();

        let future = async move {
            log::debug!("Send AskSubmitLeaf message to client");
            server_requests
                .send(Ok(rpc::get::submit_leaf_msg(keys, values_hashes)))
                .await
                .map_err(errors::send_err_to_protocol_err)?;

            // wait response from client
            let response = self.client_replies.try_next().await;

            if let Ok(Some(rpc::GetCallbackReply {
                reply:
                    Some(GetReply::SubmitLeaf(rpc::ReplySubmitLeaf {
                        search_result: Some(search_result),
                    })),
            })) = response
            {
                log::debug!("Receive SubmitLeaf({:?})", search_result);

                match search_result {
                    rpc::reply_submit_leaf::SearchResult::Found(idx) => {
                        Ok(SearchResult(Result::Ok(idx as usize)))
                    }
                    rpc::reply_submit_leaf::SearchResult::InsertionPoint(idx) => {
                        Ok(SearchResult(Result::Err(idx as usize)))
                    }
                }
            } else {
                log::warn!("Expected ReplySubmitLeaf, actually: {:?}", &response);
                Err(errors::opt_status_to_protocol_err(response.err()))
            }
        };

        future.boxed()
    }
}

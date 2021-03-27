use std::fmt::Debug;
use tonic::{Status, Streaming};
use tokio::sync::mpsc::Sender;

pub mod rpc {
    // Contains generated Grpc entities for ope Btree
    tonic::include_proto!("opebtree");
}

pub struct ClientSearchCb {
    /// Client receives client's requests from this stream
    server_requests: Streaming<rpc::GetCallback>,
    /// Server receives client responses from this stream
    client_replies: Sender<rpc::GetCallbackReply>,
}

impl ClientSearchCb {
    pub fn new(
        server_requests: Streaming<rpc::GetCallback>,
        client_replies: Sender<rpc::GetCallbackReply>,
    ) -> Self {
        ClientSearchCb {
            server_requests,
            client_replies,
        }
    }

    pub fn get_round_trip<Key, Val>(&self, key: Key) -> Result<Option<Val>, Status>
    where
        Key: Ord + Send + Clone + Debug,
        Val: Clone + Debug,
    {
        todo!()
    }
}

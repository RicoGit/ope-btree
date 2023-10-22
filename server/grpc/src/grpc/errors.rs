use crate::grpc::rpc;
use protocol::ProtocolError;
use tokio::sync::mpsc::error::SendError;
use tonic::{Response, Status};

pub fn status_to_protocol_err(status: Status) -> ProtocolError {
    let msg = format!("Receiving from client error: {:?}", status);
    ProtocolError::RpcErr { msg }
}

pub fn opt_status_to_protocol_err(status: Option<Status>) -> ProtocolError {
    status
        .map(status_to_protocol_err)
        .unwrap_or_else(|| ProtocolError::RpcErr {
            msg: "Receiving from client error: Status=None".to_string(),
        })
}

pub fn send_err_to_protocol_err<T>(err: SendError<T>) -> ProtocolError {
    let msg = format!("Sending to client error: {}", err);
    ProtocolError::RpcErr { msg }
}

pub fn send_err_to_status<T>(_msg: Result<Response<T>, Status>) -> Status {
    let msg = "Sending reply to client failed";
    Status::internal(msg)
}

pub fn unexpected_msg_status(expected: &str, actually: Option<rpc::PutCallbackReply>) -> Status {
    let msg = format!(
        "Wrong message order from client, expected {:?}, actually: {:?}",
        expected, actually
    );
    log::warn!("{}", msg);
    Status::invalid_argument(msg.to_string())
}

pub fn unexpected_msg_err(
    expected: &str,
    actually: Option<rpc::PutCallbackReply>,
) -> ProtocolError {
    let msg = format!(
        "Wrong message order from client, expected {:?}, actually: {:?}",
        expected, actually
    );
    log::warn!("{}", msg);
    ProtocolError::RpcErr { msg }
}

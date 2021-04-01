use crate::grpc::rpc;
use protocol::ProtocolError;
use server::ope_db::DbError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

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

use protocol::ProtocolError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

pub fn send_err_to_protocol_err<T>(err: SendError<T>) -> ProtocolError {
    let msg = format!("Sending to server error: {}", err);
    ProtocolError::RpcErr { msg }
}

pub fn status_to_protocol_err(status: Status) -> ProtocolError {
    let msg = format!("Receiving from server error: {:?}", status);
    ProtocolError::RpcErr { msg }
}

pub fn protocol_error(code: String, description: String) -> ProtocolError {
    let msg = format!("Error from server: code: {:?}, description: {:?}", code, description);
    ProtocolError::RpcErr { msg }
}
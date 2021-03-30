use protocol::ProtocolError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

pub fn send_err_to_protocol_err<T>(_err: SendError<T>) -> ProtocolError {
    todo!()
}

pub fn status_to_protocol_err(_status: Status) -> ProtocolError {
    todo!()
}

use protocol::ProtocolError;
use server::ope_db::DbError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

pub fn db_err_to_status(_err: DbError) -> Status {
    todo!()
}

pub fn status_to_protocol_err(_status: Status) -> ProtocolError {
    todo!()
}

pub fn opt_status_to_protocol_err(_status: Option<Status>) -> ProtocolError {
    todo!()
}

pub fn send_err_to_protocol_err<T>(_err: SendError<T>) -> ProtocolError {
    todo!()
}

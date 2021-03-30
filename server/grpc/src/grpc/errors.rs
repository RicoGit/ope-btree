use crate::grpc::rpc;
use protocol::ProtocolError;
use server::ope_db::DbError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

pub fn status_to_protocol_err(_status: Status) -> ProtocolError {
    todo!()
}

pub fn opt_status_to_protocol_err(_status: Option<Status>) -> ProtocolError {
    todo!()
}

pub fn send_err_to_protocol_err<T>(_err: SendError<T>) -> ProtocolError {
    todo!()
}

pub fn db_err_to_grpc_err(err: DbError) -> rpc::get_callback::Callback {
    rpc::get_callback::Callback::ServerError(rpc::Error {
        code: err.to_string(),
        description: format!("{:?}", err),
    })
}

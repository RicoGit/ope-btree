use futures::future::BoxFuture;

use thiserror::*;

/// OpeBtree protocol. OpeBtree is index for OpeDatabase.
pub mod btree;
/// OpeDatabase protocol
pub mod database;

/// Protocol errors
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Rpc Error: {msg:?}")]
    RpcErr { msg: String },
    #[error("Verification: {msg:?}")]
    VerificationErr { msg: String },
    // todo io error ?
}

pub type Result<V> = std::result::Result<V, ProtocolError>;

/// Future that carries result of Rpc call.
pub type RpcFuture<'f, V> = BoxFuture<'f, Result<V>>;

//! Commands that OpeBtree can perform.

pub mod put_cmd;
pub mod search_cmd;

use super::internal::node::BranchNode;
use crate::ope_btree::internal::node::LeafNode;
use common::merkle::MerklePath;
use protocol::{BtreeCallback, ClientPutDetails, SearchCallback, SearchResult};
use thiserror::Error;

use futures::future::BoxFuture;
use std::future::Future;

#[derive(Error, Debug)]
#[error("Command Error")]
pub struct CmdError {
    #[from]
    source: protocol::ProtocolError,
}

pub type Result<V> = std::result::Result<V, CmdError>;

/// Structs for any OpeBTree commands.
#[derive(Debug, Clone)]
pub struct Cmd<Cb> {
    cb: Cb,
}

impl<Cb> Cmd<Cb> {
    pub fn new(cb: Cb) -> Self {
        Cmd { cb }
    }
}

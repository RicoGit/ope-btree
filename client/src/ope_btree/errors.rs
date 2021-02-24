use crate::crypto::errors::CryptoError;
use common::Hash;
use protocol::ProtocolError;
use std::fmt::Debug;
use thiserror::*;

/// Client's BTree errors
#[derive(Error, Debug)]
pub enum ClientBTreeError {
    #[error("Protocol Error")]
    ProtocolErr {
        #[from]
        source: ProtocolError,
    },
    #[error("CryptoError Error")]
    CryptoErr {
        #[from]
        source: CryptoError,
    },
    #[error("Verification Error: {msg:?}")]
    VerificationErr { msg: String, m_root: Hash },
}

impl ClientBTreeError {
    pub fn wrong_proof<Key: Debug>(
        key: Key,
        keys: Vec<common::Key>,
        children: Vec<Hash>,
        m_root: Hash,
    ) -> ClientBTreeError {
        ClientBTreeError::VerificationErr {
            msg: format!(
                "Checksum didn't pass verifying for key={:?}, Node({:?}, {:?}), merkle root={:?}",
                key, keys, children, m_root
            ),
            m_root,
        }
    }
}

impl From<ClientBTreeError> for ProtocolError {
    fn from(err: ClientBTreeError) -> Self {
        match err {
            ClientBTreeError::ProtocolErr { source } => source,
            err @ ClientBTreeError::CryptoErr { .. } => ProtocolError::VerificationErr {
                msg: format!("{:?}", err),
            },
            err @ ClientBTreeError::VerificationErr { .. } => ProtocolError::VerificationErr {
                msg: format!("{:?}", err),
            },
        }
    }
}

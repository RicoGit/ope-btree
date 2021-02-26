use crate::crypto::errors::CryptoError;
use crate::ope_btree::put_state::PutState;
use bytes::Bytes;
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
    #[error("Illegal State Error: {msg:?}")]
    IllegalStateErr { msg: String },
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
                "Verify NodeProof failed for key={:?}, Node({:?}, {:?}), merkle root={:?}",
                key, keys, children, m_root
            ),
            m_root,
        }
    }

    pub fn wrong_put_proof<Key: Debug, Digest, Crypt>(
        put_state: &PutState<Key, Digest, Crypt>,
        server_m_root: &Bytes,
    ) -> ClientBTreeError {
        let client_m_root = put_state.get_client_root();
        ClientBTreeError::VerificationErr {
            msg: format!(
                "Verify server's Put failed for server_m_root={:?}, client_m_root={:?}, state={:?}",
                server_m_root, &client_m_root, put_state
            ),
            m_root: client_m_root,
        }
    }

    pub fn illegal_state<Key: Debug>(key: &Key, m_root: &Hash) -> ClientBTreeError {
        ClientBTreeError::IllegalStateErr {
            msg: format!(
                "Client put details isn't defined, it's should be defined at previous step, key: {:?}, merkel root={:?}",
                key, m_root
            ),
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
            err @ ClientBTreeError::IllegalStateErr { .. } => ProtocolError::VerificationErr {
                msg: format!("{:?}", err),
            },
        }
    }
}

use crate::crypto::errors::CryptoError;
use crate::ope_btree::put_state::PutState;
use bytes::Bytes;
use common::Hash;
use protocol::ProtocolError;
use std::fmt::Debug;
use thiserror::*;

/// Client's BTree errors
#[derive(Error, Debug)]
pub enum BTreeClientError {
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
    VerificationErr { msg: String },
    #[error("Illegal State Error: {msg:?}")]
    IllegalStateErr { msg: String },
}

impl BTreeClientError {
    pub fn wrong_proof<Key: Debug>(
        key: Key,
        keys: Vec<common::Key>,
        children: Vec<Hash>,
        m_root: &Hash,
    ) -> BTreeClientError {
        BTreeClientError::VerificationErr {
            msg: format!(
                "Verify NodeProof failed for key={:?}, Node({:?}, {:?}), merkle root={:?}",
                key, keys, children, m_root
            ),
        }
    }

    pub fn wrong_put_proof<Key: Debug, Digest, Crypt>(
        put_state: &PutState<Key, Digest, Crypt>,
        server_m_root: &Bytes,
    ) -> BTreeClientError {
        BTreeClientError::VerificationErr {
            msg: format!(
                "Verify server's Put failed for server_m_root={:?}, client_m_root={:?}, state={:?}",
                server_m_root,
                put_state.m_root(),
                put_state
            ),
        }
    }

    pub fn illegal_state<Key: Debug>(key: &Key, m_root: &Hash) -> BTreeClientError {
        BTreeClientError::IllegalStateErr {
            msg: format!(
                "Client put details isn't defined, it's should be defined at previous step, key: {:?}, merkel root={:?}",
                key, m_root
            ),
        }
    }
}

impl From<BTreeClientError> for ProtocolError {
    fn from(err: BTreeClientError) -> Self {
        match err {
            BTreeClientError::ProtocolErr { source } => source,
            err @ BTreeClientError::CryptoErr { .. } => ProtocolError::VerificationErr {
                msg: format!("{:?}", err),
            },
            err @ BTreeClientError::VerificationErr { .. } => ProtocolError::VerificationErr {
                msg: format!("{:?}", err),
            },
            err @ BTreeClientError::IllegalStateErr { .. } => ProtocolError::VerificationErr {
                msg: format!("{:?}", err),
            },
        }
    }
}

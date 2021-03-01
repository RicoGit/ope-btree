use crate::crypto::errors::CryptoError;
use crate::ope_btree::errors::BTreeClientError;
use protocol::ProtocolError;
use thiserror::Error;

/// Database client errors
#[derive(Error, Debug)]
pub enum DatabaseClientError {
    #[error("Index Error")]
    IndexErr {
        #[from]
        source: BTreeClientError,
    },
    #[error("Crypto Error")]
    CryptoErr {
        #[from]
        source: CryptoError,
    },
    #[error("Protocol Error")]
    ProtocolErr {
        #[from]
        source: ProtocolError,
    },
}

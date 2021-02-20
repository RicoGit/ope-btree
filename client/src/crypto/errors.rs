use thiserror::*;

/// Crypto errors
#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Crypto Error: {msg:?}")]
    CryptoErr { msg: String },
    #[error("Illegal State Error: {msg:?}")]
    IllegalStateErr { msg: String },
}

impl CryptoError {
    pub fn illegal_state(msg: &str) -> CryptoError {
        CryptoError::IllegalStateErr {
            msg: msg.to_owned(),
        }
    }
}

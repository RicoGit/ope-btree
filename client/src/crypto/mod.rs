use crate::crypto::errors::CryptoError;
use bytes::Bytes;

pub mod cypher_search;
pub mod errors;

pub type Result<V> = std::result::Result<V, CryptoError>;

/// Encrypts 'data'
pub trait Encryptor {
    type PlainData;

    fn encrypt(&self, data: Self::PlainData) -> Result<Bytes>;
}

/// Decrypts 'data'
pub trait Decryptor {
    type PlainData;

    fn decrypt(&self, encrypted_data: &[u8]) -> Result<Self::PlainData>;
}

#[derive(Default, Debug)]
struct NoOpCrypt {}

impl Encryptor for NoOpCrypt {
    type PlainData = String;

    fn encrypt(&self, data: String) -> Result<Bytes> {
        Ok(Bytes::from(data))
    }
}

impl Decryptor for NoOpCrypt {
    type PlainData = String;

    fn decrypt(&self, encrypted_data: &[u8]) -> Result<String> {
        Ok(String::from_utf8_lossy(encrypted_data).to_string())
    }
}

// todo AES implementation for KeyCrypt

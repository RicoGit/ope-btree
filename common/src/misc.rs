use super::{Bytes, Hash, Key};
use crate::STR_END_SIGN;
use bytes::BytesMut;
use sha3::digest::generic_array::typenum::U512;
use sha3::digest::generic_array::GenericArray;

impl From<Key> for BytesMut {
    fn from(key: Key) -> Self {
        key.0
    }
}

impl From<Hash> for BytesMut {
    fn from(hash: Hash) -> Self {
        hash.0
    }
}

/// Allows to get inner state (`Bytes`) of all New Types that wraps `Bytes`
/// and implement From<Bytes>.
///
/// # Examples
///
/// ```
/// # use bytes::BytesMut;
/// # use crate::common::Hash;
/// # use crate::common::misc::ToBytes;
///
/// let h1 = Hash(BytesMut::from("hash"));
/// let h2 = Hash(BytesMut::from("hash"));
/// let bytes1 = h1.0;        // 0 is hard to read
/// let bytes2 = h2.bytes();  // the same, but more readable
/// assert_eq!(bytes1, bytes2);
/// ```
pub trait ToBytes: Sized {
    /// Returns inner Bytes
    fn bytes(self) -> Bytes;

    /// Returns inner BytesMut
    fn bytes_mut(self) -> BytesMut;
}

impl<T: Into<BytesMut>> ToBytes for T {
    fn bytes(self) -> Bytes {
        self.into().freeze()
    }
    fn bytes_mut(self) -> BytesMut {
        self.into()
    }
}

pub trait AsString {
    /// Converts self to UTF-8 string and truncate tail with zeroes.
    fn as_str(&self) -> Result<String, String>;
}

impl AsString for GenericArray<u8, U512> {
    fn as_str(&self) -> Result<String, String> {
        String::from_utf8(self.to_vec())
            .map(|str| str.split(STR_END_SIGN as char).take(1).collect())
            .map_err(|err| err.to_string())
    }
}

impl AsString for Hash {
    fn as_str(&self) -> Result<String, String> {
        String::from_utf8(self.0.to_vec())
            .map(|str| str.split(STR_END_SIGN as char).take(1).collect())
            .map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn to_bytes_test() {
        let origin = BytesMut::from("hash");
        let hash = Hash(origin.clone());
        assert_eq!(origin.clone().freeze(), hash.bytes());

        let hash = Hash(origin.clone());
        assert_eq!(origin.clone(), hash.bytes_mut());
    }
}

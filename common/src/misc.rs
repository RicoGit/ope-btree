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

impl From<Bytes> for Key {
    fn from(bytes: Bytes) -> Self {
        Key(BytesMut::from(bytes.as_ref()))
    }
}

impl From<Hash> for BytesMut {
    fn from(hash: Hash) -> Self {
        hash.0
    }
}

impl From<Bytes> for Hash {
    fn from(bytes: Bytes) -> Self {
        Hash(BytesMut::from(bytes.as_ref()))
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

/// Replaces ''item'' in ''vector'' for specified ''idx''
pub fn replace<T>(vector: &mut Vec<T>, item: T, idx: usize) {
    let _ = std::mem::replace(&mut vector[idx], item.into());
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

    #[test]
    fn replace_test() {
        let item = Bytes::from("###");
        let item1 = Bytes::from("item1");
        let item2 = Bytes::from("item2");
        let item3 = Bytes::from("item3");

        let mut vec = vec![item1.clone(), item2.clone(), item3.clone()];

        replace(&mut vec, item.clone(), 0);
        assert_eq!(vec, vec![item.clone(), item2.clone(), item3.clone()]);

        replace(&mut vec, item.clone(), 1);
        assert_eq!(vec, vec![item.clone(), item.clone(), item3.clone()]);

        replace(&mut vec, item.clone(), 2);
        assert_eq!(vec, vec![item.clone(), item.clone(), item.clone()]);
    }
}

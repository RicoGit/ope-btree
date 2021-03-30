use super::{Bytes, Hash, Key};
use crate::STR_END_SIGN;
use bytes::BytesMut;
use digest::generic_array::typenum::U512;
use digest::generic_array::GenericArray;

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

impl ToBytes for Hash {
    fn bytes(self) -> Bytes {
        self.0.freeze()
    }
    fn bytes_mut(self) -> BytesMut {
        self.into()
    }
}

impl ToBytes for Key {
    fn bytes(self) -> Bytes {
        self.0.freeze()
    }
    fn bytes_mut(self) -> BytesMut {
        self.into()
    }
}

impl ToBytes for Vec<u8> {
    fn bytes(self) -> Bytes {
        self.into()
    }
    fn bytes_mut(self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self);
        buf
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

pub trait FromVecBytes {
    /// Converts into Bytes without copying
    fn into_byte_vec(self) -> Vec<Vec<u8>>;
}

impl FromVecBytes for Vec<Bytes> {
    fn into_byte_vec(self) -> Vec<Vec<u8>> {
        self.into_iter().map(|bytes| bytes.to_vec()).collect()
    }
}

pub trait ToVecBytes {
    /// Clone as bytes
    fn clone_as_bytes(&self) -> Vec<Bytes>;

    /// Converts into Bytes without copying
    fn into_bytes(self) -> Vec<Bytes>;
}

impl<T: ToBytes + Clone> ToVecBytes for Vec<T> {
    fn clone_as_bytes(&self) -> Vec<Bytes> {
        self.iter().map(|t| t.clone().bytes()).collect()
    }

    fn into_bytes(self) -> Vec<Bytes> {
        self.into_iter().map(|t| t.bytes()).collect()
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

        let key = Key(origin.clone());
        assert_eq!(origin.clone().freeze(), key.bytes());

        let key = Key(origin.clone());
        assert_eq!(origin.clone(), key.bytes_mut());

        let vec = origin.to_vec();
        assert_eq!(Bytes::from(vec.clone()), vec.bytes());

        let vec = origin.to_vec();
        assert_eq!(origin.clone(), vec.bytes_mut());
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

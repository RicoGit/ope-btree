use super::{Bytes, Hash, Key};
use bytes::BytesMut;

impl From<Key> for Bytes {
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
    /// Returns inner Bytes, does the same thing ```key.0```
    fn bytes(self) -> BytesMut;
}

impl<T: Into<BytesMut>> ToBytes for T {
    fn bytes(self) -> BytesMut {
        self.into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn key_valref_hash_converters() {
        let origin = BytesMut::from("hash");
        let hash = Hash(origin.clone());
        assert_eq!(origin.clone(), hash.bytes());
    }
}

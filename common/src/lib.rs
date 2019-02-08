//! Common structures and traits for all submodules. Maybe it's a temp solution.

use bytes::Bytes;

#[derive(Clone)]
pub struct Hash(pub Bytes);

impl From<Hash> for Bytes {
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
/// # use bytes::Bytes;
/// # use common::Hash;
/// # use common::ToBytes;
/// let h1 = Hash(Bytes::from("hash"));
/// let h2 = Hash(Bytes::from("hash"));
/// let bytes1 = h1.0;        // 0 is hard to read
/// let bytes2 = h2.bytes();  // the same, but more readable
/// assert_eq!(bytes1, bytes2);
/// ```
pub trait ToBytes: Sized {
    /// Returns inner Bytes, does the same thing ```key.0```
    fn bytes(self) -> Bytes;
}

impl<T: Into<Bytes>> ToBytes for T {
    fn bytes(self) -> Bytes {
        self.into()
    }
}

#[cfg(test)]
mod test {
    use super::ToBytes;
    use crate::Hash;
    use bytes::Bytes;

    #[test]
    fn key_valref_hash_converters() {
        let origin = Bytes::from("hash");
        let hash = Hash(origin.clone());
        assert_eq!(origin.clone(), hash.bytes());
    }

}

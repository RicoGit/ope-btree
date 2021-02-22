//! Common structures and traits for all submodules. Maybe it's a temp solution.

pub mod gen;
pub mod merkle;
pub mod misc;

pub mod noop_hasher;

use crate::misc::AsString;
use bytes::{BufMut, Bytes, BytesMut};

use serde::{Deserialize, Serialize};
use sha3::digest::generic_array::ArrayLength;
use sha3::digest::generic_array::GenericArray;
pub use sha3::Digest;
use std::fmt::{Debug, Display, Formatter};

pub const STR_END_SIGN: u8 = 0_u8;

/// A ciphered key for retrieve a value.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
pub struct Key(pub BytesMut);

impl Key {
    /// Returns empty Key.
    pub fn empty() -> Self {
        Key(BytesMut::new())
    }

    /// Turns str to Key (for test purpose)
    pub fn from_str(str: &str) -> Key {
        Key(BytesMut::from(str))
    }
}

/// A hash of anything.
#[derive(Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
pub struct Hash(pub BytesMut);

impl Hash {
    /// Returns empty Hash.
    pub fn empty() -> Self {
        Hash(BytesMut::new())
    }

    /// Turns str to Hash (for test purpose)
    pub fn from_str(str: &str) -> Hash {
        Hash(BytesMut::from(str))
    }

    /// Calculates Hash from specified data
    pub fn build<D: Digest, R: AsRef<[u8]>>(slice: R) -> Hash {
        Hash::from(D::digest(slice.as_ref()))
    }

    /// Returns true is hash is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Adds the specified hash to self as a tail.
    pub fn concat(&mut self, hash: Hash) {
        self.0.put(hash.0)
    }

    /// Adds all specified hashes to self as a tail.
    pub fn concat_all<I: IntoIterator<Item = Hash>>(&mut self, hashes: I) {
        for hash in hashes.into_iter() {
            self.concat(hash)
        }
    }
}

impl From<Key> for Hash {
    fn from(key: Key) -> Self {
        Hash(key.0)
    }
}

impl<L: ArrayLength<u8>> From<GenericArray<u8, L>> for Hash {
    fn from(ga: GenericArray<u8, L>) -> Self {
        Hash(BytesMut::from(ga.as_slice()))
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = self.as_str().unwrap_or_else(|_| "[]".to_string());
        write!(f, "Hash[{}, {}]", self.0.len(), str)
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = self.as_str().unwrap_or_else(|_| "[]".to_string());
        write!(f, "Hash[{}, {}]", self.0.len(), str)
    }
}

#[cfg(test)]
mod tests {
    use crate::noop_hasher::NoOpHasher;
    use crate::{Hash, STR_END_SIGN};
    use bytes::{BufMut, BytesMut};
    use sha3::Sha3_256;

    #[test]
    fn is_empty_test() {
        let empty = Hash::empty();
        assert!(empty.is_empty());
        let non_empty = Hash::from_str("non empty");
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn hash_build_test() {
        let test_hash = Hash::build::<NoOpHasher, _>("test");
        assert_eq!(test_hash.to_string(), "Hash[512, [test]]");

        let bin_hash = Hash::build::<Sha3_256, _>("test");
        let expected =
            b"6\xf0(X\x0b\xb0,\xc8'*\x9a\x02\x0fB\0\xe3F\xe2v\xaefNE\xee\x80tUt\xe2\xf5\xab\x80";
        assert_eq!(bin_hash, Hash(BytesMut::from(&expected[..])));
    }

    #[test]
    fn concat_test() {
        let empty = Hash::empty();
        let mut one = Hash::from_str("_1_");
        let two = Hash::from_str("_2_");

        one.concat(empty);
        assert_eq!(one, Hash::from_str("_1_"));

        one.concat(two);
        assert_eq!(one, Hash::from_str("_1__2_"));
    }

    #[test]
    fn concat_all_test() {
        let mut one = Hash::from_str("_1_");

        let many = vec![Hash::from_str("_A_"), Hash::from_str("_B_")];
        let empty1 = vec![];
        let empty2 = vec![Hash::empty()];

        one.concat_all(empty1);
        assert_eq!(Hash::from_str("_1_"), one);

        one.concat_all(empty2);
        assert_eq!(Hash::from_str("_1_"), one);

        one.concat_all(many);
        assert_eq!(Hash::from_str("_1__A__B_"), one);
    }

    #[test]
    fn hash_from_gen_arr_test() {
        use sha3::digest::generic_array::typenum::U8;
        use sha3::digest::generic_array::GenericArray;
        let vec = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let ga: GenericArray<u8, U8> =
            GenericArray::from_exact_iter(vec.clone().into_iter()).unwrap();

        assert_eq!(Hash(BytesMut::from(vec.as_slice())), ga.into())
    }

    #[test]
    fn hash_as_ref_test() {
        assert_eq!("hash".as_bytes(), Hash::from_str("hash").as_ref())
    }

    #[test]
    fn display_test() {
        let mut input = Hash::from_str("start");
        input.0.put_u8(STR_END_SIGN);
        input.concat(Hash::from_str("end"));
        assert_eq!(input.to_string(), "Hash[9, start]");

        let hash = Hash::build::<NoOpHasher, _>("test");
        assert_eq!(hash.to_string(), "Hash[512, [test]]");
    }
}

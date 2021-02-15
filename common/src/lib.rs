//! Common structures and traits for all submodules. Maybe it's a temp solution.

pub mod gen;
pub mod merkle;
pub mod misc;

#[cfg(test)]
pub mod noop_hasher;

use crate::misc::AsString;
use bytes::{Bytes, BytesMut};
use misc::ToBytes;
use serde::{Deserialize, Serialize};
use sha3::digest::generic_array::ArrayLength;
use sha3::digest::generic_array::GenericArray;
pub use sha3::Digest;
use std::fmt::{Display, Formatter};

pub const STR_END_SIGN: u8 = 0_u8;

/// A ciphered key for retrieve a value.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
pub struct Key(pub BytesMut);

/// A hash of anything.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
pub struct Hash(pub BytesMut);

impl Hash {
    /// Returns empty hash.
    pub fn empty() -> Self {
        Hash(BytesMut::new())
    }

    /// Returns true is hash is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Adds the specified hash to self as a tail.
    pub fn concat(&mut self, hash: Hash) {
        let this = &mut self.0;
        this.extend(hash.bytes());
    }

    /// Adds all specified hashes to self as a tail.
    pub fn concat_all<I: IntoIterator<Item = Hash>>(&mut self, hashes: I) {
        for hash in hashes.into_iter() {
            self.concat(hash)
        }
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

/// Calculates hash from specified data
pub fn get_hash<D: Digest, R: AsRef<[u8]>>(slice: R) -> Hash {
    Hash::from(D::digest(slice.as_ref()))
}

impl Display for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = self.as_str().unwrap_or_else(|_| "[]".to_string());
        write!(f, "Hash[{}, {}]", str.len(), str)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Hash, STR_END_SIGN};
    use bytes::{BufMut, BytesMut};

    #[test]
    fn is_empty_test() {
        let empty = Hash::empty();
        assert!(empty.is_empty());
        let non_empty = hash("non empty");
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn concat_test() {
        let empty = Hash::empty();
        let mut one = hash("_1_");
        let two = hash("_2_");

        one.concat(empty);
        assert_eq!(hash("_1_"), one);

        one.concat(two);
        assert_eq!(hash("_1__2_"), one);
    }

    #[test]
    fn concat_all_test() {
        let mut one = hash("_1_");

        let many = vec![hash("_A_"), hash("_B_")];
        let empty1 = vec![];
        let empty2 = vec![Hash::empty()];

        one.concat_all(empty1);
        assert_eq!(hash("_1_"), one);

        one.concat_all(empty2);
        assert_eq!(hash("_1_"), one);

        one.concat_all(many);
        assert_eq!(hash("_1__A__B_"), one);
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
        assert_eq!("hash".as_bytes(), hash("hash").as_ref())
    }

    #[test]
    fn display_test() {
        let mut input = hash("start");
        input.0.put_u8(STR_END_SIGN);
        input.concat(hash("end"));
        assert_eq!(format!("{}", input), "Hash[5, start]")
    }

    fn hash(str: &str) -> Hash {
        Hash(BytesMut::from(str))
    }
}

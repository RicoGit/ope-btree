//! `No-operation` implementation for `Digest`, for debug purpose only.

use sha3::digest::generic_array::typenum::U512;
use sha3::digest::generic_array::GenericArray;
use sha3::Digest;
use std::string::ToString;

pub struct NoOpHasher {
    /// Characters that wrap hashed string around.
    wrapper: (char, char),
    /// A sign that divides hashed string with a tail with zeros.
    end_sign: u8,
    /// Data for hashing
    data: Vec<u8>,
}

pub const HASH_SIZE: usize = 512;

impl Digest for NoOpHasher {
    type OutputSize = U512; // allow {HASH_SIZE} bytes length result hash

    fn new() -> Self {
        let mut data = Vec::with_capacity(HASH_SIZE);
        NoOpHasher {
            wrapper: ('[', ']'),
            end_sign: 0_u8,
            data,
        }
    }

    fn input<B: AsRef<[u8]>>(&mut self, data: B) {
        if self.data.is_empty() {
            self.data.push(b'[')
        }
        self.data.extend_from_slice(data.as_ref());
    }

    fn chain<B: AsRef<[u8]>>(mut self, data: B) -> Self
    where
        Self: Sized,
    {
        self.input(data);
        self
    }

    fn result(mut self) -> GenericArray<u8, Self::OutputSize> {
        self.result_reset()
    }

    fn result_reset(&mut self) -> GenericArray<u8, Self::OutputSize> {
        let NoOpHasher {
            wrapper,
            end_sign,
            data,
        } = self;

        if !data.is_empty() {
            // add closed wrapper char
            data.extend_from_slice(wrapper.1.to_string().as_bytes());
        }
        data.push(*end_sign);
        if data.len() < HASH_SIZE {
            // add zeroes to and of the vector. 'data' must have size == HASH_SIZE!
            let num = HASH_SIZE - data.len();
            for n in 0..num {
                data.push(b'_')
            }
        }

        if data.len() > HASH_SIZE {
            // 'data' must have size == HASH_SIZE !
            data.truncate(HASH_SIZE)
        }

        let data = data.clone();
        self.reset();

        GenericArray::from_exact_iter(data.into_iter()).expect(&format!(
            "NoOpHasher::result() failed, cause data must to have size={}",
            HASH_SIZE
        ))
    }

    fn reset(&mut self) {
        self.data.clear()
    }

    fn output_size() -> usize {
        HASH_SIZE
    }

    fn digest(data: &[u8]) -> GenericArray<u8, Self::OutputSize> {
        let mut hasher = Self::new();
        hasher.input(data);
        hasher.result()
    }
}

pub trait AsString {
    fn as_str(&self) -> Result<String, String>;
}

impl AsString for GenericArray<u8, U512> {
    /// Converts 'GenericArray' to UTF-8 string and truncate tail with zeroes.
    fn as_str(&self) -> Result<String, String> {
        String::from_utf8(self.to_vec())
            .map(|str| str.split(0_u8 as char).take(1).collect())
            .map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use sha3::Digest;

    #[test]
    fn input_test() {
        let mut hasher = NoOpHasher::new();
        hasher.input("test");
        let result = hasher.result();
        assert_eq!(Ok(String::from("[test]")), result.as_str());
    }

    #[test]
    fn chain_test() {
        let mut hasher = NoOpHasher::new();
        let result = hasher.chain("1").chain("_").chain("2").result();
        assert_eq!(Ok(String::from("[1_2]")), result.as_str());
    }

    #[test]
    fn reset_test() {
        let mut hasher = NoOpHasher::new();
        hasher.input("test");
        hasher.reset();
        let result = hasher.result();
        assert_eq!(Ok(String::from("")), result.as_str())
    }

    #[test]
    fn result_reset_test() {
        let mut hasher = NoOpHasher::new();
        hasher.input("test");
        let result1 = hasher.result_reset();
        assert_eq!(Ok(String::from("[test]")), result1.as_str());
        let result2 = hasher.result_reset();
        assert_eq!(Ok(String::from("")), result2.as_str());
        hasher.input("dog");
        let result3 = hasher.result_reset();
        assert_eq!(Ok(String::from("[dog]")), result3.as_str());
    }

    #[test]
    fn digest_test() {
        let result1 = NoOpHasher::digest("test".as_bytes());
        let result2 = NoOpHasher::digest(result1.as_str().unwrap().as_bytes());
        assert_eq!(Ok(String::from("[[test]]")), result2.as_str())
    }

    #[test]
    fn big_hash_test() {
        let mut hasher = NoOpHasher::new();

        hasher.input(big_str(1000));
        let result: String = hasher.result().as_str().unwrap();
        assert_eq!(NoOpHasher::output_size(), result.len());
        assert!(result.starts_with("[0123456789"))
    }

    fn big_str(size: usize) -> String {
        let mut str = String::with_capacity(size);
        for n in 0..size {
            str.push_str(&n.to_string())
        }
        str
    }

}

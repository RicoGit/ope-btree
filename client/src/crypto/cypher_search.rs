use crate::crypto::{Decryptor, Result};

use protocol::SearchResult;

use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug)]
pub struct CipherSearch<Decryptor> {
    pub decryptor: Decryptor,
}

impl<PlainData, D> CipherSearch<D>
where
    PlainData: Ord + Debug,
    D: Decryptor<PlainData = PlainData>,
{
    pub fn new(decryptor: D) -> Self {
        CipherSearch { decryptor }
    }

    /// Searches an index of the specified element in encrypted values using the binary search.
    /// The vector should be sorted with the same `Ordering` before calling, otherwise, the results are undefined.
    ///
    /// `coll` Ordered collection of encrypted elements to search in.
    /// `ordering` The ordering to be used to compare elements.
    ///
    /// Returns [`Result::Ok`] value containing the index corresponding to the search element in the
    /// sequence. A [`Result::Err`] value containing the index where the element would be inserted if
    ///  the search element is not found in the sequence.
    pub fn binary_search(&self, slice: &Vec<&[u8]>, key: PlainData) -> Result<SearchResult> {
        let mut size = slice.len();
        if size == 0 {
            return Ok(Err(0));
        }

        let mut base = 0;

        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let mid_key = self
                .decryptor
                .decrypt(unsafe { slice.get_unchecked(mid) })?;

            base = match mid_key.cmp(&key) {
                Ordering::Greater => base,
                _ => mid,
            };
            size -= half;
        }

        let base_key = self
            .decryptor
            .decrypt(unsafe { slice.get_unchecked(base) })?;

        let res = match key.cmp(&base_key) {
            Ordering::Greater => Err(base + 1),
            Ordering::Equal => Ok(base),
            Ordering::Less => Err(base),
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::NoOpCrypt;

    #[test]
    fn binary_search_empty_test() {
        let cs = CipherSearch::new(NoOpCrypt::default());

        let vec: Vec<&[u8]> = vec![];
        let res = cs.binary_search(&vec, "key".to_string()).unwrap();
        assert_eq!(res, Err(0))
    }

    #[test]
    fn binary_search_one_elem_test() {
        let cs = CipherSearch::new(NoOpCrypt::default());

        let vec = vec!["k2".as_bytes()];
        assert_eq!(cs.binary_search(&vec, k(1)).unwrap(), Err(0));
        assert_eq!(cs.binary_search(&vec, k(2)).unwrap(), Ok(0));
        assert_eq!(cs.binary_search(&vec, k(3)).unwrap(), Err(1));
    }

    #[test]
    fn binary_search_many_elem_test() {
        let cs = CipherSearch::new(NoOpCrypt::default());

        let vec = vec![
            "k1".as_bytes(),
            "k2".as_bytes(),
            "k4".as_bytes(),
            "k7".as_bytes(),
            "k8".as_bytes(),
        ];
        assert_eq!(cs.binary_search(&vec, k(0)).unwrap(), Err(0));
        assert_eq!(cs.binary_search(&vec, k(1)).unwrap(), Ok(0));
        assert_eq!(cs.binary_search(&vec, k(3)).unwrap(), Err(2));
        assert_eq!(cs.binary_search(&vec, k(5)).unwrap(), Err(3));
        assert_eq!(cs.binary_search(&vec, k(7)).unwrap(), Ok(3));
        assert_eq!(cs.binary_search(&vec, k(9)).unwrap(), Err(5));
    }

    fn k(idx: usize) -> String {
        format!("k{}", idx)
    }
}

use crate::crypto::{Decryptor, Result};

use protocol::btree::SearchResult;

use common::Key;
use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Clone, Debug)]
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
    pub fn binary_search(&self, slice: &[Key], key: PlainData) -> Result<SearchResult> {
        let mut size = slice.len();
        if size == 0 {
            return Ok(SearchResult(Err(0)));
        }

        let mut base = 0;

        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let mid_key = self
                .decryptor
                .decrypt(unsafe { slice.get_unchecked(mid).as_ref() })?;

            base = match mid_key.cmp(&key) {
                Ordering::Greater => base,
                _ => mid,
            };
            size -= half;
        }

        let base_key = self
            .decryptor
            .decrypt(unsafe { slice.get_unchecked(base).as_ref() })?;

        let res = match key.cmp(&base_key) {
            Ordering::Greater => SearchResult(Err(base + 1)),
            Ordering::Equal => SearchResult(Ok(base)),
            Ordering::Less => SearchResult(Err(base)),
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

        let vec: Vec<Key> = vec![];
        let res = cs.binary_search(&vec, "key".to_string()).unwrap();
        assert_eq!(res, SearchResult(Err(0)))
    }

    #[test]
    fn binary_search_one_elem_test() {
        let cs = CipherSearch::new(NoOpCrypt::default());

        let vec = vec![Key::from_string("k2")];
        assert_eq!(cs.binary_search(&vec, k(1)).unwrap(), SearchResult(Err(0)));
        assert_eq!(cs.binary_search(&vec, k(2)).unwrap(), SearchResult(Ok(0)));
        assert_eq!(cs.binary_search(&vec, k(3)).unwrap(), SearchResult(Err(1)));
    }

    #[test]
    fn binary_search_many_elem_test() {
        let cs = CipherSearch::new(NoOpCrypt::default());

        let vec = vec![
            Key::from_string("k1"),
            Key::from_string("k2"),
            Key::from_string("k4"),
            Key::from_string("k7"),
            Key::from_string("k8"),
        ];
        assert_eq!(cs.binary_search(&vec, k(0)).unwrap(), SearchResult(Err(0)));
        assert_eq!(cs.binary_search(&vec, k(1)).unwrap(), SearchResult(Ok(0)));
        assert_eq!(cs.binary_search(&vec, k(3)).unwrap(), SearchResult(Err(2)));
        assert_eq!(cs.binary_search(&vec, k(5)).unwrap(), SearchResult(Err(3)));
        assert_eq!(cs.binary_search(&vec, k(7)).unwrap(), SearchResult(Ok(3)));
        assert_eq!(cs.binary_search(&vec, k(9)).unwrap(), SearchResult(Err(5)));
    }

    fn k(idx: usize) -> String {
        format!("k{}", idx)
    }
}

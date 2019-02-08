//! Implementation of Btree node.

use crate::ope_btree::ValueRef;
use common::{Hash, Key};
use std::ops::Deref;

type NodeRef = usize;

/// Tree node representation.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Leaf(LeafNode),
    Branch(BranchNode),
}

/// A leaf element of the tree, contains references of stored values with
/// corresponding keys and other supporting data. All leaves are located at the
/// same depth (maximum depth) in the tree. All arrays are in a one to one
/// relationship. It means that:
/// `keys.size == values_refs.size == values_hashes.size == kv_hashes.size == size`
///
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct LeafNode {
    /// Search keys
    keys: Vec<Key>,

    /// Stored values references
    values_refs: Vec<ValueRef>,

    /// Array of hashes for each encrypted stored value. **Not a hashes of value
    /// reference!**
    values_hashes: Vec<Hash>,

    /// Array of hashes for each pair with key and value hash.
    /// 'hash(key + hash_of_value)' (optimization, decreases recalculation)
    kv_hashes: Vec<Hash>,

    /// Number of keys inside this leaf. Actually a size of each array in the leaf.
    size: usize,

    /// The hash of the leaf state (the hash of concatenated `kv_hashes`)
    hash: Hash,

    /// A reference to the right sibling leaf. Rightmost leaf don't have right sibling.
    right_sibling: Option<NodeRef>,
}

/// Branch node of the tree, do not contains any business values, contains
/// references to children nodes. Number of children == number of keys in all
/// branches except last(rightmost) for any tree level. The rightmost branch
/// contain (size + 1) children
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct BranchNode {
    /// Search keys
    keys: Vec<Key>,

    /// Children references
    children_refs: Vec<NodeRef>,

    /// Array of hashes for each child node. **Not a checksum of child reference!**
    children_hashes: Vec<Hash>,

    /// Number of keys inside this branch
    size: usize,

    /// Hash of branch state
    hash: Hash,
}

//
// Node Operations.
//

/// The root of tree elements hierarchy.
pub trait TreeNode {
    /// Stored search keys
    fn keys() -> Vec<Key>;

    /// Number of keys inside this tree node (optimization)
    fn size() -> usize;

    /// Digest of this node state
    fn hash() -> Hash;
}

impl LeafNode {
    fn do_leaf(&self) -> Hash {
        Hash(bytes::Bytes::from("leaf")) // todo remove!
    }
}

impl BranchNode {
    fn do_branch(&self) -> Hash {
        Hash(bytes::Bytes::from("branch")) // todo remove!
    }
}

#[cfg(test)]
pub mod tests {
    use super::Key;
    use super::ValueRef;
    use crate::ope_btree::node::BranchNode;
    use crate::ope_btree::node::LeafNode;
    use crate::ope_btree::node::Node;
    use bytes::Bytes;
    use common::Hash;
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    #[test]
    fn leaf_serde_test() {
        let leaf = create_leaf();
        let mut buf = Vec::new();
        leaf.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(leaf, Deserialize::deserialize(&mut de).unwrap());
    }

    #[test]
    fn branch_serde_test() {
        let branch = create_branch();
        let mut buf = Vec::new();
        branch.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(branch, Deserialize::deserialize(&mut de).unwrap());
    }

    #[test]
    fn layout() {
        // todo remove later
        let node = Node::Leaf(create_leaf());

        match node {
            Node::Leaf(leaf) => leaf.do_leaf(),
            Node::Branch(branch) => branch.do_branch(),
        };

        dbg!(std::mem::size_of::<Node>());
    }

    pub fn create_leaf() -> LeafNode {
        LeafNode {
            keys: vec![Key(Bytes::from("key1")), Key(Bytes::from("key2"))],
            values_refs: vec![ValueRef(Bytes::from("ref1")), ValueRef(Bytes::from("ref2"))],
            values_hashes: vec![Hash(Bytes::from("hash1")), Hash(Bytes::from("hash2"))],
            kv_hashes: vec![Hash(Bytes::from("kv_hash1")), Hash(Bytes::from("kv_hash2"))],
            size: 2,
            hash: Hash(Bytes::from("leaf_hash")),
            right_sibling: None,
        }
    }

    pub fn create_branch() -> BranchNode {
        BranchNode {
            keys: vec![Key(Bytes::from("key1")), Key(Bytes::from("key2"))],
            children_refs: vec![1, 2],
            size: 2,
            hash: Hash(Bytes::from("leaf_hash")),
            children_hashes: vec![
                Hash(Bytes::from("child_hash1")),
                Hash(Bytes::from("child_hash2")),
            ],
        }
    }

}

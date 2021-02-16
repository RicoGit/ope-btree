//! Implementation of Btree node.

use crate::ope_btree::ValueRef;
use bytes::Bytes;
use common::merkle::{MerkleError, NodeProof};
use common::misc::ToBytes;
use common::{Digest, Hash, Key};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

type NodeRef = usize;

/// Tree node representation.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum Node {
    Leaf(LeafNode),
    Branch(BranchNode),
}

impl Node {
    /// Creates and returns a new empty Leaf.
    pub fn empty_leaf() -> Node {
        Node::Leaf(LeafNode::empty())
    }

    /// Creates and returns a new empty Branch.
    pub fn empty_branch() -> Node {
        Node::Branch(BranchNode::new())
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }
}

/// A leaf element of the tree, contains references of stored values with
/// corresponding keys and other supporting data. All leaves are located at the
/// same depth (maximum depth) in the tree. All arrays are in a one to one
/// relationship. It means that:
/// `keys.size == values_refs.size == values_hashes.size == kv_hashes.size == size`
///
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
pub struct LeafNode {
    /// Search keys
    pub keys: Vec<Key>,

    /// Stored values references
    pub values_refs: Vec<ValueRef>,

    /// Array of hashes for each encrypted stored value. **Not a hashes of value
    /// reference!**
    pub values_hashes: Vec<Hash>,

    /// Array of hashes for each pair with key and value hash.
    /// 'hash(key + hash_of_value)' (optimization, decreases recalculation)
    pub kv_hashes: Vec<Hash>,

    /// Number of keys inside this leaf. Actually a size of each array in the leaf.
    pub size: usize,

    /// The hash of the leaf state (the hash of concatenated `kv_hashes`)
    pub hash: Hash,

    /// A reference to the right sibling leaf. Rightmost leaf don't have right sibling.
    pub right_sibling: Option<NodeRef>,
}

impl LeafNode {
    /// Creates and returns a new empty LeafNode.
    pub fn empty() -> Self {
        LeafNode::default()
    }

    /// Create new leaf with specified ''key'' and ''value''
    pub fn create<D: Digest>(
        key: Key,
        value_ref: ValueRef,
        value_hash: Hash,
    ) -> Result<LeafNode, MerkleError> {
        let keys = vec![key];
        let values_refs = vec![value_ref];
        let values_hashes = vec![value_hash];
        let kv_hashes = LeafNode::build_checksum::<D>(keys.clone(), values_hashes.clone());
        let hash =
            NodeProof::try_new(Hash::empty(), kv_hashes.clone(), None)?.calc_checksum::<D>(None);
        let leaf = LeafNode {
            keys,
            values_refs,
            values_hashes,
            kv_hashes,
            size: 1,
            hash,
            right_sibling: None,
        };

        Ok(leaf)
    }

    /// Returns array of checksums for each key-value pair
    fn build_checksum<D: Digest>(keys: Vec<Key>, values: Vec<Hash>) -> Vec<Hash> {
        keys.into_iter()
            .zip(values.into_iter())
            .map(|(k, v)| {
                let mut key: Hash = k.into();
                key.concat(v);
                key
            })
            .collect()
    }
}

/// Branch node of the tree, do not contains any business values, contains
/// references to children nodes. Number of children == number of keys in all
/// branches except last(rightmost) for any tree level. The rightmost branch
/// contain (size + 1) children
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize, Default)]
pub struct BranchNode {
    /// Search keys
    pub keys: Vec<Key>,

    /// Children references
    pub children_refs: Vec<NodeRef>,

    /// Array of hashes for each child node. **Not a checksum of child reference!**
    pub children_hashes: Vec<Hash>,

    /// Number of keys inside this branch
    pub size: usize,

    /// Hash of branch state
    pub hash: Hash,
}

impl BranchNode {
    /// Creates and returns a new empty BranchNode.
    pub fn new() -> Self {
        BranchNode::default()
    }
}

//
// Node Operations.
//

/// The root of tree elements hierarchy.
pub trait TreeNode {
    /// Stored search keys
    fn keys(&self) -> Vec<Key>;

    /// Number of keys inside this tree node (optimization)
    fn size(&self) -> usize;

    /// Digest of this node state
    fn hash(&self) -> Hash;
}

impl TreeNode for Node {
    fn keys(&self) -> Vec<Key> {
        match self {
            Node::Leaf(leaf) => leaf.keys.clone(),
            Node::Branch(branch) => branch.keys.clone(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Node::Leaf(leaf) => leaf.size,
            Node::Branch(branch) => branch.size,
        }
    }

    fn hash(&self) -> Hash {
        match self {
            Node::Leaf(leaf) => leaf.hash.clone(),
            Node::Branch(branch) => branch.hash.clone(),
        }
    }
}

/// Wrapper for the node with its corresponding id
#[derive(Clone, Debug, Default)]
pub struct NodeWithId<Id, Node> {
    pub id: Id,
    pub node: Node,
}

impl<Id, Node: TreeNode> NodeWithId<Id, Node> {
    pub fn new(id: Id, node: Node) -> Self {
        NodeWithId { id, node }
    }
}

/// Wrapper of the child id and the checksum
#[derive(Clone, Debug, Default)]
pub struct ChildRef<Id> {
    id: Id,
    checksum: Hash,
}

pub trait AsBytes {
    /// Clone as bytes
    fn clone_as_bytes(&self) -> Vec<Bytes>;

    /// Converts into Bytes without copying
    fn into_bytes(self) -> Vec<Bytes>;
}

impl<T: ToBytes + Clone> AsBytes for Vec<T> {
    fn clone_as_bytes(&self) -> Vec<Bytes> {
        self.iter().map(|t| t.clone().bytes()).collect()
    }

    fn into_bytes(self) -> Vec<Bytes> {
        self.into_iter().map(|t| t.bytes()).collect()
    }
}

#[cfg(test)]
pub mod tests {
    use super::Key;
    use super::ValueRef;
    use crate::ope_btree::internal::node::BranchNode;
    use crate::ope_btree::internal::node::LeafNode;
    use crate::ope_btree::internal::node::Node;
    use bytes::{Bytes, BytesMut};
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

    pub fn create_leaf() -> LeafNode {
        LeafNode {
            keys: vec![Key(BytesMut::from("key1")), Key(BytesMut::from("key2"))],
            values_refs: vec![ValueRef(Bytes::from("ref1")), ValueRef(Bytes::from("ref2"))],
            values_hashes: vec![Hash(BytesMut::from("hash1")), Hash(BytesMut::from("hash2"))],
            kv_hashes: vec![
                Hash(BytesMut::from("kv_hash1")),
                Hash(BytesMut::from("kv_hash2")),
            ],
            size: 2,
            hash: Hash(BytesMut::from("leaf_hash")),
            right_sibling: None,
        }
    }

    pub fn create_branch() -> BranchNode {
        BranchNode {
            keys: vec![Key(BytesMut::from("key1")), Key(BytesMut::from("key2"))],
            children_refs: vec![1, 2],
            size: 2,
            hash: Hash(BytesMut::from("leaf_hash")),
            children_hashes: vec![
                Hash(BytesMut::from("child_hash1")),
                Hash(BytesMut::from("child_hash2")),
            ],
        }
    }
}

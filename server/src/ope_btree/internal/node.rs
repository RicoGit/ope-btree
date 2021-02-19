//! Implementation of Btree node.

use std::ops::Deref;
use std::ptr::replace;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use common::merkle::{MerkleError, NodeProof};
use common::misc::ToBytes;
use common::{misc, Digest, Hash, Key};

use crate::ope_btree::{NodeId, ValueRef};

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
        Node::Branch(BranchNode::empty())
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
    pub val_refs: Vec<ValueRef>,

    /// Array of hashes for each encrypted stored value. **Not a hashes of value
    /// reference!**
    pub val_hashes: Vec<Hash>,

    /// Array of hashes for each pair with key and value hash.
    /// 'hash(key + hash_of_value)' (optimization, decreases recalculation)
    pub kv_hashes: Vec<Hash>,

    /// Number of keys inside this leaf. Actually a size of each array in the leaf.
    pub size: usize,

    /// The hash of the leaf state (the hash of concatenated `kv_hashes`)
    pub hash: Hash,

    /// A reference to the right sibling leaf. Rightmost leaf don't have right sibling.
    pub right_sibling: Option<NodeId>,
}

impl LeafNode {
    /// Creates and returns a new empty LeafNode.
    pub fn empty() -> Self {
        LeafNode::default()
    }

    /// Create new leaf with specified ''key'' and ''value''
    pub fn new<D: Digest>(key: Key, value_ref: ValueRef, value_hash: Hash) -> Self {
        let keys = vec![key];
        let values_refs = vec![value_ref];
        let values_hashes = vec![value_hash];
        let kv_hashes = LeafNode::build_kv_hashes::<D>(&keys, &values_hashes);
        let hash = LeafNode::leaf_hash::<D>(kv_hashes.clone());
        LeafNode {
            keys,
            val_refs: values_refs,
            val_hashes: values_hashes,
            kv_hashes,
            size: 1,
            hash,
            right_sibling: None,
        }
    }

    /// Returns array of checksums for each key-value pair
    fn build_kv_hashes<D: Digest>(keys: &[Key], values: &[Hash]) -> Vec<Hash> {
        keys.into_iter()
            .cloned()
            .zip(values.into_iter().cloned())
            .map(|(k, v)| {
                let mut key: Hash = k.into();
                key.concat(v);
                key
            })
            .collect()
    }

    /// Calculates checksum of leaf by folding ''kv_hashes''
    pub fn leaf_hash<D: Digest>(kv_hashes: Vec<Hash>) -> Hash {
        NodeProof::new(Hash::empty(), kv_hashes, None).calc_checksum::<D>(None)
    }

    /// Replace old key into Leaf with new one
    pub fn update<D: Digest>(
        mut self,
        key: Key,
        val_ref: ValueRef,
        val_hash: Hash,
        idx: usize,
    ) -> LeafNode {
        assert!(
            idx != 0 || idx >= self.size,
            format!(
                "Index should be between 0 and size of leaf, idx={}, leaf.size={}",
                idx, self.size
            )
        );

        misc::replace(&mut self.keys, key, idx);
        misc::replace(&mut self.val_refs, val_ref, idx);
        misc::replace(&mut self.val_hashes, val_hash, idx);

        self.kv_hashes = LeafNode::build_kv_hashes::<D>(&self.keys, &self.val_hashes);
        self.hash = LeafNode::leaf_hash::<D>(self.kv_hashes.clone());
        self
    }

    /// Insert new key into Leaf
    pub fn insert<D: Digest>(
        mut self,
        key: Key,
        val_ref: ValueRef,
        val_hash: Hash,
        idx: usize,
    ) -> LeafNode {
        assert!(
            idx <= self.size,
            format!(
                "Index should be between 0 and size of leaf, idx={}, leaf.size={}",
                idx, self.size
            )
        );

        self.keys.insert(idx, key);
        self.val_refs.insert(idx, val_ref);
        self.val_hashes.insert(idx, val_hash);

        self.kv_hashes = LeafNode::build_kv_hashes::<D>(&self.keys, &self.val_hashes);
        self.hash = LeafNode::leaf_hash::<D>(self.kv_hashes.clone());
        self.size += 1;
        self
    }

    pub fn has_overflow(&self, max: usize) -> bool {
        self.size > max
    }

    pub fn to_proof(&self, substitution_idx: usize) -> NodeProof {
        NodeProof::new(
            Hash::empty(),
            self.kv_hashes.clone(),
            Some(substitution_idx),
        )
    }

    /// Splits current LeafNode
    pub fn split<D: Digest>(self, right_leaf_id: NodeId) -> (Self, Self) {
        assert_eq!(
            self.size % 2,
            1,
            "Leaf size before splitting should be odd, but size={}",
            self.size
        );

        let split_idx = self.size / 2;
        let (left_keys, right_keys) = self.keys.split_at(split_idx);
        let (left_val_refs, right_val_refs) = self.val_refs.split_at(split_idx);
        let (left_val_hashes, right_val_hashes) = self.val_hashes.split_at(split_idx);

        let left_leaf_kv_hashes = LeafNode::build_kv_hashes::<D>(left_keys, left_val_hashes);
        let right_leaf_kv_hashes = LeafNode::build_kv_hashes::<D>(right_keys, right_val_hashes);

        let left_leaf = LeafNode {
            keys: left_keys.to_vec(),
            val_refs: left_val_refs.to_vec(),
            val_hashes: left_val_hashes.to_vec(),
            kv_hashes: left_leaf_kv_hashes.clone(),
            size: left_keys.len(),
            hash: LeafNode::leaf_hash::<D>(left_leaf_kv_hashes),
            // left leaf points to right leaf
            right_sibling: Some(right_leaf_id),
        };

        let right_leaf = LeafNode {
            keys: right_keys.to_vec(),
            val_refs: right_val_refs.to_vec(),
            val_hashes: right_val_hashes.to_vec(),
            kv_hashes: right_leaf_kv_hashes.clone(),
            size: right_keys.len(),
            hash: LeafNode::leaf_hash::<D>(right_leaf_kv_hashes),
            // reference to right sibling isn't change, right leaf should points to it
            right_sibling: self.right_sibling,
        };

        (left_leaf, right_leaf)
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
    pub children_refs: Vec<NodeId>,

    /// Array of hashes for each child node. **Not a checksum of child reference!**
    pub children_hashes: Vec<Hash>,

    /// Number of keys inside this branch
    pub size: usize,

    /// Hash of branch state
    pub hash: Hash,
}

impl BranchNode {
    /// Creates and returns a new empty BranchNode.
    pub fn empty() -> Self {
        BranchNode::default()
    }

    /// Creates new branch node with specified ''key'' and 2 child nodes
    pub fn new<D: Digest>(key: Key, left_child: ChildRef, right_child: ChildRef) -> Self {
        let keys = vec![key];
        let children_refs = vec![left_child.id, right_child.id];
        let children_hashes = vec![left_child.checksum, right_child.checksum];
        let hash = BranchNode::branch_hash::<D>(keys.clone(), children_hashes.clone());
        BranchNode {
            keys,
            children_refs,
            children_hashes,
            size: 1,
            hash,
        }
    }

    /// Returns checksum of branch node
    pub fn branch_hash<D: Digest>(keys: Vec<Key>, children_hashes: Vec<Hash>) -> Hash {
        NodeProof::new_proof::<D>(keys, children_hashes, None).calc_checksum::<D>(None)
    }

    pub fn has_overflow(&self, max: usize) -> bool {
        self.size > max
    }

    pub fn update_child_checksum<D: Digest>(&mut self, new_child_hash: Hash, idx: usize) {
        misc::replace(&mut self.children_hashes, new_child_hash, idx);
        self.hash = BranchNode::branch_hash::<D>(self.keys.clone(), self.children_hashes.clone());
    }

    pub fn update_child_ref<D: Digest>(&mut self, child_ref: ChildRef, idx: usize) {
        misc::replace(&mut self.children_refs, child_ref.id, idx);
        misc::replace(&mut self.children_hashes, child_ref.checksum, idx);
        self.hash = BranchNode::branch_hash::<D>(self.keys.clone(), self.children_hashes.clone());
    }

    pub fn insert_child<D: Digest>(&mut self, key: Key, child_ref: ChildRef, ins_idx: usize) {
        let idx = if self.is_rightmost() {
            // todo @check, is it possible idx=-1? Should I wrap to Option?
            // this child for inserting is rightmost child of rightmost parent branch, we take last branch idx as insert index
            self.size
        } else {
            ins_idx
        };

        self.keys.insert(idx, key);
        self.children_refs.insert(idx, child_ref.id);
        self.children_hashes.insert(idx, child_ref.checksum);
        self.size += 1;
        self.hash = BranchNode::branch_hash::<D>(self.keys.clone(), self.children_hashes.clone());
    }

    /// Splits current LeafNode
    pub fn split<D: Digest>(self) -> (Self, Self) {
        let split_idx = self.size / 2;
        let (left_keys, right_keys) = self.keys.split_at(split_idx);
        let (left_children_ref, right_children_ref) = self.children_refs.split_at(split_idx);
        let (left_children_hashes, right_children_hashes) =
            self.children_hashes.split_at(split_idx);

        let left_leaf = BranchNode {
            keys: left_keys.to_vec(),
            children_refs: left_children_ref.to_vec(),
            children_hashes: left_children_hashes.to_vec(),
            size: left_keys.len(),
            hash: BranchNode::branch_hash::<D>(left_keys.to_vec(), left_children_hashes.to_vec()),
        };

        let right_leaf = BranchNode {
            keys: right_keys.to_vec(),
            children_refs: right_children_ref.to_vec(),
            children_hashes: right_children_hashes.to_vec(),
            size: right_keys.len(),
            hash: BranchNode::branch_hash::<D>(right_keys.to_vec(), right_children_hashes.to_vec()),
        };

        (left_leaf, right_leaf)
    }

    /// Returns ''true'' if current branch is rightmost (the last) on this level of tree, ''false'' otherwise
    pub fn is_rightmost(&self) -> bool {
        self.children_refs.len() > self.size
    }

    pub fn to_proof<D: Digest>(&self, substitution_idx: usize) -> NodeProof {
        NodeProof::new_proof::<D>(
            self.keys.clone(),
            self.children_hashes.clone(),
            Some(substitution_idx),
        )
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
pub struct NodeWithId<Id, Node: TreeNode> {
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
pub struct ChildRef {
    id: NodeId,
    checksum: Hash,
}

impl ChildRef {
    pub fn new(id: NodeId, checksum: Hash) -> Self {
        ChildRef { id, checksum }
    }
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
    use bytes::{Bytes, BytesMut};
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    use common::noop_hasher::NoOpHasher;
    use common::Hash;

    use crate::ope_btree::internal::node::LeafNode;
    use crate::ope_btree::internal::node::Node;
    use crate::ope_btree::internal::node::{BranchNode, ChildRef};

    use super::Key;
    use super::ValueRef;

    //
    // Leaf tests
    //

    #[test]
    fn leaf_serde_test() {
        let leaf = create_leaf();
        let mut buf = Vec::new();
        leaf.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(leaf, Deserialize::deserialize(&mut de).unwrap());
    }

    #[test]
    fn leaf_create_test() {
        let k1 = Key::from_str("k1");
        let r1 = ValueRef::from_str("r1");
        let h1 = Hash::from_str("h1");
        let leaf = LeafNode::new::<NoOpHasher>(k1.clone(), r1.clone(), h1.clone());
        assert_eq!(leaf.keys, vec![k1]);
        assert_eq!(leaf.val_refs, vec![r1]);
        assert_eq!(leaf.val_hashes, vec![h1]);
        assert_eq!(leaf.size, 1);
        assert_eq!(leaf.kv_hashes, vec![Hash::from_str("k1h1")]);
        assert_eq!(leaf.hash.to_string(), "Hash[512, [k1h1]]");
    }

    #[test]
    fn leaf_build_kv_hashes_test() {
        let leaf = create_leaf();
        let kv_res = LeafNode::build_kv_hashes::<NoOpHasher>(&leaf.keys, &leaf.val_hashes);
        assert_eq!(kv_res, vec![Hash::from_str("k1h1"), Hash::from_str("k2h2")]);

        let empty = LeafNode::build_kv_hashes::<NoOpHasher>(&[], &[]);
        assert!(empty.is_empty());
    }

    #[test]
    fn leaf_hash_test() {
        let leaf = create_leaf();
        let kv_res = LeafNode::build_kv_hashes::<NoOpHasher>(&leaf.keys, &leaf.val_hashes);
        let hash = LeafNode::leaf_hash::<NoOpHasher>(kv_res);
        assert_eq!(hash.to_string(), "Hash[512, [k1h1k2h2]]");
    }

    #[test]
    #[should_panic]
    fn leaf_hash_empty_test() {
        LeafNode::leaf_hash::<NoOpHasher>(vec![]);
    }

    #[test]
    fn leaf_update_test() {
        let leaf = create_leaf();
        let k = Key::from_str("k#");
        let r = ValueRef::from_str("r#");
        let h = Hash::from_str("h#");
        let leaf = leaf.update::<NoOpHasher>(k.clone(), r.clone(), h.clone(), 1);

        assert_eq!(leaf.keys, vec![Key::from_str("k1"), k]);
        assert_eq!(leaf.val_refs, vec![ValueRef::from_str("r1"), r]);
        assert_eq!(leaf.val_hashes, vec![Hash::from_str("h1"), h]);
        assert_eq!(leaf.size, 2);
        assert_eq!(
            leaf.kv_hashes,
            vec![Hash::from_str("k1h1"), Hash::from_str("k#h#")]
        );
        assert_eq!(leaf.hash.to_string(), "Hash[512, [k1h1k#h#]]");
    }

    #[test]
    fn leaf_insert_test() {
        let leaf = create_leaf();
        let k = Key::from_str("k#");
        let r = ValueRef::from_str("r#");
        let h = Hash::from_str("h#");
        let leaf = leaf.insert::<NoOpHasher>(k.clone(), r.clone(), h.clone(), 1);

        assert_eq!(leaf.keys, vec![Key::from_str("k1"), k, Key::from_str("k2")]);
        assert_eq!(
            leaf.val_refs,
            vec![ValueRef::from_str("r1"), r, ValueRef::from_str("r2")]
        );
        assert_eq!(
            leaf.val_hashes,
            vec![Hash::from_str("h1"), h, Hash::from_str("h2")]
        );
        assert_eq!(leaf.size, 3);
        assert_eq!(
            leaf.kv_hashes,
            vec![
                Hash::from_str("k1h1"),
                Hash::from_str("k#h#"),
                Hash::from_str("k2h2")
            ]
        );
        assert_eq!(leaf.hash.to_string(), "Hash[512, [k1h1k#h#k2h2]]");
    }

    #[test]
    fn leaf_split_test() {
        let leaf = create_big_leaf();
        let (left, right) = leaf.split::<NoOpHasher>(42);

        assert_eq!(left.keys, vec![Key::from_str("k1")]);
        assert_eq!(left.val_hashes, vec![Hash::from_str("h1")]);
        assert_eq!(left.val_refs, vec![ValueRef::from_str("r1")]);
        assert_eq!(left.size, 1);
        assert_eq!(left.kv_hashes, vec![Hash::from_str("k1h1")]);
        assert_eq!(left.hash.to_string(), "Hash[512, [k1h1]]");
        assert_eq!(left.right_sibling, Some(42));

        assert_eq!(right.keys, vec![Key::from_str("k2"), Key::from_str("k3")]);
        assert_eq!(
            right.val_hashes,
            vec![Hash::from_str("h2"), Hash::from_str("h3")]
        );
        assert_eq!(
            right.val_refs,
            vec![ValueRef::from_str("r2"), ValueRef::from_str("r3")]
        );
        assert_eq!(right.size, 2);
        assert_eq!(
            right.kv_hashes,
            vec![Hash::from_str("k2h2"), Hash::from_str("k3h3")]
        );
        assert_eq!(right.hash.to_string(), "Hash[512, [k2h2k3h3]]");
        assert_eq!(right.right_sibling, Some(5));
    }

    pub fn create_leaf() -> LeafNode {
        LeafNode {
            keys: vec![Key::from_str("k1"), Key::from_str("k2")],
            val_refs: vec![ValueRef::from_str("r1"), ValueRef::from_str("r2")],
            val_hashes: vec![Hash::from_str("h1"), Hash::from_str("h2")],
            kv_hashes: vec![Hash::from_str("k1h1"), Hash::from_str("k2h2")],
            size: 2,
            hash: Hash(BytesMut::from("[k1h1k2h2]")),
            right_sibling: None,
        }
    }

    pub fn create_big_leaf() -> LeafNode {
        LeafNode {
            keys: vec![
                Key::from_str("k1"),
                Key::from_str("k2"),
                Key::from_str("k3"),
            ],
            val_refs: vec![
                ValueRef::from_str("r1"),
                ValueRef::from_str("r2"),
                ValueRef::from_str("r3"),
            ],
            val_hashes: vec![
                Hash::from_str("h1"),
                Hash::from_str("h2"),
                Hash::from_str("h3"),
            ],
            kv_hashes: vec![
                Hash::from_str("k1h1"),
                Hash::from_str("k2h2"),
                Hash::from_str("k3h3"),
            ],
            size: 3,
            hash: Hash(BytesMut::from("[k1h1k2h2k3h3]")),
            right_sibling: Some(5),
        }
    }

    //
    // Branch tests
    //

    #[test]
    fn branch_serde_test() {
        let branch = create_branch();
        let mut buf = Vec::new();
        branch.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(branch, Deserialize::deserialize(&mut de).unwrap());
    }

    #[test]
    fn branch_create_test() {
        let k1 = Key::from_str("k1");
        let h1 = Hash::from_str("h1");
        let left = ChildRef::new(1, h1.clone());
        let h2 = Hash::from_str("h2");
        let right = ChildRef::new(2, h2.clone());
        let branch = BranchNode::new::<NoOpHasher>(k1.clone(), left, right);

        assert_eq!(branch.keys, vec![k1]);
        assert_eq!(branch.children_refs, vec![1, 2]);
        assert_eq!(branch.children_hashes, vec![h1, h2]);
        assert_eq!(branch.size, 1);
        assert_eq!(branch.hash.to_string(), "Hash[512, [[k1]h1h2]]");
    }

    #[test]
    fn branch_hash_test() {
        let branch = create_branch();
        let keys = vec![Key::from_str("k1"), Key::from_str("k2")];
        let children = vec![
            Hash::from_str("h1"),
            Hash::from_str("h2"),
            Hash::from_str("h3"),
        ];
        let hash = BranchNode::branch_hash::<NoOpHasher>(keys, children);
        assert_eq!(hash.to_string(), "Hash[512, [[k1][k2]h1h2h3]]")
    }

    #[test]
    fn branch_update_child_checksum_test() {
        let mut branch = create_branch();
        branch.update_child_checksum::<NoOpHasher>(Hash::from_str("h#"), 1);
        assert_eq!(
            branch.children_hashes,
            vec![
                Hash::from_str("h1"),
                Hash::from_str("h#"),
                Hash::from_str("h3"),
            ]
        );
        assert_eq!(branch.hash.to_string(), "Hash[512, [[k1][k2]h1h#h3]]");
    }

    #[test]
    fn branch_update_child_ref_test() {
        let mut branch = create_branch();
        branch.update_child_ref::<NoOpHasher>(ChildRef::new(10, Hash::from_str("h#")), 0);
        assert_eq!(branch.children_refs, vec![10, 2, 3]);
        assert_eq!(
            branch.children_hashes,
            vec![
                Hash::from_str("h#"),
                Hash::from_str("h2"),
                Hash::from_str("h3"),
            ]
        );
        assert_eq!(branch.hash.to_string(), "Hash[512, [[k1][k2]h#h2h3]]");
    }

    #[test]
    fn branch_insert_child_test() {
        let mut branch = create_branch();
        let k = Key::from_str("k#");
        let h = Hash::from_str("h#");

        branch.insert_child::<NoOpHasher>(k.clone(), ChildRef::new(42, h.clone()), 2);

        assert_eq!(
            branch.keys,
            vec![Key::from_str("k1"), Key::from_str("k2"), k,]
        );
        assert_eq!(branch.children_refs, vec![1, 2, 42, 3]);
        assert_eq!(
            branch.children_hashes,
            vec![
                Hash::from_str("h1"),
                Hash::from_str("h2"),
                h,
                Hash::from_str("h3"),
            ]
        );
        assert_eq!(branch.size, 3);
        assert_eq!(branch.hash.to_string(), "Hash[512, [[k1][k2][k#]h1h2h#h3]]");
    }

    #[test]
    fn branch_split_test() {
        let branch = create_big_branch();
        let (left, right) = branch.split::<NoOpHasher>();

        assert_eq!(left.keys, vec![Key::from_str("k1"), Key::from_str("k2"),]);
        assert_eq!(left.children_refs, vec![1, 2]);
        assert_eq!(
            left.children_hashes,
            vec![Hash::from_str("h1"), Hash::from_str("h2"),]
        );
        assert_eq!(left.size, 2);
        assert_eq!(left.hash.to_string(), "Hash[512, [[k1][k2]h1h2]]");

        assert_eq!(right.keys, vec![Key::from_str("k3"), Key::from_str("k4"),]);
        assert_eq!(right.children_refs, vec![3, 4, 5]);
        assert_eq!(
            right.children_hashes,
            vec![
                Hash::from_str("h3"),
                Hash::from_str("h4"),
                Hash::from_str("h5"),
            ]
        );
        assert_eq!(right.size, 2);
        assert_eq!(right.hash.to_string(), "Hash[512, [[k3][k4]h3h4h5]]");
    }

    pub fn create_branch() -> BranchNode {
        BranchNode {
            keys: vec![Key::from_str("k1"), Key::from_str("k2")],
            children_refs: vec![1, 2, 3],
            size: 2,
            hash: Hash::from_str("leaf_hash"),
            children_hashes: vec![
                Hash::from_str(r"h1"),
                Hash::from_str("h2"),
                Hash::from_str("h3"),
            ],
        }
    }

    pub fn create_big_branch() -> BranchNode {
        let number = 5;
        let mut branch = BranchNode::empty();
        for idx in 1..number {
            branch.keys.push(Key::from_str(&format!("k{}", idx)));
            branch.children_refs.push(idx);
            branch
                .children_hashes
                .push(Hash::from_str(&format!("h{}", idx)));
        }
        branch.children_refs.push(number);
        branch
            .children_hashes
            .push(Hash::from_str(&format!("h{}", number)));
        branch.size = number;
        branch.hash = Hash::from_str("leaf_hash");
        branch
    }
}

use crate::ope_btree::internal::node::BranchNode;

/// Path traversed from the root to a leaf (leaf is excluded). Contains all the information you need
/// to climb up the tree.
#[derive(Clone, Debug)]
pub struct TreePath<Id> {
    /// The path from root to leaf
    branches: Vec<PathElem<Id>>,
}

impl<Id> TreePath<Id> {
    pub fn new(branch_id: Id, branch: BranchNode, next_child_idx: usize) -> TreePath<Id> {
        TreePath {
            branches: vec![PathElem {
                branch_id,
                branch,
                next_child_idx,
            }],
        }
    }

    pub fn empty() -> TreePath<Id> {
        TreePath { branches: vec![] }
    }

    /// Return parent of current node - in other words the previous node of the current node
    /// (last element which was pushed into ''branches'').
    pub fn get_parent(&self) -> Option<&PathElem<Id>> {
        self.branches.first()
    }

    /// Pushes new tree element to tail of sequence. Returns new version of [[TreePath]].
    pub fn push(&mut self, new_id: Id, new_tree: BranchNode, next_child_idx: usize) {
        let elem = PathElem {
            branch_id: new_id,
            branch: new_tree,
            next_child_idx,
        };
        self.branches.push(elem);
    }
}

/// Branch node with its corresponding id and next child position idx.
#[derive(Clone, Debug)]
pub struct PathElem<Id> {
    /// Current branch node id (used for saving node to Store)
    branch_id: Id,
    /// Current branch node
    branch: BranchNode,
    /// Next child position index.
    next_child_idx: usize,
}

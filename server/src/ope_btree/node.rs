//! Implementation of Btree node.

#[derive(Debug, PartialOrd, PartialEq, Serialize, Deserialize, Clone)]
pub struct Node {
    pub size: usize,
    // todo fill
}

impl Node {
    pub fn new(size: usize) -> Self {
        Node { size }
    }
}

#[cfg(test)]
mod tests {
    use super::Node;
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    #[test]
    fn node_serde_test() {
        let node = Node { size: 13 };

        let mut buf = Vec::new();
        node.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let mut de = Deserializer::new(&buf[..]);
        assert_eq!(node, Deserialize::deserialize(&mut de).unwrap());
    }

}

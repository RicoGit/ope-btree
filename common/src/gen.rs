//! General purpose generator

pub trait Generator {
    type Item;
    /// Generates next element of type `Item`
    fn next(&mut self) -> Self::Item;
}

#[derive(Debug, Clone)]
pub struct NumGen(pub usize);

impl Generator for NumGen {
    type Item = usize;

    fn next(&mut self) -> usize {
        let next = self.0 + 1;
        self.0 = next;
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_test() {
        let mut gen = NumGen(0);

        assert_eq!(gen.next(), 1);
        assert_eq!(gen.next(), 2);
        assert_eq!(gen.next(), 3);

        let mut gen2 = NumGen(1042);
        assert_eq!(gen2.next(), 1043);
        assert_eq!(gen2.next(), 1044);
        assert_eq!(gen2.next(), 1045);
    }
}

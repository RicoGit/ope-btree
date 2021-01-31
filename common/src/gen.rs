//! Generator trait

pub trait Generator {
    type Item;
    fn gen(&mut self) -> Self::Item;
}

#[derive(Debug, Clone)]
pub struct NumGen(pub usize);

impl Generator for NumGen {
    type Item = usize;

    fn gen(&mut self) -> usize {
        let next = self.0 + 1;
        self.0 = next;
        next
    }
}

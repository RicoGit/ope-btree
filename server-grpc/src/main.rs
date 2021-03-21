use common::noop_hasher::NoOpHasher;
use server::ope_btree::OpeBTreeConf;

// Grpc-based protocol implementation
mod grpc;

fn main() {
    let conf = OpeBTreeConf {
        arity: 4,
        alpha: 0.25,
    };
    let (rx, _tx) = tokio::sync::mpsc::channel(1);
    let _db = grpc::new_in_memory_db::<NoOpHasher>(conf, rx);

    // todo

    println!("Hello world!")
}

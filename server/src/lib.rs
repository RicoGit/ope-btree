//! todo

// todo remove 'allow'
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

extern crate async_kvstore;

#[macro_use]
extern crate error_chain;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp_serde as rmps;

// module with persistent level for any tree
mod node_store;
// implementation of ope-btree
mod btree;

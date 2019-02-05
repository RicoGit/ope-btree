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

// implementation of ope-btree
pub mod ope_btree;

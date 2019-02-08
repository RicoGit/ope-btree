//! todo

// todo remove 'allow'
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

extern crate async_kvstore;
extern crate common;
extern crate protocol;

#[macro_use]
extern crate error_chain;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp_serde as rmps;

// Order-preserving encryption Btree implementation
pub mod ope_btree;

// Order-preserving encryption Database
pub mod ope_db;

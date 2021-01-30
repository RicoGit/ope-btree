//! todo docs

// todo remove 'allow'
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use async_kvstore;
use common;
use protocol;

use serde;

extern crate rmp_serde as rmps;

// Order-preserving encryption Btree implementation
pub mod ope_btree;

// Order-preserving encryption Database
pub mod ope_db;

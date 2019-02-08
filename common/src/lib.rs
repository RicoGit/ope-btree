//! Common structures and traits for all submodules. Maybe it's a temp solution.

#[macro_use]
extern crate serde_derive;

pub mod merkle;
pub mod misc;

use bytes::Bytes;

/// A ciphered key for retrieve a value.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Key(pub Bytes);

/// A hash of anything.
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Hash(pub Bytes);

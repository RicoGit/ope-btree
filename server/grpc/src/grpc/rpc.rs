// Contains generated Grpc entities for ope Btree
tonic::include_proto!("opebtree");

use common::misc::FromVecBytes;
use prost::bytes::Bytes;

pub mod get {
    use super::*;
    use server::ope_db::DbError;

    pub fn value_msg(value: Option<Bytes>) -> GetCallback {
        GetCallback {
            callback: Some(get_callback::Callback::Value(GetValue {
                value: value.map(|vec| vec.to_vec()),
            })),
        }
    }

    pub fn next_child_idx_msg(keys: Vec<Bytes>, children_checksums: Vec<Bytes>) -> GetCallback {
        GetCallback {
            callback: Some(get_callback::Callback::NextChildIdx(AskNextChildIndex {
                keys: keys.into_byte_vec(),
                children_checksums: children_checksums.into_byte_vec(),
            })),
        }
    }

    pub fn submit_leaf_msg(keys: Vec<Bytes>, values_checksums: Vec<Bytes>) -> GetCallback {
        GetCallback {
            callback: Some(get_callback::Callback::SubmitLeaf(AskSubmitLeaf {
                keys: keys.into_byte_vec(),
                values_checksums: values_checksums.into_byte_vec(),
            })),
        }
    }

    pub fn server_error_msg(err: DbError) -> GetCallback {
        GetCallback {
            callback: Some(get_callback::Callback::ServerError(Error {
                code: err.to_string(),
                description: format!("{:?}", err),
            })),
        }
    }
}

pub mod put {
    use super::*;
    // todo impl
}

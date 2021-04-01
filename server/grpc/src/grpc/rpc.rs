// Contains generated Grpc entities for ope Btree
tonic::include_proto!("opebtree");

use common::misc::FromVecBytes;
use prost::bytes::Bytes;
use server::ope_db::DbError;

pub mod get {
    use super::*;

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

    pub fn value_msg(value: Option<Bytes>) -> PutCallback {
        PutCallback {
            callback: Some(put_callback::Callback::Value(PreviousValue {
                value: value.map(|vec| vec.to_vec()),
            })),
        }
    }

    pub fn next_child_idx_msg(keys: Vec<Bytes>, children_checksums: Vec<Bytes>) -> PutCallback {
        PutCallback {
            callback: Some(put_callback::Callback::NextChildIdx(AskNextChildIndex {
                keys: keys.into_byte_vec(),
                children_checksums: children_checksums.into_byte_vec(),
            })),
        }
    }

    pub fn put_details_msg(keys: Vec<Bytes>, values_checksums: Vec<Bytes>) -> PutCallback {
        PutCallback {
            callback: Some(put_callback::Callback::PutDetails(AskPutDetails {
                keys: keys.into_byte_vec(),
                values_checksums: values_checksums.into_byte_vec(),
            })),
        }
    }

    pub fn verify_changes_msg(server_merkle_root: Bytes, was_split: bool) -> PutCallback {
        PutCallback {
            callback: Some(put_callback::Callback::VerifyChanges(AskVerifyChanges {
                server_merkle_root: server_merkle_root.to_vec(),
                was_split,
            })),
        }
    }

    pub fn changes_stored_msg() -> PutCallback {
        PutCallback {
            callback: Some(put_callback::Callback::ChangesStored(AskChangesStored {})),
        }
    }

    pub fn server_error_msg(err: DbError) -> PutCallback {
        PutCallback {
            callback: Some(put_callback::Callback::ServerError(Error {
                code: err.to_string(),
                description: format!("{:?}", err),
            })),
        }
    }
}

// Contains generated Grpc entities for ope Btree
tonic::include_proto!("opebtree");

use bytes::Bytes;
use protocol::btree::SearchResult;

pub mod get {
    use super::*;

    pub fn db_info_msg(dataset_id: Bytes, version: usize) -> GetCallbackReply {
        GetCallbackReply {
            reply: Some(get_callback_reply::Reply::DatasetInfo(DatasetInfo {
                id: dataset_id.to_vec(),
                version: version as i64,
            })),
        }
    }

    pub fn next_child_idx_msg(idx: usize) -> GetCallbackReply {
        GetCallbackReply {
            reply: Some(get_callback_reply::Reply::NextChildIdx(
                ReplyNextChildIndex { index: idx as u32 },
            )),
        }
    }

    pub fn submit_leaf_msg(search_result: SearchResult) -> GetCallbackReply {
        let result = match search_result.0 {
            Err(idx) => reply_submit_leaf::SearchResult::InsertionPoint(idx as i32),
            Ok(idx) => reply_submit_leaf::SearchResult::Found(idx as i32),
        };
        GetCallbackReply {
            reply: Some(get_callback_reply::Reply::SubmitLeaf(ReplySubmitLeaf {
                search_result: Some(result),
            })),
        }
    }
}

pub mod put {
    use super::*;
    use protocol::btree::ClientPutDetails;

    pub fn db_info_msg(dataset_id: Bytes, version: usize) -> PutCallbackReply {
        PutCallbackReply {
            reply: Some(put_callback_reply::Reply::DatasetInfo(DatasetInfo {
                id: dataset_id.to_vec(),
                version: version as i64,
            })),
        }
    }

    pub fn value_msg(value: Bytes) -> PutCallbackReply {
        PutCallbackReply {
            reply: Some(put_callback_reply::Reply::Value(PutValue {
                value: value.to_vec(),
            })),
        }
    }

    pub fn next_child_idx_msg(idx: usize) -> PutCallbackReply {
        PutCallbackReply {
            reply: Some(put_callback_reply::Reply::NextChildIdx(
                ReplyNextChildIndex { index: idx as u32 },
            )),
        }
    }

    pub fn put_details_msg(put_details: ClientPutDetails) -> PutCallbackReply {
        let result = match put_details.search_result.0 {
            Err(idx) => reply_put_details::SearchResult::InsertionPoint(idx as i32),
            Ok(idx) => reply_put_details::SearchResult::Found(idx as i32),
        };
        PutCallbackReply {
            reply: Some(put_callback_reply::Reply::PutDetails(ReplyPutDetails {
                key: put_details.key.to_vec(),
                checksum: put_details.val_hash.to_vec(),
                search_result: Some(result),
            })),
        }
    }

    pub fn verify_changes_msg(signature: Bytes) -> PutCallbackReply {
        PutCallbackReply {
            reply: Some(put_callback_reply::Reply::VerifyChanges(
                ReplyVerifyChanges {
                    signature: signature.to_vec(),
                },
            )),
        }
    }

    pub fn changes_stored_msg() -> PutCallbackReply {
        PutCallbackReply {
            reply: Some(put_callback_reply::Reply::ChangesStored(
                ReplyChangesStored {},
            )),
        }
    }
}

use nekoton_utils::serde_optional_u64;
use serde::{Deserialize, Serialize};

use super::Request;

/// TON-compatible block identity. Hashes are base64, shard is signed decimal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "ton.blockIdExt")]
pub struct BlockId {
    pub workchain: i32,
    pub shard: String,
    pub seqno: u32,
    pub root_hash: String,
    pub file_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBlockHeaderRequest {
    pub workchain: i32,
    pub shard: String,
    pub seqno: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_hash: Option<String>,
}

impl Request for GetBlockHeaderRequest {
    type ResponseContainer = Self::Response;
    type Response = BlockHeader;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "blocks.header")]
pub struct BlockHeader {
    pub id: BlockId,
    pub global_id: i32,
    pub version: u32,
    pub after_merge: bool,
    pub after_split: bool,
    pub before_split: bool,
    pub want_merge: bool,
    pub want_split: bool,
    pub validator_list_hash_short: u32,
    pub catchain_seqno: u32,
    pub min_ref_mc_seqno: u32,
    pub is_key_block: bool,
    pub prev_key_block_seqno: u32,
    #[serde(with = "decimal_u64")]
    pub start_lt: u64,
    #[serde(with = "decimal_u64")]
    pub end_lt: u64,
    pub gen_utime: u32,
    pub prev_blocks: Vec<BlockId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMasterchainInfoRequest {}

impl Request for GetMasterchainInfoRequest {
    type ResponseContainer = Self::Response;
    type Response = GetMasterchainInfoResponse;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "blocks.masterchainInfo")]
pub struct GetMasterchainInfoResponse {
    pub last: BlockId,
    pub state_root_hash: String,
    pub init: BlockId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetShardsRequest {
    pub seqno: u32,
}

impl Request for GetShardsRequest {
    type ResponseContainer = Self::Response;
    type Response = GetShardsResponse;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "@type", rename = "blocks.shards")]
pub struct GetShardsResponse {
    pub shards: Vec<BlockId>,
}

/// Exactly one selector is required. Lookup stays within the specified shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupBlockRequest {
    pub workchain: i32,
    pub shard: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seqno: Option<u32>,
    #[serde(
        default,
        with = "serde_optional_u64",
        skip_serializing_if = "Option::is_none"
    )]
    pub lt: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unixtime: Option<u32>,
}

impl Request for LookupBlockRequest {
    type ResponseContainer = Self::Response;
    type Response = BlockId;
}

mod decimal_u64 {
    pub use nekoton_utils::serde_u64::deserialize;

    pub fn serialize<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
}

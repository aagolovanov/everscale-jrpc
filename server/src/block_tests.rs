use std::path::PathBuf;
use std::sync::Arc;

use axum::body::{Body, HttpBody};
use axum::http::Request;
use serde_json::{json, Value};
use ton_block::{BlkPrevInfo, Block, BlockExtra, BlockIdExt, BlockInfo, ExtBlkRef, ShardIdent};
use tower::ServiceExt;

use crate::storage::blocks::{block_id, StoredBlock};
use crate::storage::{DbOptions, PersistentStorage};
use crate::{ApiConfig, Config, PersistentStorageConfig, RpcState};

/// Network fixtures are downloaded once; this test needs no live endpoint.
#[test]
fn real_boc_blocks_parse_into_rpc_headers() {
    use ton_block::Deserializable;
    use ton_indexer::utils::BlockStuff;

    let manifest: Value =
        serde_json::from_str(include_str!("../tests/fixtures/real-blocks/manifest.json")).unwrap();
    for (metadata, bytes) in manifest["fixtures"].as_array().unwrap().iter().zip([
        include_bytes!("../tests/fixtures/real-blocks/masterchain-key-block.boc").as_slice(),
        include_bytes!("../tests/fixtures/real-blocks/shardchain-block.boc").as_slice(),
    ]) {
        let name = metadata["name"].as_str().unwrap();
        let file_hash = ton_types::UInt256::calc_file_hash(bytes);
        assert_eq!(
            hex::encode(file_hash.as_slice()),
            metadata["boc_sha256"],
            "{name}: BOC changed"
        );
        let root = ton_types::deserialize_tree_of_cells(&mut &bytes[..]).unwrap();
        let root_hash = root.repr_hash();
        if let Some(expected) = metadata["root_hash"].as_str() {
            assert_eq!(
                hex::encode(root_hash.as_slice()),
                expected,
                "{name}: API root hash mismatch"
            );
        }
        let block = Block::construct_from_cell(root).unwrap();
        let info = block.read_info().unwrap();
        assert_eq!(
            info.shard().workchain_id() as i64,
            metadata["workchain"].as_i64().unwrap()
        );
        if let Some(seqno) = metadata["seqno"].as_u64() {
            assert_eq!(info.seq_no() as u64, seqno, "{name}: API seqno mismatch");
        }
        let id = BlockIdExt {
            shard_id: *info.shard(),
            seq_no: info.seq_no(),
            root_hash,
            file_hash,
        };
        // Exercise the exact indexer BOC decoder, then our RPC conversion.
        let decoded = BlockStuff::deserialize_checked(id.clone(), bytes).unwrap();
        let parsed = StoredBlock::from_block(
            decoded.id(),
            decoded.block(),
            &decoded.block().read_info().unwrap(),
        )
        .unwrap();
        assert_eq!(parsed.header.id, block_id(&id));
        assert!(parsed.header.start_lt < parsed.header.end_lt);
        assert!(!parsed.header.prev_blocks.is_empty());
        assert!(parsed
            .header
            .prev_blocks
            .iter()
            .all(|prev| prev.seqno < id.seq_no));
        assert_eq!(parsed.header.is_key_block, id.shard_id.is_masterchain());
        assert_eq!(parsed.shards.is_empty(), !id.shard_id.is_masterchain());
        let json = serde_json::to_value(&parsed.header).unwrap();
        assert_eq!(json["start_lt"], info.start_lt().to_string());
        assert_eq!(json["end_lt"], info.end_lt().to_string());
        let _: crate::models::jrpc::BlockHeader = serde_json::from_value(json).unwrap();
        println!(
            "{}",
            serde_json::json!({
                "fixture": name,
                "bytes": bytes.len(),
                "root_hash": hex::encode(id.root_hash.as_slice()),
                "header": parsed.header,
                "shards": parsed.shards,
            })
        );
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("everscale-block-tests-{}", uuid::Uuid::new_v4())))
    }

    fn config(&self) -> PersistentStorageConfig {
        PersistentStorageConfig {
            persistent_db_path: self.0.clone(),
            persistent_db_options: DbOptions {
                max_memory_usage: 24 << 20,
                min_caches_capacity: 8 << 20,
                min_compaction_memory_budget: 8 << 20,
            },
            shard_split_depth: 0,
            transactions_gc_options: None,
        }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fixture(
    shard: ShardIdent,
    seqno: u32,
    utime: u32,
    start_lt: u64,
) -> (BlockIdExt, Block, BlockInfo) {
    let mut info = BlockInfo::new();
    info.set_shard(shard);
    info.set_seq_no(seqno).unwrap();
    info.set_gen_utime(utime.into());
    info.set_start_lt(start_lt);
    info.set_end_lt(start_lt + 100);
    info.set_prev_stuff(
        false,
        &BlkPrevInfo::Block {
            prev: ExtBlkRef {
                seq_no: seqno - 1,
                end_lt: start_lt,
                ..Default::default()
            },
        },
    )
    .unwrap();
    let mut extra = BlockExtra::new();
    if shard.is_masterchain() {
        let mut custom = ton_block::McBlockExtra::default();
        let descr = ton_block::ShardDescr {
            seq_no: 7,
            root_hash: [7; 32].into(),
            file_hash: [8; 32].into(),
            ..Default::default()
        };
        custom
            .hashes_mut()
            .set(
                &0,
                &ton_block::InRefValue(ton_block::BinTree::with_item(&descr).unwrap()),
            )
            .unwrap();
        extra.write_custom(Some(&custom)).unwrap();
    }
    let block = Block::with_params(
        42,
        info.clone(),
        Default::default(),
        Default::default(),
        extra,
    )
    .unwrap();
    let id = BlockIdExt {
        shard_id: shard,
        seq_no: seqno,
        root_hash: [seqno as u8; 32].into(),
        file_hash: [seqno as u8 + 1; 32].into(),
    };
    (id, block, info)
}

#[test]
fn block_history_survives_reopen_and_handles_boundaries() {
    let dir = TestDir::new();
    let shard = ShardIdent::full(0);
    let tag = shard.shard_prefix_with_tag();
    {
        let db = PersistentStorage::new(&dir.config()).unwrap();
        for (seqno, utime, lt) in [(10, 1000, 100), (11, 1002, 200), (12, 1002, 400)] {
            let (id, block, info) = fixture(shard, seqno, utime, lt);
            let stored = StoredBlock::from_block(&id, &block, &info).unwrap();
            db.store_block(&stored).unwrap();
            db.store_block(&stored).unwrap(); // Replay must not duplicate entries.
        }
        assert_eq!(
            db.block_by_lt(0, tag, 199)
                .unwrap()
                .unwrap()
                .header
                .id
                .seqno,
            10
        );
        assert_eq!(
            db.block_by_lt(0, tag, 200)
                .unwrap()
                .unwrap()
                .header
                .id
                .seqno,
            11
        );
        for lt in [0, 99, 300, 399, 500, u64::MAX] {
            assert!(db.block_by_lt(0, tag, lt).unwrap().is_none());
        }
        assert_eq!(
            db.block_by_utime(0, tag, 1001)
                .unwrap()
                .unwrap()
                .header
                .id
                .seqno,
            11
        );
        assert_eq!(
            db.block_by_utime(0, tag, 1002)
                .unwrap()
                .unwrap()
                .header
                .id
                .seqno,
            11
        );
        assert!(db.block_by_utime(0, tag, 1003).unwrap().is_none());
        assert!(db.block_by_utime(-1, tag, 1).unwrap().is_none());
        assert!(db
            .block_by_lt(0, shard.split().unwrap().0.shard_prefix_with_tag(), 100)
            .unwrap()
            .is_none());
        // Replacing a position must remove obsolete time/LT indexes.
        let (id, block, info) = fixture(shard, 12, 1004, 600);
        db.store_block(&StoredBlock::from_block(&id, &block, &info).unwrap())
            .unwrap();
        assert!(db.block_by_lt(0, tag, 400).unwrap().is_none());
        assert_eq!(
            db.block_by_utime(0, tag, 1003)
                .unwrap()
                .unwrap()
                .header
                .gen_utime,
            1004
        );
    }
    let db = PersistentStorage::new(&dir.config()).unwrap();
    assert_eq!(
        db.get_block(0, tag, 10).unwrap().unwrap().header.start_lt,
        100
    );
    assert_eq!(
        db.block_by_lt(0, tag, 650)
            .unwrap()
            .unwrap()
            .header
            .id
            .seqno,
        12
    );
    assert!(db.get_block(0, tag, 99).unwrap().is_none());
}

#[test]
fn header_preserves_split_merge_predecessors_and_decimal_lts() {
    let parent = ShardIdent::full(0);
    let (left, right) = parent.split().unwrap();
    let (mut id, block, mut info) = fixture(left, 10, 1000, u64::MAX - 100);
    info.set_after_split(true);
    let stored = StoredBlock::from_block(&id, &block, &info).unwrap();
    assert_eq!(
        stored.header.prev_blocks[0].shard,
        (parent.shard_prefix_with_tag() as i64).to_string()
    );
    let json = serde_json::to_value(&stored.header).unwrap();
    assert_eq!(json["end_lt"], u64::MAX.to_string());
    assert_eq!(json["@type"], "blocks.header");
    let _: crate::models::jrpc::BlockHeader = serde_json::from_value(json).unwrap();

    info.set_after_split(false);
    info.set_shard(parent);
    id.shard_id = parent;
    let refs = BlkPrevInfo::new(vec![
        ExtBlkRef {
            seq_no: 8,
            ..Default::default()
        },
        ExtBlkRef {
            seq_no: 9,
            ..Default::default()
        },
    ])
    .unwrap();
    info.set_prev_stuff(true, &refs).unwrap();
    let stored = StoredBlock::from_block(&id, &block, &info).unwrap();
    assert_eq!(stored.header.prev_blocks.len(), 2);
    assert_eq!(
        stored.header.prev_blocks[0].shard,
        (left.shard_prefix_with_tag() as i64).to_string()
    );
    assert_eq!(
        stored.header.prev_blocks[1].shard,
        (right.shard_prefix_with_tag() as i64).to_string()
    );
    assert_eq!(stored.header.prev_blocks[1].seqno, 9);
}

async fn call(router: &axum::Router, method: &str, params: Value) -> Value {
    let response = router
        .clone()
        .oneshot(
            Request::post("/rpc")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"jsonrpc":"2.0", "id":7, "method":method, "params":params}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["id"], 7);
    value
}

fn router(state: Arc<RpcState>) -> axum::Router {
    axum::Router::new()
        .route("/rpc", axum::routing::post(crate::jrpc::jrpc_router))
        .with_state(crate::server::Server::new(state).unwrap())
}

#[tokio::test]
async fn block_rpc_routes_return_matching_ids_and_errors() {
    let dir = TestDir::new();
    let state = Arc::new(
        RpcState::new(Config {
            listen_address: "127.0.0.1:0".parse().unwrap(),
            api_config: ApiConfig::Full {
                common: Default::default(),
                storage: dir.config(),
            },
        })
        .unwrap(),
    );
    let (mc_id, mc, mc_info) = fixture(ShardIdent::masterchain(), 20, 1000, 100);
    let zero = BlockIdExt {
        seq_no: 0,
        ..mc_id.clone()
    };
    state.set_zero_state_id(&zero);
    state
        .process_block_parts(&mc_id, &mc, &mc_info, None)
        .await
        .unwrap();
    let (old_id, old, old_info) = fixture(ShardIdent::masterchain(), 19, 999, 0);
    state
        .process_block_parts(&old_id, &old, &old_info, None)
        .await
        .unwrap();
    let app = router(state.clone());
    let info = call(&app, "getMasterchainInfo", json!({})).await;
    assert_eq!(
        info["result"]["last"],
        serde_json::to_value(block_id(&mc_id)).unwrap()
    );
    assert_eq!(info["result"]["init"]["seqno"], 0);
    assert_eq!(
        info["result"]["state_root_hash"],
        base64::encode(mc.read_state_update().unwrap().new_hash.as_slice())
    );
    let params = json!({"workchain":-1,"shard":"-9223372036854775808","seqno":20});
    let header = call(&app, "getBlockHeader", params.clone()).await;
    assert_eq!(header["result"]["id"], info["result"]["last"]);
    assert_eq!(header["result"]["start_lt"], "100");
    let shards = call(&app, "getShards", json!({"seqno":20})).await;
    assert_eq!(
        shards["result"]["shards"],
        json!([{
            "@type":"ton.blockIdExt", "workchain":0, "shard":"-9223372036854775808",
            "seqno":7, "root_hash":base64::encode([7;32]), "file_hash":base64::encode([8;32])
        }])
    );
    for selector in [
        json!({"seqno":20}),
        json!({"lt":"100"}),
        json!({"unixtime":1000}),
    ] {
        let mut params = json!({"workchain":-1,"shard":"-9223372036854775808"});
        params
            .as_object_mut()
            .unwrap()
            .extend(selector.as_object().unwrap().clone());
        assert_eq!(
            call(&app, "lookupBlock", params).await["result"],
            info["result"]["last"]
        );
    }
    let mut with_hash = params.clone();
    with_hash["root_hash"] = hex::encode(mc_id.root_hash.as_slice()).into();
    assert!(call(&app, "getBlockHeader", with_hash.clone())
        .await
        .get("result")
        .is_some());
    with_hash["root_hash"] = base64::encode([0; 32]).into();
    assert_eq!(
        call(&app, "getBlockHeader", with_hash).await["error"]["code"],
        -32009
    );
    for invalid in [
        json!({"shard":"0"}),
        json!({"shard":"bad"}),
        json!({"lt":"1"}),
        json!({"root_hash":"bad"}),
    ] {
        let mut request = params.clone();
        request
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        let method = if invalid.get("lt").is_some() {
            "lookupBlock"
        } else {
            "getBlockHeader"
        };
        assert_eq!(call(&app, method, request).await["error"]["code"], -32602);
    }
    assert_eq!(
        call(
            &app,
            "lookupBlock",
            json!({"workchain":0,"shard":"-9223372036854775808"})
        )
        .await["error"]["code"],
        -32602
    );
    assert_eq!(
        call(&app, "getShards", json!({"seqno":99})).await["error"]["code"],
        -32009
    );
    let caps = call(&app, "getCapabilities", json!({})).await;
    for method in [
        "getBlockHeader",
        "getMasterchainInfo",
        "getShards",
        "lookupBlock",
    ] {
        assert!(caps["result"].as_array().unwrap().contains(&json!(method)));
    }
}

#[tokio::test]
async fn simple_mode_reports_capabilities_and_missing_head() {
    let state = Arc::new(
        RpcState::new(Config {
            listen_address: "127.0.0.1:0".parse().unwrap(),
            api_config: Default::default(),
        })
        .unwrap(),
    );
    let app = router(state.clone());
    assert_eq!(
        call(&app, "getMasterchainInfo", json!({})).await["error"]["code"],
        -32001
    );
    assert_eq!(
        call(&app, "getShards", json!({"seqno":1})).await["error"]["code"],
        -32002
    );
    let caps = call(&app, "getCapabilities", json!({})).await;
    assert!(caps["result"]
        .as_array()
        .unwrap()
        .contains(&json!("getMasterchainInfo")));
    assert!(!caps["result"]
        .as_array()
        .unwrap()
        .contains(&json!("lookupBlock")));
    let (id, block, info) = fixture(ShardIdent::masterchain(), 1, 1, 100);
    state.set_zero_state_id(&BlockIdExt {
        seq_no: 0,
        ..id.clone()
    });
    state
        .process_block_parts(&id, &block, &info, None)
        .await
        .unwrap();
    assert_eq!(
        call(&app, "getMasterchainInfo", json!({})).await["result"]["last"]["seqno"],
        1
    );
}

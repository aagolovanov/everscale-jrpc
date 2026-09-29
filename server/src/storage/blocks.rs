use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use weedb::{rocksdb, ColumnFamily, Table};

use super::PersistentStorage;
use crate::models::jrpc::{BlockHeader, BlockId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredBlock {
    pub header: BlockHeader,
    pub shards: Vec<BlockId>,
}

pub fn block_id(id: &ton_block::BlockIdExt) -> BlockId {
    BlockId {
        workchain: id.shard_id.workchain_id(),
        shard: (id.shard_id.shard_prefix_with_tag() as i64).to_string(),
        seqno: id.seq_no,
        root_hash: base64::encode(id.root_hash.as_slice()),
        file_hash: base64::encode(id.file_hash.as_slice()),
    }
}

impl StoredBlock {
    pub fn from_block(
        id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        info: &ton_block::BlockInfo,
    ) -> Result<Self> {
        let header = BlockHeader {
            id: block_id(id),
            global_id: block.global_id,
            version: info.version(),
            after_merge: info.after_merge(),
            after_split: info.after_split(),
            before_split: info.before_split(),
            want_merge: info.want_merge(),
            want_split: info.want_split(),
            validator_list_hash_short: info.gen_validator_list_hash_short(),
            catchain_seqno: info.gen_catchain_seqno(),
            min_ref_mc_seqno: info.min_ref_mc_seqno(),
            is_key_block: info.key_block(),
            prev_key_block_seqno: info.prev_key_block_seqno(),
            start_lt: info.start_lt(),
            end_lt: info.end_lt(),
            gen_utime: info.gen_utime().as_u32(),
            prev_blocks: info.read_prev_ids()?.iter().map(block_id).collect(),
        };
        let mut shards = Vec::new();
        if id.shard_id.is_masterchain() {
            let custom = block
                .read_extra()?
                .read_custom()?
                .context("Missing masterchain extra")?;
            custom.hashes().iterate_shards(|shard, descr| {
                shards.push(block_id(&ton_block::BlockIdExt {
                    shard_id: shard,
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash,
                }));
                Ok(true)
            })?;
            shards.sort_by(|a, b| (a.workchain, &a.shard).cmp(&(b.workchain, &b.shard)));
        }
        Ok(Self { header, shards })
    }
}

impl PersistentStorage {
    /// Metadata survives the indexer's raw-block GC. Replayed blocks are idempotent.
    pub fn store_block(&self, block: &StoredBlock) -> Result<()> {
        let h = &block.header;
        let shard = h.id.shard.parse::<i64>()? as u64;
        let key = block_key(h.id.workchain, shard, h.id.seqno);
        let mut batch = rocksdb::WriteBatch::default();
        // Remove old secondary keys if the same position is replayed/replaced.
        if let Some(old) = self.blocks.get(key)? {
            let old: StoredBlock = serde_json::from_slice(&old)?;
            batch.delete_cf(&self.blocks_by_lt.cf(), index_key(&key, old.header.end_lt));
            batch.delete_cf(
                &self.blocks_by_utime.cf(),
                index_key(&key, old.header.gen_utime as u64),
            );
        }
        batch.put_cf(&self.blocks.cf(), key, serde_json::to_vec(block)?);
        batch.put_cf(&self.blocks_by_lt.cf(), index_key(&key, h.end_lt), key);
        batch.put_cf(
            &self.blocks_by_utime.cf(),
            index_key(&key, h.gen_utime as u64),
            key,
        );
        self.inner.raw().write(batch)?;
        Ok(())
    }

    pub fn get_block(&self, workchain: i32, shard: u64, seqno: u32) -> Result<Option<StoredBlock>> {
        self.blocks
            .get(block_key(workchain, shard, seqno))?
            .map(|value| serde_json::from_slice(&value).map_err(Into::into))
            .transpose()
    }

    /// First indexed block with gen_utime >= the requested time.
    pub fn block_by_utime(
        &self,
        workchain: i32,
        shard: u64,
        utime: u32,
    ) -> Result<Option<StoredBlock>> {
        self.seek_block(&self.blocks_by_utime, workchain, shard, utime as u64)
    }

    /// Logical time must lie in the returned block's [start_lt, end_lt) interval.
    pub fn block_by_lt(&self, workchain: i32, shard: u64, lt: u64) -> Result<Option<StoredBlock>> {
        let Some(next_lt) = lt.checked_add(1) else {
            return Ok(None);
        };
        Ok(self
            .seek_block(&self.blocks_by_lt, workchain, shard, next_lt)?
            .filter(|block| block.header.start_lt <= lt))
    }

    fn seek_block<T: ColumnFamily>(
        &self,
        table: &Table<T>,
        workchain: i32,
        shard: u64,
        value: u64,
    ) -> Result<Option<StoredBlock>> {
        let key = block_key(workchain, shard, 0);
        let seek = index_key(&key, value);
        let mut iter = table.iterator(rocksdb::IteratorMode::From(
            &seek,
            rocksdb::Direction::Forward,
        ));
        let Some(entry) = iter.next() else {
            return Ok(None);
        };
        let (index, block_key) = entry?;
        if !index.starts_with(&key[..12]) {
            return Ok(None);
        }
        let data = self
            .blocks
            .get(&block_key)?
            .context("Missing indexed block")?;
        Ok(Some(serde_json::from_slice(&data)?))
    }
}

fn block_key(workchain: i32, shard: u64, seqno: u32) -> [u8; 16] {
    let mut key = [0; 16];
    key[..4].copy_from_slice(&workchain.to_be_bytes());
    key[4..12].copy_from_slice(&shard.to_be_bytes());
    key[12..].copy_from_slice(&seqno.to_be_bytes());
    key
}

fn index_key(block_key: &[u8; 16], value: u64) -> [u8; 24] {
    let mut key = [0; 24];
    key[..12].copy_from_slice(&block_key[..12]);
    key[12..20].copy_from_slice(&value.to_be_bytes());
    key[20..].copy_from_slice(&block_key[12..]);
    key
}

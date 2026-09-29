use weedb::rocksdb::{BlockBasedOptions, DBCompressionType, Options};
use weedb::{Caches, ColumnFamily};

/// Key: workchain (i32), tagged shard (u64), seqno (u32), all big endian.
/// Value: JSON block header and masterchain shard list.
pub struct Blocks;

/// Key: workchain, shard, end_lt (u64), seqno. Value: Blocks key.
pub struct BlocksByLt;

/// Key: workchain, shard, gen_utime (u64), seqno. Value: Blocks key.
pub struct BlocksByUtime;

macro_rules! block_table {
    ($table:ident, $name:literal) => {
        impl ColumnFamily for $table {
            const NAME: &'static str = $name;

            fn options(opts: &mut Options, caches: &Caches) {
                default_block_based_table_factory(opts, caches);
            }
        }
    };
}

block_table!(Blocks, "blocks");
block_table!(BlocksByLt, "blocks_by_lt");
block_table!(BlocksByUtime, "blocks_by_utime");

/// - Key: `workchain: i8, account: u256, lt: u64`
/// - Value: transaction: bytes`
pub struct Transactions;

impl Transactions {
    pub const KEY_LEN: usize = 1 + 32 + 8;
}

impl ColumnFamily for Transactions {
    const NAME: &'static str = "transactions";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

/// - Key: `tx_hash: u256`
/// - Value: `workchain: i8, account: u256, lt: u64`
pub struct TransactionsByHash;

impl ColumnFamily for TransactionsByHash {
    const NAME: &'static str = "transactions_by_hash";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

/// - Key: `in_msg_hash: u256`
/// - Value: `workchain: i8, account: u256, lt: u64`
pub struct TransactionsByInMsg;

impl ColumnFamily for TransactionsByInMsg {
    const NAME: &'static str = "transactions_by_in_msg";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

/// - Key: `code_hash: u256, workchain: i8, account: u256`
/// - Value: empty
pub struct CodeHashes;

impl CodeHashes {
    pub const KEY_LEN: usize = 32 + 1 + 32;
}

impl ColumnFamily for CodeHashes {
    const NAME: &'static str = "code_hashes";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

/// - Key: `workchain: i8, account: u256`
/// - Value: `code_hash: u256`
pub struct CodeHashesByAddress;

impl CodeHashesByAddress {
    pub const KEY_LEN: usize = 1 + 32;
}

impl ColumnFamily for CodeHashesByAddress {
    const NAME: &'static str = "code_hashes_by_address";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

fn default_block_based_table_factory(opts: &mut Options, caches: &Caches) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    opts.set_block_based_table_factory(&block_factory);
    opts.set_compression_type(DBCompressionType::Zstd);
}

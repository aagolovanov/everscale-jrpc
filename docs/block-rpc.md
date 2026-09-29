# Block RPC example

These four JSON-RPC methods return TON-style snake_case result fields inside the
existing `{"jsonrpc":"2.0","id":1,"result":...}` envelope. They are not added to
the protobuf transport. Block IDs contain `workchain`, signed-decimal `shard`,
`seqno`, and base64 `root_hash` / `file_hash`. Header logical times are decimal
strings, including values smaller than JavaScript's safe integer limit.

| Method | Params | Result |
| --- | --- | --- |
| `getMasterchainInfo` | `{}` | Latest observed masterchain `last` block ID, matching `state_root_hash`, network zerostate `init` ID |
| `getBlockHeader` | `workchain`, `shard`, `seqno`; optional `root_hash`, `file_hash` | `blocks.header`: ID, global ID, version, split/merge flags, validator/catchain fields, key-block fields, LT range, generation time, previous block IDs |
| `getShards` | Masterchain `seqno` | `blocks.shards` containing `shards`: block IDs referenced by that masterchain block |
| `lookupBlock` | `workchain`, `shard`, exactly one of `seqno`, `lt`, `unixtime` | Matching `ton.blockIdExt` |

`getMasterchainInfo` supports both API modes. The other methods require
`rpc_config.type: full` and are advertised in `getCapabilities` only in that mode.
Header hash filters accept base64 or 64-character hex and are checked against the
stored ID. `lt` accepts a decimal string or a JSON integer.

Lookup behavior in this example:

- `seqno`: exact match within the specified workchain and shard.
- `lt`: block containing the value in `[start_lt, end_lt)`; gaps return not found.
- `unixtime`: first indexed block with `gen_utime >= unixtime`; ties use lowest
  seqno. Before the retained range, this returns its first block; after the range,
  it returns not found. This is not a nearest-distance timestamp search.
- Lookup stays within the exact shard ID. It does not follow parent/child shards
  across historical splits or merges. Header predecessor IDs do account for both.

Errors: `-32602` invalid params, `-32001` missing masterchain information,
`-32002` history methods in simple mode, `-32009` missing indexed block or hash
mismatch. A missing block does not prove it never existed on-chain.

## Storage and limitations

Full mode creates three RocksDB column families: block metadata, LT index, and
time index. Each record and both indexes are written atomically. Metadata survives
node raw-block GC and RPC process restart. It has no automatic retention policy;
disk usage grows with indexed history.

Only blocks delivered to this RPC instance's subscriber are indexed. Existing
transaction data does not backfill block metadata. For earlier history, replay
archives with a suitable `node_settings.start_from` on a fresh node database;
this depends on peers still serving those archives. Immediately after upgrading
an existing database, the latest masterchain ID can precede available metadata;
wait for another processed block before running the smoke test.

Embedders should call `RpcState::set_zero_state_id(&global_config.zero_state)`
before `initialize`; the bundled node does this. This avoids requiring a local
zerostate file when bootstrapping from an init block. Initialization also attempts
to read the zerostate for existing integrations that omit that call.

## Local tests without a blockchain connection

```sh
cargo test --locked -p everscale-rpc-models -p everscale-rpc-server
```

Tests use synthetic Everscale blocks and temporary RocksDB databases. They cover
RPC routes, result fields, errors, hash validation, split/merge predecessors,
lookup boundaries, replay, database reopen, and masterchain head ordering.

On ARM Macs, prefix Cargo commands with `RUSTFLAGS=''` to override the repository's
x86 SIMD flags. The model/server tests were exercised this way.

## Local node connected to Everscale

Supply a valid Everscale network global config as `global-config.json`. Network
peers and ADNL UDP connectivity are required; this is not an isolated local chain.
From the repository root:

```sh
mkdir -p db
cargo build --locked -p everscale-rpc-node
target/debug/everscale-rpc-node \
  --config node/block-rpc.config.yaml \
  --global-config global-config.json
```

After sync and at least one new processed masterchain block, in another terminal:

```sh
bash scripts/test-block-rpc.sh http://127.0.0.1:8081/rpc
```

The script needs `curl` and `jq`. It calls all four methods and all three lookup
selectors, checking IDs and returned timestamps against their headers.

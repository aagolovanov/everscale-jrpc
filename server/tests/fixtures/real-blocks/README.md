# Real Everscale BOC samples

Downloaded from public endpoints on 2026-09-29. `manifest.json` records sources,
requests, byte lengths, SHA-256 checksums, and independently returned metadata.

| File | Block | Size |
| --- | --- | --- |
| `masterchain-key-block.boc` | Workchain -1, seqno 62456968 | 19,655 bytes |
| `shardchain-block.boc` | Workchain 0, seqno 72402170 | 859 bytes |

The masterchain sample comes from `getLatestKeyBlock`; its seqno above was decoded
locally. That RPC reserializes the block, so the checksum of this BOC is not an
independent assertion of the original network file hash. The shard sample comes
from an Evercloud GraphQL response, which also supplied its root hash and seqno;
the test compares both against the decoded BOC.

The test runs `BlockStuff::deserialize_checked`, our header/shard-list converter,
and JSON serialization. It checks payload checksums, available API metadata, LT
representation, and predecessor references. It does not verify validator
signatures or establish chain consensus. These two samples have no split/merge;
those cases remain covered by synthetic tests.

Run from the repository root, without network access or a synced node:

```sh
RUSTFLAGS='' cargo test --offline --locked -p everscale-rpc-server real_boc -- --nocapture
```

The empty `RUSTFLAGS` override is needed on ARM Macs because the repository
config enables x86 SIMD flags. The command prints the parsed headers and shards.

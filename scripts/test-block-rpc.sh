#!/usr/bin/env bash
set -euo pipefail

endpoint=${1:-http://127.0.0.1:8081/rpc}
rpc() {
  local response
  response=$(curl --fail --silent --show-error "$endpoint" \
    -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg method "$1" --argjson params "$2" \
      '{jsonrpc:"2.0",id:1,method:$method,params:$params}')")
  if ! jq -e 'has("result") and (has("error") | not)' <<<"$response" >/dev/null; then
    printf '%s\n' "$response" >&2
    return 1
  fi
  jq -c '.result' <<<"$response"
}

info=$(rpc getMasterchainInfo '{}')
id=$(jq -c '.last' <<<"$info")
params=$(jq -c '{workchain,shard,seqno}' <<<"$id")
header=$(rpc getBlockHeader "$params")
jq -e --argjson id "$id" '.id == $id' <<<"$header" >/dev/null
shards=$(rpc getShards "$(jq -c '{seqno}' <<<"$id")")
jq -e '.shards | type == "array"' <<<"$shards" >/dev/null

by_seqno=$(rpc lookupBlock "$params")
jq -en --argjson expected "$id" --argjson actual "$by_seqno" \
  '$expected == $actual' >/dev/null

lt_params=$(jq -c '{workchain:.id.workchain,shard:.id.shard,lt:.start_lt}' <<<"$header")
by_lt=$(rpc lookupBlock "$lt_params")
jq -en --argjson expected "$id" --argjson actual "$by_lt" \
  '$expected == $actual' >/dev/null

time_params=$(jq -c '{workchain:.id.workchain,shard:.id.shard,unixtime:.gen_utime}' <<<"$header")
by_time=$(rpc lookupBlock "$time_params")
time_header=$(rpc getBlockHeader "$(jq -c '{workchain,shard,seqno}' <<<"$by_time")")
jq -e --argjson expected "$(jq '.gen_utime' <<<"$header")" \
  '.gen_utime == $expected' <<<"$time_header" >/dev/null

printf 'Block RPC smoke test passed: masterchain seqno=%s, shards=%s\n' \
  "$(jq -r '.seqno' <<<"$id")" "$(jq '.shards | length' <<<"$shards")"

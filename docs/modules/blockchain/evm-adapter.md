# EVM Adapter

`services/chain-crawler-go/internal/adapters/evm` is the production-facing EVM
collection adapter. It normalizes ERC token logs, classifies provider failures,
tracks finality/reorg state, and treats WebSocket subscriptions as a low-latency
signal layered on top of RPC backfill.

## Supported Chains

The adapter has configurable provider defaults for:

| Chain | Default finality depth | Default reorg window | Default max `eth_getLogs` range |
| --- | ---: | ---: | ---: |
| Ethereum | 64 | 128 | 5,000 |
| BSC | 20 | 64 | 5,000 |
| Polygon | 128 | 256 | 3,000 |
| Arbitrum | 64 | 128 | 10,000 |
| Optimism | 64 | 128 | 10,000 |
| Base | 64 | 128 | 10,000 |
| Avalanche C-Chain | 8 | 32 | 2,048 |

All values are overrideable through `Config`. Operators should tune them for the
actual RPC provider contract because hosted providers often impose smaller log
range or response-size limits than the chain itself.

## `eth_getLogs` Ranges

`FetchRange` uses adaptive chunking:

- starts from `InitialBlockRange`, capped by `MaxBlockRange`
- shrinks the current chunk when the provider reports `block_range_too_large`
- retries retryable chunks for timeout, 429, 5xx, WebSocket-style disconnects,
  and inconsistent responses
- grows successful chunks by `AdaptiveGrowFactor` until `MaxBlockRange`
- continues after a chunk fails and returns partial events plus `RangeFetchError`
  with retryable `FailedLogChunk` metadata

This means a single bad chunk does not poison the whole range. The caller can
persist emitted events and retry the failed chunk ranges from the error payload.

## RPC Error Kinds

The adapter classifies errors into stable kinds:

- `timeout`
- `rate_limited`
- `server_error`
- `invalid_params`
- `block_range_too_large`
- `missing_block`
- `pruned_data`
- `execution_reverted`
- `websocket_disconnect`
- `inconsistent_response`

Classification uses HTTP status, JSON-RPC code/message, context/net timeout
errors, and malformed/mismatched JSON-RPC responses.

## WebSocket Policy

`SubscribeNewHeads` and `SubscribeLogs` reconnect on subscription channel close
or heartbeat timeout using bounded backoff. WebSocket data is never treated as
complete by itself:

- `newHeads` reconnects backfill missing block headers through RPC
- `logs` reconnects backfill missing log ranges through adaptive `eth_getLogs`
- duplicate log events are suppressed within the subscription window

Consumers should still checkpoint by block range because WebSocket delivery is a
latency optimization, not the source of final completeness.

## Decode Coverage

The decoder supports:

- ERC20 `Transfer(address,address,uint256)`
- ERC721 `Transfer(address,address,uint256 indexed tokenId)`
- ERC1155 `TransferSingle`
- ERC1155 `TransferBatch`

Amounts and token IDs remain raw integer strings. The adapter does not require
token decimals, so unknown tokens and missing decimals are safe. Malformed known
logs produce `decode_error` events with `discard_reason=malformed_log` instead
of failing the entire fetch.

## Finality And Reorgs

Events are marked:

- `FINALITY_STATUS_PENDING` until `latest - block >= FinalityDepth`
- `FINALITY_STATUS_FINALIZED` after the configured finality depth
- `FINALITY_STATUS_REORGED` when an RPC log has `removed=true`

`FinalityTracker` retains block refs inside `ReorgWindow`. Same-height hash
changes and parent-hash mismatches produce `ReorgEvent` data and indicate
whether the mismatch is still inside the correctable reorg window.

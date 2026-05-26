# chain-crawler-go

`chain-crawler-go` is the multi-chain crawler service skeleton for Telemetry
Fabric. It owns chain RPC connectivity, adapter boundaries, block cursoring,
checkpointing, reorg detection hooks, rate limiting, retries, and normalized
event export.

This first version intentionally does not implement real EVM logic. The default
runtime uses a mock adapter that emits example normalized transfer events.

## Run the mock crawler

```sh
cd services/chain-crawler-go
go test ./...
go run ./cmd/crawler
```

The crawler starts with:

- mock chain: `mockchain/local`
- health and metrics: `http://127.0.0.1:18080`
- stdout JSONL event export

Useful endpoints:

```sh
curl http://127.0.0.1:18080/healthz
curl http://127.0.0.1:18080/readyz
curl http://127.0.0.1:18080/metrics
```

To write events to a file:

```sh
go run ./cmd/crawler --exporter file --export-file crawler-events.jsonl
```

For one local smoke cycle:

```sh
go run ./cmd/crawler --run-once
```

To validate configuration without opening exporters, starting HTTP listeners, or
printing RPC URLs:

```sh
go run ./cmd/crawler --config crawler.json --check-config
```

## Configuration

`internal/config` currently provides a small JSON and environment loader. The
default config is enough for local mock execution.

Environment overrides include:

- `CRAWLER_CHAIN`
- `CRAWLER_NETWORK`
- `CRAWLER_CHAIN_ID`
- `CRAWLER_START_HEIGHT`
- `CRAWLER_BATCH_SIZE`
- `CRAWLER_POLL_INTERVAL` such as `2s`
- `CRAWLER_HTTP_LISTEN`
- `CRAWLER_EXPORTER` as `stdout` or `file`
- `CRAWLER_EXPORT_FILE`
- `CRAWLER_EXPORT_QUEUE_SIZE`
- `CRAWLER_EXPORT_BACKPRESSURE` as `slowdown`, `drop`, or `pause`
- `CRAWLER_RATE_LIMIT_RPS`
- `CRAWLER_RETRY_ATTEMPTS`
- `CRAWLER_RETRY_BUDGET`

Invalid configuration fails fast during startup. RPC endpoint URLs may contain
secrets, so config summaries and endpoint errors use endpoint IDs or redacted
scheme/host names instead of printing full URLs.

## Production Operation

Run the crawler under a supervisor that sends `SIGTERM` for termination. On
shutdown the process marks readiness false, stops accepting new scheduler
cycles, lets the in-flight cycle finish, persists the latest checkpoint, flushes
the bounded exporter, shuts down HTTP, and exits with:

- `0` for normal exit or graceful signal handling.
- `1` for configuration or runtime startup failure.
- `2` when graceful crawler shutdown times out.

Backpressure is enforced by a bounded exporter queue:

- `slowdown`: block the scheduler when the exporter queue is full.
- `pause`: return backpressure to the scheduler without advancing checkpoint.
- `drop`: drop only degradable `DROP`/`AGGREGATE` events when full; critical
  events use a priority queue and are not dropped by policy.

Treat reorged events and events with `event_type=critical` as critical. Keep
queue size finite; start with `CRAWLER_EXPORT_QUEUE_SIZE=128` and tune from
`crawler_exporter_backpressure_total`.

RPC operation should use multiple endpoints with stable endpoint IDs. The
`rpcpool` package tracks endpoint latency, error rate, rate limiting, health
score, circuit-open state, and half-open recovery. Rate-limit and permanent
errors are classified separately so retries stay bounded by retry budget and do
not loop forever.

Kubernetes probes should use:

- `/healthz` for process liveness only.
- `/readyz` for readiness. It checks config, adapter, exporter, and checkpoint
  store.
- `/metrics` for Prometheus scraping.

Recommended local verification before release:

```sh
go test ./...
CGO_ENABLED=1 go test -race ./...
```

On Windows, `go test -race` also requires a C compiler such as `gcc` in `PATH`.

## Adding a new adapter

1. Add a package under `internal/adapters/<name>`.
2. Implement `pkg/adapter.ChainAdapter`.
3. Map raw chain data into `pkg/event.NormalizedEvent` at the adapter boundary.
4. Keep RPC endpoint selection, retries, and rate limits outside the adapter
   unless a chain needs adapter-specific behavior.
5. Wire the adapter in `cmd/crawler/main.go` or a future adapter registry.

The stable adapter contract is:

```go
type ChainAdapter interface {
    Name() string
    Chain() string
    Network() string
    LatestHeight(ctx context.Context) (uint64, error)
    FetchRange(ctx context.Context, from uint64, to uint64) ([]NormalizedEvent, error)
    SubscribeHeads(ctx context.Context) (<-chan BlockHeader, error)
    HealthCheck(ctx context.Context) (*NodeHealth, error)
    FinalityDepth() uint64
}
```

## Service boundaries

This service is responsible for source-side blockchain collection only:

- chain RPC clients and node connectivity
- chain adapter interfaces
- checkpoints, replay cursors, and reorg hooks
- source-side rate limits and retries
- normalized blockchain event export through shared contracts

This service is not responsible for complex downstream analysis, enrichment,
feature extraction, CUDA or native acceleration, or control-plane ownership. It
must not directly pollute or import private internals from `platform-core`,
other services, or the Rust workspace. Cross-service communication should go
through `contracts/` generated code or stable storage/message boundaries once
those integrations are added.

## Metrics reserved

The `/metrics` endpoint exposes Prometheus text format and reserves:

- `crawler_chain_head_height`
- `crawler_processed_height`
- `crawler_lag_blocks`
- `crawler_lag_seconds`
- `crawler_rpc_latency_ms`
- `crawler_rpc_errors_total`
- `crawler_rpc_rate_limited_total`
- `crawler_reorg_events_total`
- `crawler_discarded_events_total`
- `crawler_kept_events_total`
- `crawler_checkpoint_height`
- `crawler_block_gap_total`
- `crawler_duplicate_events_total`
- `crawler_ws_reconnect_total`
- `crawler_worker_inflight`
- `crawler_exporter_backpressure_total`

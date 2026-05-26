-- RPC/node health samples.
-- Contract alignment: NodeHealth.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_node_health
(
    tenant_id LowCardinality(String) DEFAULT '',
    node_id String,
    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64,

    rpc_endpoint_id String DEFAULT '',
    source_rpc LowCardinality(String) DEFAULT '',
    status LowCardinality(String) DEFAULT 'NODE_STATUS_UNSPECIFIED',
    synced UInt8 DEFAULT 0,
    latest_block UInt64 DEFAULT 0,
    finalized_block UInt64 DEFAULT 0,
    block_lag UInt64 DEFAULT 0,
    peer_count UInt32 DEFAULT 0,
    latency_ms UInt32 DEFAULT 0,
    error_rate Float64 DEFAULT 0,
    client_version String DEFAULT '',
    last_error String DEFAULT '',
    checked_at DateTime64(3, 'UTC'),
    health_date Date MATERIALIZED toDate(checked_at),
    warnings Array(String),
    attributes Map(String, String),

    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    source LowCardinality(String) DEFAULT '',
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT ingest_time,
    trace_id String DEFAULT '',
    dedupe_key String DEFAULT concat(chain, ':', network, ':health:', node_id, ':', rpc_endpoint_id, ':', toString(checked_at)),

    INDEX idx_node_id node_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_rpc_endpoint_id rpc_endpoint_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_status status TYPE set(16) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(health_date))
ORDER BY (tenant_id, chain, network, node_id, rpc_endpoint_id, checked_at)
TTL checked_at + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

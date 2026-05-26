-- Blockchain block storage.
-- Contract alignment:
-- - ChainEvent: chain, network, chain_id, block_number, block_hash,
--   block_timestamp, finality_status, confirmations, source_rpc, reorged.
-- - Envelope: tenant_id, source, schema_version, ingest_time, trace_id,
--   dedupe_key, attributes.

CREATE DATABASE IF NOT EXISTS telemetry_fabric;

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_blocks
(
    tenant_id LowCardinality(String) DEFAULT '',
    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64,

    block_number UInt64,
    block_hash String,
    parent_hash String DEFAULT '',
    block_timestamp DateTime64(3, 'UTC'),
    block_date Date MATERIALIZED toDate(block_timestamp),

    tx_count UInt32 DEFAULT 0,
    log_count UInt32 DEFAULT 0,
    gas_used UInt64 DEFAULT 0,
    gas_limit UInt64 DEFAULT 0,
    base_fee_per_gas String DEFAULT '',
    miner_address String DEFAULT '',

    finality_status LowCardinality(String) DEFAULT 'FINALITY_STATUS_UNSPECIFIED',
    confirmations UInt32 DEFAULT 0,
    source_rpc LowCardinality(String) DEFAULT '',
    reorged UInt8 DEFAULT 0,

    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    source LowCardinality(String) DEFAULT '',
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT ingest_time,
    trace_id String DEFAULT '',
    dedupe_key String DEFAULT concat(chain, ':', network, ':block:', toString(block_number), ':', block_hash),
    attributes Map(String, String),

    INDEX idx_block_hash block_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_source_rpc source_rpc TYPE set(1024) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(block_date))
ORDER BY (tenant_id, chain, network, block_number, block_hash)
TTL block_timestamp + INTERVAL 3650 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

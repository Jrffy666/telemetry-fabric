-- Chain reorganization event storage.
-- Contract alignment: ReorgEvent and ReorgedBlock.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_reorg_events
(
    tenant_id LowCardinality(String) DEFAULT '',
    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64,

    depth UInt64,
    old_head_number UInt64,
    old_head_hash String,
    new_head_number UInt64,
    new_head_hash String,
    common_ancestor_number UInt64,
    common_ancestor_hash String,

    removed_blocks Array(Tuple(block_number UInt64, block_hash String, block_timestamp DateTime64(3, 'UTC'))),
    added_blocks Array(Tuple(block_number UInt64, block_hash String, block_timestamp DateTime64(3, 'UTC'))),

    detected_at DateTime64(3, 'UTC'),
    event_date Date MATERIALIZED toDate(detected_at),
    source_rpc LowCardinality(String) DEFAULT '',
    attributes Map(String, String),

    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    source LowCardinality(String) DEFAULT '',
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT ingest_time,
    trace_id String DEFAULT '',
    dedupe_key String DEFAULT concat(chain, ':', network, ':reorg:', toString(common_ancestor_number), ':', old_head_hash, ':', new_head_hash),

    INDEX idx_old_head_hash old_head_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_new_head_hash new_head_hash TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(event_date))
ORDER BY (tenant_id, chain, network, detected_at, common_ancestor_number, old_head_hash, new_head_hash)
TTL detected_at + INTERVAL 3650 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

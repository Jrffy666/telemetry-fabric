-- Address activity rollup storage.
-- Writers should insert one row per activity bucket and batch_id. Re-inserting
-- the same batch replaces the row; queries sum across batch_id values.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_address_activity
(
    tenant_id LowCardinality(String) DEFAULT '',
    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64,

    activity_date Date,
    bucket_start DateTime64(3, 'UTC'),
    bucket_end DateTime64(3, 'UTC'),

    address String,
    address_role LowCardinality(String) DEFAULT '',
    contract_address String DEFAULT '',
    token_address String DEFAULT '',
    token_symbol LowCardinality(String) DEFAULT '',
    token_standard LowCardinality(String) DEFAULT 'TOKEN_STANDARD_UNSPECIFIED',
    event_type LowCardinality(String) DEFAULT '',

    tx_count UInt64 DEFAULT 0,
    log_count UInt64 DEFAULT 0,
    transfer_count UInt64 DEFAULT 0,
    success_count UInt64 DEFAULT 0,
    failed_count UInt64 DEFAULT 0,

    amount_raw_sum String DEFAULT '',
    amount_decimal_sum String DEFAULT '',
    amount_usd_sum String DEFAULT '',
    amount_usd_value Nullable(Decimal(38, 18)) MATERIALIZED toDecimal128OrNull(amount_usd_sum, 18),

    first_block_number UInt64 DEFAULT 0,
    last_block_number UInt64 DEFAULT 0,
    first_seen DateTime64(3, 'UTC') DEFAULT bucket_start,
    last_seen DateTime64(3, 'UTC') DEFAULT bucket_end,

    batch_id String DEFAULT '',
    metric_version DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    source LowCardinality(String) DEFAULT '',
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    trace_id String DEFAULT '',
    dedupe_key String DEFAULT concat(chain, ':', network, ':addr:', address, ':', toString(activity_date), ':', contract_address, ':', token_address, ':', event_type, ':', address_role, ':', batch_id),
    attributes Map(String, String),

    INDEX idx_address address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_contract_address contract_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_token_address token_address TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(metric_version)
PARTITION BY (chain, network, toYYYYMM(activity_date))
ORDER BY (tenant_id, chain, network, address, activity_date, bucket_start, contract_address, token_address, event_type, address_role, batch_id)
TTL bucket_start + INTERVAL 1095 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

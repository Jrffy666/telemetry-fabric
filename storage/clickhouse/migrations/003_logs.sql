-- Blockchain log/event storage.
-- This table keeps raw log coordinates plus normalized ChainEvent fields.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_logs
(
    tenant_id LowCardinality(String) DEFAULT '',
    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64,

    block_number UInt64,
    block_hash String,
    block_timestamp DateTime64(3, 'UTC'),
    block_date Date MATERIALIZED toDate(block_timestamp),

    tx_hash String,
    tx_index UInt32 DEFAULT 0,
    log_index UInt32,

    event_type LowCardinality(String) DEFAULT '',
    event_signature String DEFAULT '',
    contract_address String DEFAULT '',
    from_address String DEFAULT '',
    to_address String DEFAULT '',

    token_address String DEFAULT '',
    token_symbol LowCardinality(String) DEFAULT '',
    token_standard LowCardinality(String) DEFAULT 'TOKEN_STANDARD_UNSPECIFIED',
    amount_raw String DEFAULT '',
    amount_decimal String DEFAULT '',
    amount_usd String DEFAULT '',
    amount_decimal_value Nullable(Decimal(76, 18)) MATERIALIZED toDecimal256OrNull(amount_decimal, 18),
    amount_usd_value Nullable(Decimal(38, 18)) MATERIALIZED toDecimal128OrNull(amount_usd, 18),

    topic0 String DEFAULT '',
    topic1 String DEFAULT '',
    topic2 String DEFAULT '',
    topic3 String DEFAULT '',
    topics Array(String),
    data String DEFAULT '',

    gas_used UInt64 DEFAULT 0,
    gas_price String DEFAULT '',
    success UInt8 DEFAULT 0,
    finality_status LowCardinality(String) DEFAULT 'FINALITY_STATUS_UNSPECIFIED',
    confirmations UInt32 DEFAULT 0,
    source_rpc LowCardinality(String) DEFAULT '',
    reorged UInt8 DEFAULT 0,

    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    source LowCardinality(String) DEFAULT '',
    event_time DateTime64(3, 'UTC') DEFAULT block_timestamp,
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT ingest_time,
    trace_id String DEFAULT '',
    dedupe_key String DEFAULT concat(chain, ':', network, ':log:', tx_hash, ':', toString(log_index), ':', toString(block_number)),
    attributes Map(String, String),

    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_contract_address contract_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_event_signature event_signature TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_topic0 topic0 TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(block_date))
ORDER BY (tenant_id, chain, network, contract_address, block_number, tx_hash, log_index)
TTL block_timestamp + INTERVAL 1095 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

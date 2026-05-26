-- Blockchain transaction storage.
-- ChainEvent fields are stored losslessly; large numeric values stay as
-- strings to match the protobuf contract and avoid precision loss.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_transactions
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
    tx_index UInt32,
    from_address String DEFAULT '',
    to_address String DEFAULT '',
    contract_address String DEFAULT '',
    nonce UInt64 DEFAULT 0,

    value_raw String DEFAULT '',
    value_decimal String DEFAULT '',
    value_usd String DEFAULT '',
    value_usd_value Nullable(Decimal(38, 18)) MATERIALIZED toDecimal128OrNull(value_usd, 18),

    gas_used UInt64 DEFAULT 0,
    gas_limit UInt64 DEFAULT 0,
    gas_price String DEFAULT '',
    max_fee_per_gas String DEFAULT '',
    max_priority_fee_per_gas String DEFAULT '',

    success UInt8 DEFAULT 0,
    event_type LowCardinality(String) DEFAULT 'transaction',
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
    dedupe_key String DEFAULT concat(chain, ':', network, ':tx:', tx_hash, ':', toString(block_number)),
    attributes Map(String, String),

    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_from_address from_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_to_address to_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_contract_address contract_address TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(block_date))
ORDER BY (tenant_id, chain, network, block_number, tx_index, tx_hash)
TTL block_timestamp + INTERVAL 1095 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

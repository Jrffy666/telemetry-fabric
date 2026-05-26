-- Token transfer storage.
-- Built from normalized ChainEvent transfer payloads. Amount columns keep the
-- contract string values and expose nullable decimal materialized columns for
-- common analytics.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_token_transfers
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
    log_index UInt32 DEFAULT 0,
    transfer_index UInt32 DEFAULT 0,

    event_type LowCardinality(String) DEFAULT 'token_transfer',
    token_address String DEFAULT '',
    contract_address String DEFAULT '',
    token_symbol LowCardinality(String) DEFAULT '',
    token_standard LowCardinality(String) DEFAULT 'TOKEN_STANDARD_UNSPECIFIED',
    token_id String DEFAULT '',

    from_address String DEFAULT '',
    to_address String DEFAULT '',
    amount_raw String DEFAULT '',
    amount_decimal String DEFAULT '',
    amount_usd String DEFAULT '',
    amount_decimal_value Nullable(Decimal(76, 18)) MATERIALIZED toDecimal256OrNull(amount_decimal, 18),
    amount_usd_value Nullable(Decimal(38, 18)) MATERIALIZED toDecimal128OrNull(amount_usd, 18),

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
    dedupe_key String DEFAULT concat(chain, ':', network, ':transfer:', tx_hash, ':', toString(log_index), ':', toString(transfer_index), ':', toString(block_number)),
    attributes Map(String, String),

    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_from_address from_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_to_address to_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_token_address token_address TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(block_date))
ORDER BY (tenant_id, chain, network, token_address, from_address, to_address, block_number, tx_hash, log_index, transfer_index)
TTL block_timestamp + INTERVAL 1095 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

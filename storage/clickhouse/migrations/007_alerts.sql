-- Alert event/state storage.
-- Contract alignment: AlertEvent plus Envelope metadata.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_alert_events
(
    alert_id String,
    tenant_id LowCardinality(String) DEFAULT '',
    severity LowCardinality(String) DEFAULT 'ALERT_SEVERITY_UNSPECIFIED',
    title String DEFAULT '',
    description String DEFAULT '',

    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64 DEFAULT 0,
    source LowCardinality(String) DEFAULT '',
    event_type LowCardinality(String) DEFAULT '',
    rule_id String DEFAULT '',
    rule_version UInt32 DEFAULT 0,
    dedupe_key String DEFAULT alert_id,

    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC'),
    alert_date Date MATERIALIZED toDate(first_seen),
    occurrence_count UInt32 DEFAULT 1,
    attributes Map(String, String),

    resolved UInt8 DEFAULT 0,
    resolved_at Nullable(DateTime64(3, 'UTC')),
    resolution_reason String DEFAULT '',

    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT last_seen,
    trace_id String DEFAULT '',

    INDEX idx_alert_id alert_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_rule_id rule_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_severity severity TYPE set(16) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY (chain, network, toYYYYMM(alert_date))
ORDER BY (tenant_id, chain, network, alert_id, rule_id, dedupe_key)
TTL last_seen + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

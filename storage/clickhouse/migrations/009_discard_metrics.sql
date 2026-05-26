-- Discard/drop metrics.
-- Built from Envelope.discard_reason and FilterRule/RuleMatch action metadata.
-- Writers should insert one row per metric bucket and batch_id.

CREATE TABLE IF NOT EXISTS telemetry_fabric.chain_discard_metrics
(
    tenant_id LowCardinality(String) DEFAULT '',
    chain LowCardinality(String),
    network LowCardinality(String),
    chain_id UInt64 DEFAULT 0,

    bucket_start DateTime64(3, 'UTC'),
    bucket_end DateTime64(3, 'UTC'),
    metric_date Date MATERIALIZED toDate(bucket_start),

    source LowCardinality(String) DEFAULT '',
    event_type LowCardinality(String) DEFAULT '',
    priority LowCardinality(String) DEFAULT 'PRIORITY_UNSPECIFIED',

    rule_id String DEFAULT '',
    rule_version UInt32 DEFAULT 0,
    rule_name String DEFAULT '',
    action_type LowCardinality(String) DEFAULT 'RULE_ACTION_TYPE_UNSPECIFIED',
    route String DEFAULT '',
    discard_reason LowCardinality(String) DEFAULT '',
    matched_rules Array(String),

    batch_id String DEFAULT '',
    event_count UInt64 DEFAULT 0,
    byte_count UInt64 DEFAULT 0,
    first_event_time DateTime64(3, 'UTC') DEFAULT bucket_start,
    last_event_time DateTime64(3, 'UTC') DEFAULT bucket_end,

    metric_version DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    schema_version LowCardinality(String) DEFAULT 'blockchain.v1',
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    trace_id String DEFAULT '',
    dedupe_key String DEFAULT concat(chain, ':', network, ':discard:', toString(bucket_start), ':', source, ':', event_type, ':', rule_id, ':', toString(rule_version), ':', discard_reason, ':', batch_id),
    attributes Map(String, String),

    INDEX idx_rule_id rule_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_discard_reason discard_reason TYPE set(1024) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(metric_version)
PARTITION BY (chain, network, toYYYYMM(metric_date))
ORDER BY (tenant_id, chain, network, metric_date, bucket_start, source, event_type, rule_id, rule_version, discard_reason, batch_id)
TTL bucket_start + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

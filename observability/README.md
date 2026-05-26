# Observability

`observability/` contains operational telemetry assets for platform core and
modular services.

## Responsibilities

This directory owns:

- Dashboards.
- Alert rules.
- SLO definitions.
- Runbook links.
- Metric naming conventions.
- Log and trace correlation guidance.
- Service health and dependency views.

## Coverage

Observability should cover:

- Rust data-plane ingestion, queue, processing, export, retry, and backpressure.
- Elixir control-plane registration, heartbeat, config fetch, command, audit,
  readiness, and persistence health.
- Go chain crawler lag, RPC health, reorg handling, adapter errors, and
  checkpoint progress.
- Stream processor lag, drop counts, deduplication, and materialization health.
- Python analytics job duration, freshness, failure rates, and data quality.
- C++/CUDA accelerator latency, throughput, GPU utilization, and fallback rate.
- ClickHouse, Kafka, and S3 storage health.

## Boundary Rules

- Keep dashboards and alerts business-aware but implementation-light.
- Do not embed crawler or analytics business logic here.
- Prefer metrics and labels that are stable across service versions.
- Use module-specific panels only when the metric contract is documented.
- Runbooks should link to service documentation instead of duplicating it.

## Layout

```text
observability/
  prometheus/
    rules/
      crawler.rules.yaml
      rpc.rules.yaml
      checkpoint.rules.yaml
      storage.rules.yaml
      kafka.rules.yaml
  grafana/
    dashboards/
      crawler-overview.json
      chain-lag.json
      rpc-health.json
      checkpoint.json
      filter-discard.json
      storage-health.json
      alerts.json
  alertmanager/
    templates/
      crawler.tmpl
```

## Metric Contract

The crawler dashboards and rules expect the following crawler metrics:

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

The rule set also references common dependency metrics when available:

- `crawler_worker_up` or crawler `up{job=~".*crawler.*"}` scrape state.
- `crawler_queue_depth` for internal or topic queue pressure.
- `crawler_clickhouse_insert_errors_total` or `clickhouse_insert_errors_total`.
- `kafka_consumergroup_lag`, `kafka_consumergroup_lag_sum`,
  `kafka_topic_partition_current_offset`, and `kafka_consumergroup_current_offset`.

## Alerts

Rules cover these operational conditions:

- Chain lag too high: `ChainLagTooHigh`
- RPC endpoint unhealthy: `RpcEndpointUnhealthy`
- Kafka lag high: `KafkaLagHigh`
- ClickHouse insert failure: `ClickHouseInsertFailure`
- Block gap detected: `BlockGapDetected`
- Reorg spike: `ReorgSpike`
- Crawler worker down: `CrawlerWorkerDown`
- Queue depth high: `QueueDepthHigh` and `KafkaQueueDepthHigh`
- Discard ratio abnormal: `DiscardRatioAbnormal`

Thresholds are conservative defaults. Tune them per chain finality, expected RPC
latency, Kafka retention, ClickHouse write volume, and crawler throughput.

## Helm and Kubernetes Integration

The repository Helm chart already includes ServiceMonitor templates under
`deploy/helm/telemetry-fabric/templates/`. Enable scraping with values like:

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
  labels:
    release: kube-prometheus-stack
```

Install or upgrade the chart:

```sh
helm upgrade --install telemetry-fabric deploy/helm/telemetry-fabric \
  --namespace telemetry-fabric \
  --create-namespace \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release=kube-prometheus-stack
```

### Prometheus Operator

For kube-prometheus-stack or another Prometheus Operator deployment, wrap the
rule group files in `PrometheusRule` resources. Keep the `groups:` content from
each file under `spec.groups`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: telemetry-fabric-crawler
  namespace: monitoring
  labels:
    release: kube-prometheus-stack
spec:
  groups:
    # paste groups from observability/prometheus/rules/*.rules.yaml here
```

If your GitOps tool supports raw Prometheus rule files, point it directly at
`observability/prometheus/rules/`.

### Grafana Dashboards

The JSON files in `observability/grafana/dashboards/` are importable dashboard
skeletons. They use a Prometheus datasource variable named `datasource`.

With the Grafana sidecar pattern, create a ConfigMap per dashboard:

```sh
kubectl create configmap telemetry-fabric-crawler-overview \
  --namespace monitoring \
  --from-file=crawler-overview.json=observability/grafana/dashboards/crawler-overview.json \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl label configmap telemetry-fabric-crawler-overview \
  --namespace monitoring grafana_dashboard=1 --overwrite
```

Repeat for the remaining dashboard JSON files or let GitOps generate the
ConfigMaps.

### Alertmanager Templates

Mount `observability/alertmanager/templates/crawler.tmpl` into Alertmanager and
reference it from the Alertmanager configuration:

```yaml
templates:
  - /etc/alertmanager/templates/*.tmpl

receivers:
  - name: crawler-platform
    slack_configs:
      - channel: "#crawler-alerts"
        title: '{{ template "telemetry_fabric.title" . }}'
        text: '{{ template "telemetry_fabric.slack" . }}'
```

For kube-prometheus-stack, this is usually modeled through
`alertmanager.alertmanagerSpec.secrets` or `alertmanager.config` depending on
how the chart is managed in the target cluster.

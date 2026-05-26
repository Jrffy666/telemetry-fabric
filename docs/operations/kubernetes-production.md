# Kubernetes Production Deployment

Use Helm for production deployment. The chart supports the platform-core
agent, control plane, chain crawler, stream processor, optional in-cluster
Redpanda/ClickHouse, and optional Prometheus/Grafana.

## Prerequisites

- Kubernetes 1.27 or newer.
- Helm 3.12 or newer.
- A namespace for the release, for example `observability`.
- Container images pinned to immutable tags or digests.
- Secrets created outside Helm for database URLs, RPC credentials, operator
  tokens, Kafka credentials, and TLS material.
- A production Kafka/Redpanda and ClickHouse service, preferably managed or
  operator-backed. The chart's built-in storage templates are for local or
  controlled small-cluster deployments.

## Render And Lint

```sh
make helm-lint
make k8s-render
```

For a production values file:

```sh
helm lint deploy/helm/telemetry-fabric -f deploy/helm/telemetry-fabric/values-production.example.yaml
helm template telemetry-fabric deploy/helm/telemetry-fabric \
  --namespace observability \
  -f deploy/helm/telemetry-fabric/values-production.example.yaml
```

## Install Order

1. Create namespace and image pull secrets.
2. Install or connect storage dependencies:
   - Kafka/Redpanda topics from `storage/kafka/topics.yaml`.
   - ClickHouse schema migrations.
3. Create Kubernetes Secrets:
   - control-plane database URL
   - agent/operator tokens
   - crawler RPC credentials
   - Kafka and ClickHouse credentials
   - TLS certificates
4. Deploy the control plane with PostgreSQL storage.
5. Deploy the agent DaemonSet.
6. Deploy crawler and stream-processor workloads.
7. Enable ServiceMonitor resources or install the optional observability stack.
8. Run smoke checks and inspect dashboards.

## Production Helm Install

```sh
helm upgrade --install telemetry-fabric deploy/helm/telemetry-fabric \
  --namespace observability \
  --create-namespace \
  -f deploy/helm/telemetry-fabric/values-production.example.yaml \
  --wait \
  --timeout 10m
```

## Health Checks

The chart defines `startupProbe`, `readinessProbe`, and `livenessProbe` for
long-running workloads. Rollout automation should wait for readiness before
moving to the next stage.

```sh
kubectl -n observability rollout status deploy/telemetry-fabric-control-plane
kubectl -n observability rollout status ds/telemetry-fabric
kubectl -n observability get pods -l app.kubernetes.io/instance=telemetry-fabric
```

## Capacity Defaults

Each workload has resource requests and limits in `values.yaml`. Production
overrides should be based on real traffic:

- Agent: queue pressure, ingest bytes per second, exporter latency.
- Control plane: request rate, PostgreSQL latency, command queue depth.
- Crawler: block range, RPC latency, Kafka produce rate.
- Stream processor: consumer lag, rule count, enrichment cost.
- Storage: disk IOPS, retention window, compaction pressure.

## Secrets

Prefer references to existing Secrets over inline Helm `stringData`.
Inline secret creation is available only for controlled environments.

No deployment should hard-code RPC keys, database passwords, or Kafka
credentials in values files committed to git.

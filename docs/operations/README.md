# Operations

This directory is reserved for core operational runbooks for the Rust agent and
Elixir control plane.

The Helm chart in `deploy/helm/telemetry-fabric` is the primary deployment
artifact. The raw manifests in `deploy/k8s` remain lightweight examples.

## Helm PrometheusRule

The chart can render optional Prometheus Operator alert rules when the
Prometheus Operator CRDs are installed:

```bash
helm upgrade --install telemetry-fabric deploy/helm/telemetry-fabric \
  --set serviceMonitor.enabled=true \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=prometheus
```

Rules are disabled by default. Thresholds live under
`prometheusRule.agent` and `prometheusRule.controlPlane`.

# Rollout And Rollback

This runbook covers rolling updates, canary releases, rollback, config
rollback, and migration order.

## Rolling Update

Default strategy:

- Control plane: Deployment rolling update with `maxUnavailable: 0` and
  `maxSurge: 1` in production.
- Agent: DaemonSet rolling update with `maxUnavailable: 10%`.
- Crawler and stream processor: Deployment rolling update with
  `maxUnavailable: 0` and `maxSurge: 1`.

Use:

```sh
helm upgrade telemetry-fabric deploy/helm/telemetry-fabric \
  --namespace observability \
  -f deploy/helm/telemetry-fabric/values-production.example.yaml \
  --wait \
  --timeout 10m
```

## Canary Strategy

Canary is done with a second Helm release and narrower scheduling or routing:

1. Create `values-canary.yaml`.
2. Set crawler and stream-processor replicas to `1`.
3. Use a canary Kafka consumer group and output topics when testing processors.
4. Use `nodeSelector`, `tolerations`, or a small watched-contract set for
   crawler canaries.
5. Install as `telemetry-fabric-canary`.

```sh
helm upgrade --install telemetry-fabric-canary deploy/helm/telemetry-fabric \
  --namespace observability \
  -f values-canary.yaml \
  --wait \
  --timeout 10m
```

Promote by applying the same image tag and config to the primary release after
canary lag, error rate, and DLQ rate remain stable.

## Rollback

Application rollback:

```sh
helm history telemetry-fabric -n observability
helm rollback telemetry-fabric <REVISION> -n observability --wait --timeout 10m
```

Kubernetes workload rollback, when Helm is unavailable:

```sh
kubectl -n observability rollout undo deploy/telemetry-fabric-control-plane
kubectl -n observability rollout undo deploy/telemetry-fabric-chain-crawler
kubectl -n observability rollout undo deploy/telemetry-fabric-stream-processor
```

DaemonSet rollback:

```sh
kubectl -n observability rollout undo ds/telemetry-fabric
```

## Config Rollback

Config changes are rolled out through Helm values and ConfigMaps.

1. Find the last known good Helm revision.
2. Compare values with `helm get values`.
3. Roll back the Helm release or re-apply the previous ConfigMap values.
4. Wait for pods with checksum annotations to restart and become ready.
5. Confirm Kafka consumer lag and DLQ rate return to normal.

For crawler config, verify checkpoints before rolling back block range,
finality depth, or watched-contract filters.

## Migration Order

Use expand-and-contract migrations:

1. Apply storage expansions first: new nullable columns, new topics, new
   optional schema fields.
2. Deploy producers that can write both old and new fields.
3. Deploy consumers that understand both versions.
4. Backfill or replay if needed.
5. Remove old fields only after all consumers have moved and retention windows
   have expired.

Control-plane database migrations run as a Helm init container when
`controlPlane.databaseUrlSecret.name` is set. ClickHouse migrations should run
before crawler or stream-processor deployments that depend on new tables.

## Abort Criteria

Stop or roll back a rollout when any of these occur:

- readiness does not converge before timeout
- Kafka consumer lag grows without recovery
- DLQ rate increases above baseline
- crawler RPC error rate spikes
- control-plane command or heartbeat errors increase
- ClickHouse insert failures appear

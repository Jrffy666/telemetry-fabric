# Local Stack

The Docker Compose stack supports a base platform-core deployment and optional
profiles for blockchain infrastructure and observability.

## Base Stack

Runs PostgreSQL, control plane, and telemetry agent:

```sh
docker compose -f deploy/docker/docker-compose.yml up -d --build
```

Or:

```sh
make docker-smoke
```

## Blockchain Infrastructure

Runs Redpanda and ClickHouse in addition to the base services:

```sh
make deploy-local
make kafka-topics
make clickhouse-migrate
make blockchain-smoke
```

`make deploy-local` uses:

```sh
docker compose -f deploy/docker/docker-compose.yml \
  --profile blockchain \
  --profile observability \
  up -d --build
```

## Optional Crawler And Stream Processor

The compose file includes `chain-crawler` and `stream-processor` skeleton
services. They are profile-gated because their images may be built by separate
service pipelines.

```sh
CHAIN_CRAWLER_IMAGE=registry.example.com/telemetry-fabric/chain-crawler-go:dev \
STREAM_PROCESSOR_IMAGE=registry.example.com/telemetry-fabric/stream-processor:dev \
docker compose -f deploy/docker/docker-compose.yml \
  --profile blockchain \
  --profile crawler \
  --profile stream \
  up -d
```

## Observability

Prometheus and Grafana are optional:

```sh
docker compose -f deploy/docker/docker-compose.yml --profile observability up -d
```

Default endpoints:

- Control plane: `http://127.0.0.1:4001`
- Agent health: `http://127.0.0.1:13133`
- Redpanda Kafka: `127.0.0.1:9092`
- Redpanda admin: `http://127.0.0.1:9644`
- ClickHouse HTTP: `http://127.0.0.1:8123`
- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000`

## Shutdown

```sh
docker compose -f deploy/docker/docker-compose.yml down
```

Remove local data:

```sh
docker compose -f deploy/docker/docker-compose.yml down -v --remove-orphans
```

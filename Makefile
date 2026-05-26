COMPOSE_FILE ?= deploy/docker/docker-compose.yml
HELM_RELEASE ?= telemetry-fabric
HELM_NAMESPACE ?= observability
HELM_CHART ?= deploy/helm/telemetry-fabric
LOCAL_PROFILES ?= --profile blockchain --profile observability
PYTHON ?= python
CMAKE ?= cmake
CTEST ?= ctest
CMAKE_GENERATOR ?=
CMAKE_ARCH ?=
CMAKE_BUILD_DIR ?= services/compute-accelerator/build
CMAKE_CONFIGURE_ARGS ?= -DACCELERATOR_ENABLE_TESTS=ON
CMAKE_BUILD_ARGS ?=
CTEST_ARGS ?= --output-on-failure
CMAKE_GENERATOR_ARG = $(if $(strip $(CMAKE_GENERATOR)),-G "$(CMAKE_GENERATOR)")
CMAKE_ARCH_ARG = $(if $(strip $(CMAKE_ARCH)),-A "$(CMAKE_ARCH)")

.PHONY: test rust-test elixir-test go-test python-test cpp-test agent-self-test docker-smoke deploy-local k8s-render helm-lint blockchain-smoke clickhouse-migrate kafka-topics

test: rust-test elixir-test go-test python-test cpp-test

rust-test:
	cargo test --workspace

agent-self-test:
	cargo run -p telemetry-agent -- --self-test

elixir-test:
	cd apps/control_plane && mix test

go-test:
	cd services/chain-crawler-go && go test ./...

python-test:
	cd services/analytics-python && "$(PYTHON)" -m pytest

cpp-test:
	"$(CMAKE)" -S services/compute-accelerator -B "$(CMAKE_BUILD_DIR)" $(CMAKE_GENERATOR_ARG) $(CMAKE_ARCH_ARG) $(CMAKE_CONFIGURE_ARGS)
	"$(CMAKE)" --build "$(CMAKE_BUILD_DIR)" $(CMAKE_BUILD_ARGS)
	"$(CTEST)" --test-dir "$(CMAKE_BUILD_DIR)" $(CTEST_ARGS)

docker-smoke:
	bash scripts/ci_smoke_compose.sh

deploy-local:
	docker compose -f $(COMPOSE_FILE) $(LOCAL_PROFILES) up -d --build

k8s-render:
	helm template $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)

helm-lint:
	helm lint $(HELM_CHART)

blockchain-smoke:
	docker compose -f $(COMPOSE_FILE) --profile blockchain up -d redpanda clickhouse
	docker compose -f $(COMPOSE_FILE) exec redpanda rpk cluster health --brokers=127.0.0.1:9092
	docker compose -f $(COMPOSE_FILE) exec clickhouse clickhouse-client --query "SELECT 1"

clickhouse-migrate:
	docker compose -f $(COMPOSE_FILE) --profile blockchain up -d clickhouse
	bash -lc 'for f in storage/clickhouse/migrations/*.sql; do docker compose -f "$(COMPOSE_FILE)" exec -T clickhouse clickhouse-client --multiquery < "$$f"; done'

kafka-topics:
	docker compose -f $(COMPOSE_FILE) --profile blockchain up -d redpanda
	docker compose -f $(COMPOSE_FILE) exec redpanda rpk topic create chain.events.raw chain.events.critical chain.events.important chain.events.aggregate chain.events.dead_letter chain.alerts chain.reorgs chain.node_health --brokers=127.0.0.1:9092 --if-not-exists

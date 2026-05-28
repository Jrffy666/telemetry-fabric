COMPOSE_FILE ?= deploy/docker/docker-compose.yml
HELM_RELEASE ?= telemetry-fabric
HELM_NAMESPACE ?= observability
HELM_CHART ?= deploy/helm/telemetry-fabric
LOCAL_PROFILES ?= --profile observability

.PHONY: test rust-test elixir-test agent-self-test docker-smoke deploy-local k8s-render helm-lint

test: rust-test elixir-test

rust-test:
	cargo test --workspace

agent-self-test:
	cargo run -p telemetry-agent -- --self-test

elixir-test:
	cd apps/control_plane && mix test

docker-smoke:
	bash scripts/ci_smoke_compose.sh

deploy-local:
	docker compose -f $(COMPOSE_FILE) $(LOCAL_PROFILES) up -d --build

k8s-render:
	helm template $(HELM_RELEASE) $(HELM_CHART) --namespace $(HELM_NAMESPACE)

helm-lint:
	helm lint $(HELM_CHART)

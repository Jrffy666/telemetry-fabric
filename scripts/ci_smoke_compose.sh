#!/usr/bin/env bash
set -Eeuo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker/docker-compose.yml}"
CONTROL_URL="${CONTROL_URL:-http://127.0.0.1:4001}"
AGENT_URL="${AGENT_URL:-http://127.0.0.1:13133}"
AGENT_TOKEN="${TELEMETRY_FABRIC_CONTROL_AGENT_TOKEN:-local-agent-token}"
OPERATOR_TOKEN="${TELEMETRY_FABRIC_CONTROL_OPERATOR_TOKEN:-local-operator-token}"

cleanup() {
  status=$?
  if [ "${status}" -ne 0 ]; then
    docker compose -f "${COMPOSE_FILE}" ps || true
    docker compose -f "${COMPOSE_FILE}" logs --no-color postgres control-plane telemetry-agent || true
  fi
  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans || true
}
trap cleanup EXIT

wait_for_http() {
  local name="$1"
  local url="$2"

  for attempt in $(seq 1 60); do
    if curl -fsS "${url}" >/dev/null; then
      return 0
    fi
    sleep 2
    echo "waiting for ${name} (${attempt}/60)"
  done

  echo "${name} did not become ready" >&2
  return 1
}

post_json() {
  local token="$1"
  local path="$2"
  local body="$3"

  curl -fsS \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "${CONTROL_URL}${path}" >/dev/null
}

docker compose -f "${COMPOSE_FILE}" up -d --build

wait_for_http "control plane readiness" "${CONTROL_URL}/readyz"
wait_for_http "agent readiness" "${AGENT_URL}/readyz"

curl -fsS "${CONTROL_URL}/healthz" >/dev/null
curl -fsS "${CONTROL_URL}/readyz" >/dev/null
curl -fsS "${AGENT_URL}/healthz" >/dev/null

post_json "${AGENT_TOKEN}" "/v1/agents/register" \
  '{"agent_id":"smoke-agent","tenant_id":"default","hostname":"ci","version":"smoke"}'

post_json "${OPERATOR_TOKEN}" "/v1/agents/commands" \
  '{"agent_id":"smoke-agent","kind":"pause_exports","reason":"ci smoke test"}'

post_json "${AGENT_TOKEN}" "/v1/agents/heartbeat" \
  '{"agent_id":"smoke-agent","tenant_id":"default","config_version":0,"queue_depth_bytes":0,"ingest_bytes_per_second":0}'

metrics="$(curl -fsS "${CONTROL_URL}/metrics")"
grep -q "telemetry_fabric_control_http_requests_total" <<<"${metrics}"

agent_metrics="$(curl -fsS "${AGENT_URL}/metrics")"
grep -q "telemetry_agent_queue_bytes" <<<"${agent_metrics}"

docker compose -f "${COMPOSE_FILE}" ps
docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans
trap - EXIT

echo "docker compose smoke test passed"

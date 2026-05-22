.PHONY: test rust-test elixir-test agent-self-test docker-smoke

test: rust-test elixir-test

rust-test:
	cargo test --workspace

agent-self-test:
	cargo run -p telemetry-agent -- --self-test

elixir-test:
	cd apps/control_plane && mix test

docker-smoke:
	bash scripts/ci_smoke_compose.sh

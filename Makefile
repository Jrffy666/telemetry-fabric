.PHONY: test rust-test elixir-test agent-self-test

test: rust-test elixir-test

rust-test:
	cargo test --workspace

agent-self-test:
	cargo run -p telemetry-agent -- --self-test

elixir-test:
	cd apps/control_plane && mix test

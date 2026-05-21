FROM rust:1.94-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --release -p telemetry-agent

FROM debian:bookworm-slim
RUN useradd --system --home /var/lib/telemetry-fabric telemetry-fabric \
  && mkdir -p /var/lib/telemetry-fabric \
  && chown -R telemetry-fabric:telemetry-fabric /var/lib/telemetry-fabric
COPY --from=builder /workspace/target/release/telemetry-agent /usr/local/bin/telemetry-agent
USER telemetry-fabric
ENTRYPOINT ["/usr/local/bin/telemetry-agent"]

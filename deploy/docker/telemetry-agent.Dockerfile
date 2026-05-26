FROM rust:1.94-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --release -p telemetry-agent

FROM debian:bookworm-slim
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --system --home /var/lib/telemetry-fabric telemetry-fabric \
  && mkdir -p /var/lib/telemetry-fabric \
  && chown -R telemetry-fabric:telemetry-fabric /var/lib/telemetry-fabric
COPY --from=builder /workspace/target/release/telemetry-agent /usr/local/bin/telemetry-agent
USER telemetry-fabric
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:13133/readyz >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/telemetry-agent"]

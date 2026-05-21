FROM elixir:1.18-otp-27-slim AS builder

ENV MIX_ENV=prod
WORKDIR /workspace/apps/control_plane

RUN mix local.hex --force && mix local.rebar --force

COPY apps/control_plane/mix.exs apps/control_plane/mix.lock ./
RUN mix deps.get --only prod

COPY apps/control_plane/config ./config
COPY apps/control_plane/lib ./lib
COPY apps/control_plane/priv ./priv

RUN mix compile && mix release

FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 ncurses-base openssl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --system --home /var/lib/telemetry-fabric/control-plane telemetry-fabric

COPY --from=builder /workspace/apps/control_plane/_build/prod/rel/telemetry_fabric_control /opt/telemetry-fabric-control

ENV HOME=/var/lib/telemetry-fabric/control-plane \
  TELEMETRY_FABRIC_CONTROL_DATA_DIR=/var/lib/telemetry-fabric/control-plane \
  TELEMETRY_FABRIC_CONTROL_HTTP_LISTEN=0.0.0.0:4001

RUN mkdir -p /var/lib/telemetry-fabric/control-plane \
  && chown -R telemetry-fabric:telemetry-fabric /var/lib/telemetry-fabric /opt/telemetry-fabric-control

USER telemetry-fabric
EXPOSE 4001

ENTRYPOINT ["/opt/telemetry-fabric-control/bin/telemetry_fabric_control"]
CMD ["start"]

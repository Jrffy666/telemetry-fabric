# Contracts

This directory defines the stable cross-service communication contracts for
Telemetry Fabric. Services in Go, Rust, Elixir, Python, and C++ should exchange
data through generated code from these files, not by importing each other's
internal structs or schemas.

## Layout

- `proto/platform`: platform-wide envelope, module, priority, and checkpoint
  contracts.
- `proto/blockchain`: blockchain domain events, configuration, filtering,
  node health, alerts, and reorg events.
- `examples`: JSON examples using proto field names (`snake_case`).
- `jsonschema`: reserved for JSON Schema artifacts generated from proto.
- `openapi`: reserved for OpenAPI artifacts generated from proto-backed APIs.

## Envelope

All event traffic should use `telemetry.fabric.platform.v1.Envelope` as the
outer message. The envelope carries routing, tenancy, priority, schema version,
replay, deduplication, tracing, and rule-match metadata. Domain payloads are
stored in `payload` and are identified by `event_type` and `schema_version`.

Use stable event type names such as:

- `blockchain.chain_event.transfer`
- `blockchain.node_health`
- `blockchain.alert`
- `blockchain.reorg`

## Schema Version Strategy

Use semantic schema versions in `schema_version`:

```text
<domain>.<message>.v<major>.<minor>.<patch>
```

Examples:

- `blockchain.chain_event.v1.0.0`
- `blockchain.node_health.v1.0.0`

Version rules:

- Increment `patch` for comment-only clarifications, example fixes, and
  documentation updates that do not change generated code behavior.
- Increment `minor` for backward-compatible additions such as new optional
  fields, new enum values, or new messages.
- Increment `major` for breaking changes such as field type changes, required
  semantic changes, removed fields, renamed fields, or incompatible enum
  behavior.
- Proto package versions (`telemetry.fabric.*.v1`) should change only for a
  major wire-contract break.

## Backward Compatibility

Contract changes must follow these rules:

- Never reuse a field number.
- Never change a field type or meaning in place.
- Prefer adding new fields over changing existing fields.
- If a field is removed, reserve its field number and name in the proto file.
- Enum values must be append-only; keep zero as the `*_UNSPECIFIED` value.
- Consumers must tolerate unknown fields and unknown enum values.
- Producers must keep sending fields with their existing meaning until all
  downstream consumers have migrated.
- Large integers, token amounts, and decimal monetary values should be encoded
  as strings in JSON examples and APIs to avoid precision loss.
- Services should map internal domain structs to contract messages at their
  boundary. They must not import another service's internal Go, Rust, Elixir,
  Python, or C++ types to communicate.

## Code Generation

Use `contracts/proto` as the proto include root:

```sh
protoc -I contracts/proto ...
```

Recommended approaches:

- Go: use `protoc-gen-go` or Buf. Because the proto files do not yet pin a
  repository-specific `go_package`, centralize Go import mappings in `buf.gen.yaml`
  or pass `M<proto>=<go/import/path>` options from the generating repository.
- Rust: use `prost-build` or `tonic-build` from `build.rs`, with
  `contracts/proto` in the include path. Keep generated modules in a shared
  crate instead of copying generated files into service crates.
- Python: use `grpcio-tools` or Buf to generate `_pb2.py` files into a package
  owned by the service or SDK. Configure JSON parsing to preserve proto field
  names when consuming the examples.
- Elixir: use `protobuf` with `protoc-gen-elixir`, generating modules from the
  same include root. Keep generated modules in a dedicated app or dependency.
- C++: use `protoc --cpp_out` or Buf-generated C++ artifacts and link them as a
  shared contract library.

If a Makefile target is added later, it should only orchestrate generation from
`contracts/proto`; business services should not maintain separate handwritten
copies of these contracts.

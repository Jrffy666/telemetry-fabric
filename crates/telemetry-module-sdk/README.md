# telemetry-module-sdk

`telemetry-module-sdk` is the Rust boundary for business modules that emit
domain-specific events into the Telemetry Fabric data plane.

The SDK provides a generic `EventEnvelope` with:

- tenant, module, source, and event type metadata
- `Priority`
- checkpoint metadata
- optional dedupe key
- generic attributes
- bytes or JSON payloads
- routing metadata such as route, partition, shard, and preferred exporters

The SDK intentionally does not define blockchain, payment, infrastructure, or
other business payload fields. Domain fields belong in the envelope payload or
in module-specific schemas generated from `contracts/`.

## Example

```rust
use telemetry_module_sdk::{
    CheckpointMetadata, EventEnvelope, Payload, Priority, RoutingMetadata,
};

let envelope = EventEnvelope::new(
    "tenant-a",
    "payments",
    "checkout-worker",
    "payment.authorized",
    Payload::json(br#"{"amount":"42.00"}"#.to_vec()),
)
.with_priority(Priority::Important)
.with_dedupe_key("payments:authorized:123")
.with_checkpoint(CheckpointMetadata::new(
    "cursor-1",
    "payments",
    "partition-0",
    "123",
))
.with_routing(
    RoutingMetadata::default()
        .with_route_key("payments/authorized")
        .with_partition_key("merchant-1"),
);

let record = envelope.into_telemetry_record()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`into_telemetry_record` bridges module events into the existing durable data
plane without changing the core `TelemetryRecord` model. Exporters and
processors can use the generated `module.*` attributes for generic routing
while payload decoding remains owned by the module or downstream service.

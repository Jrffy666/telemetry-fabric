# gRPC Boundary

The gRPC API contract lives in `../proto/compute_accelerator.proto`.

Generated files should stay under this service directory, for example:

```text
grpc/generated/
```

Phase 1 keeps `server_skeleton.cpp` dependency-free so the CPU skeleton can
compile without generated protobuf or gRPC libraries. Future work should map
generated request messages into the C++ `ComputeJob` model and preserve:

- job id propagation
- timeout enforcement
- cancellation
- structured error details
- selected accelerator and CPU fallback reporting
- job metrics in status responses

The gRPC service must expose batch job APIs only. It must not expose crawler
RPC, chain RPC, or low-latency stream hot-path APIs.

# C++ Service Skeleton

This directory owns CPU-side orchestration for the compute accelerator service.

Current files:

- `accelerator_service.h`: service data model and CPU fallback interface.
- `accelerator_service.cpp`: validation, accelerator selection, cancellation
  placeholder, and skeleton metrics.
- `service_main.cpp`: minimal executable for local CPU-only smoke builds.

Future responsibilities:

- Parse generated gRPC messages into `ComputeJob`.
- Validate Kafka, Arrow, and Parquet batch inputs.
- Dispatch CPU baseline implementations.
- Dispatch CUDA kernels for GPU-eligible workloads.
- Persist batch outputs to configured storage.

The code here intentionally does not include blockchain RPC clients, Rust FFI,
or crawler path dependencies.

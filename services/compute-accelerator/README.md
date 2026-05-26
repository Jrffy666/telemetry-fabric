# Compute Accelerator

`services/compute-accelerator` is an independent, optional, and replaceable
C++/CUDA batch compute service. It is not part of the Rust workspace, the Go
crawler, Python analytics packages, or the control plane runtime.

The service exists for heavy offline or asynchronous computation after data has
already landed in Kafka, object storage, ClickHouse, Arrow, or Parquet.

## Responsibilities

- Batch graph computation.
- N-hop fund flow search.
- Batch risk scoring.
- Feature computation.
- Model inference.
- CUDA kernel ownership.
- CPU fallback and CPU baselines.
- Benchmark harnesses for CPU/GPU comparison.

## Hard Boundaries

- Does not connect to blockchain RPC.
- Does not run in the crawler hot path.
- Does not import or link Rust crates.
- Does not introduce FFI into the Rust data plane.
- Does not affect the Rust `unsafe_code = forbid` constraint.
- Serves analytics and batch compute only.

## Layout

```text
CMakeLists.txt       Optional native CPU-only build entrypoint.
cpp/                 C++ service interface, validation, CPU fallback skeleton.
cuda/                CUDA kernel placeholder and memory-transfer guidance.
proto/               gRPC protobuf API contract skeleton.
grpc/                gRPC server integration boundary.
benchmarks/          CPU/GPU benchmark harness placeholder.
docker/              CPU/GPU container skeleton.
```

Additional design notes live under `../../docs/accelerator/`.

## Service Interface

`proto/compute_accelerator.proto` defines:

- `SubmitJob` for batch job submission.
- `GetJobStatus` for status and metrics.
- `CancelJob` for cancellation.
- `Health` for readiness and GPU availability.
- job id, timeout, fallback policy, accelerator selection, and error model.

The proto reserves job specs for graph compute, N-hop fund flow search, batch
risk score, feature computation, and model inference. It also reserves Kafka,
Arrow, and Parquet batch input shapes.

## CPU-Only Build

The phase 1 C++ skeleton is compileable without gRPC or CUDA dependencies:

Required local tools:

- CMake 3.22 or newer.
- A C++17 compiler such as GCC, Clang, or MSVC.

```sh
cd services/compute-accelerator
cmake -S . -B build
cmake --build build
./build/compute_accelerator --listen 0.0.0.0:50071
./build/accelerator_benchmark 1000000
```

On Windows with Visual Studio generators, run the generated executable from the
selected config directory, for example `build/Debug/compute_accelerator.exe`.

CUDA is optional:

```sh
cmake -S . -B build-cuda -DACCELERATOR_ENABLE_CUDA=ON
cmake --build build-cuda
```

## CPU Fallback

If GPU is unavailable, jobs can either:

- fail clearly with `ERROR_CODE_GPU_UNAVAILABLE`, or
- run through CPU fallback when `FALLBACK_POLICY_USE_CPU_IF_GPU_UNAVAILABLE`
  is set.

The C++ phase 1 skeleton demonstrates accelerator selection and fallback
reporting, but does not execute real workloads yet.

## When To Use GPU

GPU acceleration is appropriate when:

- Work is large, batch-oriented, and parallelizable.
- The same operation runs across many graph edges, accounts, vectors, or model
  rows.
- Input batches are large enough to amortize host-to-device transfer cost.
- Latency budgets tolerate queued batch execution.
- CPU baseline benchmarks show meaningful speedup.

## When Not To Use GPU

Do not use GPU for:

- Crawler ingestion, RPC polling, or chain head processing.
- Small single-record requests where transfer overhead dominates.
- Business logic that requires frequent branching or irregular tiny workloads.
- Control-plane validation, routing, or configuration tasks.
- Anything that would require Rust FFI before a narrow reviewed design exists.

## Observability

The service API and docs reserve these metric families:

- job duration
- job error count
- GPU memory usage placeholder
- batch size
- throughput
- queue wait time
- CPU fallback count

See `../../docs/accelerator/observability.md`.

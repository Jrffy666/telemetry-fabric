# CUDA Boundary

CUDA code belongs only in `services/compute-accelerator/cuda`. It must stay
behind the standalone service boundary and must not introduce Rust FFI or
crawler hot-path dependencies.

## Memory Transfer Strategy

Use the following pattern for GPU workloads:

1. Load Kafka, Arrow, or Parquet input on CPU.
2. Validate schema and convert to contiguous buffers.
3. Use pinned host memory for large repeated transfers.
4. Copy each batch stage to device memory once.
5. Run kernels over arrays or graph structures such as CSR/CSC.
6. Copy compact results back to CPU.
7. Write outputs to the requested batch output URI.

Avoid per-record transfers. If a workload cannot batch data into large
contiguous buffers, keep it on CPU.

## Batch Size Guidance

Start with CPU fallback. Promote to CUDA only after benchmarks show speedup.

Practical starting points:

- vector/model scoring: tens of thousands of rows per batch
- graph traversal: hundreds of thousands of edges per batch
- feature computation: enough columns and rows to amortize transfer overhead

Track host-to-device transfer, kernel time, device-to-host transfer, total job
duration, and GPU memory high-water mark for every benchmark.

## When Not To Use GPU

Do not use GPU for small jobs, one-off requests, branch-heavy business rules,
control-plane checks, crawler ingestion, or direct blockchain RPC interaction.

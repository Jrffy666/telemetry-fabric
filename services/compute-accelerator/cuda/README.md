# CUDA Boundary

This directory owns CUDA kernels and GPU-specific implementation details.

Phase 1 contains only `kernels.cu`, a placeholder identity-style batch kernel.
The default service build does not require CUDA. Build with
`-DACCELERATOR_ENABLE_CUDA=ON` only on machines with a CUDA toolkit.

## Memory Transfer Strategy

Future CUDA implementations should follow this boundary:

1. Read Kafka, Arrow, or Parquet input on the CPU side.
2. Normalize batch buffers into contiguous host memory.
3. Use pinned host memory for large repeated transfers.
4. Copy batch buffers to device memory once per job stage.
5. Run kernels over contiguous arrays or CSR/CSC graph buffers.
6. Copy only compact results back to host memory.
7. Write outputs to the configured batch output URI.

Avoid per-record host/device transfers. A workload that requires tiny,
interactive, or branch-heavy transfers should stay on CPU.

## Batch Size Guidance

Start with CPU fallback unless a benchmark proves GPU value. GPU candidates
should usually have at least:

- tens of thousands of rows for vector/model scoring workloads
- hundreds of thousands of edges for graph traversal workloads
- enough repeated work to amortize host-to-device transfer costs

Record batch size, transfer time, kernel time, and total job time before
promoting a CUDA path.

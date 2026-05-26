# Accelerator Interface

The service interface is defined by:

```text
services/compute-accelerator/proto/compute_accelerator.proto
```

The phase 1 interface covers:

- `SubmitJob`
- `GetJobStatus`
- `CancelJob`
- `Health`

Jobs include:

- stable `tenant_id`
- caller-provided or service-generated `job_id`
- `JobKind`
- Kafka, Arrow, or Parquet input
- output URI
- timeout in milliseconds
- requested accelerator: CPU, GPU, or auto
- fallback policy
- labels
- one job-specific spec

Responses include:

- job id
- status
- selected accelerator
- whether CPU fallback was used
- structured `ErrorDetail`
- metrics for duration, batch size, throughput, and GPU memory usage

## Error Model

The proto reserves stable error codes for invalid arguments, unsupported job
kinds, unavailable GPUs, timeouts, cancellation, missing input/output, and
internal failures.

Errors should be explicit. A request that requires GPU must fail with
`ERROR_CODE_GPU_UNAVAILABLE` when no GPU is available and CPU fallback is not
allowed.

## Cancellation

Cancellation is cooperative. `CancelJob` marks queued jobs as cancelled and asks
running jobs to stop between batch stages. CUDA kernels should be launched in
bounded stages so long-running jobs can observe cancellation between launches.

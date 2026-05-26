# Accelerator Observability

The accelerator should emit service-level and job-level metrics. Phase 1 only
reserves the metric model; implementation can use Prometheus, OpenTelemetry, or
the platform's existing telemetry pipeline later.

Reserved metrics:

```text
accelerator_jobs_submitted_total
accelerator_jobs_cancelled_total
accelerator_job_duration_ms
accelerator_job_errors_total
accelerator_job_timeout_total
accelerator_cpu_fallback_total
accelerator_batch_rows
accelerator_batch_bytes
accelerator_throughput_rows_per_second
accelerator_gpu_memory_used_bytes
accelerator_gpu_memory_free_bytes
accelerator_gpu_transfer_h2d_ms
accelerator_gpu_kernel_ms
accelerator_gpu_transfer_d2h_ms
```

Minimum labels:

- `tenant_id`
- `job_kind`
- `selected_accelerator`
- `status`
- `error_code`

Avoid high-cardinality labels such as raw job ids in always-on metrics. Job ids
belong in logs, traces, and status APIs.

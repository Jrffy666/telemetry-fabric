#include "accelerator_service.h"

#include <iostream>
#include <string>

namespace {

using telemetry_fabric::compute::AcceleratorKind;
using telemetry_fabric::compute::ComputeAcceleratorService;
using telemetry_fabric::compute::ComputeJob;
using telemetry_fabric::compute::ErrorCode;
using telemetry_fabric::compute::FallbackPolicy;
using telemetry_fabric::compute::JobKind;
using telemetry_fabric::compute::JobStatus;
using telemetry_fabric::compute::ServiceOptions;

bool Check(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << "FAILED: " << message << "\n";
    return false;
  }
  return true;
}

ComputeJob ValidJob() {
  ComputeJob job;
  job.tenant_id = "tenant-a";
  job.job_id = "job-1";
  job.kind = JobKind::kFeatureCompute;
  job.input_uri = "s3://bucket/input.parquet";
  job.output_uri = "s3://bucket/output.parquet";
  job.options.timeout_ms = 1000;
  job.options.max_batch_rows = 1024;
  return job;
}

bool AcceptsCpuFallbackWhenGpuIsUnavailable() {
  ServiceOptions options;
  options.gpu_available = false;
  options.allow_cpu_fallback = true;
  const ComputeAcceleratorService service(options);

  ComputeJob job = ValidJob();
  job.options.accelerator = AcceleratorKind::kAuto;
  job.options.fallback_policy = FallbackPolicy::kUseCpuIfGpuUnavailable;
  const auto result = service.SubmitJob(job);
  const auto status = service.GetJobStatus(job.tenant_id, result.job_id);

  return Check(result.status == JobStatus::kAccepted, "job should be accepted") &&
         Check(result.selected_accelerator == AcceleratorKind::kCpu,
               "CPU fallback should be selected") &&
         Check(result.cpu_fallback_used, "CPU fallback flag should be set") &&
         Check(result.error.code == ErrorCode::kNone,
               "accepted job should not have an error") &&
         Check(status.status == JobStatus::kAccepted,
               "accepted job status should be queryable") &&
         Check(status.job_id == result.job_id,
               "queried job status should use submitted job id");
}

bool FailsGpuOnlyJobWhenGpuIsUnavailable() {
  ServiceOptions options;
  options.gpu_available = false;
  options.allow_cpu_fallback = false;
  const ComputeAcceleratorService service(options);

  ComputeJob job = ValidJob();
  job.options.accelerator = AcceleratorKind::kGpu;
  job.options.fallback_policy = FallbackPolicy::kFailIfGpuUnavailable;
  const auto result = service.SubmitJob(job);
  const auto status = service.GetJobStatus(job.tenant_id, result.job_id);

  return Check(result.status == JobStatus::kFailed, "GPU-only job should fail") &&
         Check(result.error.code == ErrorCode::kGpuUnavailable,
               "GPU-only failure should report gpu_unavailable") &&
         Check(result.error.retryable, "GPU unavailable should be retryable") &&
         Check(status.status == JobStatus::kFailed,
               "failed job status should be queryable") &&
         Check(status.error.code == ErrorCode::kGpuUnavailable,
               "queried failed job should keep gpu_unavailable");
}

bool RejectsInvalidJobs() {
  const ComputeAcceleratorService service(ServiceOptions{});
  ComputeJob job = ValidJob();
  job.tenant_id.clear();
  const auto result = service.SubmitJob(job);

  return Check(result.status == JobStatus::kFailed, "invalid job should fail") &&
         Check(result.error.code == ErrorCode::kInvalidArgument,
               "missing tenant should be invalid_argument") &&
         Check(!result.error.retryable, "invalid arguments should not be retryable");
}

bool StoresRejectedSubmittedJobStatus() {
  const ComputeAcceleratorService service(ServiceOptions{});
  ComputeJob job = ValidJob();
  job.job_id = "invalid-job";
  job.input_uri.clear();
  const auto submitted = service.SubmitJob(job);
  const auto status = service.GetJobStatus(job.tenant_id, submitted.job_id);

  return Check(submitted.status == JobStatus::kFailed,
               "invalid submitted job should fail") &&
         Check(status.status == JobStatus::kFailed,
               "invalid submitted job status should be queryable") &&
         Check(status.error.code == ErrorCode::kInvalidArgument,
               "queried invalid job should keep validation error");
}

bool RejectsInvalidCancellation() {
  const ComputeAcceleratorService service(ServiceOptions{});
  const auto result = service.CancelJob("", "job-1", "test");

  return Check(result.status == JobStatus::kFailed, "invalid cancellation should fail") &&
         Check(result.error.code == ErrorCode::kInvalidArgument,
               "invalid cancellation should be invalid_argument");
}

bool RejectsUnknownJobStatusLookup() {
  const ComputeAcceleratorService service(ServiceOptions{});
  const auto result = service.GetJobStatus("tenant-a", "missing-job");

  return Check(result.status == JobStatus::kFailed,
               "unknown job lookup should fail") &&
         Check(result.error.code == ErrorCode::kInvalidArgument,
               "unknown job lookup should be invalid_argument");
}

bool CancelsSubmittedJobAndPersistsStatus() {
  const ComputeAcceleratorService service(ServiceOptions{});
  ComputeJob job = ValidJob();
  job.job_id = "cancel-me";
  const auto submitted = service.SubmitJob(job);
  const auto cancelled =
      service.CancelJob(job.tenant_id, submitted.job_id, "test cancellation");
  const auto status = service.GetJobStatus(job.tenant_id, submitted.job_id);

  return Check(submitted.status == JobStatus::kAccepted,
               "submitted job should start accepted") &&
         Check(cancelled.status == JobStatus::kCancelled,
               "submitted job should cancel") &&
         Check(cancelled.error.code == ErrorCode::kCancelled,
               "cancelled job should report cancelled") &&
         Check(status.status == JobStatus::kCancelled,
               "cancelled status should be persisted");
}

bool RejectsUnknownCancellation() {
  const ComputeAcceleratorService service(ServiceOptions{});
  const auto result =
      service.CancelJob("tenant-a", "missing-job", "test cancellation");

  return Check(result.status == JobStatus::kFailed,
               "unknown cancellation should fail") &&
         Check(result.error.code == ErrorCode::kInvalidArgument,
               "unknown cancellation should be invalid_argument");
}

bool DoesNotCancelTerminalJob() {
  ServiceOptions options;
  options.gpu_available = false;
  options.allow_cpu_fallback = false;
  const ComputeAcceleratorService service(options);

  ComputeJob job = ValidJob();
  job.job_id = "terminal-job";
  job.options.accelerator = AcceleratorKind::kGpu;
  job.options.fallback_policy = FallbackPolicy::kFailIfGpuUnavailable;
  const auto submitted = service.SubmitJob(job);
  const auto cancelled =
      service.CancelJob(job.tenant_id, submitted.job_id, "test cancellation");
  const auto status = service.GetJobStatus(job.tenant_id, submitted.job_id);

  return Check(submitted.status == JobStatus::kFailed,
               "terminal job should start failed") &&
         Check(cancelled.status == JobStatus::kFailed,
               "terminal job should not cancel") &&
         Check(cancelled.error.code == ErrorCode::kInvalidArgument,
               "terminal cancellation should be invalid_argument") &&
         Check(status.status == JobStatus::kFailed,
               "terminal job status should remain failed") &&
         Check(status.error.code == ErrorCode::kGpuUnavailable,
               "terminal job should preserve original failure");
}

}  // namespace

int main() {
  bool ok = true;
  ok = AcceptsCpuFallbackWhenGpuIsUnavailable() && ok;
  ok = FailsGpuOnlyJobWhenGpuIsUnavailable() && ok;
  ok = RejectsInvalidJobs() && ok;
  ok = StoresRejectedSubmittedJobStatus() && ok;
  ok = RejectsInvalidCancellation() && ok;
  ok = RejectsUnknownJobStatusLookup() && ok;
  ok = CancelsSubmittedJobAndPersistsStatus() && ok;
  ok = RejectsUnknownCancellation() && ok;
  ok = DoesNotCancelTerminalJob() && ok;
  return ok ? 0 : 1;
}

#include "accelerator_service.h"

#include <chrono>
#include <mutex>
#include <utility>

namespace telemetry_fabric {
namespace compute {

namespace {

bool IsGpuRequested(AcceleratorKind accelerator) {
  return accelerator == AcceleratorKind::kGpu ||
         accelerator == AcceleratorKind::kAuto;
}

std::uint64_t ElapsedMillis(std::chrono::steady_clock::time_point started) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
}

std::string JobKey(const std::string& tenant_id, const std::string& job_id) {
  return tenant_id + '\x1f' + job_id;
}

bool IsCancellable(JobStatus status) {
  return status == JobStatus::kAccepted || status == JobStatus::kQueued ||
         status == JobStatus::kRunning;
}

}  // namespace

struct ComputeAcceleratorService::Registry {
  std::mutex mutex;
  std::unordered_map<std::string, JobResult> jobs;
};

ComputeAcceleratorService::ComputeAcceleratorService(ServiceOptions options)
    : options_(std::move(options)), registry_(std::make_shared<Registry>()) {}

JobResult ComputeAcceleratorService::SubmitJob(const ComputeJob& job) const {
  const auto started = std::chrono::steady_clock::now();
  JobResult result;
  result.job_id = job.job_id.empty() ? "phase1-skeleton-job" : job.job_id;
  const auto store_result = [&]() {
    if (job.tenant_id.empty() || result.job_id.empty()) {
      return;
    }
    std::lock_guard<std::mutex> lock(registry_->mutex);
    registry_->jobs[JobKey(job.tenant_id, result.job_id)] = result;
  };

  result.error = ValidateJob(job);
  if (result.error.code != ErrorCode::kNone) {
    result.status = JobStatus::kFailed;
    result.metrics.duration_ms = ElapsedMillis(started);
    store_result();
    return result;
  }

  bool cpu_fallback_used = false;
  result.selected_accelerator = SelectAccelerator(job, &cpu_fallback_used);
  result.cpu_fallback_used = cpu_fallback_used;

  if (result.selected_accelerator == AcceleratorKind::kGpu &&
      !options_.gpu_available) {
    result.status = JobStatus::kFailed;
    result.error = {
        ErrorCode::kGpuUnavailable,
        "GPU was requested but no GPU is available and CPU fallback is disabled",
        true,
    };
    result.metrics.duration_ms = ElapsedMillis(started);
    store_result();
    return result;
  }

  // Phase 1 accepts the job and records skeleton metrics. Real execution will
  // be added behind this service boundary.
  result.status = JobStatus::kAccepted;
  result.metrics.duration_ms = ElapsedMillis(started);
  result.metrics.input_rows = job.options.max_batch_rows;
  result.metrics.output_rows = 0;
  result.metrics.batch_bytes = 0;
  result.metrics.throughput_rows_per_second = 0.0;
  result.metrics.gpu_memory_used_bytes = 0;
  store_result();
  return result;
}

JobResult ComputeAcceleratorService::GetJobStatus(
    const std::string& tenant_id, const std::string& job_id) const {
  JobResult result;
  result.job_id = job_id;
  if (tenant_id.empty() || job_id.empty()) {
    result.status = JobStatus::kFailed;
    result.error = {
        ErrorCode::kInvalidArgument,
        "tenant_id and job_id are required to query a job",
        false,
    };
    return result;
  }

  std::lock_guard<std::mutex> lock(registry_->mutex);
  const auto found = registry_->jobs.find(JobKey(tenant_id, job_id));
  if (found == registry_->jobs.end()) {
    result.status = JobStatus::kFailed;
    result.error = {
        ErrorCode::kInvalidArgument,
        "job was not found",
        false,
    };
    return result;
  }
  return found->second;
}

JobResult ComputeAcceleratorService::CancelJob(const std::string& tenant_id,
                                               const std::string& job_id,
                                               const std::string& reason) const {
  JobResult result;
  result.job_id = job_id;
  if (tenant_id.empty() || job_id.empty()) {
    result.status = JobStatus::kFailed;
    result.error = {
        ErrorCode::kInvalidArgument,
        "tenant_id and job_id are required to cancel a job",
        false,
    };
    return result;
  }

  std::lock_guard<std::mutex> lock(registry_->mutex);
  const auto found = registry_->jobs.find(JobKey(tenant_id, job_id));
  if (found == registry_->jobs.end()) {
    result.status = JobStatus::kFailed;
    result.error = {
        ErrorCode::kInvalidArgument,
        "job was not found",
        false,
    };
    return result;
  }

  if (!IsCancellable(found->second.status)) {
    result = found->second;
    result.error = {
        ErrorCode::kInvalidArgument,
        std::string("job cannot be cancelled from status ") +
            ToString(found->second.status),
        false,
    };
    return result;
  }

  found->second.status = JobStatus::kCancelled;
  found->second.error = {
      ErrorCode::kCancelled,
      reason.empty() ? "job cancellation accepted"
                     : "job cancellation accepted: " + reason,
      false,
  };
  return found->second;
}

bool ComputeAcceleratorService::IsReady() const { return true; }

const ServiceOptions& ComputeAcceleratorService::options() const {
  return options_;
}

ErrorDetail ComputeAcceleratorService::ValidateJob(const ComputeJob& job) const {
  if (job.tenant_id.empty()) {
    return {ErrorCode::kInvalidArgument, "tenant_id is required", false};
  }
  if (job.kind == JobKind::kUnspecified) {
    return {ErrorCode::kUnsupportedJobKind, "job kind is required", false};
  }
  if (job.input_uri.empty()) {
    return {ErrorCode::kInvalidArgument, "input_uri is required", false};
  }
  if (job.output_uri.empty()) {
    return {ErrorCode::kInvalidArgument, "output_uri is required", false};
  }
  if (job.options.timeout_ms == 0) {
    return {ErrorCode::kInvalidArgument, "timeout_ms must be greater than zero",
            false};
  }
  return {};
}

AcceleratorKind ComputeAcceleratorService::SelectAccelerator(
    const ComputeJob& job, bool* cpu_fallback_used) const {
  *cpu_fallback_used = false;
  if (job.options.accelerator == AcceleratorKind::kCpu) {
    return AcceleratorKind::kCpu;
  }
  if (IsGpuRequested(job.options.accelerator) && options_.gpu_available) {
    return AcceleratorKind::kGpu;
  }
  if (job.options.fallback_policy ==
          FallbackPolicy::kUseCpuIfGpuUnavailable &&
      options_.allow_cpu_fallback) {
    *cpu_fallback_used = IsGpuRequested(job.options.accelerator);
    return AcceleratorKind::kCpu;
  }
  return AcceleratorKind::kGpu;
}

const char* ToString(JobStatus status) {
  switch (status) {
    case JobStatus::kAccepted:
      return "accepted";
    case JobStatus::kQueued:
      return "queued";
    case JobStatus::kRunning:
      return "running";
    case JobStatus::kSucceeded:
      return "succeeded";
    case JobStatus::kFailed:
      return "failed";
    case JobStatus::kCancelled:
      return "cancelled";
    case JobStatus::kTimedOut:
      return "timed_out";
  }
  return "unknown";
}

const char* ToString(AcceleratorKind accelerator) {
  switch (accelerator) {
    case AcceleratorKind::kCpu:
      return "cpu";
    case AcceleratorKind::kGpu:
      return "gpu";
    case AcceleratorKind::kAuto:
      return "auto";
  }
  return "unknown";
}

const char* ToString(ErrorCode code) {
  switch (code) {
    case ErrorCode::kNone:
      return "none";
    case ErrorCode::kInvalidArgument:
      return "invalid_argument";
    case ErrorCode::kUnsupportedJobKind:
      return "unsupported_job_kind";
    case ErrorCode::kGpuUnavailable:
      return "gpu_unavailable";
    case ErrorCode::kTimeout:
      return "timeout";
    case ErrorCode::kCancelled:
      return "cancelled";
    case ErrorCode::kInternal:
      return "internal";
  }
  return "unknown";
}

}  // namespace compute
}  // namespace telemetry_fabric

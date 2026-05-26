#ifndef TELEMETRY_FABRIC_COMPUTE_ACCELERATOR_SERVICE_H_
#define TELEMETRY_FABRIC_COMPUTE_ACCELERATOR_SERVICE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace telemetry_fabric {
namespace compute {

enum class JobKind {
  kUnspecified,
  kBatchGraphCompute,
  kNHopFundFlow,
  kBatchRiskScore,
  kFeatureCompute,
  kModelInference,
};

enum class JobStatus {
  kAccepted,
  kQueued,
  kRunning,
  kSucceeded,
  kFailed,
  kCancelled,
  kTimedOut,
};

enum class AcceleratorKind {
  kCpu,
  kGpu,
  kAuto,
};

enum class FallbackPolicy {
  kFailIfGpuUnavailable,
  kUseCpuIfGpuUnavailable,
};

enum class ErrorCode {
  kNone,
  kInvalidArgument,
  kUnsupportedJobKind,
  kGpuUnavailable,
  kTimeout,
  kCancelled,
  kInternal,
};

struct ErrorDetail {
  ErrorCode code = ErrorCode::kNone;
  std::string message;
  bool retryable = false;
};

struct ExecutionOptions {
  AcceleratorKind accelerator = AcceleratorKind::kAuto;
  FallbackPolicy fallback_policy = FallbackPolicy::kUseCpuIfGpuUnavailable;
  std::uint64_t timeout_ms = 300000;
  std::uint64_t max_batch_rows = 0;
};

struct ComputeJob {
  std::string tenant_id;
  std::string job_id;
  JobKind kind = JobKind::kUnspecified;
  std::string input_uri;
  std::string output_uri;
  ExecutionOptions options;
  std::unordered_map<std::string, std::string> labels;
};

struct JobMetrics {
  std::uint64_t duration_ms = 0;
  std::uint64_t input_rows = 0;
  std::uint64_t output_rows = 0;
  std::uint64_t batch_bytes = 0;
  double throughput_rows_per_second = 0.0;
  std::uint64_t gpu_memory_used_bytes = 0;
};

struct JobResult {
  std::string job_id;
  JobStatus status = JobStatus::kAccepted;
  AcceleratorKind selected_accelerator = AcceleratorKind::kCpu;
  bool cpu_fallback_used = false;
  ErrorDetail error;
  JobMetrics metrics;
};

struct ServiceOptions {
  std::string listen_address = "0.0.0.0:50071";
  bool gpu_available = false;
  bool allow_cpu_fallback = true;
};

class ComputeAcceleratorService {
 public:
  explicit ComputeAcceleratorService(ServiceOptions options);

  JobResult SubmitJob(const ComputeJob& job) const;
  JobResult GetJobStatus(const std::string& tenant_id,
                         const std::string& job_id) const;
  JobResult CancelJob(const std::string& tenant_id, const std::string& job_id,
                      const std::string& reason) const;
  bool IsReady() const;
  const ServiceOptions& options() const;

 private:
  ErrorDetail ValidateJob(const ComputeJob& job) const;
  AcceleratorKind SelectAccelerator(const ComputeJob& job,
                                    bool* cpu_fallback_used) const;

  struct Registry;

  ServiceOptions options_;
  std::shared_ptr<Registry> registry_;
};

const char* ToString(JobStatus status);
const char* ToString(AcceleratorKind accelerator);
const char* ToString(ErrorCode code);

}  // namespace compute
}  // namespace telemetry_fabric

#endif  // TELEMETRY_FABRIC_COMPUTE_ACCELERATOR_SERVICE_H_

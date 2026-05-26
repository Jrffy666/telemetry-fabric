#include "accelerator_service.h"

#include <string>
#include <utility>

namespace telemetry_fabric {
namespace compute {
namespace grpc {

struct SubmitJobResult {
  std::string job_id;
  JobStatus status = JobStatus::kAccepted;
  AcceleratorKind selected_accelerator = AcceleratorKind::kCpu;
  bool cpu_fallback_used = false;
};

class ComputeAcceleratorGrpcServer {
 public:
  ComputeAcceleratorGrpcServer(std::string listen_address,
                               ComputeAcceleratorService service)
      : listen_address_(std::move(listen_address)),
        service_(std::move(service)) {}

  SubmitJobResult SubmitSkeletonJob(const ComputeJob& job) const {
    const auto result = service_.SubmitJob(job);
    return SubmitJobResult{
        result.job_id,
        result.status,
        result.selected_accelerator,
        result.cpu_fallback_used,
    };
  }

  JobResult GetSkeletonJobStatus(const std::string& tenant_id,
                                 const std::string& job_id) const {
    return service_.GetJobStatus(tenant_id, job_id);
  }

  JobResult CancelSkeletonJob(const std::string& tenant_id,
                              const std::string& job_id,
                              const std::string& reason) const {
    return service_.CancelJob(tenant_id, job_id, reason);
  }

  const std::string& listen_address() const { return listen_address_; }

 private:
  std::string listen_address_;
  ComputeAcceleratorService service_;
};

}  // namespace grpc
}  // namespace compute
}  // namespace telemetry_fabric

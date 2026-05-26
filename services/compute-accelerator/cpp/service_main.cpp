#include "accelerator_service.h"

#include <iostream>
#include <string>

namespace {

bool HasFlag(int argc, char** argv, const std::string& flag) {
  for (int index = 1; index < argc; ++index) {
    if (flag == argv[index]) {
      return true;
    }
  }
  return false;
}

std::string ValueAfter(int argc, char** argv, const std::string& flag,
                       const std::string& fallback) {
  for (int index = 1; index + 1 < argc; ++index) {
    if (flag == argv[index]) {
      return argv[index + 1];
    }
  }
  return fallback;
}

}  // namespace

int main(int argc, char** argv) {
  telemetry_fabric::compute::ServiceOptions options;
  options.listen_address =
      ValueAfter(argc, argv, "--listen", options.listen_address);
  options.gpu_available = HasFlag(argc, argv, "--gpu-available");
  options.allow_cpu_fallback = !HasFlag(argc, argv, "--disable-cpu-fallback");

  const telemetry_fabric::compute::ComputeAcceleratorService service(options);

  telemetry_fabric::compute::ComputeJob startup_probe;
  startup_probe.tenant_id = "local";
  startup_probe.job_id = "startup-probe";
  startup_probe.kind = telemetry_fabric::compute::JobKind::kFeatureCompute;
  startup_probe.input_uri = "arrow://startup-probe";
  startup_probe.output_uri = "file://startup-probe";
  startup_probe.options.accelerator =
      telemetry_fabric::compute::AcceleratorKind::kAuto;
  startup_probe.options.fallback_policy =
      telemetry_fabric::compute::FallbackPolicy::kUseCpuIfGpuUnavailable;
  startup_probe.options.timeout_ms = 1000;
  startup_probe.options.max_batch_rows = 1024;

  const auto result = service.SubmitJob(startup_probe);

  std::cout << "compute-accelerator skeleton\n";
  std::cout << "listen_address=" << service.options().listen_address << "\n";
  std::cout << "gpu_available="
            << (service.options().gpu_available ? "true" : "false") << "\n";
  std::cout << "allow_cpu_fallback="
            << (service.options().allow_cpu_fallback ? "true" : "false")
            << "\n";
  std::cout << "startup_probe_status="
            << telemetry_fabric::compute::ToString(result.status) << "\n";
  std::cout << "startup_probe_accelerator="
            << telemetry_fabric::compute::ToString(result.selected_accelerator)
            << "\n";
  std::cout << "cpu_fallback_used="
            << (result.cpu_fallback_used ? "true" : "false") << "\n";

  if (result.error.code != telemetry_fabric::compute::ErrorCode::kNone) {
    std::cout << "error_code="
              << telemetry_fabric::compute::ToString(result.error.code)
              << "\n";
    std::cout << "error_message=" << result.error.message << "\n";
    return 1;
  }

  std::cout << "phase1_execution=not_implemented\n";
  return 0;
}

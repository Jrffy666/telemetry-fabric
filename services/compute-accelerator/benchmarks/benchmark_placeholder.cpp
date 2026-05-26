#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

std::size_t ParseBatchSize(int argc, char** argv) {
  if (argc < 2) {
    return 1024;
  }

  const long long parsed = std::atoll(argv[1]);
  if (parsed <= 0) {
    return 1024;
  }
  return static_cast<std::size_t>(parsed);
}

float CpuBaselineSum(const std::vector<float>& values) {
  float total = 0.0F;
  for (float value : values) {
    total += value;
  }
  return total;
}

}  // namespace

int main(int argc, char** argv) {
  const std::size_t batch_size = ParseBatchSize(argc, argv);
  const std::vector<float> values(batch_size, 1.0F);
  const auto started = std::chrono::steady_clock::now();
  const float total = CpuBaselineSum(values);
  const auto finished = std::chrono::steady_clock::now();
  const auto elapsed_micros =
      std::chrono::duration_cast<std::chrono::microseconds>(finished - started)
          .count();
  const double elapsed_seconds =
      static_cast<double>(elapsed_micros == 0 ? 1 : elapsed_micros) / 1000000.0;
  const double throughput = static_cast<double>(batch_size) / elapsed_seconds;

  std::cout << "benchmark=placeholder_cpu_baseline\n";
  std::cout << "batch_size=" << batch_size << "\n";
  std::cout << "input_bytes=" << batch_size * sizeof(float) << "\n";
  std::cout << "cpu_baseline_sum=" << total << "\n";
  std::cout << "duration_micros=" << elapsed_micros << "\n";
  std::cout << "throughput_rows_per_second=" << throughput << "\n";
  std::cout << "gpu_transfer_h2d_micros=0\n";
  std::cout << "gpu_kernel_micros=0\n";
  std::cout << "gpu_transfer_d2h_micros=0\n";
  std::cout << "gpu_memory_used_bytes=0\n";
  std::cout << "gpu_implemented=false\n";
  return 0;
}

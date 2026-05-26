#include <cuda_runtime.h>

extern "C" __global__ void placeholder_batch_score_kernel(
    const float* input,
    float* output,
    int count) {
  const int index = blockIdx.x * blockDim.x + threadIdx.x;
  if (index >= count) {
    return;
  }

  output[index] = input[index];
}

extern "C" int launch_placeholder_batch_score(
    const float* device_input,
    float* device_output,
    int count,
    cudaStream_t stream) {
  if (count <= 0) {
    return 0;
  }

  constexpr int kThreadsPerBlock = 256;
  const int blocks = (count + kThreadsPerBlock - 1) / kThreadsPerBlock;
  placeholder_batch_score_kernel<<<blocks, kThreadsPerBlock, 0, stream>>>(
      device_input, device_output, count);
  return static_cast<int>(cudaGetLastError());
}

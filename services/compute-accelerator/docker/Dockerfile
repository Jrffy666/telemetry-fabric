# syntax=docker/dockerfile:1

FROM nvidia/cuda:12.4.1-devel-ubuntu22.04 AS build

WORKDIR /workspace/services/compute-accelerator

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Phase 1 builds the CPU-only service and benchmark skeletons. CUDA remains
# optional via CMake -DACCELERATOR_ENABLE_CUDA=ON in a future GPU build stage.
RUN cmake -S . -B build \
    && cmake --build build --target compute_accelerator accelerator_benchmark

FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

WORKDIR /app
COPY --from=build /workspace/services/compute-accelerator/build/compute_accelerator /app/compute_accelerator
COPY --from=build /workspace/services/compute-accelerator/build/accelerator_benchmark /app/accelerator_benchmark
COPY --from=build /workspace/services/compute-accelerator/proto /app/proto

EXPOSE 50071

CMD ["/app/compute_accelerator", "--listen", "0.0.0.0:50071"]

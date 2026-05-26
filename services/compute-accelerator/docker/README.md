# Docker

This directory contains container skeletons for the compute accelerator service.

Build from the service root:

```sh
cd services/compute-accelerator
docker build -f docker/Dockerfile -t telemetry-compute-accelerator:local .
```

The Dockerfile builds the CPU-only C++ skeleton by default. It uses an NVIDIA
CUDA base image so a later GPU target can be added without changing the service
boundary, but the phase 1 binary does not require a GPU to start.

Runtime GPU access requires a host with NVIDIA drivers and the NVIDIA Container
Toolkit, then a runtime flag such as:

```sh
docker run --gpus all telemetry-compute-accelerator:local
```

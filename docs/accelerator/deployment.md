# Accelerator Deployment

The accelerator is independently deployable and optional. It is not part of the
Rust workspace build.

## Docker

Build from the service root:

```sh
cd services/compute-accelerator
docker build -f docker/Dockerfile -t telemetry-compute-accelerator:local .
```

Run CPU skeleton:

```sh
docker run --rm telemetry-compute-accelerator:local
```

Run with GPU access on hosts with NVIDIA drivers and NVIDIA Container Toolkit:

```sh
docker run --rm --gpus all telemetry-compute-accelerator:local
```

## Kubernetes GPU Scheduling

GPU deployments should be isolated from default control-plane and crawler nodes.

Example scheduling shape:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: compute-accelerator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: compute-accelerator
  template:
    metadata:
      labels:
        app: compute-accelerator
    spec:
      nodeSelector:
        accelerator: nvidia
      tolerations:
        - key: "nvidia.com/gpu"
          operator: "Exists"
          effect: "NoSchedule"
      containers:
        - name: compute-accelerator
          image: telemetry-compute-accelerator:local
          resources:
            limits:
              nvidia.com/gpu: "1"
          ports:
            - containerPort: 50071
```

Production deployments should add requests/limits for CPU and memory, readiness
checks, persistent output credentials, and node pool isolation.

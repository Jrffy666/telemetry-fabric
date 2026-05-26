# Accelerator Service

The compute accelerator is an optional C++/CUDA batch service under
`services/compute-accelerator`. It is designed to be replaceable: the Rust data
plane, Go crawler, and Python analytics services communicate through service
contracts and batch storage, not through native FFI.

Use it for analytics and batch compute workloads such as graph computation,
N-hop fund flow search, batch risk scoring, feature computation, and model
inference.

Do not use it for crawler ingestion, blockchain RPC polling, control-plane
configuration, or low-latency routing.

See:

- `interface.md` for gRPC and batch job contract notes
- `cuda-boundary.md` for GPU memory transfer and batch sizing
- `deployment.md` for Docker and Kubernetes GPU scheduling
- `observability.md` for metrics and operational signals

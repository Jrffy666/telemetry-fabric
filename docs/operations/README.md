# Operations

This directory documents production deployment and local operations for
Telemetry Fabric on Kubernetes.

Core runbooks:

- [Kubernetes production deployment](kubernetes-production.md)
- [Rollout and rollback](rollout.md)
- [Local stack](local-stack.md)

The Helm chart in `deploy/helm/telemetry-fabric` is the primary deployment
artifact. The raw manifests in `deploy/k8s` remain lightweight examples.

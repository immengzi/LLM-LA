# infra/patches/ — container image patches

Source-level fixes baked into the vLLM / LMCache-Ascend image
(`lmcache-ascend:hccl-p2p`) before it is pushed to the registry — most notably
the negative Prometheus-counter crash guard for LMCache P2P + host-staging.

**Full explanation (why each patch exists, how to apply, how to verify a running
pod) lives in the docs:**

> [`docs/operations/image-patches.md`](../../docs/operations/image-patches.md)

Quick start (from the repo root):

```bash
docker build -f infra/patches/Dockerfile.hccl-p2p-metrics-fix \
  -t reg.local:32000/lmcache-ascend:hccl-p2p-metrics-fix .
docker push reg.local:32000/lmcache-ascend:hccl-p2p-metrics-fix
```

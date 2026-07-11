# `old/autoscaling/`

KEDA autoscaling validation on a dense Qwen3-8B, as an isolated release.
Requires `make monitoring && make keda` (see `docs/operations/autoscaling.md`).
Re-enable only when specifically testing autoscaling; not a standing deployment.

| Config | Purpose |
| --- | --- |
| `prod-bz-autoscaling-qwen.yaml` | BZ KEDA autoscaling validation (dense Qwen3-8B). |
| `prod-yz-shadow-autoscaling-qwen.yaml` | YZ shadow-pool equivalent. |

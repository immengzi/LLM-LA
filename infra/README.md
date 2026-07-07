# infra/ — cluster preparation

Executable automation that turns a fresh Kubernetes cluster into one ready to run
LA-Boom. **The documentation lives in the docs tree:**

> [`docs/operations/cluster-prep-automation.md`](../docs/operations/cluster-prep-automation.md)
> — what each layer does, prerequisites, and `make` usage.
> ([cluster-setup.md](../docs/operations/cluster-setup.md) is the manual "why".)

## Layout

| Path | Purpose |
|------|---------|
| `ansible/` | The playbook (`site.yml`), roles, and `group_vars/all.yml` config. |
| `Makefile` | `make prep` / `check` / `verify` and per-layer targets. |
| `preflight.sh` | Read-only cluster preflight checks (`make verify`). |
| `grafana-dashboards/` | Dashboards deployed by the `monitoring` layer. |
| `patches/` | Source patches baked into the vLLM/LMCache-Ascend image — see [image-patches.md](../docs/operations/image-patches.md). |

Quick start:

```bash
cd infra/ansible && cp inventory.example.ini inventory.ini   # edit hosts
cd .. && make check && make prep
```

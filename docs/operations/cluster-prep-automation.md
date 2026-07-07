# Cluster preparation (automation)

Automated, idempotent equivalent of the manual checklist in
[cluster-setup.md](cluster-setup.md). It takes a cluster with Kubernetes already
installed and makes it ready to run LA-Boom.

> This is the **how** doc for the executable playbook in
> [`infra/`](../../infra/). When this and [cluster-setup.md](cluster-setup.md)
> disagree, treat the playbook as the source of truth for **how** and
> cluster-setup.md as the source of truth for **why**.

## What it does

| Layer | Role | Steps (cluster-setup.md) |
|-------|------|--------------------------|
| Per-node OS config | `common` | §4 `/etc/hosts`, §5 containerd `certs.d`, §6 `NO_PROXY` drop-in, §3 kube-proxy iptables deps |
| NFS server | `nfs_server` | §7 `/etc/exports` + `exportfs -ra` |
| NPU verify | `npu` | §1 assert Ascend driver paths, `npu-smi` probe |
| Registry | `registry` | §8 deploy in-cluster registry + catalog check |
| Images | `images` | §9 build/push service images + mirror external images |
| Cluster bootstrap | `k8s_bootstrap` | §2 labels, §14 untaint, §1 device plugin, §3 kube-proxy restart, §12 LWS, §11 PV/PVC |
| Monitoring | `monitoring` | Deploy kube-prometheus-stack (Prometheus + Grafana + Alertmanager) via Helm, expose Grafana on NodePort |
| Autoscaling | `keda` | Install KEDA + LeaderWorkerSet scale RBAC. **Opt-in** (tagged `never`); only for `autoscaling.enabled=true` deploys. See [autoscaling.md](autoscaling.md) |

**Not automated** (hardware / vendor): NPU driver/CANN/firmware install and
physical RoCE cabling + `/etc/hccn.conf` IPs. The playbook *verifies* these and
fails fast if missing.

The `images` layer builds/pushes the service images and mirrors external ones.
The vLLM / LMCache-Ascend image additionally needs source patches baked in
before push — see [image-patches.md](image-patches.md).

## Prerequisites

- Ansible on your workstation (`pipx install ansible` or `pip install ansible`).
- SSH access (key-based) as a sudo-capable user to every node.
- `kubectl` + `helm` configured on the `control_plane` host.
- Docker + a repo checkout on the `build` host (for `images`).

## Usage

```bash
cd infra/ansible
cp inventory.example.ini inventory.ini   # edit hosts + node names
$EDITOR group_vars/all.yml               # edit IPs, subnets, labels, images

cd ..
make ping        # SSH connectivity check
make check       # dry-run the whole thing (--check --diff)
make prep        # run the full preparation

make verify      # read-only preflight checks (cluster-setup §13-14)
```

Run a single layer with its tag:

```bash
make node        # per-node OS config only
make nfs         # NFS exports only
make registry    # deploy the registry only
make images      # build + push images only
make k8s         # cluster bootstrap only
make monitoring  # deploy Prometheus + Grafana
make keda        # install KEDA + LWS scale RBAC (autoscaling only; not run by `make prep`)
```

## Configuration

Everything tunable lives in
[`infra/ansible/group_vars/all.yml`](../../infra/ansible/group_vars/all.yml):
registry/NFS IPs, proxy + `NO_PROXY` entries, NFS export subnets, per-node
scheduling labels (`avoid` / `roce-pair` / `vllm-pool`), Ascend paths, and the
image build/mirror lists. The defaults mirror the documented reference cluster.

## Notes

- **Fixes the `NO_PROXY` char-split bug**: the `common` role renders a correct
  comma-joined `NO_PROXY` for containerd, replacing the broken list-style value.
- **No containerd restart for registry config**: `certs.d` is read dynamically;
  the proxy drop-in change is the only thing that triggers a restart.
- **PV is `Retain`**: re-running `make k8s` is safe and won't disturb model data.

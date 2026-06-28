# Cluster setup

This document covers everything required **after** a bare Kubernetes cluster
is installed to make it ready for LA-Boom deployments. It is the canonical
post-install checklist; for a deeper infrastructure reference (NFS internals,
PV lifecycle, registry internals, diagnostics), see
[multi-node-setup-guide.md](multi-node-setup-guide.md).

> **Automated equivalent:** the steps below are scripted as an idempotent Ansible
> playbook under [`infra/`](../../infra/README.md). Use `make prep` to run the
> whole checklist and `make verify` for the read-only preflight checks (§13-14).
> This document remains the source of truth for *why* each step is needed.

---

## Contents

- [1. Ascend NPU Driver + Device Plugin](#1-ascend-npu-driver--device-plugin)
- [2. Label NPU Worker Nodes](#2-label-npu-worker-nodes)
- [3. Remove Control-Plane Taint](#3-remove-control-plane-taint)
- [4. DNS — /etc/hosts on Every Node](#4-dns--etchosts-on-every-node)
- [5. Containerd — Insecure Registry Config](#5-containerd--insecure-registry-config)
- [6. Proxy / NO_PROXY](#6-proxy--no_proxy)
- [7. NFS Server — Exports](#7-nfs-server--exports)
- [8. Deploy the Container Registry](#8-deploy-the-container-registry)
- [9. Build and Push All Container Images](#9-build-and-push-all-container-images)
- [10. Fix kube-proxy iptables](#10-fix-kube-proxy-iptables)
- [11. Create the vllm Namespace + One-Time PV/PVC](#11-create-the-vllm-namespace--one-time-pvpvc)
- [12. Install the LWS Operator](#12-install-the-lws-operator)
- [13. Verify NPU RoCE Network](#13-verify-npu-roce-network)
- [14. RoCE Node-Pair Labeling](#14-roce-node-pair-labeling)

---

## 1. Ascend NPU Driver + Device Plugin

**Scope**: every worker node with NPUs.

The Helm chart requests `huawei.com/Ascend910` as a Kubernetes extended
resource (`40-vllm-unified.yaml` lines 276/279) and mounts host paths
from the Ascend driver stack (`_helpers.tpl` lines 80–101):

```
/usr/local/Ascend/driver/lib64
/usr/local/Ascend/driver/version.info
/usr/local/dcmi
/usr/local/bin/npu-smi
/etc/ascend_install.info
```

Install the Ascend NPU driver, firmware, and CANN toolkit on every worker
node. Deploy the Huawei Ascend device plugin DaemonSet so Kubernetes can
advertise `huawei.com/Ascend910` resources.

Verify:

```bash
# Host-level
ls /usr/local/Ascend/driver/lib64/
npu-smi info

# K8s-level
kubectl describe node <worker> | grep Ascend
```

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §7.

---

## 2. Label NPU Worker Nodes

**Scope**: every worker node with NPUs.

NPU nodes are auto-labeled `accelerator=huawei-Ascend910` by the Ascend device plugin (§1) — you do not set this manually. The vLLM stack does **not** schedule by an `accelerator` label; instead it uses:

- `avoid` (NotIn) — exclude nodes from a deployment (chart value `vllm.avoidLabelValue`, default `vllm`; e.g. `vllm-shadow` for a shadow stack).
- `roce-pair` — optional label for RoCE node-pair pinning of data-parallel groups (`pairTopologyKey`).

```bash
# Exclude a node from the primary vLLM deployment:
kubectl label node <node> avoid=vllm
# Pin a DP group to a node pair (RoCE):
kubectl label node <nodeA> <nodeB> roce-pair=pair-a
```

See [node-pair pinning](../deployment/multi-model.md#node-pair-pinning), and [shadow deployments](../configuration/helm-values.md#shadow-deployments-running-prod--shadow-side-by-side) for running prod + shadow stacks on disjoint node sets.

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §9.

---

## 3. Remove Control-Plane Taint

**Scope**: master node (if it should run workloads like router, Redis, or
the registry).

```bash
kubectl taint nodes <master> node-role.kubernetes.io/control-plane-
```

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §1.

---

## 4. DNS — /etc/hosts on Every Node

**Scope**: every node in the cluster.

Two hostnames must resolve correctly. Getting this wrong is a **critical
failure source** — NFS resolving to `127.0.0.1` inside pods causes model
loads to hang silently.

Add to `/etc/hosts` on all nodes:

```
<master-ip>      reg.local
<nfs-server-ip>  nfs.local
```

Verify:

```bash
getent hosts reg.local
getent hosts nfs.local
```

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §2.

---

## 5. Containerd — Insecure Registry Config

**Scope**: every node in the cluster.

Containerd uses the **folder-based** registry config (`config_path = "/etc/containerd/certs.d"`) with a per-registry `hosts.toml` for the plain-HTTP registry. `certs.d` changes are picked up dynamically — no containerd restart needed.

See [registry.md](registry.md) → "Containerd insecure registry" for the exact `hosts.toml` and verification commands; that is the single source of truth. (Do **not** add inline `registry.mirrors`/`registry.configs` blocks to `config.toml` — when `config_path` is set, containerd ignores them.)

---

## 6. Proxy / NO_PROXY

**Scope**: every node in the cluster.

The cluster runs behind a Cntlm HTTP proxy; containerd, kubelet, and Docker must bypass it for internal traffic (`reg.local`, `nfs.local`, node IPs, `.cluster.local`, pod/service CIDRs). Missing entries cause `ImagePullBackOff`, NFS hangs, and registry timeouts.

- Containerd `NO_PROXY`: [registry.md](registry.md) → "NO_PROXY for containerd".
- Docker daemon proxy/TLS: [docker-proxy-fix.md](docker-proxy-fix.md).
- Full list: [multi-node-setup-guide.md](multi-node-setup-guide.md) §11.

---

## 7. NFS Server — Exports

**Scope**: NFS server host (not a cluster node).

Export the NFSv4 pseudo-root (`/mnt/nvme1`, `fsid=0`), the model directory (read-only), and the registry-data directory (read-write) to the cluster subnets, then reload with `sudo exportfs -ra`.

**Critical rule**: PV paths use the NFSv4 namespace path (e.g. `/home/models/qwen3-8b`), **not** the absolute host path (`/mnt/nvme1/...`).

See [multi-node-setup-guide.md](multi-node-setup-guide.md) §3 for the exact `/etc/exports` lines, the subnet list, and the NFSv4 path rule.

---

## 8. Deploy the Container Registry

**Scope**: one-time cluster setup.

Deploy `registry:2` as a Kubernetes Deployment + NodePort Service in the
`registry` namespace. The registry image needs to be pre-loaded into
containerd on the target node (chicken-and-egg problem):

```bash
# On an internet-connected machine:
docker pull --platform linux/arm64 registry:2
docker save registry:2 -o /tmp/registry2.tar

# On the target node:
ctr -n k8s.io image import /tmp/registry2.tar
```

Then create the Deployment (NodePort 5000:32000, NFS-backed volume at
`/mnt/nvme1/registry-data`).

Verify:

```bash
curl --noproxy '*' http://reg.local:32000/v2/_catalog
```

**Reference**: [registry.md](registry.md),
[multi-node-setup-guide.md](multi-node-setup-guide.md) §10.

---

## 9. Build and Push All Container Images

**Scope**: build machine with Docker + access to `reg.local:32000`.

All build scripts default to `REGISTRY=reg.local:32000`. The following
images must be present in the registry before deploying:

| Image | Build source |
|-------|-------------|
| `kv-router` | `services/router_service/` — `docker build && docker push` |
| `kv-sidecar` | `services/sidecar/` — `docker build && docker push` |
| `kv-router-go`, `kv-sidecar-go` | `services/go/build.sh` |
| `boom-gateway` | `BooMGateway-main/BooMGateway-main/build.sh` |
| `vllm-cpu-hash` | `services/prefix_hash/` |
| `ascend/vllm-ascend` | Pull from `quay.io/ascend/vllm-ascend`, retag, push |
| `redis:7-alpine` | Pull from Docker Hub, retag, push |
| `litellm:main-stable` | `services/litellm-image.sh` |

**Reference**: [registry.md](registry.md) "Images in the registry",
individual `build.sh` scripts.

---

## 10. Fix kube-proxy iptables

**Scope**: every node in the cluster.

kube-proxy needs `libxtables.so.12` to program ClusterIP iptables rules.
Without it, ClusterIP traffic from pods on that node times out (breaks
LiteLLM, BooM, and inter-service communication).

```bash
# Check all nodes
for pod in $(kubectl get pods -n kube-system -l k8s-app=kube-proxy -o name); do
  echo "=== $pod ==="
  kubectl logs -n kube-system $pod --tail=5 | grep -i "libxtables\|iptables\|error" || echo "OK"
done

# Fix (EulerOS)
ssh <node-ip> "sudo yum install -y iptables iptables-libs"

# Restart kube-proxy cluster-wide
kubectl rollout restart daemonset/kube-proxy -n kube-system
kubectl rollout status daemonset/kube-proxy -n kube-system --timeout=120s
```

**Reference**: [quickstart.md](../getting-started/quickstart.md) §8 "One-time node setup".

---

## 11. Create the vllm Namespace + One-Time PV/PVC

**Scope**: one-time cluster setup.

The Helm chart deploys into namespace `vllm`. The NFS-backed PV/PVC for
model weights is created once and survives all subsequent Helm upgrades
(`helm.sh/resource-policy: keep`).

```bash
helm upgrade --install vllm ./vllm-kv-stack -n vllm --create-namespace \
  --set modelVolume.create=true \
  --set modelVolume.modelSubPath=placeholder
```

After this, `modelVolume.create` stays `false` forever — the PV/PVC persist
across installs.

Verify:

```bash
kubectl get pv
kubectl get pvc -n vllm
```

**Reference**: [quickstart.md](../getting-started/quickstart.md) §0,
[multi-node-setup-guide.md](multi-node-setup-guide.md) §4–§5,
`vllm-kv-stack/values.yaml` lines 99–113.

---

## 12. Install the LWS Operator

**Scope**: one-time cluster setup.

Required for multi-node data parallel deployments (MoE models like GLM-5
with DP=2).

```bash
# Air-gapped: push lws/lws:v0.8.0 to reg.local:32000 first, then:
kubectl apply --server-side -f ../deployment/lws-manifests.yaml

# Verify CRD:
kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io

# Verify controller:
kubectl get pods -n lws-system
```

**Reference**: [data_parallel_lws.md](../deployment/data-parallel-lws.md)
"Prerequisites" §1.

---

## 13. Verify NPU RoCE Network

**Scope**: every worker node with NPUs.

Confirm all 8 NPU ports show link `UP` and `net_health` `success` on each node (`hccn_tool -i <0-7> -link -g` and `-net_health -g`), and that `/etc/hccn.conf` exists. If any port is `DOWN` or shows `optical info: not present`, fix the physical layer (transceivers, fibre cabling, RoCE switch) first.

**Reference**: [data-parallel-lws.md](../deployment/data-parallel-lws.md) "Prerequisites" §2 (full commands), [glm5-dp-docker.md](../deployment/docker-reference/glm5-dp-docker.md).

---

## 14. RoCE Node-Pair Labeling

**Scope**: worker nodes cabled in RoCE pairs.

Label nodes so LWS groups are scheduled onto physically cabled pairs:

```bash
# Prod nodes: node1+node2 are one RoCE pair, node7+node8 are the other.
kubectl label node node1 node2 roce-pair=pair-a
kubectl label node node7 node8 roce-pair=pair-b
```

Verify:

```bash
kubectl get nodes -L roce-pair
# node1   roce-pair=pair-a
# node2   roce-pair=pair-a
# node7   roce-pair=pair-b
# node8   roce-pair=pair-b
```

**Reference**: [multi_model_router.md](../deployment/multi-model.md) "Node-pair
pinning", [data_parallel_lws.md](../deployment/data-parallel-lws.md).

---

## 15. Install KEDA (optional — autoscaling)

**Scope**: one-time cluster setup. Only needed if you deploy with
`autoscaling.enabled=true`.

KEDA drives the chart's per-model `ScaledObject`s (dense, multi-model, and
data-parallel). Install it plus the LeaderWorkerSet scale RBAC:

```bash
cd infra && make keda            # helm install KEDA into the `keda` namespace + LWS RBAC

# Verify:
kubectl get crd scaledobjects.keda.sh
kubectl -n keda rollout status deploy/keda-operator
```

`make prep` does **not** install KEDA (the play is tagged `never`), so existing
non-autoscaling clusters are unaffected.

**Reference**: [autoscaling.md](autoscaling.md).
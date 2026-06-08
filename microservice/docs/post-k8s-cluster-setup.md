<img width="1920" height="1280" alt="image" src="https://github.com/user-attachments/assets/35c8d559-2bfa-49f1-ab78-2973b38d2b24" /># Post-Kubernetes-Installation Setup Guide

This document covers everything required **after** a bare Kubernetes cluster
is installed to make it ready for LLM-LB deployments.

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

The vLLM Deployments use `nodeSelector: { accelerator: ascend }` to target
NPU nodes.

```bash
kubectl label nodes <worker-node> accelerator=ascend
```

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

Add to `/etc/containerd/config.toml` so containerd can pull from the
plain-HTTP registry:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."reg.local:32000"]
  endpoint = ["http://reg.local:32000"]

[plugins."io.containerd.grpc.v1.cri".registry.configs."reg.local:32000".tls]
  insecure_skip_verify = true
```

Then restart containerd:

```bash
systemctl restart containerd
```

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §10,
[registry.md](registry.md) "Node configuration".

---

## 6. Proxy / NO_PROXY

**Scope**: every node in the cluster.

The cluster runs behind a Cntlm HTTP proxy. Containerd, kubelet, and Docker
must bypass the proxy for internal traffic. `NO_PROXY` must include:

```
localhost, 127.0.0.1, ::1, reg.local, nfs.local,
<nfs-server-ip>, <master-ip>, .cluster.local,
10.233.0.0/16 (pod CIDR), 10.233.64.0/18 (service CIDR)
```

Set in `/etc/systemd/system/containerd.service.d/http-proxy.conf` (and
equivalent for Docker if present).

Missing entries cause `ImagePullBackOff`, NFS hangs, and registry
HTTPS/HTTP mismatch errors.

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §11,
[registry.md](registry.md) "NO_PROXY".

---

## 7. NFS Server — Exports

**Scope**: NFS server host (not a cluster node).

Export the model directory and registry storage directory to all cluster
subnets:

```
/mnt/nvme1              microservice. (fsid=0, NFSv4 pseudo-root)
/mnt/nvme1/registry-data    microservice. (rw, for registry image layers)
/mnt/nvme1/saeid/models/microservice. microservice. (ro, for model weights)
```

Reload after editing:

```bash
sudo exportfs -ra
sudo exportfs -v
```

**Critical rule**: PV paths use the NFSv4 namespace path
(e.g. `/saeid/models/qwen3-8b`), **not** the absolute host path
(`/mnt/nvme1/saeid/models/qwen3-8b`).

Test from every cluster node:

```bash
mount -t nfs -o nfsvers=4.1 <nfs-server>:/ /tmp/nfsroot
ls /tmp/nfsroot/saeid/models/ | head
umount /tmp/nfsroot
```

**Reference**: [multi-node-setup-guide.md](multi-node-setup-guide.md) §3.

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
| `litellm:main-stable` | `services/vllm-image-litellm.sh` |

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

**Reference**: [quickstart.md](quickstart.md) §8 "One-time node setup".

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

**Reference**: [quickstart.md](quickstart.md) §0,
[multi-node-setup-guide.md](multi-node-setup-guide.md) §4–§5,
`vllm-kv-stack/values.yaml` lines 99–113.

---

## 12. Install the LWS Operator

**Scope**: one-time cluster setup.

Required for multi-node data parallel deployments (MoE models like GLM-5
with DP=2).

```bash
# Air-gapped: push lws/lws:v0.8.0 to reg.local:32000 first, then:
kubectl apply --server-side -f lws-manifests.yaml

# Verify CRD:
kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io

# Verify controller:
kubectl get pods -n lws-system
```

**Reference**: [data_parallel_lws.md](data_parallel_lws.md)
"Prerequisites" §1.

---

## 13. Verify NPU RoCE Network

**Scope**: every worker node with NPUs.

All NPU nodes must have their RoCE network physically connected and
healthy. Run on each node:

```bash
# All 8 ports must show link status: UP
for i in {0microservice7}; do hccn_tool -i $i -link -g; done

# All must show success
for i in {0microservice7}; do hccn_tool -i $i -net_health -g; done

# Verify IP config exists
cat /etc/hccn.conf
```

If any port shows `DOWN` or `optical info: not present`, the physical layer
(transceivers, fibre cabling, RoCE switch) needs to be fixed first.

**Reference**: [data_parallel_lws.md](data_parallel_lws.md)
"Prerequisites" §2,
[glm5_dp_docker_deployment.md](glm5_dp_docker_deployment.md).

---

## 14. RoCE Node-Pair Labeling

**Scope**: worker nodes cabled in RoCE pairs.

Label nodes so LWS groups are scheduled onto physically cabled pairs:

```bash
kubectl label node node3 node4 roce-pair=pair-a
kubectl label node node5 node6 roce-pair=pair-b
```

Verify:

```bash
kubectl get nodes -L roce-pair
```

**Reference**: [multi_model_router.md](multi_model_router.md) "Node-pair
pinning", [data_parallel_lws.md](data_parallel_lws.md).

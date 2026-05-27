# vLLM Multi-Node Infrastructure Setup Guide
(Ascend NPU, Shared NFSv4 Models, Kubernetes NodePort Registry, Production-Grade Reference)

This document describes the complete infrastructure configuration used in the current multi-node environment.
It excludes Kubernetes installation and focuses on:

- Multi-node considerations
- NFSv4-backed model storage
- PersistentVolume (PV) and PersistentVolumeClaim (PVC)
- Helm PV lifecycle behavior
- Ascend NPU runtime configuration
- Kubernetes-based private container registry
- Split-horizon DNS behavior
- Proxy + NO_PROXY production requirements
- Startup probe tuning for large models
- Full diagnostic and forensic procedures
- Operational checklist before experiments

---

## 1. Cluster Topology (Current Setup)

- 1 control-plane node: `node1` (`10.50.156.65`)
- Worker nodes with Ascend NPUs:
  - `7.216.57.161`
  - `10.50.156.65`
  - `10.50.156.106`
- Dedicated NFS server: `7.242.102.243` (head node, NOT part of the cluster)
- Registry running inside Kubernetes (NodePort 32000)
- vLLM namespace used for deployments
- Container runtime: containerd (EulerOS 2.0, aarch64)

Important:
- Master may run workloads only if taint removed.
- All NPU workloads run on labeled worker nodes.
- Registry is NOT running as a standalone Docker container.
- `reg.local` must resolve to a Kubernetes node IP.

Verify nodes:

```bash
kubectl get nodes -o wide
```

If master should be schedulable:

```bash
kubectl taint nodes node1 node-role.kubernetes.io/control-plane-
```

---

## 2. Split-Horizon DNS for Registry and NFS (CRITICAL FAILURE SOURCE)

Historically observed fatal issue: NFS resolving to `127.0.0.1` inside pods.

Symptom — `/proc/mounts` shows:

```
nfs.local:/ /model ... addr=127.0.0.1
```

This causes:
- vLLM stuck at "Loading safetensors shards 0/5"
- `ls /model` hanging
- `dd` test hanging

### Current Correct DNS Rules

`nfs.local` must resolve to:
```
7.242.102.243
```

`reg.local` must resolve to:
```
10.50.156.65
```

Example `/etc/hosts` on all nodes:

```
10.50.156.65  reg.local
7.242.102.243 nfs.local
```

Verify on every node:

```bash
getent hosts reg.local
getent hosts nfs.local
```

Verify inside pod:

```bash
kubectl -n vllm exec -it <pod> -- grep /model /proc/mounts
```

Must NOT show:

```
addr=127.0.0.1
```

---

## 3. Shared Model Storage via NFSv4 (Structural Change)

NFSv4 root export (`fsid=0`) is on:

```
/mnt/nvme1
```

### /etc/exports on NFS server (`7.242.102.243`)

```
# NFSv4 pseudo-root MUST be RW if you want any subpath to be writable
/mnt/nvme1 127.0.0.1(ro,sync,no_subtree_check,fsid=0) 10.175.112.0/22(rw,sync,no_subtree_check,fsid=0) 7.216.57.0/24(rw,sync,no_subtree_check,fsid=0) 10.50.156.0/24(rw,sync,no_subtree_check,fsid=0)
# Keep model directory RO explicitly
/mnt/nvme1/saeid/models/qwen3-8b 127.0.0.1(ro,sync,no_subtree_check) 10.175.112.0/22(ro,sync,no_subtree_check) 7.216.57.0/24(ro,sync,no_subtree_check) 10.50.156.0/24(ro,sync,no_subtree_check)
# Registry directory RW
/mnt/nvme1/registry-data 10.175.112.0/22(rw,sync,no_subtree_check) 7.216.57.0/24(rw,sync,no_subtree_check) 10.50.156.0/24(rw,sync,no_subtree_check)
```

After editing, reload:

```bash
sudo exportfs -ra
sudo exportfs -v
```

### Important NFSv4 behavior

Because `fsid=0` is on `/mnt/nvme1`, clients mount:

```bash
7.242.102.243:/
```

Inside that namespace:

```
/saeid/models/qwen3-8b
/registry-data
```

**CRITICAL RULE:** Do NOT include `/mnt/nvme1` in PV path. Use NFSv4 namespace path.

Correct model PV path:
```
/saeid/models/qwen3-8b
```

Wrong (causes breakage after fsid change):
```
/mnt/nvme1/saeid/models/qwen3-8b
```

Test from every node:

```bash
mkdir /tmp/nfsroot
mount -t nfs -o nfsvers=4.1 7.242.102.243:/ /tmp/nfsroot
ls /tmp/nfsroot/saeid/models/qwen3-8b | head
umount /tmp/nfsroot
```

Test shard read:

```bash
mount -t nfs -o nfsvers=4.1 7.242.102.243:/ /tmp/nfsroot
dd if=/tmp/nfsroot/saeid/models/qwen3-8b/model-00001-of-00005.safetensors of=/dev/null bs=8M count=32
umount /tmp/nfsroot
```

If `dd` hangs → NFS bottleneck or DNS issue.

---

## 4. PersistentVolume (PV) Configuration

Key rule: Mount only the specific model directory. Never mount `/`.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: qwen-local-pv
  annotations:
    "helm.sh/resource-policy": keep
spec:
  capacity:
    storage: 20Gi
  accessModes:
    - ReadOnlyMany
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  mountOptions:
    - ro
    - nfsvers=4.1
  nfs:
    server: nfs.local
    path: /saeid/models/qwen3-8b
```

PVC:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: qwen-local-pvc
  namespace: vllm
  annotations:
    "helm.sh/resource-policy": keep
spec:
  accessModes:
    - ReadOnlyMany
  resources:
    requests:
      storage: 20Gi
  storageClassName: ""
  volumeName: qwen-local-pv
```

---

## 5. PVC Stability Rule (Helm Behavior)

If Helm conditionally creates PVC with `"helm.sh/resource-policy": keep`:

- Helm uninstall DOES NOT delete PV/PVC
- Helm upgrade DOES NOT modify immutable fields
- Changing `nfsPath` in `values.yaml` has NO effect unless PV is manually deleted

Correct recreation procedure:

```bash
helm uninstall vllm -n vllm
kubectl -n vllm delete pvc qwen-local-pvc
kubectl delete pv qwen-local-pv
helm upgrade --install vllm . -n vllm -f values.yaml
```

PVC must be Bound before deploying pods.

Verify:

```bash
kubectl get pv qwen-local-pv
kubectl -n vllm get pvc qwen-local-pvc
```

---

## 6. Mounting PVC in vLLM Deployment

```yaml
volumeMounts:
  - name: model-volume
    mountPath: /model
    readOnly: true

volumes:
  - name: model-volume
    persistentVolumeClaim:
      claimName: qwen-local-pvc
```

Verify inside pod:

```bash
kubectl -n vllm exec -it <pod> -- ls /model
kubectl -n vllm exec -it <pod> -- grep /model /proc/mounts
```

---

## 7. Ascend NPU Configuration

Common runtime failures observed:

```
aclInit error 507000
Runtime boot failed
Resources are busy
```

Causes:
- Another process holding NPU
- Driver / toolkit mismatch
- Missing `libmpi_dvpp_adapter.so`
- Wrong `LD_LIBRARY_PATH`

Inside container must mount:

```
/usr/local/Ascend/driver/lib64
/etc/ascend_install.info
/usr/local/dcmi
/usr/local/bin/npu-smi
```

Verify inside container:

```bash
npu-smi info
```

If "device is used":
- Check other pods using same NPU
- Ensure resource limits request `huawei.com/Ascend: 1`

Verify paths exist on every node:

```bash
ls /usr/local/dcmi
ls /usr/local/bin/npu-smi
ls /usr/local/Ascend/driver/lib64/
ls /usr/local/Ascend/driver/version.info
ls /etc/ascend_install.info
```

---

## 8. Startup Probe for Large Models (CRITICAL)

Large safetensors models over NFS may take 5–20+ minutes. If liveness probe starts too early, pod restarts during shard loading.

Solution:

```yaml
startupProbe:
  httpGet:
    path: /health
    port: 8200
  periodSeconds: 10
  failureThreshold: 180
```

This gives ~30 minutes before Kubernetes kills container. Never rely only on `livenessProbe` for large model loads.

---

## 9. Multi-Node Scheduling Strategy

Label NPU nodes:

```bash
kubectl label nodes <worker-node> accelerator=ascend
```

Use:

```yaml
nodeSelector:
  accelerator: ascend
```

Verify:

```bash
kubectl get pods -o wide
```

---

## 10. Private Container Registry (Kubernetes NodePort)

Registry runs as:
- Deployment + Service type NodePort
- Port: `32000`
- Namespace: `registry`
- Hosted on: `node1` (`10.50.156.65`)

### Containerd insecure registry config (on every node)

Edit `/etc/containerd/config.toml`:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."reg.local:32000"]
  endpoint = ["http://reg.local:32000"]

[plugins."io.containerd.grpc.v1.cri".registry.configs."reg.local:32000".tls]
  insecure_skip_verify = true
```

Restart containerd:

```bash
systemctl restart containerd
```

Test registry:

```bash
curl http://reg.local:32000/v2/_catalog
curl http://reg.local:32000/v2/ascend/vllm-ascend/tags/list
```

---

## 11. Proxy and NO_PROXY (Production Critical)

NO_PROXY must include:

```
localhost
127.0.0.1
::1
reg.local
nfs.local
7.242.102.243
10.50.156.65
.cluster.local
10.233.0.0/16   # pod CIDR
10.233.64.0/18  # service CIDR
```

If missing:
- `ImagePullBackOff`
- NFS hangs
- Registry HTTPS/HTTP mismatch errors

Verify on nodes:

```bash
cat /etc/systemd/system/containerd.service.d/http-proxy.conf
```

---

## 12. Diagnosing Model Load Stalls

If stuck at:

```
Loading safetensors checkpoint shards: 0% Completed
```

Checklist:

1. `grep /model /proc/mounts`
2. Verify `addr != 127.0.0.1`
3. `ls /model`
4. `dd` shard test
5. Check memory usage
6. Check `startupProbe` present
7. Ensure only one pod loading at a time (avoid NFS saturation)

---

## 13. Operational Checklist Before Experiments

```bash
# 1. Cluster health
kubectl get nodes

# 2. PV/PVC state
kubectl get pv
kubectl get pvc -n vllm

# 3. NPU availability
kubectl describe node | grep Ascend

# 4. DNS resolution on every node
getent hosts nfs.local
getent hosts reg.local

# 5. NFS mount test
mkdir /tmp/nfsroot
mount -t nfs -o nfsvers=4.1 7.242.102.243:/ /tmp/nfsroot
ls /tmp/nfsroot/saeid/models/qwen3-8b | head
umount /tmp/nfsroot

# 6. Verify /model inside pod
kubectl -n vllm exec -it <pod> -- ls /model
kubectl -n vllm exec -it <pod> -- grep /model /proc/mounts

# 7. Shard read speed
dd if=/tmp/nfsroot/saeid/models/qwen3-8b/model-00001-of-00005.safetensors of=/dev/null bs=8M count=32

# 8. Registry reachable
curl http://reg.local:32000/v2/_catalog

# 9. No other pod holds NPU
kubectl -n vllm get pods -o wide

# 10. Confirm LD_LIBRARY_PATH inside container
kubectl -n vllm exec -it <pod> -- env | grep LD_LIBRARY
```
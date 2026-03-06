## vLLM Multi-Node Infrastructure Setup Guide
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


----------------------------------------------------------------------
### 1. Cluster Topology (Current Setup)
----------------------------------------------------------------------

- 1 control-plane node
- Multiple worker nodes with Ascend NPUs
- Dedicated NFS server (7.242.102.243)
- Registry running inside Kubernetes (NodePort 32000)
- vLLM namespace used for deployments

Important:
- Master may run workloads only if taint removed.
- All NPU workloads run on labeled worker nodes.
- Registry is NOT running as a standalone Docker container anymore.
- reg.local must resolve to a Kubernetes node IP.

Verify nodes:

```bash
kubectl get nodes -o wide
```

If master should be schedulable:

kubectl taint nodes master node-role.kubernetes.io/control-plane-

----------------------------------------------------------------------
### 2. Split-Horizon DNS for Registry and NFS (CRITICAL FAILURE SOURCE)
----------------------------------------------------------------------

Historically observed fatal issue:
NFS resolving to 127.0.0.1 inside pods.

Symptom:

/proc/mounts shows:

```bash
nfs.local:/ /model ... addr=127.0.0.1
```

This causes:
- vLLM stuck at "Loading safetensors shards 0/5"
- ls /model hanging
- dd test hanging

Current Correct DNS Rules:

nfs.local must resolve to:

```bash
7.242.102.243
```

reg.local must resolve to:
A Kubernetes node IP (e.g. 10.175.113.44)

Example /etc/hosts on all nodes:

```bash
10.175.113.44 reg.local
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

```bash
addr=127.0.0.1
```

----------------------------------------------------------------------
### 3. Shared Model Storage via NFSv4 (Structural Change)
----------------------------------------------------------------------

NFSv4 root export (fsid=0) is now on:

```bash
/mnt/nvme1
```

Example /etc/exports:

```bash
/mnt/nvme1 127.0.0.1(ro,sync,no_subtree_check,fsid=0) \
            10.175.112.0/22(ro,sync,no_subtree_check,fsid=0)

/mnt/nvme1/registry-data 10.175.112.0/22(rw,sync,no_subtree_check)
```

Important NFSv4 behavior:

Because fsid=0 is on /mnt/nvme1,
clients mount:

```bash
7.242.102.243:/
```

Inside that namespace:

```bash
/saeid/models/qwen3-8b
/registry-data
```

CRITICAL RULE:
Do NOT include /mnt/nvme1 in PV path.
Use NFSv4 namespace path.

Correct model PV path:

```bash
/saeid/models/qwen3-8b
```

Wrong (causes breakage after fsid change):

```bash
/mnt/nvme1/saeid/models/qwen3-8b
/
```

Test from every node:

```bash
sudo mount -t nfs -o nfsvers=4.1 7.242.102.243:/ /tmp/nfsroot
ls /tmp/nfsroot/saeid/models/qwen3-8b | head
sudo umount /tmp/nfsroot
```

Test shard read:

```bash
sudo mount -t nfs -o nfsvers=4.1 7.242.102.243:/ /tmp/nfsroot
dd if=/tmp/nfsroot/saeid/models/qwen3-8b/model-00001-of-00005.safetensors of=/dev/null bs=8M count=32
sudo umount /tmp/nfsroot
```

If dd hangs → NFS bottleneck or DNS issue.

----------------------------------------------------------------------
### 4. PersistentVolume (PV) Configuration
----------------------------------------------------------------------

Key rule:
Mount only the specific model directory.
Never mount "/".

Correct example:

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

----------------------------------------------------------------------
### 5. PVC Stability Rule (Helm Behavior)
----------------------------------------------------------------------

If Helm conditionally creates PVC with:

```yaml
"helm.sh/resource-policy": keep
```

Then:

- Helm uninstall DOES NOT delete PV/PVC
- Helm upgrade DOES NOT modify immutable fields
- Changing nfsPath in values.yaml has NO effect
  unless PV is manually deleted

Correct recreation procedure:

```bash
helm uninstall vllm -n vllm
kubectl -n vllm delete pvc qwen-local-pvc
kubectl delete pv qwen-local-pv
helm upgrade --install vllm <chart> -n vllm -f values.yaml
```

PVC must be Bound before deploying pods.

Verify:

```bash
kubectl get pv qwen-local-pv
kubectl -n vllm get pvc qwen-local-pvc
```

----------------------------------------------------------------------
### 6. Mounting PVC in vLLM Deployment
----------------------------------------------------------------------

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

----------------------------------------------------------------------
### 7. Ascend NPU Configuration
----------------------------------------------------------------------

Common runtime failures observed:

```bash
aclInit error 507000
Runtime boot failed
Resources are busy
```

Causes:

- Another process holding NPU
- Driver / toolkit mismatch
- Missing libmpi_dvpp_adapter.so
- Wrong LD_LIBRARY_PATH

Inside container must mount:

/usr/local/Ascend/driver/lib64
/etc/ascend_install.info
/usr/local/dcmi
/usr/local/bin/npu-smi

Verify inside container:

npu-smi info

If "device is used":
- Check other pods using same NPU
- Ensure resource limits request huawei.com/Ascend910: 1

----------------------------------------------------------------------
### 8. Startup Probe for Large Models (CRITICAL)
----------------------------------------------------------------------

Large safetensors models over NFS may take 5–20+ minutes.

If liveness probe starts too early:
Pod restarts during shard loading.

Solution:

```yaml
startupProbe:
  httpGet:
    path: /health
    port: 8200
  periodSeconds: 10
  failureThreshold: 180
```

This gives ~30 minutes before Kubernetes kills container.

Never rely only on livenessProbe for large model loads.

----------------------------------------------------------------------
### 9. Multi-Node Scheduling Strategy
----------------------------------------------------------------------

Label NPU nodes:

```bash
kubectl label nodes worker1 accelerator=ascend
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

----------------------------------------------------------------------
### 10. Private Container Registry (Kubernetes NodePort)
----------------------------------------------------------------------

Registry runs as:

- Deployment
- Service type: NodePort
- Port: 32000
- Namespace: registry

Helm global imageRegistry rewriting can break image names.

If using:

```yaml
global:
  imageRegistry: reg.local:32000
```

Ensure:
- Images pushed WITHOUT duplicate registry prefix
- stripRegistry helper handles quay.io images

Test:

```bash
curl http://reg.local:32000/v2/_catalog
curl http://reg.local:32000/v2/ascend/vllm-ascend/tags/list
```

Docker insecure registries must include:

```bash
7.242.102.243:32000
reg.local:32000
10.175.113.44:32000
```

Verify:

```bash
docker info | grep -i "Insecure Registries" -A5
```

----------------------------------------------------------------------
### 11. Proxy and NO_PROXY (Production Critical)
----------------------------------------------------------------------

NO_PROXY must include:

```bash
localhost
127.0.0.1
::1
reg.local
nfs.local
7.242.102.243
.cluster.local
pod CIDR
service CIDR
```

If missing:
- ImagePullBackOff
- NFS hangs
- Registry HTTPS/HTTP mismatch errors

Verify:

```bash
systemctl show docker -p Environment
```

----------------------------------------------------------------------
### 12. Diagnosing Model Load Stalls
----------------------------------------------------------------------

If stuck at:

Loading safetensors checkpoint shards: 0% Completed

Checklist:

1. grep /model /proc/mounts
2. Verify addr != 127.0.0.1
3. ls /model
4. dd shard test
5. Check memory usage
6. Check startupProbe present
7. Ensure only one pod loading at a time (avoid NFS saturation)

----------------------------------------------------------------------
### 13. Operational Checklist Before Experiments
----------------------------------------------------------------------

1. kubectl get nodes
2. kubectl get pv
3. kubectl get pvc -n vllm
4. kubectl describe node | grep Ascend
5. Verify DNS resolution on every node
6. Verify NFS mount manually
7. Verify /model inside pod
8. Verify shard read speed
9. Confirm startupProbe configured
10. Confirm registry reachable
11. Confirm no other pod holds NPU
12. Confirm LD_LIBRARY_PATH inside container correct

## vLLM Multi-Node Infrastructure Setup Guide
(Ascend NPU, Shared NFS Models, Private Registry, Kubernetes Cluster Already Installed)

This document describes the infrastructure configuration used in the current multi-node environment. It excludes Kubernetes installation and focuses on:

- Multi-node considerations
- NFS-backed model storage
- PersistentVolume (PV) and PersistentVolumeClaim (PVC)
- Ascend NPU device plugin (or manual device exposure)
- Private container registry (split-horizon DNS)
- Node labeling and scheduling
- Proxy + NO_PROXY configuration
- Startup probe tuning for large models
- Production considerations observed in practice

----------------------------------------------------------------------
### 1. Cluster Topology (Current Setup)
----------------------------------------------------------------------

- 1 control-plane node (master)
- Multiple worker nodes with Ascend NPUs
- Shared NFS server hosting model weights
- Private container registry exposed on cluster network
- vLLM namespace used for deployments

Important:
Master node may run workloads only if taint removed.
All NPU workloads run on labeled worker nodes.

Verify nodes:

```bash
kubectl get nodes -o wide
```

If master should be schedulable:

```bash
kubectl taint nodes master node-role.kubernetes.io/control-plane-
```
----------------------------------------------------------------------  
### 2. Split-Horizon DNS for Registry and NFS (CRITICAL FAILURE SOURCE)  
----------------------------------------------------------------------

The most common fatal issue observed:
NFS resolving to 127.0.0.1 inside pods.

Symptom:

```bash
/proc/mounts shows:
nfs.local:/ /model ... addr=127.0.0.1
```

This causes:
- vLLM stuck at "Loading safetensors shards 0/5"
- ls /model hanging
- dd test hanging

Correct configuration:

On master:
/etc/hosts:

```bash
127.0.0.1 reg.local
127.0.0.1 nfs.local
```

On other nodes:
  /etc/hosts:

```bash
<MASTER_IP> reg.local nfs.local
```

Verify on every node:

```bash
getent hosts reg.local
getent hosts nfs.local
```

Master must resolve to 127.0.0.1.
Workers must resolve to MASTER_IP.
Pods must see the real NFS server IP.

Inside pod verification:

```bash
grep /model /proc/mounts
```

Must NOT show:

```bash
addr=127.0.0.1
```

----------------------------------------------------------------------  
### 3. Shared Model Storage via NFS  
----------------------------------------------------------------------

Host models centrally on NFS.
Do NOT use hostPath for models in multi-node setup.

DO NOT mount "/" as NFS path.
Mount only the actual model directory.

Wrong (causes metadata scan slowness and hangs):

```bash
nfsPath: "/"
```

Correct:
```bash
nfsPath: "/data/models/qwen3-8b"
```

Example /etc/exports:

```bash
/data/models 127.0.0.1(ro,sync,no_subtree_check,fsid=0) 10.175.112.0/22(ro,sync,no_subtree_check,fsid=0)
```

Notes:
- Include 127.0.0.1 if master mounts locally
- Include worker subnet
- fsid=0 required for NFSv4 root export

Test from every node:

```bash
mount -t nfs -o nfsvers=4.1 nfs.local:/data/models /tmp/test
ls /tmp/test | head
```

Test inside pod:

```bash
ls /model
dd if=<first_shard>.safetensors of=/dev/null bs=8M count=32
```

If dd hangs → NFS bottleneck or wrong DNS.

----------------------------------------------------------------------  
### 4. PersistentVolume (PV) Configuration  
----------------------------------------------------------------------

Key rule:
Mount only the specific model directory, never "/".

model-pv.yaml:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: model-pv
spec:
  capacity:
    storage: 2Ti
  accessModes:
    - ReadOnlyMany
  persistentVolumeReclaimPolicy: Retain
  mountOptions:
    - nfsvers=4.1
    - rsize=1048576
    - wsize=1048576
    - hard
    - timeo=600
  nfs:
    server: nfs.local
    path: /data/models/qwen3-8b
```

----------------------------------------------------------------------  
### 5. PVC Stability Rule  
----------------------------------------------------------------------

If Helm conditionally creates PVC:
DO NOT toggle create flag repeatedly.

This causes:

  persistentvolumeclaim "..." is being deleted

PVC must be Bound before deploying pods.

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
      claimName: model-pvc
```

Verify inside pod:

```bash
ls /model
grep /model /proc/mounts
```

----------------------------------------------------------------------  
### 7. Ascend NPU Configuration  
----------------------------------------------------------------------

Common runtime failures observed:

Error:
  aclInit error 507000
  Runtime boot failed
  Resources are busy

Causes:
- Another process holding NPU
- Driver / toolkit mismatch
- Missing libmpi_dvpp_adapter.so
- Wrong LD_LIBRARY_PATH

Inside container must mount:

```bash
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
  Check other pods using same NPU
  Ensure resource limits request huawei.com/Ascend910: 1

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
### 10. Private Container Registry  
----------------------------------------------------------------------

Helm global image registry rewriting can break image names.

If using:

```yaml
global:
  imageRegistry: reg.local:32000
```

Ensure:
- Images pushed WITHOUT duplicate registry prefix
- stripRegistry helper handles quay.io images

Test pull on every node:

```bash
docker pull reg.local:32000/ascend/vllm-ascend:v0.11.0rc0
```

----------------------------------------------------------------------  
### 11. Proxy and NO_PROXY (Production Critical)  
----------------------------------------------------------------------

NO_PROXY must include:

- localhost
- 127.0.0.1
- ::1
- reg.local
- nfs.local
- actual registry IP
- .cluster.local
- pod CIDR
- service CIDR

If missing:
- ImagePullBackOff
- NFS hangs
- Registry 504 errors

Verify:

```bash
systemctl show docker -p Environment
```

----------------------------------------------------------------------  
### 12. Diagnosing Model Load Stalls  
----------------------------------------------------------------------

If stuck at:

```bash
Loading safetensors checkpoint shards: 0% Completed
```

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

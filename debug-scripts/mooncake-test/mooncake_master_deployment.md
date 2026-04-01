# Mooncake Master Deployment Guide

## Environment

| Node | IP | Role |
|------|----|------|
| node1 | 7.216.57.215 | K8s control plane |
| node2 | 7.216.57.161 | K8s worker |
| node3 | 10.50.156.65 | K8s worker, mooncake-master node |
| node4 | 10.50.156.106 | K8s worker |
| 243 | 7.242.102.243 | Operations machine + NFS server |

- **NFS export**: `/mnt/nvme1` (fsid=0, must mount with path `/` not `/mnt/nvme1`)
- **Model path**: `/mnt/nvme1/haiting_jd/models/qwen3-32b`
- **Image**: `quay.io/ascend/vllm-ascend:glm5-openeuler` (mooncake_master is built-in)
- **Network cards**: node1/node2 use `enp67s0f5`, node3/node4 use `enp189s0f0`

---

## Prerequisites

### 1. Verify NFS connectivity from worker nodes
```bash
# Run on each worker node
ssh node3 "sudo mount -t nfs4 7.242.102.243:/ /tmp/nfstest && ls /tmp/nfstest && sudo umount /tmp/nfstest"
```

### 2. Verify mooncake_master binary exists in image
```bash
docker run --rm quay.io/ascend/vllm-ascend:glm5-openeuler which mooncake_master
# Expected: /usr/local/bin/mooncake_master
```

---

## Step 1: Create Test Namespace

```bash
kubectl create namespace vllm-test
```

---

## Step 2: Create PV and PVC

> **Important**: NFS server uses `fsid=0`, which means `/mnt/nvme1` is treated as the NFS4
> root. The PV `path` must be set to `/`, not `/mnt/nvme1`.

**pv.yaml**:
```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: model-pv-test
  annotations:
    "helm.sh/resource-policy": keep
  labels:
    type: model-storage
spec:
  capacity:
    storage: 500Gi
  accessModes:
    - ReadOnlyMany
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  mountOptions:
    - nfsvers=4.1
  nfs:
    server: 7.242.102.243
    path: /                    # Must be / due to fsid=0, NOT /mnt/nvme1
    readOnly: true
```

**pvc.yaml**:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: model-pvc
  namespace: vllm-test
  annotations:
    "helm.sh/resource-policy": keep
spec:
  accessModes:
    - ReadOnlyMany
  resources:
    requests:
      storage: 500Gi
  storageClassName: ""
  volumeName: model-pv-test    # Explicitly bind to the PV above
```

```bash
kubectl apply -f pv.yaml
kubectl apply -f pvc.yaml

# Verify both are Bound
kubectl get pv model-pv-test
kubectl get pvc -n vllm-test
```

Expected output:
```
NAME            STATUS   ...
model-pv-test   Bound    vllm-test/model-pvc

NAME        STATUS   VOLUME
model-pvc   Bound    model-pv-test
```

---

## Step 3: Deploy Mooncake Master

### Key design decisions

| Component | Path | Reason |
|-----------|------|--------|
| NFS (model) | mountPath: `/workspace/glm5` | Read-only, maps to NFS root `/` |
| mooncake.json | mountPath: `/etc/mooncake/mooncake.json` | Must be **outside** NFS read-only mount |
| Logs | mountPath: `/workspace/mooncake_logs` | hostPath, writable, outside NFS mount |

> **Why mooncake.json cannot be under `/workspace/glm5`**: The PVC is ReadOnlyMany.
> Kubernetes needs to create a mountpoint for the ConfigMap subPath mount, which requires
> write access. Placing it under the NFS read-only mount causes `read-only file system` error.

**mooncake_master.yaml**:
```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mooncake-config
  namespace: vllm-test
data:
  mooncake.json: |
    {
      "metadata_server": "P2PHANDSHAKE",
      "protocol": "ascend",
      "device_name": "",
      "master_server_address": "10.50.156.65:50088",
      "global_segment_size": 140000000000
    }

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mooncake-master
  namespace: vllm-test
  labels:
    app: mooncake-master
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mooncake-master
  template:
    metadata:
      labels:
        app: mooncake-master
    spec:
      nodeSelector:
        kubernetes.io/hostname: node3
      hostNetwork: true                      # Bind directly to host network
      dnsPolicy: ClusterFirstWithHostNet

      volumes:
        - name: model-storage
          persistentVolumeClaim:
            claimName: model-pvc
            readOnly: true
        - name: mooncake-config
          configMap:
            name: mooncake-config
        - name: mooncake-logs
          hostPath:
            path: /mnt/nvme1/haiting_jd/mooncake_logs
            type: DirectoryOrCreate
        - name: dcmi-volume
          hostPath:
            path: /usr/local/dcmi
            type: Directory
        - name: npu-smi-volume
          hostPath:
            path: /usr/local/bin/npu-smi
            type: File
        - name: ascend-driver-lib64-volume
          hostPath:
            path: /usr/local/Ascend/driver/lib64/
            type: Directory
        - name: version-info-volume
          hostPath:
            path: /usr/local/Ascend/driver/version.info
            type: File
        - name: ascend-install-info-volume
          hostPath:
            path: /etc/ascend_install.info
            type: File

      containers:
        - name: mooncake-master
          image: quay.io/ascend/vllm-ascend:glm5-openeuler
          imagePullPolicy: IfNotPresent
          command:
            - /bin/bash
            - -c
            - |
              set -e

              echo "--- ASCEND_TOOLKIT_HOME before sourcing: [$ASCEND_TOOLKIT_HOME]"
              source /usr/local/Ascend/ascend-toolkit/set_env.sh
              export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/Ascend/ascend-toolkit/latest/aarch64-linux/devlib:/usr/local/Ascend/ascend-toolkit/latest/aarch64-linux/lib64

              # Auto-detect NIC name and IP (compatible with enp67s0f5 and enp189s0f0)
              nic_name=$(ip addr show | grep 'inet ' | grep -v '127.0.0.1' | head -1 | awk '{print $NF}')
              local_ip="${NODE_IP}"
              echo "[mooncake-master] auto-detected nic=${nic_name} ip=${local_ip}"

              export HCCL_OP_EXPANSION_MODE="AIV"
              export HCCL_IF_IP=${local_ip}
              export GLOO_SOCKET_IFNAME=${nic_name}
              export TP_SOCKET_IFNAME=${nic_name}
              export HCCL_SOCKET_IFNAME=${nic_name}
              export HCCL_BUFFSIZE=200
              export OMP_PROC_BIND=false
              export OMP_NUM_THREADS=16
              export PYTHONHASHSEED=0
              export PYTHONPATH=${PYTHONPATH}:/vllm-workspace/vllm
              export VLLM_USE_V1=1
              export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
              export MOONCAKE_CONFIG_PATH="/etc/mooncake/mooncake.json"
              export ASCEND_BUFFER_POOL=4:8
              export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}

              mkdir -p /workspace/mooncake_logs
              timestamp=$(date "+%Y%m%d%H%M%S")

              echo "[mooncake-master] timestamp: ${timestamp}"
              echo "[mooncake-master] local_ip: ${local_ip}, nic: ${nic_name}"
              echo "[mooncake-master] MOONCAKE_CONFIG_PATH: ${MOONCAKE_CONFIG_PATH}"
              echo "[mooncake-master] Starting mooncake_master on port 50088..."

              mooncake_master \
                --port 50088 \
                --eviction_high_watermark_ratio 0.9 \
                --eviction_ratio 0.1 \
                2>&1 | split -b 5M -d -a 5 - /workspace/mooncake_logs/logs_${timestamp}_

          ports:
            - name: grpc
              containerPort: 50088
              protocol: TCP

          env:
            - name: NODE_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.hostIP
            - name: HCCL_OP_EXPANSION_MODE
              value: "AIV"
            - name: OMP_PROC_BIND
              value: "false"
            - name: OMP_NUM_THREADS
              value: "16"
            - name: VLLM_USE_V1
              value: "1"
            - name: HCCL_BUFFSIZE
              value: "200"
            - name: PYTORCH_NPU_ALLOC_CONF
              value: "expandable_segments:True"
            - name: PYTHONHASHSEED
              value: "0"
            - name: PYTHONPATH
              value: "/vllm-workspace/vllm"
            - name: MOONCAKE_CONFIG_PATH
              value: "/etc/mooncake/mooncake.json"
            - name: ASCEND_BUFFER_POOL
              value: "4:8"
            - name: LD_LIBRARY_PATH
              value: "/usr/local/lib"
            - name: HF_HUB_OFFLINE
              value: "1"
            - name: TRANSFORMERS_OFFLINE
              value: "1"
            - name: HF_HUB_DISABLE_TELEMETRY
              value: "1"
            - name: TOKENIZERS_PARALLELISM
              value: "false"

          volumeMounts:
            - name: model-storage
              mountPath: /workspace/glm5       # NFS root / maps here
              readOnly: true                   # Model at /workspace/glm5/haiting_jd/models/qwen3-32b
            - name: mooncake-config
              mountPath: /etc/mooncake/mooncake.json   # Outside NFS mount
              subPath: mooncake.json
              readOnly: true
            - name: mooncake-logs
              mountPath: /workspace/mooncake_logs      # Outside NFS mount, writable
            - name: dcmi-volume
              mountPath: /usr/local/dcmi
            - name: npu-smi-volume
              mountPath: /usr/local/bin/npu-smi
            - name: ascend-driver-lib64-volume
              mountPath: /usr/local/Ascend/driver/lib64/
            - name: version-info-volume
              mountPath: /usr/local/Ascend/driver/version.info
            - name: ascend-install-info-volume
              mountPath: /etc/ascend_install.info

          livenessProbe:
            tcpSocket:
              port: 50088
            initialDelaySeconds: 15
            periodSeconds: 30
            failureThreshold: 3
          readinessProbe:
            tcpSocket:
              port: 50088
            initialDelaySeconds: 10
            periodSeconds: 10
            failureThreshold: 5

          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
            limits:
              cpu: "4"
              memory: "8Gi"

      restartPolicy: Always
```

```bash
kubectl apply -f mooncake_master.yaml
```

---

## Step 4: Verify Deployment

### Check Pod status
```bash
kubectl get pods -n vllm-test -l app=mooncake-master
# Expected: 1/1 Running
```

### Check logs
```bash
kubectl logs -n vllm-test deployment/mooncake-master
# Expected output includes:
# Master service started on port 50088
# HTTP metrics server started on port 9003
# Task cleanup thread started
```

### Check port is listening on node3
```bash
ssh node3 "sudo ss -tlnp | grep -E '50088|9003'"
```

### Verify connectivity from all other nodes
```bash
# bash /dev/tcp is available even without nc
ssh node1 "bash -c 'echo > /dev/tcp/10.50.156.65/50088 && echo OK || echo FAIL'"
ssh node2 "bash -c 'echo > /dev/tcp/10.50.156.65/50088 && echo OK || echo FAIL'"
ssh node4 "bash -c 'echo > /dev/tcp/10.50.156.65/50088 && echo OK || echo FAIL'"
# Expected: OK from all nodes
```

---

## Pitfalls and Solutions

### 1. NFS mount path with fsid=0
The NFS server exports `/mnt/nvme1` with `fsid=0`, making it the NFS4 root.
- ❌ `path: /mnt/nvme1` or `path: /mnt/nvme1/haiting_jd/models` → `No such file or directory`
- ✅ `path: /` → mounts successfully; model accessible at `/workspace/glm5/haiting_jd/models/`

### 2. Log directory cannot be under NFS read-only mount
PVC is `ReadOnlyMany`. Creating directories under the NFS mount fails.
- ❌ `mountPath: /workspace/glm5/mooncake_logs` → `read-only file system`
- ✅ Use a separate `hostPath` volume at `/workspace/mooncake_logs`

### 3. mooncake.json cannot be mounted under NFS read-only path
ConfigMap `subPath` mount requires creating a mountpoint, which fails on read-only NFS.
- ❌ `mountPath: /workspace/glm5/mooncake.json` → `read-only file system`
- ✅ Mount outside NFS: `mountPath: /etc/mooncake/mooncake.json`
- ✅ Update env var: `MOONCAKE_CONFIG_PATH=/etc/mooncake/mooncake.json`

### 4. split pipe swallows error output
The original script pipes output to `split`, hiding mooncake_master errors from `kubectl logs`.
- For debugging: remove `| split ...` and run mooncake_master directly
- Restore `split` after confirming successful startup

### 5. Stale process from hostNetwork
With `hostNetwork: true`, ports bind directly to the host. After Pod restarts, the old
process may still hold the port.
```bash
# Check for stale process
ssh node3 "sudo ss -tlnp | grep 50088"
# Kill if found
ssh node3 "sudo kill -9 <pid>"
```

### 6. PVC namespace isolation
PVCs are namespace-scoped. A PV can only be bound to one PVC at a time.
For testing in a separate namespace, create a new PV with a different name.

### 7. sed namespace replacement pitfall
`sed 's/namespace: vllm/namespace: vllm-test/'` will also match `vllm-test` → `vllm-test-test`.
Use `$` anchor: `sed 's/namespace: vllm$/namespace: vllm-test/'`

### 8. connection error: End of file in logs (not an error)
The `tcpSocket` readiness probe connects and immediately disconnects.
mooncake_master logs this as `connection error: End of file` — this is expected and harmless.

### 9. ip command not found in container
The image does not have the `ip` command. NIC name detection falls back to empty string,
but `local_ip` is correctly injected via `NODE_IP` env var from K8s fieldRef. No impact on functionality.

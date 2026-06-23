# Private Container Registry — Operations Guide

The cluster uses a private container registry running as a Kubernetes
Deployment with a NodePort Service. All custom images (`kv-router`,
`kv-sidecar`, `boom-gateway`, `vllm-cpu-hash`, etc.) are pushed here
and pulled by pods via `reg.local:32000`.

---

## Architecture

| Component | Value |
|-----------|-------|
| Namespace | `registry` |
| Deployment | `registry` (1 replica, `registry:2` image) |
| Service | `registry` — NodePort `5000:32000` |
| DNS alias | `reg.local` → node IP (in `/etc/hosts`) |
| Storage | NFS-backed (`/mnt/nvme1/registry-data` on NFS server) |

The registry is accessible from **any** node via `<any-node-ip>:32000`
(NodePort), but `reg.local` in `/etc/hosts` should point to a reachable
node.

---

## Images in the registry

```bash
curl --noproxy '*' http://reg.local:32000/v2/_catalog
```

List tags for a specific image:

```bash
curl --noproxy '*' http://reg.local:32000/v2/kv-sidecar/tags/list
```

Current images:

| Image | Description |
|-------|-------------|
| `kv-router` | Python KV-aware router service |
| `kv-router-go` | Go implementation of the router |
| `kv-sidecar` | Python sidecar (pull worker + vLLM client) |
| `kv-sidecar-go` | Go implementation of the sidecar |
| `boom-gateway` | BooM Gateway (Rust, auth + routing proxy) |
| `vllm-cpu-hash` | Prefix hash computation service |
| `ascend/vllm-ascend` | vLLM with Ascend NPU support |
| `litellm` | LiteLLM proxy (legacy, replaced by BooM) |
| `lws/lws` | LeaderWorkerSet controller |
| `redis` | Redis for KV metadata |

---

## Node configuration (required on every node)

All three steps below are **mandatory** on every node. Missing any one
of them will cause image pulls to fail.

### 1. DNS — `/etc/hosts`

```
10.50.156.65  reg.local
```

Update this if the registry moves to a different node. NodePort works
from any node IP, but containerd resolves `reg.local` when pulling.

Verify:

```bash
getent hosts reg.local
```

### 2. Containerd insecure registry — `hosts.toml` (folder-based)

The cluster uses containerd's folder-based registry configuration via
`config_path = "/etc/containerd/certs.d"` in `/etc/containerd/config.toml`.

Create the registry host config:

```bash
mkdir -p /etc/containerd/certs.d/reg.local:32000
```

Write `/etc/containerd/certs.d/reg.local:32000/hosts.toml`:

```toml
server = "http://reg.local:32000"

[host."http://reg.local:32000"]
  capabilities = ["pull", "resolve"]
  skip_verify = true
```

Verify that `config.toml` has the `config_path` directive (should already
be present on all cluster nodes):

```bash
grep -i 'config_path' /etc/containerd/config.toml
# expected: config_path = "/etc/containerd/certs.d"
```

After creating the file:

```bash
systemctl restart containerd
```

### 3. NO_PROXY for containerd

`reg.local` **must** be in the `NO_PROXY` list in containerd's proxy
drop-in, otherwise the corporate proxy (Cntlm) intercepts registry
traffic and returns 504 Gateway Time-out.

Verify:

```bash
grep -o 'reg.local' /etc/systemd/system/containerd.service.d/http-proxy.conf
```

If missing, add it:

```bash
sed -i 's/NO_PROXY=/NO_PROXY=reg.local,/' /etc/systemd/system/containerd.service.d/http-proxy.conf
systemctl daemon-reload
systemctl restart containerd
```

The full `NO_PROXY` list should also include node IPs, cluster CIDRs,
`nfs.local`, etc. See `multi-node-setup-guide.md` section 11 for the
complete list.

### Verification (after all 3 steps)

```bash
# DNS resolves
getent hosts reg.local

# Registry reachable (bypassing shell proxy)
curl --noproxy '*' http://reg.local:32000/v2/_catalog

# Containerd can pull
ctr -n k8s.io images pull --plain-http reg.local:32000/kv-sidecar:latest
```

---

## Common operations

### Build and push an image

```bash
cd <repo-root>/src/services/sidecar
docker build -t reg.local:32000/kv-sidecar:latest .
docker push reg.local:32000/kv-sidecar:latest
```

### Load an image without the registry (air-gap fallback)

When the registry is down, load images directly into containerd on each
node that needs them:

```bash
# On the build node:
docker save reg.local:32000/kv-sidecar:latest -o /tmp/kv-sidecar.tar

# Copy to target node:
scp /tmp/kv-sidecar.tar root@<target-node>:/tmp/

# Import into containerd:
ssh root@<target-node> "ctr -n k8s.io image import /tmp/kv-sidecar.tar"
```

### Force pods to re-pull

```bash
kubectl delete pod -n vllm <pod-name>
```

Pods with `imagePullPolicy: Always` will pull the latest on restart.
Pods with `IfNotPresent` only pull if the image doesn't exist locally.

---

## Troubleshooting

### Registry pod won't start (ImagePullBackOff)

The `registry:2` image itself must exist on the node where the registry
pod is scheduled. This is a chicken-and-egg problem.

**Fix:** Load `registry:2` directly into containerd on the target node:

```bash
# On a node with Docker + internet:
docker pull --platform linux/arm64 registry:2
docker save registry:2 -o /tmp/registry2.tar

# Copy and import on target node:
scp /tmp/registry2.tar root@<node>:/tmp/
ssh root@<node> "ctr -n k8s.io image import /tmp/registry2.tar"
```

### Registry evicted due to disk-pressure

The registry pod is tiny but gets evicted when a node has disk-pressure
taint. Solutions:

1. **Free disk** on the node:

```bash
crictl rmi --prune
journalctl --vacuum-size=200M
```

2. **Move to another node** with nodeSelector:

```bash
kubectl patch deployment -n registry registry \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"node4"}}}}}'
```

3. **Tolerate disk-pressure** (the registry uses minimal disk):

```bash
kubectl patch deployment -n registry registry \
  -p '{"spec":{"template":{"spec":{"tolerations":[{"key":"node.kubernetes.io/disk-pressure","operator":"Exists","effect":"NoSchedule"}]}}}}'
```

### Connection refused on `reg.local:32000`

```bash
# 1. Check registry pod
kubectl get pods -n registry -o wide

# 2. Test bypassing proxy
curl --noproxy '*' http://reg.local:32000/v2/_catalog

# 3. Test via direct node IP (NodePort works from any node)
curl --noproxy '*' http://7.216.57.149:32000/v2/_catalog

# 4. Verify DNS
getent hosts reg.local

# 5. Check if Cntlm proxy is intercepting
# 502 with "Cntlm" in body = proxy issue, add to NO_PROXY
```

### `reg.local` resolves to wrong IP after moving registry

Update `/etc/hosts` on **all** nodes to point to the new node's IP.
Or keep it pointing to any cluster node — NodePort routes to the pod
regardless of which node it runs on.

### Eviction loop (pods keep creating and evicting)

```bash
# Stop the loop
kubectl scale deployment -n registry registry --replicas=0

# Clean up failed pods
kubectl delete pods -n registry --field-selector=status.phase=Failed

# Fix the root cause (disk/scheduling), then scale back up
kubectl scale deployment -n registry registry --replicas=1
```

---

## Current deployment state

After the disk-pressure incident on node1, the registry was moved to
node4 (10.50.156.106) with nodeSelector pinning:

```yaml
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: node4
```

This is persistent — the deployment spec is stored in etcd. If node4
becomes unavailable, patch the nodeSelector to a different node that
has the `registry:2` image available.

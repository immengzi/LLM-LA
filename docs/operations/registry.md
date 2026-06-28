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
| Storage | Environment-specific; BZ currently uses `hostPath: /data/registry` on `k8s-worker1`. |

The registry is accessible from **any** node via `<any-node-ip>:32000`
(NodePort), but `reg.local` in `/etc/hosts` should point to a reachable
node.

---

## BZ cluster registry record

The BZ cluster keeps the fixed registry address `reg.local:32000` and runs the
registry inside Kubernetes as a `registry:2` Deployment with a NodePort Service.
The goal is that every node can resolve `reg.local`, reach TCP port `32000`,
and pull images through containerd over HTTP.

### Nodes and host choice

At the time of setup, the BZ cluster nodes were:

| Node | IP | Role |
| --- | --- | --- |
| `k8s-master` | `192.168.0.79` | control-plane |
| `k8s-worker` | `192.168.0.99` | worker |
| `k8s-worker1` | `192.168.0.42` | worker |
| `k8s-worker2` | `192.168.0.69` | worker |

The old `reg.local` entry pointed to `192.168.0.79`, but
`reg.local:32000` was not serving a registry:

```bash
getent hosts reg.local
# 192.168.0.79    reg.local

curl --noproxy '*' -sS --max-time 3 http://reg.local:32000/v2/_catalog
# curl: (7) Failed to connect to reg.local port 32000
```

ICMP ping was not reliable, so the connectivity survey used existing
hostNetwork vLLM pods for TCP checks. Because those pods use host networking,
their pod IPs are node IPs and provide a useful node-level network view.

Observed TCP reachability:

| Source node | To master `192.168.0.79` | To worker `192.168.0.99` | To worker1 `192.168.0.42` | To worker2 `192.168.0.69` |
| --- | --- | --- | --- | --- |
| `k8s-master` | OK | OK | OK | FAIL |
| `k8s-worker` | OK | OK | OK | OK |
| `k8s-worker1` | OK | OK | OK | OK |
| `k8s-worker2` | FAIL | OK | OK | OK |

Conclusion:

- `k8s-worker2` cannot directly reach `k8s-master`.
- `k8s-master` also cannot directly reach `k8s-worker2`.
- Every node can reach `k8s-worker1`.
- Every node can also reach `k8s-worker`, but the current machine did not have
  SSH/root access to `k8s-worker`.

Therefore the BZ registry was placed on `k8s-worker1` (`192.168.0.42`), and
every node should resolve:

```text
192.168.0.42 reg.local
```

Do not point `reg.local` back to `k8s-master` in this cluster.

### BZ deployment parameters

| Field | Value |
| --- | --- |
| Namespace | `registry` |
| Deployment | `registry` |
| Registry image | `registry:2` |
| Pinned node | `k8s-worker1` |
| Storage | `hostPath: /data/registry` |
| Service | `NodePort` |
| Service port | `5000` |
| NodePort | `32000` |
| External address | `reg.local:32000` |

Before the registry pod can start, the target node needs the `registry:2`
image imported directly into containerd:

```bash
docker pull registry:2
docker save registry:2 | ssh -o BatchMode=yes k8s-worker1 \
  "ctr -n k8s.io images import -"
```

The registry manifest used in BZ:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: registry
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: registry
  namespace: registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: registry
  template:
    metadata:
      labels:
        app: registry
    spec:
      nodeName: k8s-worker1
      containers:
        - name: registry
          image: registry:2
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 5000
          env:
            - name: REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY
              value: /var/lib/registry
          volumeMounts:
            - name: registry-data
              mountPath: /var/lib/registry
      volumes:
        - name: registry-data
          hostPath:
            path: /data/registry
            type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: registry
  namespace: registry
spec:
  type: NodePort
  selector:
    app: registry
  ports:
    - name: http
      port: 5000
      targetPort: 5000
      nodePort: 32000
```

Expected final state:

```text
pod/registry-...       1/1 Running  ... k8s-worker1
service/registry       NodePort     ... 5000:32000/TCP
deployment/registry    1/1          ... registry:2
```

Registry data is stored on `k8s-worker1` at `/data/registry`, not on NFS. If
the registry moves to another node, copy this data or push the images again.

### BZ node configuration

Every node needs:

1. `/etc/hosts` resolving `reg.local` to `192.168.0.42`.
2. `/etc/containerd/certs.d/reg.local:32000/hosts.toml` marking the registry as
   plain HTTP.

`hosts.toml` content:

```toml
server = "http://reg.local:32000"

[host."http://reg.local:32000"]
  capabilities = ["pull", "resolve", "push"]
  skip_verify = true
```

One-shot privileged Jobs wrote this host-level configuration on every BZ node.
After they completed, those Jobs were removed and only the registry deployment
remained.

### BZ verification

Verify the Registry API:

```bash
getent hosts reg.local
# 192.168.0.42    reg.local

curl --noproxy '*' http://reg.local:32000/v2/_catalog
# {"repositories":[]}
```

Docker defaults to HTTPS for `docker push reg.local:32000/...`; without Docker
insecure-registry configuration it may fail with:

```text
http: server gave HTTP response to HTTPS client
```

For this setup, containerd was used with `--plain-http`:

```bash
docker tag registry:2 reg.local:32000/registry:2

docker save reg.local:32000/registry:2 | sudo -n ctr -n k8s.io images import -
sudo -n ctr -n k8s.io images push --plain-http reg.local:32000/registry:2

curl --noproxy '*' http://reg.local:32000/v2/_catalog
# {"repositories":["registry"]}

curl --noproxy '*' http://reg.local:32000/v2/registry/tags/list
# {"name":"registry","tags":["2"]}
```

Every BZ node verified:

| Node | `reg.local` resolution | TCP `reg.local:32000` | `ctr pull --plain-http reg.local:32000/registry:2` |
| --- | --- | --- | --- |
| `k8s-master` | `192.168.0.42` | OK | OK |
| `k8s-worker` | `192.168.0.42` | OK | OK |
| `k8s-worker1` | `192.168.0.42` | OK | OK |
| `k8s-worker2` | `192.168.0.42` | OK | OK |

Verification Job logs included warnings like:

```text
ERROR: ld.so: object '/usr/lib64/libjemalloc.so.2' from LD_PRELOAD cannot be preloaded
```

These came from chrooting into the host environment and did not affect
`getent`, TCP checks, or `ctr pull`.

### BZ mirrored chart images

After the registry was validated, images referenced by
`src/vllm-kv-stack/values.yaml` and the Helm templates were rebuilt or mirrored
into `reg.local:32000`.

Local images rebuilt from this codebase:

| Image | Source |
| --- | --- |
| `reg.local:32000/kv-router:latest` | `src/services/router_service/Dockerfile` |
| `reg.local:32000/kv-sidecar:latest` | `src/services/sidecar/Dockerfile` |
| `reg.local:32000/vllm-cpu-hash:latest` | `src/services/prefix_hash/Dockerfile` |
| `reg.local:32000/kv-router-go:latest` | `src/services/go/Dockerfile.router` |
| `reg.local:32000/kv-sidecar-go:latest` | `src/services/go/Dockerfile.sidecar` |
| `reg.local:32000/boom-gateway:v5` | `src/boom-integration/Dockerfile` |

External images mirrored for the chart:

| Image | Notes |
| --- | --- |
| `reg.local:32000/library/vllm-ascend:v0.18.0` | Matches Helm rewriting of `docker.io/library/vllm-ascend:v0.18.0`. |
| `reg.local:32000/vllm-ascend:v0.18.0` | Convenience tag for direct references. |
| `reg.local:32000/redis:7-alpine` | Used by the Redis Deployment. |
| `reg.local:32000/litellm:main-stable` | Mirrored from `ghcr.io/berriai/litellm:main-stable`; `litellm:main-stable` is not available on Docker Hub. |
| `reg.local:32000/postgres:16-alpine` | Used by optional BooM key-affinity benchmark Postgres. |
| `reg.local:32000/busybox:latest` | Used by optional cache warm hook when explicitly configured. |

Push via containerd plain HTTP:

```bash
docker tag <source-image> reg.local:32000/<repo>:<tag>
docker save reg.local:32000/<repo>:<tag> | sudo -n ctr -n k8s.io images import -
sudo -n ctr -n k8s.io images push --plain-http reg.local:32000/<repo>:<tag>
```

Check current tags:

```bash
for repo in \
  kv-router kv-sidecar kv-router-go kv-sidecar-go \
  vllm-cpu-hash vllm-ascend library/vllm-ascend \
  redis busybox litellm boom-gateway postgres registry; do
  curl --noproxy '*' "http://reg.local:32000/v2/$repo/tags/list"
  echo
done
```

Expected tags after the mirror:

```text
kv-router: latest
kv-sidecar: latest
kv-router-go: latest
kv-sidecar-go: latest
vllm-cpu-hash: latest
vllm-ascend: v0.18.0
library/vllm-ascend: v0.18.0
redis: 7-alpine
busybox: latest
litellm: main-stable
boom-gateway: v5
postgres: 16-alpine
registry: 2
```

Helm render checks confirmed that rendered image references line up with
registry contents:

```bash
helm template image-check src/vllm-kv-stack \
  -f src/vllm-kv-stack/values.yaml \
  --set modelVolume.modelSubPath=qwen3-8b \
  --set cacheWarm.enabled=true \
  --set litellm.enabled=true \
  --set boom.enabled=true \
  --set boom.keyAffinityBench=true \
  --set mooncake.enabled=true |
  rg '^\s*image:' | sort -u
```

Important details:

- `docker.io/library/vllm-ascend:v0.18.0` is rewritten by the Helm image helper
  to `reg.local:32000/library/vllm-ascend:v0.18.0`, not
  `reg.local:32000/vllm-ascend:v0.18.0`.
- `cacheWarm.image` is used directly in `templates/99-cache-warm-job.yaml` and
  does not pass through the global image-registry helper. If cache warming is
  enabled and should use the local registry, set it explicitly:

```yaml
cacheWarm:
  image: reg.local:32000/busybox:latest
```

### BZ `switch_cluster` usage

The BZ `switch_cluster` profile is the right place for cluster-local defaults:
endpoints, model paths, image names, image pull policies, and node pinning.

Because the chart images referenced by `src/vllm-kv-stack/values.yaml` have
been rebuilt or mirrored into `reg.local:32000`, the BZ profile can use:

```yaml
helm:
  values:
    global.imageRegistry: "reg.local:32000"
```

If only selected custom images are available in a future cluster or after
changing `values.yaml`, keep:

```yaml
helm:
  values:
    global.imageRegistry: ""
```

and use full references for individual images:

```yaml
helm:
  values:
    images.router: "reg.local:32000/kv-router:latest"
    images.sidecar: "reg.local:32000/kv-sidecar:latest"
```

This lets `switch_cluster: bz` describe the BZ environment without
accidentally rewriting public images that are not yet available in the local
registry.

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

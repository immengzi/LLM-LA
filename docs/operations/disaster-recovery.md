# Disaster Recovery & Incident Runbook

Recovery procedures for cluster-level failures that take down or degrade
llm-la serving. Each section follows **Symptom → Root cause → Diagnosis →
Fix → Prevention**, with copy-paste commands.

For DNS / CNI / kube-proxy failures, see the companion
[k8s-dns-troubleshooting.md](k8s-dns-troubleshooting.md) runbook — it is
referenced from here rather than duplicated.

> **Scope:** This covers operational recovery (scheduling, disk, NPU,
> nodes, crashing pods, stuck routing). It does **not** cover etcd
> backup/restore or full control-plane DR — see [Gaps](#gaps) at the end.

---

## Triage: symptom → section

| Symptom | Likely cause | Go to |
|---------|--------------|-------|
| Pod stuck `Pending`, `FailedScheduling` events | Disk pressure / no NPU / affinity | [§1](#1-disk-pressure), [§2](#2-npu-ascend910-scheduling-failures), [§3](#3-general-pod-scheduling-failures) |
| Pod was `Running`, now `Evicted` / restarted on another node | Node disk pressure or resource eviction | [§1](#1-disk-pressure) |
| `0/N nodes available: ... untolerated taint {disk-pressure}` | Node ran out of disk | [§1](#1-disk-pressure) |
| `Insufficient huawei.com/Ascend910` | NPUs all allocated / leaked | [§2](#2-npu-ascend910-scheduling-failures) |
| Node shows `NotReady` | kubelet/node down | [§4](#4-node-notready--node-lost) |
| Pod `CrashLoopBackOff` | App crash, bad config, port clash | [§5](#5-crashloopbackoff-vllm--sidecar--lws) |
| vLLM OOM / KV cache errors | Memory / `max_model_len` | [§6](#6-npu--memory-oom) |
| Router queue growing, no dispatch | CNI overlay / DNS / dead sidecars | [§7](#7-router-queue-buildup--stuck-dispatch) |
| `reg.local:32000` pulls fail, registry evicted | Registry on disk-pressure node | [§8](#8-registry-eviction) |
| Pods on specific nodes can't resolve DNS | clusterDNS / kube-proxy / Calico | [§9](#9-dns--cni-overlay-failure) |

---

## 1. Disk pressure

**Symptom:**
- Pod stuck `Pending`; a DP/LWS worker like `vllm-minimax-m2-1-1` never schedules
- `kubectl describe pod` shows:
  ```
  Warning  FailedScheduling  default-scheduler  0/8 nodes are available:
  1 node(s) had untolerated taint {node.kubernetes.io/disk-pressure: },
  3 Insufficient huawei.com/Ascend910, 4 node(s) didn't match Pod's node
  affinity/selector. ...
  ```
- A previously-`Running` pod suddenly shows `Evicted` and reschedules elsewhere
- The private registry pod gets evicted (see also [§8](#8-registry-eviction))

**Root cause:**
A node crossed the kubelet disk eviction threshold (default ~85% of the
imagefs/nodefs). kubelet adds the `node.kubernetes.io/disk-pressure:NoSchedule`
taint and may evict `BestEffort`/`Burstable` pods. Any pod that doesn't
tolerate that taint can no longer schedule onto that node — and if it was the
**only** node that satisfied the pod's NPU + affinity requirements, the pod
stays `Pending` forever. Common disk hogs on these nodes: accumulated
container images, vLLM/sidecar logs, `/tmp` model downloads, and journald.

**Diagnosis:**

```bash
# 1. Which node(s) have disk pressure?
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,DISK_PRESSURE:.status.conditions[?(@.type=="DiskPressure")].status'
# Look for DISK_PRESSURE=True

# 2. Confirm the taint
kubectl get node <node> -o jsonpath='{.spec.taints}' ; echo
# Look for: node.kubernetes.io/disk-pressure

# 3. Decode the FailedScheduling message for the stuck pod
kubectl describe pod <pending-pod> -n vllm | sed -n '/Events:/,$p'

# 4. SSH into the affected node and find what filled the disk
ssh <node-ip>
df -h /                              # root / nodefs
df -h /var/lib/containerd            # imagefs (often separate)
du -sh /var/lib/containerd/* 2>/dev/null | sort -h | tail
du -sh /var/log/* 2>/dev/null | sort -h | tail
journalctl --disk-usage
```

**Fix:**

```bash
# === On the disk-pressure node (SSH in) — free space ===

# Prune unused container images (biggest win on these nodes)
crictl rmi --prune
# (Docker-based nodes: docker system prune -af)

# Vacuum journald logs
journalctl --vacuum-size=500M

# Clean stale temp/model downloads (verify nothing live needs them first)
find /tmp -type f -mtime +3 -delete

# Re-check until below threshold (~85%)
df -h / /var/lib/containerd
```

Once usage drops below the eviction threshold, kubelet **automatically
removes** the `disk-pressure` taint (it does not need a restart) and the
`Pending` pod schedules on its own. To confirm / nudge:

```bash
# Watch the taint clear
kubectl get node <node> -o jsonpath='{.spec.taints}' ; echo

# Watch the pod schedule
kubectl get pod <pending-pod> -n vllm -o wide -w
```

If a critical pod must run **now** and you cannot free disk fast enough, move
it to a healthy node (only safe for low-disk pods like the registry — see
[§8](#8-registry-eviction)) or, as a last resort, temporarily tolerate the
taint (NOT recommended for vLLM, which writes logs/KV to disk):

```bash
# Last resort, low-disk pods only:
kubectl patch deployment -n <ns> <deploy> -p \
'{"spec":{"template":{"spec":{"tolerations":[{"key":"node.kubernetes.io/disk-pressure","operator":"Exists","effect":"NoSchedule"}]}}}}'
```

**Prevention:**
- Run `crictl rmi --prune` periodically on NPU nodes; vLLM images are large
  and accumulate across redeploys
- Keep an eye on node disk via `kubectl describe node <node>` (the
  `Allocated resources` + conditions section) or Prometheus node-exporter
- Pin tiny infra pods (registry) to a node with headroom via `nodeSelector`
- Don't co-locate model downloads in `/tmp`; use NFS-backed PVCs

---

## 2. NPU (Ascend910) scheduling failures

**Symptom:**
- `FailedScheduling: ... N Insufficient huawei.com/Ascend910`
- New vLLM / DP-worker pod is `Pending` even though nodes are `Ready`
- Total requested NPUs across pending+running exceeds cluster capacity

**Root cause:**
Either the cluster genuinely has no free Ascend910 devices, or NPUs are
**leaked** — held by terminated/zombie pods or by the device plugin after an
unclean shutdown, so the scheduler sees fewer allocatable devices than are
physically free.

**Diagnosis:**

```bash
# 1. Capacity vs allocatable per node
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,NPU_CAP:.status.capacity.huawei\.com/Ascend910,NPU_ALLOC:.status.allocatable.huawei\.com/Ascend910'

# 2. Who is holding NPUs right now?
kubectl get pods -A -o json | python3 -c '
import json,sys
d=json.load(sys.stdin)
for p in d["items"]:
    for c in p["spec"]["containers"]:
        req=c.get("resources",{}).get("requests",{})
        n=req.get("huawei.com/Ascend910")
        if n: print(p["metadata"]["namespace"], p["metadata"]["name"],
                     c["name"], n, p["status"]["phase"])
'

# 3. Look for stuck/terminating pods still holding devices
kubectl get pods -A | grep -E 'Terminating|Unknown|Error'

# 4. Device plugin health (Ascend)
kubectl get pods -n kube-system | grep -i ascend
kubectl logs -n kube-system <ascend-device-plugin-pod> --tail=40
```

**Fix:**

```bash
# Reclaim NPUs from stuck/terminating pods
kubectl delete pod <stuck-pod> -n <ns> --grace-period=0 --force

# If allocatable < capacity (leaked devices), restart the device plugin
kubectl rollout restart daemonset -n kube-system <ascend-device-plugin>
# then re-check allocatable in step 1

# If genuinely out of NPUs: scale down a lower-priority deployment to free them
kubectl scale statefulset/<other-vllm> -n vllm --replicas=0
```

**Prevention:**
- Avoid orphaned `Terminating` vLLM pods — they hold NPUs until force-deleted
- Track total NPU requests against cluster capacity before scaling up DP
- After any node reboot, verify `allocatable.huawei.com/Ascend910` returns to
  the expected count

---

## 3. General pod scheduling failures

When a pod is `Pending`, the `FailedScheduling` message is a per-reason
tally across all nodes. Decode it before acting:

```bash
kubectl describe pod <pod> -n vllm | sed -n '/Events:/,$p'
```

| Message fragment | Meaning | Section |
|------------------|---------|---------|
| `untolerated taint {node.kubernetes.io/disk-pressure}` | node out of disk | [§1](#1-disk-pressure) |
| `untolerated taint {node.kubernetes.io/not-ready}` | node `NotReady` | [§4](#4-node-notready--node-lost) |
| `Insufficient huawei.com/Ascend910` | no free NPUs | [§2](#2-npu-ascend910-scheduling-failures) |
| `Insufficient cpu` / `Insufficient memory` | no CPU/RAM headroom | scale down or pick another node |
| `didn't match Pod's node affinity/selector` | `nodeSelector`/affinity excludes node | check the workload's affinity rules |
| `had volume node affinity conflict` | PVC bound to a zone/node the pod can't use | [§5](#5-crashloopbackoff-vllm--sidecar--lws) / PVC |
| `pod has unbound immediate PersistentVolumeClaims` | PVC not bound (NFS/PV issue) | see multi-node-setup-guide |

```bash
# Inspect the pod's scheduling constraints
kubectl get pod <pod> -n vllm -o jsonpath='{.spec.nodeSelector}{"\n"}{.spec.affinity}{"\n"}' ; echo
kubectl get pod <pod> -n vllm -o jsonpath='{.spec.tolerations}' ; echo

# What does each node offer?
kubectl describe nodes | grep -A6 'Allocated resources'
```

For PVC/NFS-bound scheduling conflicts, see
[multi-node-setup-guide.md](multi-node-setup-guide.md) (NFS/PV/PVC stability).

---

## 4. Node NotReady / node lost

**Symptom:**
- `kubectl get nodes` shows a node `NotReady`
- Pods on it go `Terminating` / `Unknown`; new pods get
  `untolerated taint {node.kubernetes.io/not-ready:NoExecute}`
- After the 300s eviction toleration, pods are rescheduled elsewhere (if
  capacity + NPUs allow — otherwise they go `Pending`, see [§2](#2-npu-ascend910-scheduling-failures))

**Root cause:**
kubelet on the node stopped reporting (crashed kubelet, node reboot, network
partition, or the node fell over from disk/OOM).

**Diagnosis:**

```bash
kubectl get nodes -o wide
kubectl describe node <node> | sed -n '/Conditions:/,/Addresses:/p'

# SSH in (if reachable) and check kubelet + resources
ssh <node-ip>
systemctl status kubelet --no-pager
journalctl -u kubelet --no-pager | tail -40
df -h / ; free -h ; uptime
```

**Fix:**

```bash
# If kubelet is down but node is otherwise healthy:
ssh <node-ip> systemctl restart kubelet
# Node should return to Ready within ~30s
kubectl get node <node> -w

# If the node is unrecoverable, drain so workloads move off it:
kubectl cordon <node>
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data --force
# After repair, bring it back:
kubectl uncordon <node>
```

> After a node rejoins, re-run the **Post-Change Verification Checklist** in
> [k8s-dns-troubleshooting.md](k8s-dns-troubleshooting.md) — node changes can
> trigger the Calico port-9099 issue (Issue 4 there).

**Prevention:**
- Address disk pressure ([§1](#1-disk-pressure)) early; disk exhaustion is a
  common cause of kubelet instability
- Verify `allocatable.huawei.com/Ascend910` after any node reboot

---

## 5. CrashLoopBackOff (vLLM / sidecar / LWS)

**Symptom:**
- A container restarts repeatedly; pod shows e.g. `1/2 Running` with a high
  restart count, or `CrashLoopBackOff`
- DP/LWS groups may restart together

**Diagnosis:**

```bash
# Per-container readiness, state, and restart counts
kubectl get pod <pod> -n vllm -o jsonpath='{range .status.containerStatuses[*]}{.name}{"\t"}{.ready}{"\t"}{.restartCount}{"\t"}{.state}{"\n"}{end}'

# Logs of the failing container (use the right -c: vllm / kv-sidecar)
kubectl logs <pod> -n vllm -c <container> --tail=80
kubectl logs <pod> -n vllm -c <container> --previous --tail=80   # last crash

# Why was it killed? (OOMKilled, exit code)
kubectl describe pod <pod> -n vllm | sed -n '/Last State/,/Ready/p'
```

**Common causes & fixes:**
- **`OOMKilled`** → see [§6](#6-npu--memory-oom)
- **ZMQ "address already in use" / wrong ranks / `CMAKE_PREFIX_PATH`** (DP+EP
  via LeaderWorkerSet) → see
  [data-parallel-lws.md](../deployment/data-parallel-lws.md) Troubleshooting
- **404 model name / wrong sidecar queue / BooM lookup** → see
  [multi-model.md](../deployment/multi-model.md) Troubleshooting
- **Missing ConfigMap / connectivity / config key suffix** (Mooncake) → see
  [mooncake/glm5-production.md](../deployment/mooncake/glm5-production.md)
  Common Failure Modes
- **Stale image after a code change** → confirm the running image/digest and
  redeploy:
  ```bash
  kubectl get pod <pod> -n vllm -o jsonpath='{.status.containerStatuses[*].imageID}' ; echo
  kubectl rollout restart statefulset/<vllm> -n vllm
  ```

---

## 6. NPU / memory OOM

**Symptom:**
- Container `Last State: Terminated, Reason: OOMKilled`
- vLLM logs show KV-cache allocation failures or out-of-memory during model
  load / long-context requests

**Root cause:**
Model + KV cache exceeds device/host memory. Often triggered by too-large
`max_model_len`, too-high `gpu_memory_utilization`, excessive concurrent
requests, or very long output lengths.

**Diagnosis:**

```bash
kubectl describe pod <pod> -n vllm | grep -i -A3 oom
kubectl logs <pod> -n vllm -c vllm --previous --tail=60 | grep -iE 'oom|cache|memory|kv'
```

**Fix (config-side):** lower `max_model_len`, reduce
`gpu_memory_utilization`, or cap concurrency / `max_num_seqs` in the vLLM
launch args, then redeploy. See
[helm-values.md](../configuration/helm-values.md) for where these are set.
For load-side limits (`max_tokens`, `use_dataset_output_len`) that caused a
real outage, see the incident report
[internal/stability-test-findings.md](../internal/stability-test-findings.md).

**Prevention:**
- Set conservative `max_model_len` for the available NPU memory
- Bound output length in load configs; unbounded generation caused queue
  buildup and timeout loss in the 24h stability test

---

## 7. Router queue buildup / stuck dispatch

**Symptom:**
- `router_central_queue_length` grows without bound; requests queue but never
  dispatch
- Router logs: `ConnectTimeout` / `KVHASH` errors, or `[PushRouter]
  discovered 0 pods: []`
- All vLLM pods look healthy (`2/2 Running`)

**Root cause (most common first):**
1. **Calico overlay down** (stale process on port 9099) breaking pod-to-pod /
   DNS — this is the highest-impact recurring cause after cluster changes
2. **DNS broken** on the router's node (wrong `clusterDNS` / kube-proxy)
3. **Label selector mismatch** — router discovers 0 pods
4. **All sidecars dead/unreachable** — dispatch has nowhere to go

**Diagnosis & Fix:**
These are fully documented in
[k8s-dns-troubleshooting.md](k8s-dns-troubleshooting.md):
- Calico port-9099 → Issue 4
- Wrong `clusterDNS` → Issue 1
- kube-proxy API unreachable → Issue 2
- PushRouter selector mismatch → Issue 3

Quick check before diving in:

```bash
# Router pod and recent logs
kubectl get pods -n vllm -l app=router-service -o wide
kubectl logs -n vllm deploy/router-service --tail=50

# Are sidecars discoverable / healthy?
kubectl get pods -n vllm -l component=vllm -o wide

# End-to-end probe through the router
curl -s http://10.50.156.65:30080/health | python3 -m json.tool
```

After fixing the underlying cause, clear the stuck queue:

```bash
kubectl rollout restart deploy/router-service -n vllm
```

---

## 8. Registry eviction

`reg.local:32000` image pulls fail because the registry pod was evicted from a
disk-pressure node (the registry is tiny but `BestEffort`). This is already
documented — see **"Registry evicted due to disk-pressure"** and **"Eviction
loop"** in [registry.md](registry.md). Summary: free disk on the node
([§1](#1-disk-pressure)) or keep the registry pinned to a node with headroom
via `nodeSelector` (it is currently pinned to node4, `10.50.156.106`).

---

## 9. DNS / CNI overlay failure

Pods on specific nodes can't resolve service names or reach ClusterIPs.
Fully covered in [k8s-dns-troubleshooting.md](k8s-dns-troubleshooting.md)
(wrong `clusterDNS`, broken kube-proxy, Calico port-9099, PushRouter
selector). Start with its **Quick Diagnostic Checklist**.

---

## Cluster Reference

| Component | Value |
|---|---|
| Control-plane node | node3 (`10.50.156.65`) |
| API server | `https://10.50.156.65:6443` |
| Router NodePort (prod) | `30080` (e.g. `http://10.50.156.65:30080`) |
| Production model name | `served-model-minmax` |
| kube-dns ClusterIP | `10.233.0.10` |
| Serving namespaces | `vllm` (prod), `vllm-shadow` (shadow) |
| CoreDNS host | node1 (`7.150.1.218`) |
| Registry (pinned) | node4 (`10.50.156.106`), `reg.local:32000` |
| Known node IPs | node1 `7.150.1.218`, node2 `7.150.5.207`, node7 `7.150.6.33`, node8 `7.150.0.37` |

End-to-end smoke test (prod):

```bash
curl -s http://10.50.156.65:30080/v1/chat/completions \
  -H "Authorization: Bearer ZhongRuanChuangXin!" \
  -H "Content-Type: application/json" \
  -d '{"model":"served-model-minmax","messages":[{"role":"user","content":"Say hi"}],"max_tokens":8}' \
  | python3 -m json.tool
```

---

## §10  Sidecar pulling while vLLM is down (decommissioning)

**Symptom:** Some requests succeed and some fail with connection-refused or
timeout errors, even though the pod appears `Running` and the sidecar is
healthy.  Often happens after disk pressure, OOM, or vLLM CrashLoopBackOff —
the **sidecar keeps pulling work from the router** while vLLM behind it is
unreachable.

**Root cause:** The sidecar pull loop has no dependency on vLLM health.  It
keeps calling `POST /pull` and the router happily assigns work to it.  When
the VLLMWorker tries to forward the request to `localhost:8200`, vLLM is down
and the request fails.

**Automatic protection (after the health-gate fix):**

1. The sidecar now probes `GET {VLLM_URL}/health` every **5 seconds**.
2. If vLLM is unreachable the sidecar **stops pulling** and logs
   `vLLM health check FAILED — pausing pulls until recovery`.
3. The sidecar `/health` endpoint returns **503** with
   `{"status":"vllm_unhealthy"}` so the K8s readiness probe marks the
   container NotReady, which also tells the router's push-mode discovery
   to exclude this pod.
4. Once vLLM recovers, the next probe succeeds and pulling resumes
   automatically — `vLLM is healthy again — resuming pulls`.

**Manual decommissioning (emergency):**

If you need to remove a specific replica immediately:

```bash
# Scale down the unhealthy replica (StatefulSet)
kubectl scale statefulset/<sts-name> -n <ns> --replicas=<N-1>

# Or cordon the node so no new pods land on it
kubectl cordon <node-name>
```

**Prevention:**

- The K8s readiness/liveness probes on the sidecar container now gate on
  vLLM reachability.  A failing vLLM automatically removes the pod from
  the active pool within ~30 seconds (3 failures × 10 s period).
- Monitor the `vLLM health check FAILED` log line or scrape the sidecar
  `/health` status from Prometheus to trigger alerts.
- For disk-pressure scenarios, also follow [§1 Disk pressure](#1-disk-pressure).

---

## Gaps

Not yet documented (no procedure exists in the repo today):

- **etcd backup/restore** and full control-plane disaster recovery
- **Node bare-metal rebuild** / re-join automation beyond
  [cluster-setup.md](cluster-setup.md) + [infra/](../../infra/README.md)
- **Automated alerting** for disk pressure, NPU leaks, and calico-node restart
  spikes (currently manual via Prometheus dashboards)

When you resolve a new incident, add it here in the
**Symptom → Root cause → Diagnosis → Fix → Prevention** format so the next
operator doesn't start from zero.

# Kubernetes DNS Troubleshooting Guide

Documented DNS issues encountered in this cluster and their fixes.

---

## Issue 1: Wrong `clusterDNS` in kubelet config on worker nodes

**Date:** June 2026 (first seen), June 17 2026 (rediscovered with config file path gotcha)

**Symptom:**
- Pods on specific nodes cannot resolve Kubernetes service names (e.g., `router-service`, headless service names)
- Sidecar/vLLM logs show: `Failed to resolve ... ([Errno -3] Temporary failure in name resolution)` or `[vllm-dp-worker] DNS retry 1/30: cannot resolve ...`
- Pods on node3 (control-plane) and node4 work fine
- Only pods scheduled on cloud nodes (1, 2, 5, 6, 7, 8) are affected
- Pod `/etc/resolv.conf` shows wrong nameserver (e.g., `10.96.0.10` or `169.254.25.10`) instead of `10.233.0.10`

**Root cause:**
Kubelet on the affected nodes has the wrong `clusterDNS` value. This has happened with two different wrong values:
- `10.96.0.10` — the default kubeadm DNS IP (seen on node4)
- `169.254.25.10` — the nodelocaldns IP, configured by kubespray but nodelocaldns was never deployed (seen on all cloud nodes 1, 2, 5, 6, 7, 8)

This causes kubelet to inject the wrong nameserver into every pod's `/etc/resolv.conf` on that node.

**Critical gotcha — two kubelet config files:** On kubespray-managed nodes, kubelet reads its config from **`/etc/kubernetes/kubelet-config.yaml`**, NOT `/var/lib/kubelet/config.yaml`. We wasted time fixing the wrong file. Always check which file kubelet actually uses:

```bash
ps aux | grep kubelet | grep -v grep
# Look for: --config=/etc/kubernetes/kubelet-config.yaml
```

**Diagnosis:**

```bash
# 1. Check resolv.conf inside a pod on the broken node
kubectl exec -n vllm <pod-on-broken-node> -c vllm -- cat /etc/resolv.conf
# Look for: nameserver that is NOT 10.233.0.10 ← WRONG

# 2. Compare with a pod on a working node
kubectl exec -n vllm <pod-on-working-node> -c vllm -- cat /etc/resolv.conf
# Should show: nameserver 10.233.0.10  ← CORRECT

# 3. Find which config file kubelet reads (SSH into the broken node)
ps aux | grep kubelet | grep -oP 'config=\S+'
# Typically: /etc/kubernetes/kubelet-config.yaml

# 4. Check the REAL config file
grep -A2 clusterDNS /etc/kubernetes/kubelet-config.yaml
# If it shows anything other than 10.233.0.10, that's the problem

# 5. Also check the other file (may have been "fixed" there but kubelet ignores it)
grep -A2 clusterDNS /var/lib/kubelet/config.yaml

# 6. Find the correct DNS IP for the cluster
kubectl get svc -n kube-system kube-dns
# CLUSTER-IP column shows the correct value (10.233.0.10)

# 7. Check if nodelocaldns exists (if nameserver is 169.254.25.10)
kubectl get pods -n kube-system -l k8s-app=nodelocaldns -o wide
# If "No resources found" but pods have 169.254.25.10, that's the problem
```

**Fix:**

```bash
# On the broken node (SSH in):
# Fix BOTH config files to avoid confusion
sed -i 's/169.254.25.10/10.233.0.10/' /etc/kubernetes/kubelet-config.yaml /var/lib/kubelet/config.yaml
sed -i 's/10.96.0.10/10.233.0.10/' /etc/kubernetes/kubelet-config.yaml /var/lib/kubelet/config.yaml

# Verify the real config
grep -A2 clusterDNS /etc/kubernetes/kubelet-config.yaml
# Should show: 10.233.0.10

# Restart kubelet to pick up the change
systemctl restart kubelet

# After fixing ALL nodes, redeploy pods to get new resolv.conf
kubectl delete pods -n vllm -l component=vllm
```

**Nodes fixed:** node1, node2, node4, node5, node6, node7, node8

**Prevention:**
- When adding new nodes, verify `clusterDNS` in **both** `/etc/kubernetes/kubelet-config.yaml` and `/var/lib/kubelet/config.yaml` matches `10.233.0.10`
- When fixing kubelet config, always check `ps aux | grep kubelet` to find the actual `--config=` path first
- Avoid running kubespray on this cluster as it configures nodelocaldns (`169.254.25.10`) which is not deployed
- After any kubelet config change, always `systemctl restart kubelet` and verify new pods get the correct resolv.conf

---

## Issue 2: kube-proxy broken on worker nodes (API server unreachable)

**Date:** June 2026 (discovered alongside Issue 1)

**Symptom:**
- Same as Issue 1 — pods on certain nodes can't resolve DNS
- `kubectl logs` on kube-proxy shows: `dial tcp 127.0.0.1:6443: connect: connection refused`
- kube-proxy cannot sync iptables rules, so ClusterIP services (including kube-dns) are unreachable from that node

**Root cause:**
The kube-proxy ConfigMap was set to `server: https://127.0.0.1:6443`. This works on:
- Node3 (control-plane): API server runs locally on 127.0.0.1:6443
- Cloud nodes (1,2,5-8): They have a local API proxy (haproxy/nginx) forwarding 127.0.0.1:6443 to the real API server

But node4 has no local API proxy and is not the control-plane, so kube-proxy can't reach the API server.

**Diagnosis:**

```bash
# 1. Check kube-proxy logs on all nodes
for p in $(kubectl get pods -n kube-system -l k8s-app=kube-proxy -o name); do
  echo "--- $p ---"
  kubectl logs -n kube-system "$p" --tail=5 2>&1
done
# Look for: "connection refused" or "address already in use"

# 2. Check what API server URL kube-proxy uses
kubectl get configmap kube-proxy -n kube-system -o yaml | grep server
# If it shows https://127.0.0.1:6443, check which nodes have a local proxy

# 3. On the broken node, verify nothing listens on 6443
ss -tlnp | grep 6443
# Empty = no local API proxy

# 4. Compare with kubelet's config (kubelet usually has the correct IP)
cat /var/lib/kubelet/config.yaml | grep server
# or
ps aux | grep kubelet | grep -oP 'kubeconfig=\S+'
```

**Fix:**

```bash
# Change kube-proxy to use the real API server IP
kubectl get configmap kube-proxy -n kube-system -o yaml | \
  sed 's|server: https://127.0.0.1:6443|server: https://10.50.156.65:6443|g' | \
  kubectl apply -f -

# Restart all kube-proxy pods
kubectl rollout restart daemonset kube-proxy -n kube-system
kubectl rollout status daemonset kube-proxy -n kube-system --timeout=60s

# Verify logs are clean
kubectl logs -n kube-system <new-kube-proxy-pod-on-affected-node> --tail=10
```

**Note:** Using the real API server IP works for ALL nodes (control-plane and workers), so this is safe to apply cluster-wide.

---

## Issue 3: PushRouter label selector mismatch (zero pods discovered)

**Date:** June 2026 (earlier incident)

**Symptom:**
- Router logs show: `[PushRouter] discovered 0 pods: []`
- Requests queue up in the router but never get dispatched to vLLM pods
- All pods appear healthy (Running, 2/2)

**Root cause:**
The router's `LABEL_SELECTOR` was hardcoded to `app=vllm-qwen` in the Helm template, but multi-model deployments create pods with labels like `app=vllm-qwen3-8b`. The selector didn't match.

**Diagnosis:**

```bash
# 1. Check router environment
kubectl exec -n vllm <router-pod> -- env | grep LABEL_SELECTOR
# Shows: app=vllm-qwen

# 2. Check actual pod labels
kubectl get pods -n vllm -l component=vllm --show-labels
# Shows: app=vllm-qwen3-8b (doesn't match)

# 3. Verify with the correct selector
kubectl get pods -n vllm -l component=vllm
# This finds all vLLM pods
```

**Fix:**
Changed `vllm-kv-stack/templates/31-router.yaml` to use dynamic label selector:

```yaml
- name: LABEL_SELECTOR
{{- if .Values.models }}
  value: "component=vllm"
{{- else }}
  value: "app=vllm-qwen"
{{- end }}
```

---

## Issue 4: Calico CNI overlay broken — stale calico-node processes holding port 9099

**Date:** June 17, 2026

**Symptom:**
- Production queue growing indefinitely; router not dispatching requests
- Router logs show `KVHASH` `ConnectTimeout` errors to `vllm-cpu-hash` ClusterIP service
- Sidecars on node3 can't reach the router by service name
- DNS resolution fails from pods on node3: `socket.gaierror: [Errno -3] Temporary failure in name resolution`
- Pod-to-pod traffic on the same node works; cross-node overlay traffic to node1 fails
- Issue started ~10 hours after adding new nodes (node9, node10, node11) to the cluster

**Root cause:**
`calico-node` DaemonSet pods on node1 (and node2, node7, node8) were in `CrashLoopBackOff` (1400-1500+ restarts). The crash reason was `bind: address already in use` on port **9099** — a stale/orphaned `calico-node` process from a previous container was holding the port, preventing the new calico-node pod from starting.

Since CoreDNS runs on **node1**, and calico-node on node1 was broken, the Calico overlay tunnel from node3 → node1 was down. This meant no pod on node3 could reach CoreDNS via its pod IP, breaking all DNS resolution from node3 (where router, redis, cpu-hash infrastructure pods run).

**Why adding new nodes caused this:**
When nodes 9, 10, and 11 were added to the cluster, the Calico DaemonSet rolled out `calico-node` pods to those new nodes. This triggered Calico's BGP mesh reconfiguration across the entire cluster — every existing `calico-node` had to update its peer list and tunnel configuration for the new nodes. On nodes 1, 2, 7, and 8, this reconfiguration caused `calico-node` to restart. During the restart, the old `calico-node` process did not fully terminate (likely due to a race condition or signal handling bug in the container runtime), leaving an orphaned process holding port 9099. When the new `calico-node` container attempted to start, it couldn't bind to port 9099 and immediately crashed, entering `CrashLoopBackOff`. The exponential backoff meant that after 1400-1500+ failed attempts, Kubernetes was waiting several minutes between retries, making it appear as if calico was permanently broken. The Prometheus `router_central_queue_length` graph confirmed the sharp increase started approximately 10 hours after the new nodes were added, matching the timeline exactly.

**Diagnosis steps performed:**

```bash
# 1. Confirmed router queue growing via Prometheus metric: router_central_queue_length
# Graph showed sharp increase starting ~10 hours after new nodes were added

# 2. Checked router logs — ConnectTimeout to KVHASH service
kubectl logs -n vllm deploy/router-service --tail=50

# 3. Tested pod-to-pod connectivity from router pod on node3
# Same-node (router → redis by pod IP): WORKS
kubectl exec -n vllm deploy/router-service -- python3 -c \
  "import socket; s=socket.socket(); s.settimeout(2); s.connect(('<redis-pod-ip>', 6379)); print('ok')"

# Cross-node to node1 CoreDNS pod IP: FAILS (timeout)
kubectl exec -n vllm deploy/router-service -- python3 -c \
  "import socket; s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(2); s.sendto(b'\\x00', ('172.16.166.138', 53))"

# Cross-node to node2 vLLM by host IP (hostNetwork): WORKS
# This proved the issue was overlay-specific (pod IP routing), not general networking

# 4. Checked calico-node pods
kubectl get pods -n kube-system -l k8s-app=calico-node -o wide
# node1: CrashLoopBackOff  0/1  1504 restarts  ← CRITICAL (CoreDNS here)
# node2: CrashLoopBackOff  0/1  1465 restarts
# node7: CrashLoopBackOff  0/1  1492 restarts
# node8: CrashLoopBackOff  0/1  1488 restarts
# node3,4,5,6: Running 1/1 ← OK

# 5. Checked calico-node logs on node1
kubectl logs -n kube-system <calico-node-pod-on-node1> --tail=20
# Shows: bind: address already in use (port 9099)

# 6. SSH'd into node1 and found the stale process
ssh 7.150.1.218
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=146636,fd=7))
# PID 146636 — orphaned calico-node process from old container
```

**Fix applied — all 4 affected nodes (node1, node2, node7, node8):**

The same 3-step procedure was applied to each node. Node1 was fixed first since it hosts CoreDNS (the critical path for DNS).

```bash
# === STEP 1: SSH into the node and kill the stale process ===

# Node1 (7.150.1.218) — CRITICAL: CoreDNS runs here
ssh 7.150.1.218
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=146636,fd=7))
kill -9 146636
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=546015,fd=6))  ← new process took over

# Node2 (7.150.5.207)
ssh 7.150.5.207
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=44621,fd=7))
kill -9 44621
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=134755,fd=7))  ← new process took over

# Node7 (7.150.6.33)
ssh 7.150.6.33
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=1705623,fd=7))
kill -9 1705623
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=3272208,fd=7))  ← new process took over

# Node8 (7.150.0.37)
ssh 7.150.0.37
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=1687421,fd=7))
kill -9 1687421
ss -tlnp | grep 9099
# LISTEN  0  1024  127.0.0.1:9099  0.0.0.0:*  users:(("calico-node",pid=1413739,fd=7))  ← new process took over
```

```bash
# === STEP 2: Force pod restart from master (backoff too long after 1500+ restarts) ===

# After killing the stale process, the calico-node pod is still in CrashLoopBackOff
# with massive exponential backoff (minutes between retries). Delete the pod to force
# an immediate fresh start with 0 restarts.

kubectl delete pod -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node1
# pod "calico-node-rckzz" deleted
kubectl delete pod -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node2
# pod "calico-node-zk64z" deleted
kubectl delete pod -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node7
# pod "calico-node-svxgx" deleted
kubectl delete pod -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node8
# pod "calico-node-pmpbl" deleted
```

```bash
# === STEP 3: Verify each node came up healthy (1/1 Running, 0 restarts) ===

kubectl get pods -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node1
# calico-node-q84qh  1/1  Running  0  67s  ✓

kubectl get pods -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node2
# calico-node-h5jv2  1/1  Running  0  2m52s  ✓

kubectl get pods -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node7
# calico-node-wpcck  1/1  Running  0  75s  ✓

kubectl get pods -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=node8
# calico-node-6qwsq  1/1  Running  0  18s  ✓
```

```bash
# === STEP 4: Verify DNS recovery ===

# After fixing node1 (CoreDNS host), overlay tunnel re-established immediately.
# Direct pod IP connectivity to CoreDNS recovered first:
kubectl exec -n vllm deploy/router-service -- python3 -c \
  "import socket; s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(2); s.sendto(b'\x00', ('172.16.166.138', 53)); print('reachable'); s.close()"
# reachable ✓

# Full DNS resolution working:
kubectl exec -n vllm deploy/router-service -- python3 -c \
  "import socket; print(socket.getaddrinfo('kubernetes.default.svc.cluster.local', 443))"
# [(<AddressFamily.AF_INET: 2>, ... ('10.233.0.1', 443))] ✓

# NOTE: After killing the stale process on node1 but BEFORE deleting the pod,
# DNS was still failing. This is because the calico-node pod was stuck in
# CrashLoopBackOff with long backoff delay. Only after deleting the pod
# (forcing immediate restart) did the overlay tunnel fully recover.
```

```bash
# === STEP 5: Restart router to clear stuck queue ===
kubectl rollout restart deploy/router-service -n vllm

# === STEP 6: Verify production end-to-end ===
curl -s http://10.50.156.65:30080/v1/chat/completions \
  -H "Authorization: Bearer ZhongRuanChuangXin!" \
  -H "Content-Type: application/json" \
  -d '{"model":"served-model-minmax","messages":[{"role":"user","content":"Say hi"}],"max_tokens":8}' \
  | python3 -m json.tool
# Response received successfully with completion_tokens_details ✓

# === STEP 7: Verify kube-proxy is healthy on all nodes ===
kubectl get pods -n kube-system -l k8s-app=kube-proxy -o wide
# All 8 nodes: Running 1/1 ✓
```

**Additional context — new nodes removed:**
Before fixing calico, nodes 9, 10, and 11 (the newly added nodes that triggered this incident) were drained and removed from the cluster to prevent future issues:

```bash
kubectl drain node9 --ignore-daemonsets --delete-emptydir-data
kubectl drain node10 --ignore-daemonsets --delete-emptydir-data
kubectl drain node11 --ignore-daemonsets --delete-emptydir-data
kubectl delete node node9
kubectl delete node node10
kubectl delete node node11
```

This did NOT fix the calico issue on existing nodes (the stale processes were already orphaned), but prevents the same trigger from recurring when those nodes rejoin.

**Prevention:**
- Before adding new nodes to the cluster, ensure all `calico-node` pods on existing nodes are healthy (`1/1 Running`)
- After adding nodes, immediately check: `kubectl get pods -n kube-system -l k8s-app=calico-node -o wide`
- If any `calico-node` enters `CrashLoopBackOff` with `bind: address already in use`, SSH into that node and run `ss -tlnp | grep 9099` to find and `kill -9` the stale process, then `kubectl delete pod` to force a clean restart
- Consider adding a preStop hook or init container that kills any existing process on port 9099 before calico-node starts
- Monitor `calico-node` restart counts in Prometheus/alerting — a sudden spike in restarts across multiple nodes is a strong signal of this issue
- Keep this document (`docs/k8s-dns-troubleshooting.md`) accessible to all cluster operators — see GitHub link: `https://github.com/LA-Boom/llm-la/blob/main/docs/k8s-dns-troubleshooting.md`

---

## Quick Diagnostic Checklist

When pods on specific nodes can't resolve DNS or reach services:

```bash
# 1. Which nodes are affected?
kubectl get pods -n vllm -o wide
# Note which nodes have broken vs working pods

# 2. Test DNS from a broken pod
kubectl exec -n vllm <broken-pod> -c vllm -- python3 -c \
  "import socket; print(socket.getaddrinfo('router-service', 8080))"

# 3. Check resolv.conf (is the nameserver correct?)
kubectl exec -n vllm <broken-pod> -c vllm -- cat /etc/resolv.conf
# Should show: nameserver 10.233.0.10

# 4. Check calico-node pods (Issue 4 — most common cause after cluster changes)
kubectl get pods -n kube-system -l k8s-app=calico-node -o wide
# ALL should be 1/1 Running. If any are CrashLoopBackOff:
#   a) SSH into the affected node
#   b) ss -tlnp | grep 9099
#   c) kill -9 <stale-pid>
#   d) kubectl delete pod -n kube-system -l k8s-app=calico-node --field-selector spec.nodeName=<node>
#   e) Wait 30s, verify 1/1 Running

# 5. Check kube-proxy on the affected node
kubectl logs -n kube-system <kube-proxy-pod-on-affected-node> --tail=10
# Look for "connection refused" or "address already in use"

# 6. Check kubelet clusterDNS on the affected node (SSH in)
# IMPORTANT: Check the REAL config file first (Issue 5)
grep -A2 clusterDNS /etc/kubernetes/kubelet-config.yaml
grep -A2 clusterDNS /var/lib/kubelet/config.yaml
# BOTH should show: 10.233.0.10
# If they differ, the real one is whichever kubelet uses: ps aux | grep kubelet | grep config=

# 7. Check iptables rules for kube-dns on the affected node (SSH in)
sudo iptables -t nat -L KUBE-SERVICES 2>/dev/null | grep kube-dns
# Should show KUBE-SVC rules for kube-dns

# 8. Direct DNS test from the affected node (SSH in)
nslookup kubernetes.default.svc.cluster.local 10.233.0.10

# 9. Test overlay connectivity (distinguish DNS vs CNI issues)
# From a pod, try reaching CoreDNS by pod IP directly:
kubectl exec -n vllm deploy/router-service -- python3 -c \
  "import socket; s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(2); s.sendto(b'\x00', ('172.16.166.138', 53)); print('reachable')"
# If this fails but same-node pod traffic works → broken Calico overlay (Issue 4)
# If this works but DNS by name fails → kube-proxy iptables issue (Issue 2)
```

## Post-Change Verification Checklist for llm-la

After **any** cluster-level change (adding/removing nodes, upgrading Kubernetes components, changing CNI config, modifying kube-proxy, etc.), run through this checklist to make sure llm-la is still working. This should be done immediately after the change, not hours later.

### 1. Cluster infrastructure health

```bash
# All nodes should be Ready
kubectl get nodes -o wide

# All calico-node pods should be 1/1 Running with low restart counts
kubectl get pods -n kube-system -l k8s-app=calico-node -o wide

# All kube-proxy pods should be 1/1 Running
kubectl get pods -n kube-system -l k8s-app=kube-proxy -o wide

# CoreDNS pods should be Running
kubectl get pods -n kube-system -l k8s-app=kube-dns -o wide
```

### 2. DNS resolution from application pods

```bash
# Test DNS from the router pod (runs on node3, needs cross-node overlay to CoreDNS on node1)
kubectl exec -n vllm deploy/router-service -- python3 -c \
  "import socket; print(socket.getaddrinfo('kubernetes.default.svc.cluster.local', 443))"
# Should return a list with 10.233.0.1 — if it hangs or errors, DNS is broken
```

### 3. llm-la application pods

```bash
# All vLLM pods should be Running and Ready
kubectl get pods -n vllm -o wide

# Router should be Running
kubectl get pods -n vllm -l app=router-service

# Check router queue is not growing (should be 0 or near 0)
curl -s http://10.50.156.65:30080/health | python3 -m json.tool
```

### 4. End-to-end inference test

```bash
# Send a quick test request through the router
curl -s http://10.50.156.65:30080/v1/chat/completions \
  -H "Authorization: Bearer ZhongRuanChuangXin!" \
  -H "Content-Type: application/json" \
  -d '{"model":"served-model-minmax","messages":[{"role":"user","content":"Say hi"}],"max_tokens":8}' \
  | python3 -m json.tool
# Should return a valid chat completion response within a few seconds
```

### 5. Prometheus metrics (if available)

Check the `router_central_queue_length` metric in Prometheus/Grafana. It should be flat near 0. A sudden spike after a cluster change means something broke.

### What to do if something fails

- If calico-node is CrashLoopBackOff: see Issue 4 above (stale process on port 9099)
- If DNS fails: follow the Quick Diagnostic Checklist above
- If kube-proxy is broken: see Issue 2 above
- If pods can't resolve DNS on specific nodes only: see Issue 1 above (wrong clusterDNS)
- Contact Saeid or Haiting for help identifying which component is affected

---

## Cluster Reference

| Component | Value |
|---|---|
| kube-dns ClusterIP | `10.233.0.10` |
| Service CIDR | `10.233.0.0/16` |
| Control-plane node | node3 (`10.50.156.65`) |
| API server | `https://10.50.156.65:6443` |
| CoreDNS pods | node1 (both replicas, IPs: 172.16.166.137, 172.16.166.138) |
| Pod network CIDR | `172.16.0.0/16` |

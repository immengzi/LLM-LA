# Kubernetes DNS Troubleshooting Guide

Documented DNS issues encountered in this cluster and their fixes.

---

## Issue 1: Wrong `clusterDNS` in kubelet config on worker nodes

**Date:** June 2026

**Symptom:**
- Pods on node4 cannot resolve Kubernetes service names (e.g., `router-service`)
- Sidecar logs show: `Failed to resolve 'router-service' ([Errno -3] Temporary failure in name resolution)`
- Pods on node3 (control-plane) work fine
- Only pods scheduled on specific nodes are affected

**Root cause:**
Node4's kubelet was configured with `clusterDNS: 10.96.0.10` (the default kubeadm DNS IP), while the cluster actually uses `10.233.0.10` (kubespray default). This caused kubelet to inject the wrong nameserver into every pod's `/etc/resolv.conf` on that node.

Node3 worked because its kubelet had the correct `clusterDNS: 10.233.0.10`.

**Diagnosis:**

```bash
# 1. Check resolv.conf inside a pod on the broken node
kubectl exec -n vllm <pod-on-broken-node> -c vllm -- cat /etc/resolv.conf
# Look for: nameserver 10.96.0.10  ← WRONG for this cluster

# 2. Compare with a pod on a working node
kubectl exec -n vllm <pod-on-working-node> -c vllm -- cat /etc/resolv.conf
# Should show: nameserver 10.233.0.10  ← CORRECT

# 3. Confirm by checking kubelet config on both nodes
# On broken node:
cat /var/lib/kubelet/config.yaml | grep -A2 clusterDNS
# Shows: 10.96.0.10

# On working node:
cat /var/lib/kubelet/config.yaml | grep -A2 clusterDNS
# Shows: 10.233.0.10

# 4. Find the correct DNS IP for the cluster
kubectl get svc -n kube-system kube-dns
# CLUSTER-IP column shows the correct value (10.233.0.10)
```

**Fix:**

```bash
# On the broken node (SSH in):
sed -i 's/10.96.0.10/10.233.0.10/' /var/lib/kubelet/config.yaml
systemctl restart kubelet

# Verify:
cat /var/lib/kubelet/config.yaml | grep -A2 clusterDNS
systemctl status kubelet | head -5

# Redeploy pods so they get the correct resolv.conf:
kubectl rollout restart deployment -n vllm <deployment-name>
```

**Prevention:** When adding new nodes to the cluster, always verify `clusterDNS` in `/var/lib/kubelet/config.yaml` matches the kube-dns service ClusterIP.

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

# 4. Check kube-proxy on the affected node
kubectl logs -n kube-system <kube-proxy-pod-on-affected-node> --tail=10
# Look for "connection refused" or "address already in use"

# 5. Check kubelet clusterDNS on the affected node (SSH in)
cat /var/lib/kubelet/config.yaml | grep -A2 clusterDNS
# Should show: 10.233.0.10

# 6. Check iptables rules for kube-dns on the affected node (SSH in)
sudo iptables -t nat -L KUBE-SERVICES 2>/dev/null | grep kube-dns
# Should show KUBE-SVC rules for kube-dns

# 7. Direct DNS test from the affected node (SSH in)
nslookup kubernetes.default.svc.cluster.local 10.233.0.10
```

## Cluster Reference

| Component | Value |
|---|---|
| kube-dns ClusterIP | `10.233.0.10` |
| Service CIDR | `10.233.0.0/16` |
| Control-plane node | node3 (`10.50.156.65`) |
| API server | `https://10.50.156.65:6443` |
| CoreDNS pods | node3 (both replicas) |
| Pod network CIDR | `172.16.0.0/16` |

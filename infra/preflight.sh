#!/usr/bin/env bash
# LA-Boom cluster preflight verifier (read-only).
# Automates the operational checklist in docs/operations/cluster-setup.md §13-14.
# Run from a host with kubectl configured. Exits non-zero if any check fails.
#
#   ./preflight.sh
#
# Override defaults via env:
#   REGISTRY=reg.local:32000 NAMESPACE=vllm NPU_RES=huawei.com/Ascend ./preflight.sh

set -uo pipefail

REGISTRY="${REGISTRY:-reg.local:32000}"
NAMESPACE="${NAMESPACE:-vllm}"
NPU_RES="${NPU_RES:-huawei.com/Ascend}"

pass=0 fail=0
green=$'\033[32m'; red=$'\033[31m'; yellow=$'\033[33m'; reset=$'\033[0m'

ok()   { echo "${green}[ OK ]${reset} $1"; pass=$((pass+1)); }
bad()  { echo "${red}[FAIL]${reset} $1"; fail=$((fail+1)); }
warn() { echo "${yellow}[WARN]${reset} $1"; }

echo "=== LA-Boom cluster preflight ==="

# 1. kubectl reachable
if kubectl version --request-timeout=5s >/dev/null 2>&1; then
  ok "kubectl can reach the API server"
else
  bad "kubectl cannot reach the API server"; echo "Aborting."; exit 1
fi

# 2. all nodes Ready
notready="$(kubectl get nodes --no-headers 2>/dev/null | awk '$2!="Ready"{print $1}')"
if [ -z "$notready" ]; then
  ok "all nodes Ready"
else
  bad "nodes not Ready: $notready"
fi

# 3. NPU resource advertised
npu_total="$(kubectl get nodes -o jsonpath="{range .items[*]}{.status.allocatable.${NPU_RES//./\\.}}{'\n'}{end}" 2>/dev/null | grep -c '[0-9]')"
if [ "${npu_total:-0}" -gt 0 ]; then
  ok "$NPU_RES advertised on $npu_total node(s)"
else
  bad "no node advertises $NPU_RES (Ascend device plugin?)"
fi

# 4. LeaderWorkerSet CRD installed
if kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io >/dev/null 2>&1; then
  ok "LeaderWorkerSet CRD installed"
else
  bad "LeaderWorkerSet CRD missing (apply docs/deployment/lws-manifests.yaml)"
fi

# 5. model PVC bound
pvc_status="$(kubectl -n "$NAMESPACE" get pvc models-nfs-pvc -o jsonpath='{.status.phase}' 2>/dev/null)"
if [ "$pvc_status" = "Bound" ]; then
  ok "models-nfs-pvc is Bound in namespace $NAMESPACE"
else
  bad "models-nfs-pvc not Bound in $NAMESPACE (got: '${pvc_status:-missing}')"
fi

# 6. private registry catalog reachable
if curl -fs --noproxy '*' "http://${REGISTRY}/v2/_catalog" 2>/dev/null | grep -q repositories; then
  ok "registry $REGISTRY reachable"
else
  bad "registry $REGISTRY not reachable / proxy not bypassed"
fi

# 7. NO_PROXY not corrupted (char-split bug)
if systemctl show containerd 2>/dev/null | grep -q "NO_PROXY=\['"; then
  bad "containerd NO_PROXY is the broken char-split form (run: make node)"
else
  ok "containerd NO_PROXY format looks sane"
fi

# 8. containerd certs.d present
if [ -f "/etc/containerd/certs.d/${REGISTRY}/hosts.toml" ]; then
  ok "containerd certs.d hosts.toml present for $REGISTRY"
else
  warn "certs.d hosts.toml not found locally (only checks this host)"
fi

echo "==============================="
echo "Passed: $pass  Failed: $fail"
[ "$fail" -eq 0 ] || exit 1

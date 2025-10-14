#!/usr/bin/env bash
set -euo pipefail

# install-llmd-gateway.sh
# Installs llm-d on an existing Kubernetes cluster using Gateway API (no Istio),
# passes the Hugging Face token via environment variable HF_TOKEN,
# and patches node_exporter to use a non-conflicting port.

# Usage:
#   ./install-llmd-gateway.sh <HF_TOKEN>
# Example:
#   ./install-llmd-gateway.sh hf_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# ----- required input -----
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <HF_TOKEN>"
  exit 1
fi
export HF_TOKEN="$1"

# ----- basic settings -----
LLMD_NS="llm-d"
GATEWAY='kgateway'
LLMD_REPO="https://github.com/llm-d-incubation/llm-d-infra.git"
LLMD_DIR="$HOME/llm-lb/llm-d-infra"
NODE_EXPORTER_PORT="19100"

echo "[llm-d] Using HF_TOKEN from argument (exported for the installer)."
echo "[llm-d] Target namespace: ${LLMD_NS}"
echo "[llm-d] node_exporter will use port ${NODE_EXPORTER_PORT} instead of 9100."

# ----- fetch repo -----
echo "[llm-d] Cloning llm-d-infra..."
rm -rf "${LLMD_DIR}"
git clone "${LLMD_REPO}" "${LLMD_DIR}"

# ----- prerequisites -----
echo "[llm-d] Installing prerequisites..."
pushd "${LLMD_DIR}/quickstart" >/dev/null
chmod +x ./install-deps.sh
./install-deps.sh

# ----- Gateway API (instead of Istio) -----
echo "[llm-d] Installing Gateway API CRDs..."
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.1/standard-install.yaml

# ----- node_exporter patch in parallel -----
(
  echo "[llm-d] Waiting for node_exporter DaemonSet to appear..."
  until kubectl -n llm-d-monitoring get ds prometheus-prometheus-node-exporter >/dev/null 2>&1; do
    sleep 2
  done
  echo "[llm-d] Patching node_exporter to use port ${NODE_EXPORTER_PORT}..."
  kubectl -n llm-d-monitoring patch ds prometheus-prometheus-node-exporter \
    --type='json' \
    -p="[
      {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",
       \"value\":[\"--path.procfs=/host/proc\",\"--path.sysfs=/host/sys\",\"--path.rootfs=/host\",\"--web.listen-address=:${NODE_EXPORTER_PORT}\"]}
    ]"
  echo "[llm-d] Restarting node_exporter DaemonSet..."
  kubectl -n llm-d-monitoring rollout restart ds prometheus-prometheus-node-exporter
) &

# ----- install llm-d with Gateway ingress type -----
echo "[llm-d] Running installer with Gateway API..."
chmod +x ./llmd-infra-installer.sh
./llmd-infra-installer.sh --namespace "${LLMD_NS}" --gateway "${GATEWAY}" -r infra-inference-scheduling

popd >/dev/null

# ----- verify -----
echo "[llm-d] Verifying deployment..."
kubectl get ns "${LLMD_NS}" || true
kubectl get pods -n "${LLMD_NS}" || true
kubectl get gatewayclasses --all-namespaces || true
kubectl get gateways --all-namespaces || true

echo "[llm-d] Installation complete (Gateway API mode with patched node_exporter port)."

# ----- start port-forwards in background -----
echo "[llm-d] Starting Prometheus port-forward on :19090..."
kubectl port-forward -n llm-d-monitoring --address 0.0.0.0 \
  svc/prometheus-kube-prometheus-prometheus 19090:9090 >/tmp/prometheus-port-forward.log 2>&1 &

echo "[llm-d] Starting Grafana port-forward on :3000..."
kubectl port-forward -n llm-d-monitoring --address 0.0.0.0 \
  svc/prometheus-grafana 3000:80 >/tmp/grafana-port-forward.log 2>&1 &

echo "[llm-d] Port-forwards running in background."
echo "       Prometheus: http://YOUR_IP:19090"
echo "       Grafana:    http://YOUR_IP:3000 (default admin/admin)"


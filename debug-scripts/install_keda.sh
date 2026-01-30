#!/usr/bin/env bash
set -euo pipefail

# ---- configurable knobs ----
KEDA_NAMESPACE="${KEDA_NAMESPACE:-keda}"
KEDA_RELEASE="${KEDA_RELEASE:-keda}"

# Optionally pin a chart version (recommended for reproducibility)
# Example: export KEDA_CHART_VERSION="2.18.3"
KEDA_CHART_VERSION="${KEDA_CHART_VERSION:-}"

echo "==> Preflight: checking tools..."
command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }
command -v helm   >/dev/null 2>&1 || { echo "ERROR: helm not found"; exit 1; }

echo "==> Adding KEDA Helm repo..."
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

echo "==> Creating namespace (if needed): ${KEDA_NAMESPACE}"
kubectl get ns "${KEDA_NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${KEDA_NAMESPACE}"

echo "==> Installing/Upgrading KEDA..."
if [[ -n "${KEDA_CHART_VERSION}" ]]; then
  helm upgrade --install "${KEDA_RELEASE}" kedacore/keda \
    --namespace "${KEDA_NAMESPACE}" \
    --create-namespace \
    --version "${KEDA_CHART_VERSION}"
else
  helm upgrade --install "${KEDA_RELEASE}" kedacore/keda \
    --namespace "${KEDA_NAMESPACE}" \
    --create-namespace
fi

echo "==> Waiting for KEDA deployments to become Ready..."
kubectl rollout status deployment/keda-operator -n "${KEDA_NAMESPACE}" --timeout=180s || true

# Some chart versions also deploy this; if it exists, wait for it too
if kubectl get deploy -n "${KEDA_NAMESPACE}" | grep -q "keda-operator-metrics-apiserver"; then
  kubectl rollout status deployment/keda-operator-metrics-apiserver -n "${KEDA_NAMESPACE}" --timeout=180s || true
fi

echo "==> KEDA pods:"
kubectl get pods -n "${KEDA_NAMESPACE}" -o wide

echo "==> Checking KEDA CRDs:"
kubectl get crd | grep -E 'keda\.sh' || true

echo "==> Checking External Metrics API (what HPA uses via KEDA):"
# This endpoint should exist after KEDA is installed; it may return a list or require auth,
# but the presence of the API is the key check.
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1" | head -c 300 || true
echo
echo "==> Done. Next step: create a ScaledObject."

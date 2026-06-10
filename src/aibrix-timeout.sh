#!/usr/bin/env bash
# =============================================================================
# fix_aibrix_timeout.sh
#
# Applies (or re-applies) the AIBrix 300s timeout fix by patching the
# existing original_route in Envoy via EnvoyPatchPolicy, then verifies
# that the patch is live in the running Envoy config.
#
# Usage:
#   chmod +x fix_aibrix_timeout.sh
#   ./fix_aibrix_timeout.sh
# =============================================================================

set -euo pipefail

POLICY_NAME="aibrix-original-route-timeout-replace"
NAMESPACE="aibrix-system"
ENVOY_NS="envoy-gateway-system"
ENVOY_POD_PATTERN="envoy-aibrix-system-aibrix-eg"
ADMIN_PORT=19000
CONFIG_DUMP_FILE="envoy-config-check.json"
TIMEOUT_VALUE="3600s"

# Terminal colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log()    { echo -e "${GREEN}[+]${NC} $*"; }
warn()   { echo -e "${YELLOW}[!]${NC} $*"; }
error()  { echo -e "${RED}[✗]${NC} $*"; }
ok()     { echo -e "${GREEN}[✓]${NC} $*"; }

# =============================================================================
# 0. Pre-flight checks
# =============================================================================
log "Running pre-flight checks..."

if ! command -v kubectl &>/dev/null; then
    error "kubectl not found. Please install it and ensure it is in PATH."
    exit 1
fi

if ! command -v curl &>/dev/null; then
    error "curl not found. Please install it."
    exit 1
fi

if ! kubectl cluster-info &>/dev/null; then
    error "Cannot reach the Kubernetes cluster. Check your kubeconfig."
    exit 1
fi

ok "Pre-flight checks passed."

# =============================================================================
# 1. Remove any existing (potentially incorrect) patch
# =============================================================================
log "Removing any existing EnvoyPatchPolicy (if present)..."

if kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" &>/dev/null; then
    kubectl delete envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}"
    ok "Deleted existing policy '${POLICY_NAME}'."
else
    warn "No existing policy found — skipping delete."
fi

# =============================================================================
# 2. Apply the correct patch
# =============================================================================
log "Applying EnvoyPatchPolicy '${POLICY_NAME}' in namespace '${NAMESPACE}'..."

# NOTE: both ops use "add" — not "replace".
# "replace" fails with "missing key" if the field does not already exist in the
# route (which is the case for a freshly installed AIBrix).  "add" both creates
# the key when absent AND overwrites it when present, so it is safe in all cases.
kubectl apply -f - <<EOF
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyPatchPolicy
metadata:
  name: ${POLICY_NAME}
  namespace: ${NAMESPACE}
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: Gateway
    name: aibrix-eg
  type: JSONPatch
  jsonPatches:

  - name: ${NAMESPACE}/aibrix-eg/http
    type: type.googleapis.com/envoy.config.route.v3.RouteConfiguration
    operation:
      op: add
      path: /virtual_hosts/0/routes/0/route/timeout
      value: "${TIMEOUT_VALUE}"

  - name: ${NAMESPACE}/aibrix-eg/http
    type: type.googleapis.com/envoy.config.route.v3.RouteConfiguration
    operation:
      op: add
      path: /virtual_hosts/0/routes/0/route/idle_timeout
      value: "${TIMEOUT_VALUE}"
EOF

ok "EnvoyPatchPolicy applied."

# =============================================================================
# 3. Wait for the policy to be accepted/programmed
# =============================================================================
log "Waiting for EnvoyPatchPolicy to be accepted and programmed (up to 30s)..."

ACCEPTED=""
PROGRAMMED=""
for i in $(seq 1 30); do
    ACCEPTED=$(kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.ancestors[0].conditions[?(@.type=="Accepted")].status}' 2>/dev/null || true)
    PROGRAMMED=$(kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.ancestors[0].conditions[?(@.type=="Programmed")].status}' 2>/dev/null || true)

    if [[ "${ACCEPTED}" == "True" && "${PROGRAMMED}" == "True" ]]; then
        ok "Policy is Accepted and Programmed."
        break
    fi

    if [[ $i -eq 30 ]]; then
        if [[ "${ACCEPTED}" == "True" && "${PROGRAMMED}" == "False" ]]; then
            PROG_MSG=$(kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" \
                -o jsonpath='{.status.ancestors[0].conditions[?(@.type=="Programmed")].message}' 2>/dev/null || true)
            error "Policy was Accepted but failed to program. Controller message:"
            echo "  ${PROG_MSG}"
            echo ""
            error "This usually means a JSON patch op ('replace' vs 'add') does not match"
            error "the actual route structure. Inspect the route in the config dump below."
        else
            warn "Policy status after 30s — Accepted=${ACCEPTED} Programmed=${PROGRAMMED}"
            warn "Proceeding anyway; check the config dump for the actual route values."
        fi
    fi
    sleep 1
done

# =============================================================================
# 4. Dump live Envoy config via admin API
# =============================================================================
log "Looking for Envoy pod matching pattern '${ENVOY_POD_PATTERN}'..."

ENVOY_POD=$(kubectl get pods -n "${ENVOY_NS}" -o name \
    | grep "${ENVOY_POD_PATTERN}" \
    | head -n1 \
    | cut -d/ -f2 || true)

if [[ -z "${ENVOY_POD}" ]]; then
    error "No Envoy pod found matching '${ENVOY_POD_PATTERN}' in namespace '${ENVOY_NS}'."
    error "Available pods:"
    kubectl get pods -n "${ENVOY_NS}" || true
    exit 1
fi

ok "Found Envoy pod: ${ENVOY_POD}"

log "Starting port-forward to pod admin interface (port ${ADMIN_PORT})..."
kubectl port-forward -n "${ENVOY_NS}" "pod/${ENVOY_POD}" "${ADMIN_PORT}:${ADMIN_PORT}" \
    >/dev/null 2>&1 &
PF_PID=$!

# Ensure port-forward is cleaned up on exit
trap 'kill "${PF_PID}" 2>/dev/null; wait "${PF_PID}" 2>/dev/null || true' EXIT

sleep 2  # give port-forward time to establish

log "Dumping Envoy config to '${CONFIG_DUMP_FILE}'..."

if ! curl -sf "http://127.0.0.1:${ADMIN_PORT}/config_dump" -o "${CONFIG_DUMP_FILE}"; then
    error "curl failed — port-forward may not be ready. Retrying once after 3s..."
    sleep 3
    if ! curl -sf "http://127.0.0.1:${ADMIN_PORT}/config_dump" -o "${CONFIG_DUMP_FILE}"; then
        error "Could not reach Envoy admin API. Check that the pod is running and port ${ADMIN_PORT} is correct."
        exit 1
    fi
fi

ok "Config dump saved to '${CONFIG_DUMP_FILE}'."

# =============================================================================
# 5. Inspect the route and verify the fix
# =============================================================================
log "Inspecting 'original_route' in config dump..."
echo "----------------------------------------------------------------------"
grep -n -C 20 '"name": "original_route"' "${CONFIG_DUMP_FILE}" || {
    warn "'original_route' not found in config dump. Raw route names present:"
    grep -o '"name": "[^"]*route[^"]*"' "${CONFIG_DUMP_FILE}" | sort -u || true
}
echo "----------------------------------------------------------------------"

log "Checking for expected timeout values in original_route..."

# Extract the original_route block for inspection.
# In the Envoy config dump the "name": "original_route" key appears at the
# BOTTOM of the route object, so timeout/idle_timeout are ABOVE it — use -B.
ROUTE_BLOCK=$(grep -B 30 '"name": "original_route"' "${CONFIG_DUMP_FILE}" || true)

TIMEOUT_OK=false
IDLE_TIMEOUT_OK=false

if echo "${ROUTE_BLOCK}" | grep -q "\"timeout\": \"${TIMEOUT_VALUE}\""; then
    TIMEOUT_OK=true
fi
if echo "${ROUTE_BLOCK}" | grep -q "\"idle_timeout\": \"${TIMEOUT_VALUE}\""; then
    IDLE_TIMEOUT_OK=true
fi

EXT_PROC_FOUND=$(grep -c "ext_proc" "${CONFIG_DUMP_FILE}" || true)

echo ""
echo "======================================================================="
echo " Verification Summary"
echo "======================================================================="

if $TIMEOUT_OK; then
    ok "timeout is set to ${TIMEOUT_VALUE} on original_route."
else
    ACTUAL_TIMEOUT=$(echo "${ROUTE_BLOCK}" | grep '"timeout"' | head -1 || true)
    error "timeout NOT set to ${TIMEOUT_VALUE} on original_route."
    [[ -n "${ACTUAL_TIMEOUT}" ]] && warn "  Actual value found: ${ACTUAL_TIMEOUT}"
fi

if $IDLE_TIMEOUT_OK; then
    ok "idle_timeout is set to ${TIMEOUT_VALUE} on original_route."
else
    error "idle_timeout NOT set to ${TIMEOUT_VALUE} on original_route."
fi

if [[ "${EXT_PROC_FOUND}" -gt 0 ]]; then
    ok "ext_proc filter is present — AIBrix routing plugin is still active."
else
    error "ext_proc filter NOT found — the AIBrix routing filter may have been removed."
fi

if $TIMEOUT_OK && $IDLE_TIMEOUT_OK && [[ "${EXT_PROC_FOUND}" -gt 0 ]]; then
    echo ""
    ok "All checks passed. The 300s timeout fix is live."
else
    echo ""
    error "One or more checks failed. See above for details."
    error "Re-run this script, or inspect '${CONFIG_DUMP_FILE}' manually."
fi

echo ""
log "EnvoyPatchPolicy status:"
kubectl get envoypatchpolicy -n "${NAMESPACE}" || true

echo ""
log "Done. Config dump is available at: ${CONFIG_DUMP_FILE}"
echo "======================================================================="
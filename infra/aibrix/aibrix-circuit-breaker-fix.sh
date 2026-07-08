#!/usr/bin/env bash
# =============================================================================
# aibrix-circuit-breaker-fix.sh
#
# Diagnoses and fixes Envoy circuit breaker overflow on
# original_destination_cluster, which causes ConnectionResetError(104)
# under high-concurrency long-running requests.
#
# Root cause:
#   Envoy default max_connections=1024 is hit when your method holds more
#   simultaneous connections open than the baseline. Envoy resets the
#   overflow connections with ECONNRESET.
#
# Fix:
#   Add a circuit_breakers block to original_destination_cluster with
#   limits high enough to never be the bottleneck.
#
# Usage:
#   chmod +x infra/aibrix/aibrix-circuit-breaker-fix.sh
#   ./infra/aibrix/aibrix-circuit-breaker-fix.sh
# =============================================================================

set -euo pipefail

POLICY_NAME="aibrix-circuit-breaker-fix"
NAMESPACE="aibrix-system"
ENVOY_NS="envoy-gateway-system"
ENVOY_POD_PATTERN="envoy-aibrix-system-aibrix-eg"
ADMIN_PORT=19000
CONFIG_DUMP_FILE="envoy-config-check.json"

MAX_CONNECTIONS=100000
MAX_PENDING_REQUESTS=100000
MAX_REQUESTS=100000

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()     { echo -e "${GREEN}[+]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
error()   { echo -e "${RED}[✗]${NC} $*"; }
ok()      { echo -e "${GREEN}[✓]${NC} $*"; }
section() { echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${CYAN}  $*${NC}"
            echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

PF_PID=""

start_port_forward() {
    local POD
    POD=$(kubectl get pods -n "${ENVOY_NS}" -o name \
        | grep "${ENVOY_POD_PATTERN}" \
        | head -n1 \
        | cut -d/ -f2 || true)

    if [[ -z "${POD}" ]]; then
        error "No Envoy pod found matching '${ENVOY_POD_PATTERN}' in '${ENVOY_NS}'"
        error "Available pods:"
        kubectl get pods -n "${ENVOY_NS}" || true
        exit 1
    fi
    ok "Envoy pod: ${POD}"

    kubectl port-forward -n "${ENVOY_NS}" "pod/${POD}" \
        "${ADMIN_PORT}:${ADMIN_PORT}" >/dev/null 2>&1 &
    PF_PID=$!
    sleep 2

    if ! curl -sf "http://127.0.0.1:${ADMIN_PORT}/ready" >/dev/null; then
        warn "Admin API not ready, retrying once after 3s..."
        sleep 3
        if ! curl -sf "http://127.0.0.1:${ADMIN_PORT}/ready" >/dev/null; then
            error "Envoy admin API unreachable on port ${ADMIN_PORT}"
            exit 1
        fi
    fi
    ok "Admin API reachable"
}

stop_port_forward() {
    if [[ -n "${PF_PID}" ]]; then
        kill "${PF_PID}" 2>/dev/null || true
        wait "${PF_PID}" 2>/dev/null || true
        PF_PID=""
    fi
}

dump_config() {
    curl -sf "http://127.0.0.1:${ADMIN_PORT}/config_dump" -o "${CONFIG_DUMP_FILE}"
    ok "Config dumped to ${CONFIG_DUMP_FILE}"
}

# =============================================================================
# 0. Pre-flight
# =============================================================================
section "0. Pre-flight checks"

for cmd in kubectl curl; do
    if ! command -v "$cmd" &>/dev/null; then
        error "$cmd not found"
        exit 1
    fi
done

if ! kubectl cluster-info &>/dev/null; then
    error "Cannot reach Kubernetes cluster. Check your kubeconfig."
    exit 1
fi

ok "Pre-flight passed"

# =============================================================================
# 1. Diagnose BEFORE state
# =============================================================================
section "1. Diagnosing current state (BEFORE patch)"

log "Starting port-forward..."
start_port_forward
trap 'stop_port_forward' EXIT

dump_config

echo ""
echo "--- original_destination_cluster block ---"
grep -A 20 '"original_destination_cluster"' "${CONFIG_DUMP_FILE}" \
    || warn "original_destination_cluster not found in config dump"

echo ""
echo "--- circuit_breakers (if explicitly set) ---"
grep -A 20 '"original_destination_cluster"' "${CONFIG_DUMP_FILE}" \
    | grep -A 15 "circuit_breakers" \
    || warn "No circuit_breakers block found — Envoy defaults are in effect (max_connections=1024)"

echo ""
echo "--- Overflow counters (the smoking gun) ---"
OVERFLOW=$(curl -sf "http://127.0.0.1:${ADMIN_PORT}/stats" \
    | grep "original_destination_cluster.*overflow" || true)

if [[ -z "${OVERFLOW}" ]]; then
    warn "No overflow stats found"
else
    echo "${OVERFLOW}"
    CX_OVERFLOW=$(echo "${OVERFLOW}" \
        | grep "upstream_cx_overflow" \
        | awk -F': ' '{print $2}' || echo "0")
    if [[ "${CX_OVERFLOW}" -gt 0 ]]; then
        error "upstream_cx_overflow=${CX_OVERFLOW} — circuit breaker is tripping and causing ECONNRESET"
    else
        ok "upstream_cx_overflow=0 — no connection overflow detected yet"
        warn "Run this script during/after a load test for live overflow counts"
    fi
fi

stop_port_forward
trap - EXIT

# =============================================================================
# 2. Remove existing patch (if any)
# =============================================================================
section "2. Removing existing patch (if any)"

if kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" &>/dev/null; then
    kubectl delete envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}"
    ok "Deleted existing policy '${POLICY_NAME}'"
else
    warn "No existing policy found — skipping delete"
fi

# =============================================================================
# 3. Apply the circuit breaker patch
# =============================================================================
section "3. Applying circuit breaker patch"

# Strategy: add a full circuit_breakers block to the cluster.
# op=add works whether the field exists or not (creates or overwrites).
# The cluster resource type is envoy.config.cluster.v3.Cluster.
# The name field matches the exact cluster name in the config dump.

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

  - name: original_destination_cluster
    type: type.googleapis.com/envoy.config.cluster.v3.Cluster
    operation:
      op: add
      path: /circuit_breakers
      value:
        thresholds:
          - priority: DEFAULT
            max_connections: ${MAX_CONNECTIONS}
            max_pending_requests: ${MAX_PENDING_REQUESTS}
            max_requests: ${MAX_REQUESTS}
EOF

ok "EnvoyPatchPolicy submitted"

# =============================================================================
# 4. Wait for Accepted + Programmed
# =============================================================================
section "4. Waiting for policy to be Accepted and Programmed (up to 30s)"

ACCEPTED=""
PROGRAMMED=""
for i in $(seq 1 30); do
    ACCEPTED=$(kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.ancestors[0].conditions[?(@.type=="Accepted")].status}' \
        2>/dev/null || true)
    PROGRAMMED=$(kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.ancestors[0].conditions[?(@.type=="Programmed")].status}' \
        2>/dev/null || true)

    if [[ "${ACCEPTED}" == "True" && "${PROGRAMMED}" == "True" ]]; then
        ok "Policy is Accepted and Programmed"
        break
    fi

    if [[ $i -eq 30 ]]; then
        if [[ "${ACCEPTED}" == "True" && "${PROGRAMMED}" == "False" ]]; then
            PROG_MSG=$(kubectl get envoypatchpolicy "${POLICY_NAME}" -n "${NAMESPACE}" \
                -o jsonpath='{.status.ancestors[0].conditions[?(@.type=="Programmed")].message}' \
                2>/dev/null || true)
            error "Policy Accepted but failed to Program. Controller message:"
            echo "  ${PROG_MSG}"
            echo ""
            error "The JSON patch path or resource type may differ in your Envoy Gateway version."
            error "Inspect the cluster block and adjust accordingly:"
            echo "  grep -A 30 '\"original_destination_cluster\"' ${CONFIG_DUMP_FILE}"
        else
            warn "Policy status after 30s — Accepted=${ACCEPTED} Programmed=${PROGRAMMED}"
            warn "Proceeding to verify config dump anyway..."
        fi
    fi
    sleep 1
done

# =============================================================================
# 5. Verify AFTER state
# =============================================================================
section "5. Verifying circuit breaker state (AFTER patch)"

log "Starting port-forward for post-patch verification..."
start_port_forward
trap 'stop_port_forward' EXIT

dump_config

echo ""
echo "--- original_destination_cluster block (after patch) ---"
grep -A 30 '"original_destination_cluster"' "${CONFIG_DUMP_FILE}" \
    || warn "original_destination_cluster not found"

echo ""
echo "--- Checking new circuit_breaker values ---"
CLUSTER_BLOCK=$(grep -A 30 '"original_destination_cluster"' "${CONFIG_DUMP_FILE}" || true)

CONN_OK=false
PEND_OK=false
REQS_OK=false

echo "${CLUSTER_BLOCK}" | grep -q "\"max_connections\": ${MAX_CONNECTIONS}"         && CONN_OK=true
echo "${CLUSTER_BLOCK}" | grep -q "\"max_pending_requests\": ${MAX_PENDING_REQUESTS}" && PEND_OK=true
echo "${CLUSTER_BLOCK}" | grep -q "\"max_requests\": ${MAX_REQUESTS}"               && REQS_OK=true

echo ""
echo "--- Overflow counters after patch ---"
curl -sf "http://127.0.0.1:${ADMIN_PORT}/stats" \
    | grep "original_destination_cluster.*overflow" \
    || warn "No overflow stats found"

stop_port_forward
trap - EXIT

# =============================================================================
# 6. Summary
# =============================================================================
section "6. Verification Summary"

$CONN_OK \
    && ok "max_connections      = ${MAX_CONNECTIONS}" \
    || error "max_connections NOT updated — patch path may be wrong for your Envoy Gateway version"

$PEND_OK \
    && ok "max_pending_requests = ${MAX_PENDING_REQUESTS}" \
    || error "max_pending_requests NOT updated"

$REQS_OK \
    && ok "max_requests         = ${MAX_REQUESTS}" \
    || error "max_requests NOT updated"

echo ""
if $CONN_OK && $PEND_OK && $REQS_OK; then
    ok "All circuit breaker limits raised successfully."
    echo ""
    echo "  Re-run your load test and then check overflow counters:"
    echo "    kubectl port-forward -n ${ENVOY_NS} pod/\$(kubectl get pods -n ${ENVOY_NS} -o name | grep ${ENVOY_POD_PATTERN} | head -n1 | cut -d/ -f2) 19000:19000 &"
    echo "    curl -s http://127.0.0.1:19000/stats | grep 'original_destination_cluster.*overflow'"
    echo ""
    echo "  upstream_cx_overflow should remain 0 throughout the run."
else
    error "One or more checks failed. Inspect the cluster block manually:"
    echo "  grep -A 30 '\"original_destination_cluster\"' ${CONFIG_DUMP_FILE}"
    echo ""
    echo "  Then adjust the patch path in this script and re-run."
fi

echo ""
log "EnvoyPatchPolicy status:"
kubectl get envoypatchpolicy -n "${NAMESPACE}"

echo ""
log "Config dump available at: ${CONFIG_DUMP_FILE}"
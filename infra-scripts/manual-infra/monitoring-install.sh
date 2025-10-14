#!/usr/bin/env bash
set -euo pipefail

# Minimal Prometheus + monitoring stack installer with a vLLM PodMonitor,
# then patches Prometheus & Grafana Services to NodePort so you can open them in a browser.
#
# Default action: install
# Optional: --uninstall (or -u) to remove what this script installed.
#
# Flags:
#   -n, --namespace  <name>   Observability namespace (default: observability)
#   -r, --release    <name>   Helm release name       (default: prometheus)
#   -h, --help                Show help
#
# Env overrides (for PodMonitor):
#   TARGET_NS=vllm             # namespace where vLLM pods run
#   APP_LABEL=vllm-qwen        # pod label to match (metadata.labels.app)
#   PORT_NAME=http             # named port exposing /metrics
#   SCRAPE_INTERVAL=15s        # scrape interval
#
# Env overrides (NodePort defaults set to earlier values you used):
#   PROM_HTTP_NODEPORT=31190       # Prometheus UI (9090) NodePort
#   PROM_RELOADER_NODEPORT=30934   # Prometheus reloader (8080) NodePort
#   GRAFANA_NODEPORT=31300         # Grafana UI (80 -> 3000) NodePort
#
# Examples:
#   ./monitoring.sh
#   PROM_HTTP_NODEPORT=31190 GRAFANA_NODEPORT=31300 ./monitoring.sh
#   ./monitoring.sh --uninstall

ACTION="install"
OBS_NS="observability"
RELEASE="prometheus"

USE_GPU="false"
USE_NPU="true"

TARGET_NS="${TARGET_NS:-vllm}"
APP_LABEL="${APP_LABEL:-vllm-qwen}"
PORT_NAME="${PORT_NAME:-http}"
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-15s}"
TIMEOUT="600s"

# Default NodePorts to previously used values
PROM_HTTP_NODEPORT="${PROM_HTTP_NODEPORT:-31190}"
PROM_RELOADER_NODEPORT="${PROM_RELOADER_NODEPORT:-30934}"
GRAFANA_NODEPORT="${GRAFANA_NODEPORT:-31300}"

log() { printf "➡️  %s\n" "$*"; }
ok()  { printf "✅ %s\n" "$*"; }
err() { printf "❌ %s\n" "$*" >&2; }

usage() {
  sed -n '1,200p' "$0" | sed 's/^# \{0,1\}//' | sed '/^$/q'
}

check_cmd() {
  command -v "$1" &>/dev/null || { err "Required command not found: $1"; exit 1; }
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --uninstall|-u) ACTION="uninstall"; shift ;;
      -n|--namespace) OBS_NS="${2:?}"; shift 2 ;;
      -r|--release)   RELEASE="${2:?}"; shift 2 ;;
      -h|--help)      usage; exit 0 ;;
      *) err "Unknown option: $1"; usage; exit 1 ;;
    esac
  done
}

install_stack() {
  # Validate mutually exclusive options
  if [[ "$USE_GPU" == "true" && "$USE_NPU" == "true" ]]; then
    log "ERROR: Cannot set both USE_GPU and USE_NPU to true simultaneously"
    return 1
  fi

  # Log the hardware configuration being used
  if [[ "$USE_GPU" == "true" ]]; then
    log "Installing with GPU monitoring support (NVIDIA dcgm-exporter)"
  elif [[ "$USE_NPU" == "true" ]]; then
    log "Installing with NPU monitoring support"
  else
    log "Installing basic monitoring stack (no GPU/NPU specific components)"
  fi

  # Ensure the required namespaces exist
  log "Ensuring namespaces: ${OBS_NS} (observability) and ${TARGET_NS} (target app)"
  kubectl create namespace "${OBS_NS}"    --dry-run=client -o yaml | kubectl apply -f -
  kubectl create namespace "${TARGET_NS}" --dry-run=client -o yaml | kubectl apply -f -

  # Add and update the Helm repository for Prometheus
  log "Adding/Updating Helm repo: prometheus-community"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
  helm repo update 1>/dev/null

  # Create a temporary file for Helm values
  log "Preparing kube-prometheus-stack values (Grafana enabled, cross-namespace discovery)"
  TMP_VALUES="$(mktemp)"
  trap 'rm -f "${TMP_VALUES}"' EXIT
  cat >"${TMP_VALUES}" <<EOF
grafana:
  enabled: true
  adminPassword: admin
  service:
    type: ClusterIP

alertmanager:
  service:
    type: ClusterIP

prometheus:
  service:
    type: ClusterIP
  prometheusSpec:
    serviceMonitorSelectorNilUsesHelmValues: false
    serviceMonitorSelector: {}
    serviceMonitorNamespaceSelector: {}
    podMonitorSelectorNilUsesHelmValues: false
    podMonitorSelector: {}
    podMonitorNamespaceSelector: {}
    maximumStartupDurationSeconds: 300
EOF

  # Install or upgrade the kube-prometheus-stack chart
  log "Installing/Upgrading kube-prometheus-stack (release: ${RELEASE}) to namespace ${OBS_NS}"
  if [[ "$USE_NPU" == "true" ]]; then
    # NPU version uses specific chart version
    helm upgrade --install "${RELEASE}" prometheus-community/kube-prometheus-stack \
      --namespace "${OBS_NS}" \
      --version "45.7.1" \
      --values "${TMP_VALUES}"
  else
    # GPU or default version uses upgrade -i syntax
    helm upgrade -i "${RELEASE}" prometheus-community/kube-prometheus-stack \
      -n "${OBS_NS}" \
      -f "${TMP_VALUES}"
  fi

  # Wait for core components to be ready
  log "Waiting for Prometheus & Grafana pods to be Ready (timeout ${TIMEOUT})"
  kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=prometheus -n "${OBS_NS}" --timeout="${TIMEOUT}" || true
  kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=grafana     -n "${OBS_NS}" --timeout="${TIMEOUT}" || true

  # Wait for PodMonitor CRD to be available
  log "Waiting for PodMonitor CRD to be available"
  for _ in {1..120}; do
    kubectl get crd podmonitors.monitoring.coreos.com &>/dev/null && break
    sleep 2
  done

  # Apply PodMonitor for application monitoring
  log "Applying PodMonitor for pods labeled app=${APP_LABEL} in namespace ${TARGET_NS} (port: ${PORT_NAME}, interval: ${SCRAPE_INTERVAL})"
  cat <<EOF | kubectl apply -f -
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: ${APP_LABEL}
  namespace: ${TARGET_NS}
spec:
  selector:
    matchLabels:
      app: ${APP_LABEL}
  podMetricsEndpoints:
    - path: /metrics
      port: ${PORT_NAME}
      interval: ${SCRAPE_INTERVAL}
EOF

  ok "Installed. Prometheus will scrape pods with app=${APP_LABEL} in ${TARGET_NS}. Grafana admin password: admin"

  # Patch Services to NodePort (Prometheus & Grafana)
  patch_services_nodeport

}


# Build a JSON object for a port with optional nodePort field.
# args: <name> <port> <targetPort> <maybe_nodePort>
_build_port_json() {
  local _name="$1" _port="$2" _tport="$3" _nport="${4:-}"
  if [[ -n "${_nport}" ]]; then
    printf '{"name":"%s","port":%s,"targetPort":%s,"protocol":"TCP","nodePort":%s}' \
      "${_name}" "${_port}" "${_tport}" "${_nport}"
  else
    printf '{"name":"%s","port":%s,"targetPort":%s,"protocol":"TCP"}' \
      "${_name}" "${_port}" "${_tport}"
  fi
}

patch_services_nodeport() {
  # Prometheus service -> NodePort 31190 (9090) and 30934 (8080)
  log "Patching Service prometheus-kube-prometheus-prometheus to NodePort…"
  prom_http_port_json=$(_build_port_json "http-web" 9090 9090 "${PROM_HTTP_NODEPORT}")
  prom_reload_port_json=$(_build_port_json "reloader-web" 8080 8080 "${PROM_RELOADER_NODEPORT}")
  cat > /tmp/prom-svc-patch.json <<JSON
{
  "spec": {
    "type": "NodePort",
    "ports": [ ${prom_http_port_json}, ${prom_reload_port_json} ]
  }
}
JSON
  kubectl -n "${OBS_NS}" patch svc prometheus-kube-prometheus-prometheus -p "$(cat /tmp/prom-svc-patch.json)"

  # Grafana service -> NodePort 31300 (80 -> 3000)
  log "Patching Service prometheus-grafana to NodePort…"
  graf_port_json=$(_build_port_json "service" 80 3000 "${GRAFANA_NODEPORT}")
  cat > /tmp/grafana-svc-patch.json <<JSON
{
  "spec": {
    "type": "NodePort",
    "ports": [ ${graf_port_json} ]
  }
}
JSON
  kubectl -n "${OBS_NS}" patch svc prometheus-grafana -p "$(cat /tmp/grafana-svc-patch.json)"

  # Show final ports
  local prom_http_np prom_rel_np graf_np
  prom_http_np=$(kubectl -n "${OBS_NS}" get svc prometheus-kube-prometheus-prometheus -o jsonpath='{.spec.ports[?(@.port==9090)].nodePort}')
  prom_rel_np=$(kubectl -n "${OBS_NS}" get svc prometheus-kube-prometheus-prometheus -o jsonpath='{.spec.ports[?(@.port==8080)].nodePort}')
  graf_np=$(kubectl -n "${OBS_NS}" get svc prometheus-grafana -o jsonpath='{.spec.ports[?(@.port==80)].nodePort}')
  ok "Prometheus UI -> NodePort: ${prom_http_np}  (http://<node-ip>:${prom_http_np})"
  ok "Prometheus reloader -> NodePort: ${prom_rel_np}"
  ok "Grafana UI -> NodePort: ${graf_np}  (http://<node-ip>:${graf_np})"
}

uninstall_stack() {
  log "Deleting PodMonitor '${APP_LABEL}' from namespace '${TARGET_NS}' (if present)"
  kubectl delete podmonitor "${APP_LABEL}" -n "${TARGET_NS}" --ignore-not-found

  log "Uninstalling Helm release '${RELEASE}' from namespace '${OBS_NS}'"
  helm uninstall "${RELEASE}" -n "${OBS_NS}" || true

  ok "Uninstall complete. (Namespaces left intact.)"
}

main() {
  check_cmd kubectl
  check_cmd helm
  parse_args "$@"

  if ! kubectl cluster-info &>/dev/null; then
    err "kubectl cannot reach a running Kubernetes cluster."
    exit 1
  fi

  case "${ACTION}" in
    install)   install_stack ;;
    uninstall) uninstall_stack ;;
    *) err "Unknown action: ${ACTION}"; exit 1 ;;
  esac
}

main "$@"

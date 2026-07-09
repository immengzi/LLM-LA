#!/usr/bin/env bash
#
# rolling_restart_vllm.sh — safe, one-group-at-a-time restart of the vLLM
# LeaderWorkerSet (DP) engines, WITHOUT re-running sweep_methods.py.
#
# Why this exists:
#   The engines run as a LeaderWorkerSet with RecreateGroupOnPodRestart, so the
#   restart unit is a whole DP GROUP (leader + its worker), not a single pod.
#   This script restarts one group at a time, keeps the other group(s) serving,
#   and waits for full readiness + /health before moving on.
#
#   Everything survives a restart automatically now, including the per-pod
#   NodePort service `vllm-pp-<pod>`: it selects on the built-in
#   `statefulset.kubernetes.io/pod-name` label (controller-managed), so it
#   re-attaches on its own — the script only verifies the endpoint came back.
#     - PodMonitor scraping (keys off the template label component=vllm)
#     - router / boom / prometheus / litellm / mooncake-master Services (stable)
#     - NDS mounts, xds paths, nodeSelectors, sidecar, env (in the pod template)
#     - node labels (vllm-role=*) and inotify sysctls (node-scoped)
#     - sidecar->router registration and engine->mooncake handshake (self-heal)
#   In-memory LMCache is wiped and re-warms (expected); NDS on-disk spill persists.
#
# Usage:
#   src/client/rolling_restart_vllm.sh                 # restart all groups of model=minimax-m2 in ns vllm
#   NS=vllm SELECTOR='model=minimax-m2' src/client/rolling_restart_vllm.sh
#   DRY_RUN=true src/client/rolling_restart_vllm.sh    # print what it would do, change nothing
#   GROUPS='1 0' src/client/rolling_restart_vllm.sh    # restart specific group-indexes, in this order
#   FORCE=true src/client/rolling_restart_vllm.sh      # skip the confirmation prompt
#
# Env knobs (all optional):
#   NS               namespace                       (default: vllm)
#   SELECTOR         label selector for the model    (default: model=minimax-m2)
#   HEALTH_PORT      in-container health port         (default: 8200)
#   HEALTH_PATH      in-container health path         (default: /health)
#   VLLM_CONTAINER   engine container name            (default: vllm)
#   READY_TIMEOUT    seconds to wait per group        (default: 1800)
#   POLL_INTERVAL    seconds between readiness polls   (default: 10)
#   SETTLE_AFTER     seconds to pause after a group is healthy (default: 20)
#   GROUPS           space-separated group-indexes    (default: auto-discovered)
#   DRY_RUN          true|false                       (default: false)
#   FORCE            true|false skip prompt            (default: false)

set -euo pipefail

NS="${NS:-vllm}"
SELECTOR="${SELECTOR:-model=minimax-m2}"
HEALTH_PORT="${HEALTH_PORT:-8200}"
HEALTH_PATH="${HEALTH_PATH:-/health}"
VLLM_CONTAINER="${VLLM_CONTAINER:-vllm}"
READY_TIMEOUT="${READY_TIMEOUT:-1800}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"
SETTLE_AFTER="${SETTLE_AFTER:-20}"
DRY_RUN="${DRY_RUN:-false}"
FORCE="${FORCE:-false}"

LWS_WORKER_IDX_LABEL="leaderworkerset.sigs.k8s.io/worker-index"
LWS_GROUP_IDX_LABEL="leaderworkerset.sigs.k8s.io/group-index"

# jsonpath-escaped versions (dots in the key must be escaped)
JP_GROUP_IDX='leaderworkerset\.sigs\.k8s\.io/group-index'

c_reset=$'\033[0m'; c_bold=$'\033[1m'; c_grn=$'\033[32m'; c_yel=$'\033[33m'; c_red=$'\033[31m'; c_blu=$'\033[36m'

log()  { echo "${c_blu}[$(date +%H:%M:%S)]${c_reset} $*"; }
ok()   { echo "${c_grn}[$(date +%H:%M:%S)] OK ${c_reset} $*"; }
warn() { echo "${c_yel}[$(date +%H:%M:%S)] WARN${c_reset} $*"; }
err()  { echo "${c_red}[$(date +%H:%M:%S)] ERR ${c_reset} $*" >&2; }

kc() { kubectl -n "$NS" "$@"; }

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "   ${c_yel}DRY-RUN>${c_reset} kubectl -n $NS $*"
  else
    kubectl -n "$NS" "$@"
  fi
}

require() { command -v "$1" >/dev/null 2>&1 || { err "'$1' not found in PATH"; exit 1; }; }

require kubectl

# ---------------------------------------------------------------------------
# Discover the leader pods (worker-index=0 => one per DP group) and their groups
# ---------------------------------------------------------------------------
log "Namespace=${c_bold}$NS${c_reset}  selector=${c_bold}$SELECTOR${c_reset}"

_leaders_jp='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.'"$JP_GROUP_IDX"'}{"\n"}{end}'
mapfile -t LEADERS < <(
  kc get pods -l "$SELECTOR,$LWS_WORKER_IDX_LABEL=0" \
    -o jsonpath="$_leaders_jp" \
    2>/dev/null | sort -t $'\t' -k2 -n
)

if [[ "${#LEADERS[@]}" -eq 0 ]]; then
  err "No leader pods found for selector '$SELECTOR' with $LWS_WORKER_IDX_LABEL=0 in ns '$NS'."
  err "Check: kubectl -n $NS get pods -l '$SELECTOR' --show-labels"
  exit 1
fi

declare -A LEADER_OF_GROUP=()
DISCOVERED_GROUPS=()
for line in "${LEADERS[@]}"; do
  pod="${line%%$'\t'*}"
  grp="${line##*$'\t'}"
  [[ -z "$pod" || -z "$grp" ]] && continue
  LEADER_OF_GROUP["$grp"]="$pod"
  DISCOVERED_GROUPS+=("$grp")
done

# Order of groups to process
if [[ -n "${GROUPS:-}" ]]; then
  read -r -a ORDER <<< "$GROUPS"
else
  ORDER=("${DISCOVERED_GROUPS[@]}")
fi

log "Discovered ${c_bold}${#DISCOVERED_GROUPS[@]}${c_reset} group(s):"
for grp in "${DISCOVERED_GROUPS[@]}"; do
  echo "    group $grp -> leader ${LEADER_OF_GROUP[$grp]}"
done
log "Restart order: ${c_bold}${ORDER[*]}${c_reset}"

if [[ "${#DISCOVERED_GROUPS[@]}" -lt 2 ]]; then
  warn "Only one group found — restarting it means a full outage for this model (no HA)."
fi

if [[ "$FORCE" != "true" && "$DRY_RUN" != "true" ]]; then
  read -r -p "Proceed with rolling restart? [y/N] " ans
  [[ "$ans" =~ ^[Yy]$ ]] || { log "Aborted."; exit 0; }
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Count pods in a group and how many are Running+Ready.
# Prints "<ready> <total>".
group_ready_counts() {
  local grp="$1" total=0 ready=0 phase cond
  while IFS=$'\t' read -r name phase cond; do
    [[ -z "$name" ]] && continue
    total=$((total + 1))
    if [[ "$phase" == "Running" && "$cond" == "True" ]]; then
      ready=$((ready + 1))
    fi
  done < <(
    kc get pods -l "$SELECTOR,$LWS_GROUP_IDX_LABEL=$grp" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}{end}' \
      2>/dev/null
  )
  echo "$ready $total"
}

pod_uid() {
  kc get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true
}

# Wait until the leader pod has been recreated (new UID) AND the whole group is
# Running+Ready at the expected member count.
wait_group_recreated_ready() {
  local grp="$1" leader="$2" old_uid="$3" expected="$4"
  local deadline=$(( $(date +%s) + READY_TIMEOUT ))
  local seen_recreated=false

  while true; do
    local now; now=$(date +%s)
    if (( now > deadline )); then
      err "Timeout (${READY_TIMEOUT}s) waiting for group $grp to come back ready."
      kc get pods -l "$SELECTOR,$LWS_GROUP_IDX_LABEL=$grp" -o wide || true
      return 1
    fi

    local new_uid; new_uid="$(pod_uid "$leader")"
    if [[ "$seen_recreated" == "false" ]]; then
      if [[ -n "$new_uid" && "$new_uid" != "$old_uid" ]]; then
        seen_recreated=true
        log "  leader $leader recreated (uid ${old_uid:0:8}.. -> ${new_uid:0:8}..)"
      fi
    fi

    if [[ "$seen_recreated" == "true" ]]; then
      read -r ready total < <(group_ready_counts "$grp")
      log "  group $grp readiness: ${ready}/${total} (expect ${expected})"
      if [[ "$total" -eq "$expected" && "$ready" -eq "$expected" ]]; then
        return 0
      fi
    fi
    sleep "$POLL_INTERVAL"
  done
}

health_check() {
  local leader="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "   ${c_yel}DRY-RUN>${c_reset} exec $leader -c $VLLM_CONTAINER -- curl -sf localhost:${HEALTH_PORT}${HEALTH_PATH}"
    return 0
  fi
  local code
  code="$(kc exec "$leader" -c "$VLLM_CONTAINER" -- \
    curl -s -o /dev/null -w '%{http_code}' "localhost:${HEALTH_PORT}${HEALTH_PATH}" 2>/dev/null || true)"
  if [[ "$code" == "200" ]]; then
    ok "  /health on $leader returned 200"
    return 0
  fi
  warn "  /health on $leader returned '${code:-<none>}' (Ready probe already passed; continuing)"
  return 0
}

relabel_leader() {
  local leader="$1"
  # No manual re-label needed anymore: the per-pod NodePort service selects on
  # the built-in `statefulset.kubernetes.io/pod-name` label, which the controller
  # re-applies automatically on restart. We just verify the endpoint re-attached.
  local svc="vllm-pp-${leader}"; svc="${svc:0:63}"
  if [[ "$DRY_RUN" != "true" ]]; then
    local eps
    eps="$(kc get endpoints "$svc" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)"
    if [[ -n "$eps" ]]; then
      ok "  per-pod service $svc endpoint(s) re-attached automatically: $eps"
    else
      warn "  per-pod service $svc has no endpoint yet (may just need a few seconds)."
    fi
  fi
}

# ---------------------------------------------------------------------------
# Main loop: one group at a time
# ---------------------------------------------------------------------------
total_groups="${#ORDER[@]}"
idx=0
for grp in "${ORDER[@]}"; do
  idx=$((idx + 1))
  leader="${LEADER_OF_GROUP[$grp]:-}"
  if [[ -z "$leader" ]]; then
    err "No leader found for group '$grp' — skipping."
    continue
  fi

  echo
  log "${c_bold}=== [$idx/$total_groups] Restarting group $grp (leader $leader) ===${c_reset}"

  read -r _pre_ready pre_total < <(group_ready_counts "$grp")
  if [[ "$pre_total" -eq 0 ]]; then
    warn "Group $grp currently has 0 pods; skipping."
    continue
  fi
  log "  group $grp has $pre_total pod(s); expecting $pre_total back after restart"

  old_uid="$(pod_uid "$leader")"

  # Deleting the leader triggers RecreateGroupOnPodRestart -> whole group recreates.
  log "  deleting leader pod (recreates the whole group)"
  run delete pod "$leader" --wait=false

  if [[ "$DRY_RUN" == "true" ]]; then
    log "  DRY-RUN: would wait for group $grp ready, health-check, and re-label."
    continue
  fi

  if ! wait_group_recreated_ready "$grp" "$leader" "$old_uid" "$pre_total"; then
    err "Group $grp did not become ready. STOPPING before touching any other group."
    err "Investigate:  kubectl -n $NS describe pod $leader"
    exit 1
  fi
  ok "  group $grp is ${pre_total}/${pre_total} Running+Ready"

  health_check "$leader"
  relabel_leader "$leader"

  if (( idx < total_groups )); then
    log "  settling ${SETTLE_AFTER}s before next group..."
    sleep "$SETTLE_AFTER"
  fi
done

echo
ok "${c_bold}Rolling restart complete.${c_reset} All targeted groups restarted one at a time."
log "Sanity check:"
echo "    kubectl -n $NS get pods -l '$SELECTOR' -o wide"
for grp in "${ORDER[@]}"; do
  leader="${LEADER_OF_GROUP[$grp]:-}"
  [[ -n "$leader" ]] && echo "    kubectl -n $NS get endpoints vllm-pp-${leader}"
done

#!/usr/bin/env bash
#
# Collect native-crash evidence for the dyn-pd wake crash (corrupted size vs.
# prev_size). Run ON the remote dyn-pd host:
#
#   ssh dynpd 'bash -s' < scripts/pd-timeshare-phase0/collect_core_evidence.sh
#
# Output: /workspace/mengzi/logs/core-evidence-<timestamp>/
# (cores, gdb backtraces when possible, pod logs, /dev/shm + ipcs, dmesg,
#  npu-smi, and a README.txt summarizing what succeeded / failed)
#
# Read-only with respect to the cluster: it does not modify pods or the chart.
# It only reads logs, copies core files from known hostPath locations, and
# writes into /workspace/mengzi/logs.
#
set -u

NS="${NS:-dyn-pd}"
export KUBECONFIG="${KUBECONFIG:-/workspace/mengzi/kubeconfig}"
OUT_BASE="/workspace/mengzi/logs"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="${OUT_BASE}/core-evidence-${TS}"
CORE_DIRS="/cores /workspace/mengzi/cores /var/lib/dynpd-cores"

mkdir -p "${OUT}"
echo "OUT=${OUT}" | tee "${OUT}/README.txt"

{
    echo "collected at: $(date -Is)"
    echo "host: $(hostname)"
    echo "---- pod inventory ----"
    kubectl get pods -n "${NS}" -o wide 2>&1 || true
    echo "---- deployment ----"
    kubectl get deployment -n "${NS}" 2>&1 || true
} >> "${OUT}/README.txt"

# 1) core files
for d in ${CORE_DIRS}; do
    [ -d "$d" ] || continue
    find "$d" -maxdepth 2 -type f -name 'core*' -newermt '-48 hours' -exec ls -lh {} \; \
        >> "${OUT}/README.txt" 2>&1 || true
    find "$d" -maxdepth 2 -type f -name 'core*' -newermt '-48 hours' \
        -exec cp -n {} "${OUT}/" \; 2>/dev/null || true
done
ls -la "${OUT}" >/dev/null

# 2) gdb backtrace for each copied core (host gdb; container gdb is tried via
#    the pods only when a core is copied into the container -- see below)
if command -v gdb >/dev/null 2>&1; then
    for core in "${OUT}"/core*; do
        [ -f "$core" ] || continue
        # The core embeds container-side binary paths; host gdb often cannot
        # open them, so this is best-effort. The container-side attempt below
        # is the reliable path when the image has gdb.
        file "$core" >> "${OUT}/README.txt" 2>&1 || true
        gdb -batch -ex "set pagination off" -ex "thread apply all bt full" \
            -c "$core" > "${OUT}/bt-$(basename "$core").txt" 2>&1 || true
    done
else
    echo "no host gdb; backtraces skipped" >> "${OUT}/README.txt"
fi

# 3) pod logs (crash + previous container)
for pod in $(kubectl get pods -n "${NS}" --no-headers -o custom-columns=:.metadata.name 2>/dev/null | grep vllm-qwen-pd || true); do
    for c in vllm-prefill vllm-decode; do
        kubectl logs -n "${NS}" "${pod}" -c "${c}" --tail=3000 \
            > "${OUT}/log-${pod}-${c}.txt" 2>&1 || true
        kubectl logs -n "${NS}" "${pod}" -c "${c}" --previous --tail=3000 \
            > "${OUT}/log-${pod}-${c}-previous.txt" 2>&1 || true
    done
    # The last [camem] wake_up remap line, to correlate with the crash.
    grep -h "\[camem\] wake_up" "${OUT}"/log-${pod}*.txt 2>/dev/null | tail -5 \
        > "${OUT}/camem-wakeup-${pod}.txt" 2>&1 || true
done

# 4) shared-memory / semaphore leak inventory
{
    echo "---- /dev/shm ----"
    ls -la /dev/shm 2>&1 || true
    echo "count: $(ls /dev/shm 2>/dev/null | wc -l)"
    echo "---- ipcs ----"
    ipcs -m -s -p 2>&1 || true
} >> "${OUT}/README.txt"

# 5) host + device state
{
    echo "---- dmesg tail ----"
    dmesg -T 2>&1 | tail -100 || true
    echo "---- npu-smi ----"
    npu-smi info 2>&1 || true
    echo "---- df ----"
    df -h /workspace/mengzi 2>&1 || true
    echo "---- vllm processes alive ----"
    ps -eo pid,ppid,etime,args 2>/dev/null | grep -E "vllm|mooncake|pool_worker" | grep -v grep || true
} >> "${OUT}/README.txt"

# 6) try container-side gdb for copied cores (image may have gdb)
if ls "${OUT}"/core* >/dev/null 2>&1; then
    for pod in $(kubectl get pods -n "${NS}" --no-headers -o custom-columns=:.metadata.name 2>/dev/null | grep vllm-qwen-pd || true); do
        if kubectl exec -n "${NS}" "${pod}" -c vllm-prefill -- sh -c "command -v gdb" >/dev/null 2>&1; then
            for core in "${OUT}"/core*; do
                [ -f "$core" ] || continue
                corename="$(basename "$core")"
                kubectl cp -n "${NS}" "$core" "${pod}:/tmp/${corename}" >/dev/null 2>&1 || continue
                kubectl exec -n "${NS}" "${pod}" -c vllm-prefill -- \
                    gdb -batch -ex "set pagination off" -ex "thread apply all bt full" \
                    -c "/tmp/${corename}" > "${OUT}/bt-container-${corename}.txt" 2>&1 || true
                kubectl exec -n "${NS}" "${pod}" -c vllm-prefill -- rm -f "/tmp/${corename}" >/dev/null 2>&1 || true
            done
            break
        fi
    done
fi

tar czf "${OUT}.tar.gz" -C "$(dirname "${OUT}")" "$(basename "${OUT}")" 2>/dev/null
echo "done: ${OUT}.tar.gz"

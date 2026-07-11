#!/bin/bash
# Continuously streams logs from all containers in the vllm namespace to local files.
# Each container gets its own log file: logs/<pod>__<container>.log

NAMESPACE="vllm"
LOGDIR="$(pwd)/vllm-logs"
mkdir -p "$LOGDIR"

declare -A PIDS

cleanup() {
    echo "Stopping all log streams..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    echo "Done."
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "Logging all containers in namespace '$NAMESPACE' to $LOGDIR/"
echo "Press Ctrl+C to stop."
echo ""

for POD in $(kubectl get pods -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}'); do
    PODDIR="$LOGDIR/$POD"
    mkdir -p "$PODDIR"
    CONTAINERS=$(kubectl get pod "$POD" -n "$NAMESPACE" -o jsonpath='{.spec.containers[*].name}')
    for CONTAINER in $CONTAINERS; do
        LOGFILE="$PODDIR/${CONTAINER}.log"
        echo "  -> $POD/$CONTAINER -> $LOGFILE"
        kubectl logs -f -n "$NAMESPACE" "$POD" -c "$CONTAINER" --tail=1000 >> "$LOGFILE" 2>&1 &
        PIDS["${POD}__${CONTAINER}"]=$!
    done
done

echo ""
echo "Streaming ${#PIDS[@]} log streams. Ctrl+C to stop."

# Wait and restart any dead streams
while true; do
    sleep 30
    for KEY in "${!PIDS[@]}"; do
        if ! kill -0 "${PIDS[$KEY]}" 2>/dev/null; then
            POD="${KEY%%__*}"
            CONTAINER="${KEY##*__}"
            LOGFILE="$LOGDIR/${POD}/${CONTAINER}.log"
            mkdir -p "$LOGDIR/${POD}"
            echo "[$(date)] Restarting log stream for $KEY"
            kubectl logs -f -n "$NAMESPACE" "$POD" -c "$CONTAINER" --tail=100 >> "$LOGFILE" 2>&1 &
            PIDS["$KEY"]=$!
        fi
    done
done
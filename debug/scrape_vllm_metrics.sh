#!/bin/bash
#
# Scrape vLLM /metrics from all running vLLM pods via their NodePort or pod IP.
#
# Usage:
#   bash scrape_vllm_metrics.sh              # one-shot scrape
#   bash scrape_vllm_metrics.sh --watch 10   # scrape every 10 seconds
#   bash scrape_vllm_metrics.sh --raw        # dump full prometheus output

NODE_IP="10.50.156.65"
NAMESPACE="vllm"
WATCH_INTERVAL=0
RAW=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --watch) WATCH_INTERVAL="$2"; shift 2 ;;
        --raw)   RAW=true; shift ;;
        *)       shift ;;
    esac
done

scrape_once() {
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
    echo ""

    pods=$(kubectl get pods -n "$NAMESPACE" -l component=vllm -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\n"}{end}' 2>/dev/null)

    if [ -z "$pods" ]; then
        pods=$(kubectl get pods -n "$NAMESPACE" -o wide --no-headers | grep vllm-minimax | awk '{print $1, $6}')
    fi

    while read -r pod_name pod_ip; do
        [ -z "$pod_name" ] && continue
        [ -z "$pod_ip" ] && continue

        echo "--- $pod_name ($pod_ip:8200) ---"

        if $RAW; then
            curl -s --max-time 5 "http://${pod_ip}:8200/metrics" 2>/dev/null || echo "  UNREACHABLE"
        else
            metrics=$(curl -s --max-time 5 "http://${pod_ip}:8200/metrics" 2>/dev/null)
            if [ -z "$metrics" ]; then
                echo "  UNREACHABLE"
                continue
            fi

            echo "$metrics" | grep -E "^vllm:" | grep -E \
                "num_requests_running|num_requests_waiting|gpu_cache_usage|num_preemptions|avg_prompt_throughput|avg_generation_throughput|request_success|e2e_request_latency" \
                | head -20

            if echo "$metrics" | grep -q "lmcache"; then
                echo ""
                echo "  [LMCache]"
                echo "$metrics" | grep "lmcache" | head -10
            fi
        fi
        echo ""
    done <<< "$pods"
}

if [ "$WATCH_INTERVAL" -gt 0 ] 2>/dev/null; then
    while true; do
        clear
        scrape_once
        sleep "$WATCH_INTERVAL"
    done
else
    scrape_once
fi

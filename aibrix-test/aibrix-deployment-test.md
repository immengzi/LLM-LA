# Install aibrix components #
```bash
kubectl apply -f aibrix-dependency-v0.5.0.yaml
kubectl apply -f aibrix-core-v0.5.0.yaml
```
run kubectl get pods -n aibrix-system to check pod status util they are ready:

```bash
NAME                                         READY   STATUS    RESTARTS   AGE
aibrix-controller-manager-56576666d6-gsl8s   1/1     Running   0          5h24m
aibrix-gateway-plugins-c6cb7545-r4xwj        1/1     Running   0          5h24m
aibrix-gpu-optimizer-89b9d9895-t8wnq         1/1     Running   0          5h24m
aibrix-kuberay-operator-6dcf94b49f-l4522     1/1     Running   0          5h24m
aibrix-metadata-service-6b4d44d5bd-h5g2r     1/1     Running   0          5h24m
aibrix-redis-master-84769768cb-fsq45         1/1     Running   0          5h24m
```

# Deploy base model #
```bash
kubectl apply -f ascend_test_deploy.yaml
```
# Send request with routing strategy #

```bash
curl http://10.175.113.44:31639/v1/chat/completions \
  -H "routing-strategy: least-request" \
  -H "Content-Type: application/json" \
  -H "model: qwen3-8b" \
  -d '{
    "model": "qwen3-8b",
    "messages": [
      {"role": "user", "content": "既然你能看到 metrics，说明你已经 Ready 了。请自我介绍一下！"}
    ],
    "stream": false
  }'
  ```

## Routing strategies that supported by AIBrix:

AIBrix ships with a set of built-in algorithms, each optimized for different workload patterns:

* `random`: routes request to a random pod.

* `least-request`: routes request to a pod with the fewest ongoing requests.

* `throughput`: routes request to a pod which has processed the lowest total weighted tokens.

* `prefix-cache`: routes request to a pod which already has a KV cache matching the request’s prompt prefix, includes load balancing and multiturn conversation.

* `least-busy-time`: routes request to the pod with the least cumulative busy processing time.

* `least-kv-cache`: routes request to the pod with the smallest current KV cache size (least VRAM used).

* `least-latency`: routes request to the pod with the lowest average processing latency.

* `prefix-cache-preble`: routes request considering both prefix cache hits and pod load, implementation is based of Preble: Efficient Distributed Prompt Scheduling for LLM Serving: https://arxiv.org/abs/2407.00023.

* `vtc-basic`: routes request using a hybrid score balancing fairness (user token count) and pod utilization. It is a simple variant of Virtual Token Counter (VTC) algorithm. See more details at Ying1123/VTC-artifact

* `pd`: routes request for prefill-decode disaggregation, splitting processing between prefill and decode pods for optimized performance.

* `session-affinity`: enables sticky session routing by encoding the target pod’s address (IP:Port) into a base64-encoded value in the x-session-id header. On subsequent requests, if the header is present and valid, the gateway attempts to route to the same pod. If the pod is no longer ready (e.g., scaled down or evicted), it falls back to selecting a random ready pod and issues a new session ID.

Link to the AIbrix website: https://aibrix.readthedocs.io/latest/designs/aibrix-router.html


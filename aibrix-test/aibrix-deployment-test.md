# AIBrix Deployment Repo#
Repo link: `https://aibrix.readthedocs.io/latest/getting_started/quickstart.html` 

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
The request hit the LoadBalancer port on 31639, the LoadBalancer is deployed in envoy gateway with `kubectl get svc -n envoy-gateway-system`:
```bash
NAME                                     TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)                                   AGE
envoy-aibrix-system-aibrix-eg-903790dc   LoadBalancer   10.107.167.62    <pending>     80:31639/TCP                              15d
envoy-gateway                            ClusterIP      10.101.250.146   <none>        18000/TCP,18001/TCP,18002/TCP,19001/TCP   15d
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

# Varify the routing strategy #
To varify the effectiveness of the selected routing strategy, check the log of the aibrix-gateway-plugin:
``` bash
kubectl logs -n aibrix-system aibrix-gateway-plugins-c799bdf64-xknrx
```
This is an example of using `prefix-cache` strategy:
``` bash
I0226 10:00:05.411473       1 gateway_rsp_body.go:157] request end, requestID: 1ff62efd-95d2-4486-8d00-1ee689c4e2d4 - targetPod: 10.244.2.126:8000, elapsed: 1m20.298186s
I0226 10:00:35.862250       1 gateway.go:94] "processing request" requestID="8e6b18b0-0ba0-4dee-890a-ecb3fcf7af7f"
I0226 10:00:35.862990       1 prefix_cache.go:390] "prefix_hashes" request_id="8e6b18b0-0ba0-4dee-890a-ecb3fcf7af7f" prefix_hashes=[11101383234103911057,10567911157487975518,10940731342814491049,14662744175302431366,6416781936656417679,2326810982398165491,10744583744877777871,12614930204957311465,5497798066146226109,8791514844250438609]
I0226 10:00:35.863052       1 prefix_cache.go:395] "prefix_cache_matched_pods" request_id="8e6b18b0-0ba0-4dee-890a-ecb3fcf7af7f" target_pod="qwen3-8b-5697f55846-4lrjg" target_pod_ip="10.244.2.126" matched_pods={"qwen3-8b-5697f55846-4lrjg":100} pod_request_count={"qwen3-8b-5697f55846-4lrjg":0,"qwen3-8b-5697f55846-ljbpz":0}
I0226 10:00:35.863102       1 gateway_req_body.go:91] "request start" requestID="8e6b18b0-0ba0-4dee-890a-ecb3fcf7af7f" requestPath="/v1/chat/completions" model="qwen3-8b" stream=false routingAlgorithm="prefix-cache" targetPodIP="10.244.2.126:8000" routingDuration="722.6µs"
I0226 10:01:57.198942       1 gateway_rsp_body.go:157] request end, requestID: 8e6b18b0-0ba0-4dee-890a-ecb3fcf7af7f - targetPod: 10.244.2.126:8000, elapsed: 1m21.33657004s
```

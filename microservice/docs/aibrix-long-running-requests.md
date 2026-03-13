AIBrix 300s Timeout Issue — Root Cause and Permanent Fix

## Problem
-------
Requests sent through the AIBrix Gateway using the header

```bash
    routing-strategy: <value>
```

were failing with:

```bash
    504 Gateway Timeout
    response_code_details: stream_idle_timeout
    duration ≈ 300000 ms
```

The failure occurred even when the backend model (vLLM) was still processing the request.

## Root Cause
----------
The issue originates from the Envoy route used by AIBrix for dynamic routing.

Relevant route in Envoy:

```bash
    name: original_route
    cluster: original_destination_cluster
```

This route is responsible for requests that use the `routing-strategy` header.

The default timeout configuration for this route was effectively limiting long-running
requests, leading Envoy to terminate the stream.

Important detail:
The route contains a required AIBrix routing filter:

```bash
    typed_per_filter_config:
      envoy.filters.http.ext_proc/...
```

This filter allows the AIBrix gateway plugin to select the correct backend pod.

If this filter is removed or bypassed, Envoy cannot determine the upstream target and
returns:

```bash
    503 Service Unavailable
    response_code_details: no_healthy_upstream
```

Therefore, the solution must modify the existing route without removing this filter.

## Incorrect Attempt (Do NOT Use)
------------------------------
Adding a new route via EnvoyPatchPolicy:

```bash
    op: add
    path: /virtual_hosts/0/routes/0
```

This creates a new route without the required ext-proc filter and causes:

```bash
    503 no_healthy_upstream
```

## Correct Solution
----------------
Modify the existing `original_route` in place by updating only:

```bash
    route.timeout
    route.idle_timeout
```

while keeping the rest of the route unchanged.

## Implementation
--------------
Apply the following EnvoyPatchPolicy.

First remove any incorrect patch:

kubectl delete envoypatchpolicy -n aibrix-system aibrix-original-route-timeout-replace

Then apply the correct patch:

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyPatchPolicy
metadata:
  name: aibrix-original-route-timeout-replace
  namespace: aibrix-system
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: Gateway
    name: aibrix-eg
  type: JSONPatch
  jsonPatches:

  - name: aibrix-system/aibrix-eg/http
    type: type.googleapis.com/envoy.config.route.v3.RouteConfiguration
    operation:
      op: replace
      path: /virtual_hosts/0/routes/0/route/timeout
      value: "3600s"

  - name: aibrix-system/aibrix-eg/http
    type: type.googleapis.com/envoy.config.route.v3.RouteConfiguration
    operation:
      op: add
      path: /virtual_hosts/0/routes/0/route/idle_timeout
      value: "3600s"
EOF
```

## Verification
------------
Confirm the patch is active.

1. Verify the patch resource:

kubectl get envoypatchpolicy -n aibrix-system

2. Dump the live Envoy configuration:

```bash
POD=$(kubectl get pods -n envoy-gateway-system -o name \
      | grep envoy-aibrix-system-aibrix-eg \
      | head -n1 | cut -d/ -f2)

kubectl port-forward -n envoy-gateway-system pod/$POD 19000:19000 >/dev/null 2>&1 &
PF_PID=$!
sleep 2

curl -s http://127.0.0.1:19000/config_dump > envoy-config-check.json

kill $PF_PID
wait $PF_PID 2>/dev/null || true
```

3. Inspect the route:

```bash
grep -n -C 20 '"name": "original_route"' envoy-config-check.json
```

Expected result:

```bash
    "cluster": "original_destination_cluster",
    "timeout": "3600s",
    "idle_timeout": "3600s",
    "typed_per_filter_config": { ... }
```

The presence of `typed_per_filter_config` confirms the AIBrix routing plugin is still active.

## Persistence
-----------
EnvoyPatchPolicy is a Kubernetes resource.

Once created it will:

    - persist across Envoy restarts
    - persist across Gateway controller restarts
    - be automatically reapplied by Envoy Gateway

You only need to reapply it if:

    - the resource is deleted
    - the namespace is recreated
    - the gateway configuration is reset by a deployment.

## Summary
-------
Problem:
    Envoy route timeout causing 300s stream termination.

Fix:
    Patch the existing `original_route` to increase timeout values while preserving
    the AIBrix ext-proc routing filter.

Result:
    Long-running LLM inference requests complete successfully without gateway timeout.

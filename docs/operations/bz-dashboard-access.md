# Accessing BZ dashboards & notebooks from outside

The BZ (blue zone) cluster sits on a private LAN (`192.168.0.0/24`). The nodes have
**no public IP**, and the cloud firewall on the public host (`159.138.24.94`) only
exposes SSH (port `18800`) — not Kubernetes NodePorts or app ports. So you cannot hit
`http://159.138.24.94:<port>` directly; **tunnel the port over SSH** instead.

## General pattern (SSH local port-forward)

Run on your **local laptop**:

```bash
ssh -L <local_port>:<target_host>:<target_port> bz94saeid
# background it (no shell held open):
ssh -fN -L <local_port>:<target_host>:<target_port> bz94saeid
```

Then open `http://localhost:<local_port>` in your local browser.

- `bz94saeid` is the `~/.ssh/config` alias (HostName `159.138.24.94`, User `saeid`,
  Port `18800`, your key). Explicit form: `ssh -p 18800 ... saeid@159.138.24.94`.
- `<target_host>` depends on how the service is exposed (see below):
  - **NodePort** services → use a node IP (e.g. `192.168.0.79`), *not* `127.0.0.1`
    (NodePorts are served by kube-proxy on the node IP).
  - **A process / `kubectl port-forward` bound to localhost on the server** → use `localhost`.
- Keep the SSH session open — the tunnel lives only as long as the connection.
- Close a backgrounded tunnel: `pkill -f '<local_port>:<target_host>:<target_port>'`.
- Node internal IPs: `k8s-master 192.168.0.79`, `k8s-worker 192.168.0.99`,
  `k8s-worker1 192.168.0.42`, `k8s-worker2 192.168.0.69`.

## Prometheus  (deployed)

NodePort service `prometheus-kube-prometheus-prometheus` in ns `monitoring`
(`9090 → 31190`).

```bash
ssh -fN -L 31190:192.168.0.79:31190 bz94saeid
```

- UI:      http://localhost:31190
- Targets: http://localhost:31190/targets
- Example query (Graph box): `vllm:num_requests_running`

> Note: Prometheus may only discover a subset of the vLLM pods (e.g. one of the two
> `vllm-minimax-m2-*`), so per-pod numbers can look near-zero even when a pod is busy.
> For complete per-pod vLLM metrics, the sweep's `metrics_prom.py` scrapes each pod's
> NodePort directly — cross-check there if Prometheus looks empty.

## Grafana  (not currently deployed — general steps)

There is no Grafana service in the cluster today. When one is added (typically ns
`monitoring`, container port `3000`), expose it the same way:

```bash
# If it's a NodePort service (find the port):
kubectl -n monitoring get svc | grep grafana          # e.g. 3000:3<NNNN>/TCP
ssh -fN -L 3000:192.168.0.79:3<NNNN> bz94saeid          # -> http://localhost:3000

# If it's ClusterIP only, port-forward on the server first, then tunnel:
#   on server:  kubectl -n monitoring port-forward svc/<grafana-svc> 3000:3000 --address 0.0.0.0
#   on laptop:  ssh -fN -L 3000:192.168.0.79:3000 bz94saeid   -> http://localhost:3000
```

Default Grafana login is usually `admin` / (chart-set password); check the chart's
secret: `kubectl -n monitoring get secret <grafana> -o jsonpath='{.data.admin-password}' | base64 -d`.

## Jupyter (analysis notebooks)

The analysis notebooks live in `analysis-notebooks/` (e.g. `bz-analysis.ipynb`). Jupyter
runs as a **process on the server bound to localhost**, so tunnel to `localhost`
(not a node IP):

```bash
# on the server (BZ), from the repo:
cd analysis-notebooks
jupyter lab --no-browser --port 8888 --ip 127.0.0.1     # note the printed ?token=...

# on your laptop:
ssh -fN -L 8888:localhost:8888 bz94saeid                 # -> http://localhost:8888
```

Open `http://localhost:8888/?token=<token>` locally. If `8888` is busy on the server,
pick another port and match both sides. (Use the `central` conda env, which has the
analysis deps: `~/miniconda3/envs/central/bin/jupyter lab ...`.)

## Notes / gotchas

- The admin changed the SSH port to **`18800`** (not `22`).
- NodePort tunnels must target a **node IP** (`192.168.0.79`); localhost-of-the-server
  won't answer for NodePorts. Localhost is only correct for processes / port-forwards
  bound to `127.0.0.1` on the server (e.g. Jupyter).
- VS Code / Cursor Remote auto-forwards localhost ports from the server, so a
  `kubectl port-forward`/`jupyter` bound to `127.0.0.1` there often shows up on your
  laptop's `localhost` without a manual `ssh -L`.

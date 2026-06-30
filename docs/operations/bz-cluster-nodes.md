# BZ Cluster — Node Inventory & Schematic

Reference for the **BZ** Kubernetes cluster (this machine's cluster): node names,
public/private IPs, SSH access, NPU capacity, and where key workloads run.

> Companion docs: [k8s-dns-troubleshooting.md](k8s-dns-troubleshooting.md) (networking runbook),
> [bz-dashboard-access.md](bz-dashboard-access.md) (dashboard access over SSH tunnels).

---

## Node inventory

| Hostname | Role | Private IP | Public IP | SSH alias | SSH port | NPU (Ascend) |
|---|---|---|---|---|---|---|
| **k8s-master** | control-plane | `192.168.0.79` | `159.138.24.94` | `bz94root` / `bz94saeid` | **18800** | 8 |
| **k8s-worker** | worker | `192.168.0.99` | `121.91.168.191` | `bz191` | 22 | 8 |
| **k8s-worker1** | worker | `192.168.0.42` | `159.138.134.134` | `bz134` | 22 | 8 |
| **k8s-worker2** | worker | `192.168.0.69` | `183.87.46.77` | `bz77` | 22 | 8 |

- OS: Huawei Cloud EulerOS 2.0 (aarch64), Kubernetes `v1.29.15`, containerd `1.7.27`.
- All four nodes carry 8× Ascend NPUs.
- `k8s-master` SSH listens on the non-default port **18800**; workers use 22.
- `k8s-worker` (`192.168.0.99`) has **password SSH disabled** (key-only). `k8s-worker1`
  (`192.168.0.42`) uses a different root password than `k8s-worker2`. Key-based root
  access from `k8s-master` is set up for all nodes.

## Network facts

| Item | Value |
|---|---|
| CNI | Flannel (VXLAN backend, **UDP port 8472**) |
| Pod network CIDR | `10.244.0.0/16` (per-node `/24`) |
| Service CIDR / kube-dns | `kube-dns` ClusterIP `10.96.0.10` |
| Node underlay subnet | `192.168.0.0/24` |
| CoreDNS pods | **k8s-master only** (`10.244.0.81`, `10.244.0.83`) |

Per-node pod CIDR:

| Node | podCIDR |
|---|---|
| k8s-master | `10.244.0.0/24` |
| k8s-worker | `10.244.1.0/24` |
| k8s-worker1 | `10.244.2.0/24` |
| k8s-worker2 | `10.244.3.0/24` |

## Where key workloads run (namespace `vllm`)

| Component | Node(s) |
|---|---|
| CoreDNS (`kube-system`) | k8s-master |
| `router-service`, `boom-proxy` | k8s-worker1 |
| `vllm-minimax-m2-0` (LWS leader) + `-0-1` (DP worker) | k8s-master + k8s-worker |
| `vllm-minimax-m2-1` (LWS leader) + `-1-1` (DP worker) | k8s-worker2 + k8s-worker1 |

(vLLM pod placement is dynamic; values above reflect the current deployment.)

## Cluster schematic

```
                         Public internet
                                |
        +-----------------------+-----------------------+------------------------+
        |                       |                       |                        |
  159.138.24.94:18800     121.91.168.191:22      159.138.134.134:22       183.87.46.77:22
     (bz94)                  (bz191)                 (bz134)                  (bz77)
        |                       |                       |                        |
+---------------+      +----------------+      +-----------------+      +-----------------+
|  k8s-master   |      |   k8s-worker   |      |   k8s-worker1   |      |   k8s-worker2   |
| 192.168.0.79  |      | 192.168.0.99   |      | 192.168.0.42    |      | 192.168.0.69    |
| control-plane |      | worker         |      | worker          |      | worker          |
| pods 10.244.0 |      | pods 10.244.1  |      | pods 10.244.2   |      | pods 10.244.3   |
| 8x NPU        |      | 8x NPU         |      | 8x NPU          |      | 8x NPU          |
|               |      |                |      |                 |      |                 |
| CoreDNS       |      | vllm m2-0-1    |      | router-service  |      | vllm m2-1       |
| vllm m2-0     |      | (DP worker)    |      | boom-proxy      |      | (leader)        |
| (leader)      |      |                |      | vllm m2-1-1     |      |                 |
+-------+-------+      +--------+-------+      +--------+--------+      +--------+--------+
        |                       |                      |                        |
        +-----------------------+----------------------+------------------------+
                    Private VPC subnet 192.168.0.0/24
            Flannel VXLAN overlay (UDP 8472) carries all pod traffic
```

## Known networking issue (k8s-worker2)

The VPC **security group** for `k8s-worker2` (`192.168.0.69`) is more restrictive than the
other nodes: it permits TCP control-plane traffic (e.g. API server `6443`) but **blocks ICMP
and Flannel VXLAN (UDP 8472)** to/from `k8s-master`. Because CoreDNS runs only on the master,
this breaks DNS for every pod on worker2, so a vLLM replica scheduled there never registers
with the router. OS firewalls are off on both nodes and Flannel is configured correctly — the
drop is in the cloud security group.

**Fix:** allow all intra-cluster traffic from `192.168.0.0/24` (at minimum UDP 8472 + ICMP) in
the security groups of `192.168.0.69` and `192.168.0.79`, matching `k8s-worker1`. Verify with
`ping -c2 192.168.0.69` from the master. Full write-up:
[k8s-dns-troubleshooting.md](k8s-dns-troubleshooting.md).

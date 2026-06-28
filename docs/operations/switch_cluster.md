# Cluster Switch Knob

`switch_cluster` is a top-level config knob that lets one experiment config select cluster-specific defaults from a nearby profile file. It keeps portable workload configs free of hardcoded node IPs, local model paths, registry names, image pull policies, and other environment details.

Use it when the same experiment should run on different clusters, for example `yz` and `bz`, while preserving the same workload shape and Helm deployment intent.

## Quick Start

Add `switch_cluster` at the top level of an experiment YAML:

```yaml
switch_cluster: bz
backend: "boom"

total_requests: 1
prompt_source: "file"
```

Then keep cluster-dependent values out of the experiment YAML unless the experiment intentionally overrides them. A full example is available in `src/configs/1-1-boom-qwen-mooncake-bz.yaml`.

## Profile Location

Profiles are loaded from `clusters.yaml` in the same directory as the experiment config being loaded. For configs under `src/configs`, the active profile file is:

```text
src/configs/clusters.yaml
```

The file can either contain a top-level `clusters:` mapping or be a direct mapping of cluster names. The current file uses:

```yaml
clusters:
  yz:
    router_url: "http://10.50.156.65:30080"
    # ...

  bz:
    router_url: "http://192.168.0.79:30080"
    # ...
```

## Merge Semantics

`load_config()` reads the experiment YAML first. If `switch_cluster` is present and non-empty, it loads the matching profile and deep-merges the two mappings before dataclass parsing.

Precedence is:

1. Explicit values in the experiment config
2. Values from the selected cluster profile
3. Python defaults and Helm chart defaults

This means the profile provides defaults, not forced values. For example, a profile can set `boom.base_url`, while a specific experiment can still override only that field:

```yaml
switch_cluster: bz

boom:
  base_url: "http://192.168.0.79:30402"
```

Nested maps are merged recursively. If both the profile and experiment define `helm.values`, only the explicitly provided keys are replaced; other profile keys remain available.

If `switch_cluster` is absent or blank, `clusters.yaml` is not read and the config follows the legacy behavior.

## What Belongs In A Cluster Profile

Put values in a profile when they are properties of the cluster or its local deployment environment:

- Client endpoints: `router_url`, `metrics.prometheus_base_url`, `transport.results_zmq`, `boom.base_url`
- Experiment output root: `experiments_root`
- Dataset and tokenizer locations: `hf_lmsys.dataset_name`, `hf_lmsys.tokenizer_name`
- Host model paths: `helm.nfs_path`, `helm.model_host_path`
- Registry and image names: `helm.values.global.imageRegistry`, `helm.values.images.*`
- Local-image pull behavior: `helm.values.router.imagePullPolicy`, `helm.values.sidecar.imagePullPolicy`, `helm.values.boom.imagePullPolicy`
- Ascend runtime differences: `helm.values.cpuHash.ascendDriverMounts`
- Mooncake and pinning details: `helm.mooncake_master_server_address`, `helm.values.pin.enabled`, `helm.values.pin.nodeName`

Keep workload and experiment-specific knobs in the experiment YAML:

- Request shape: `total_requests`, `prompt_source`, prompt filters, generation lengths
- Load pattern: `load_pattern.rate_rps`, duration, warmup, burst, step, or random schedule
- Backend behavior being tested: routing strategy, cache warmup, SLO-aware routing, autoscaling decisions
- Model topology when the topology is part of the experiment rather than a fixed cluster constraint

## BZ Local Registry

The current `bz` cluster has a local Docker Registry deployed at `reg.local:32000`. The registry is pinned to `k8s-worker1` (`192.168.0.42`) because the cluster has asymmetric node reachability: `k8s-worker2` cannot directly reach `k8s-master`, and `k8s-master` cannot directly reach `k8s-worker2`, while every node can reach `k8s-worker1`.

The node-level setup is:

```text
reg.local -> 192.168.0.42
/etc/containerd/certs.d/reg.local:32000/hosts.toml -> HTTP registry with pull/resolve/push
```

See the BZ cluster registry record in `docs/operations/registry.md` for the full connectivity survey, deployment manifest, node configuration, and pull verification.

For `switch_cluster: bz`, registry values should be treated carefully:

- Use `helm.values.global.imageRegistry: "reg.local:32000"` only after all images referenced by the Helm chart have been pushed to the local registry. This global setting rewrites every chart image, including public images such as `redis` and Ascend vLLM images.
- If only selected custom images have been pushed, keep `global.imageRegistry` empty and set full image references individually, for example `images.router: "reg.local:32000/kv-router:latest"`.
- Keep the registry address and image pull policy in the cluster profile when they describe the cluster environment. Keep model-specific image choices in the experiment config when they are part of the workload being tested.

## Adding A New Cluster

1. Add a new key under `clusters:` in the relevant `clusters.yaml`.
2. Copy only the values that are genuinely cluster-specific.
3. Start from an existing portable experiment config and set `switch_cluster: <name>`.
4. Remove inline endpoint, path, registry, and pinning fields that should come from the profile.
5. Run a small smoke test and inspect the generated effective Helm values for the expected image, path, and NodePort settings.

## Troubleshooting

`FileNotFoundError: switch_cluster='...' requires .../clusters.yaml` means the experiment config directory does not contain a `clusters.yaml`. Add one next to the config or move the config under a directory that already has profiles.

`ValueError: Unknown switch_cluster '...'` means the selected cluster name is not present in `clusters.yaml`. Check spelling and the keys under `clusters:`.

If a profile value does not appear to take effect, check whether the experiment YAML sets the same key inline. Inline values intentionally win over the selected cluster profile.

## Validation Notes

- Manual `bz` smoke: `completed=1 lost=0`, experiment `src/experiments/1`.
- `switch_cluster: bz` smoke: `completed=1 lost=0`, experiment `src/experiments/2`.
- Default chart render compatibility check passed against commit `c451232` with `modelVolume.modelSubPath=qwen3-8b`: `DEFAULT_HELM_TEMPLATE_MATCH`.


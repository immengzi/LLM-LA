# Reproducible Build: `lmcache-ascend:hccl-p2p` (aarch64 Ascend NPU)

This document reproduces the **`lmcache-ascend:hccl-p2p`** Docker
image from scratch, end to end, on an aarch64 Ascend NPU node. Every command is
copy‑pasteable. Every referenced script is reproduced in full below **and** committed
to this repo at `infra/LMCache-docker-build/patch-vllm.sh` and
`infra/LMCache-docker-build/Dockerfile.p2p`.

Follow the sections in order (STEP 0 → STEP 7). If you follow it top to bottom with
no other context, you will produce the identical image and verify it.

> **Two different locations — do not confuse them:**
> - `infra/LMCache-docker-build/` — where reference copies of the two scripts live **in
>   this (`llm-la`) repo**, for version control and review.
> - `docker/` — the directory **inside the cloned fork checkout**
>   (`~/LMCache-Ascend-p2p/docker/`) that the build actually reads. The fork already
>   ships `docker/lmcache-controller.diff`, `docker/vllm-utils.diff`,
>   `docker/vllm-sched.diff`, and the build creates `docker/patch-vllm.sh` +
>   `docker/Dockerfile.p2p` there.
>
> All `docker/...` paths in STEP 1–9 below refer to the **fork checkout** layout (this
> is what `-f`, `COPY .`, and `/workspace/LMCache-Ascend/docker/...` resolve to) — keep
> them as-is. The `infra/LMCache-docker-build/` copies are byte‑identical to the
> `docker/patch-vllm.sh` and `docker/Dockerfile.p2p` you create in STEP 4/5.

> **Placeholders** (site‑specific, marked `<LIKE_THIS>`): only two exist.
> - `<PROXY_URL>` — an HTTP proxy such as `http://127.0.0.1:3128`. Only relevant on
>   proxied nodes; see STEP 0 and the "loopback‑proxy" build in STEP 5.
> - `<REGISTRY>` — your local OCI registry host:port, e.g. `127.0.0.1:32000` /
>   `reg.local:32000`. Used only in STEP 7 (push & distribute).
>
> Everything else (commit hashes, image ids, paths, flags) is an exact literal.

---

## 1. Overview

`lmcache-ascend:hccl-p2p` is a vLLM‑Ascend runtime image that adds
**LMCache P2P KV‑cache sharing** compiled for Ascend NPUs. It is built by taking the
official Ascend vLLM base image and baking in three changes:

1. **LMCache 0.4.3** installed from the wheel, then patched with
   `docker/lmcache-controller.diff` — adds a `target_worker_id` to the P2P controller
   lookup so KV can be shared P2P when **tensor parallel > 1** (TP>1).
2. **vLLM ARM `sched_yield` fix** — on Arm, `os.sched_yield()` does not release the
   GIL, causing a CPU‑bound busy loop. The fix forces `USE_SCHED_YIELD=False` on ARM.
   It is applied as **intent** via `docker/patch-vllm.sh` (not `git apply`; see §3
   pitfall 2 in the source‑of‑truth note and §11 Troubleshooting for why).
3. **lmcache-ascend** (the C++ `kvcache-ops` kernels) compiled and installed editable.

> **Install method — which package is editable, and why (important):**
> Only **change 3 (`lmcache-ascend`, the fork we compile) is an editable install**
> (`pip install -e .`), because it builds the `kvcache-ops` C++ kernels from the source
> checkout and must import from that tree.
>
> **Change 1 (`lmcache`, the PyPI package) is a plain wheel install (NOT `-e`)**, and
> its patch is applied by `git apply`‑ing `lmcache-controller.diff` **directly onto the
> installed `.py` files in site‑packages** (the location `pip show lmcache` reports).
> An editable install is **not required** for this patch to take effect: `cache_controller`
> is pure Python, so we edit the exact file Python imports and the change is live
> immediately — no rebuild. Verified on the built image: `lmcache` has no
> `direct_url.json` (⇒ normal wheel), `pip show lmcache` → `Location:
> .../site-packages`, yet `target_worker_id` is present in
> `.../site-packages/lmcache/v1/cache_controller/utils.py` and
> `import lmcache.v1.cache_controller.utils` resolves to that same patched file.
>
> "The patch needs an editable install" is only true for a *different* workflow — one
> where you patch a separate `lmcache` **source checkout** but still `pip install` the
> PyPI wheel; there the wheel (not your edited source) is what gets imported, so you
> would need `pip install -e .` (or a rebuilt wheel) to pick up the change. This build
> avoids that by patching site‑packages in place. If your team's convention is to carry
> the `lmcache` patch as an editable source install instead, see §11 (row 9) for the
> equivalent recipe.

| Property | Value |
|---|---|
| Target hardware | aarch64 (arm64) Ascend **NPU**, EulerOS/openEuler |
| Base image | `quay.io/ascend/vllm-ascend:v0.18.0-openeuler` (short id `bab0bb869c9c`) |
| vLLM source path in base | `/vllm-workspace/vllm/vllm` (NOT site‑packages) |
| lmcache wheel | `0.4.3` (installed, then patched) |
| lmcache-ascend version | `0.4.4.dev27` (setuptools_scm; see §10) |
| Primary tag | `lmcache-ascend:hccl-p2p` |
| Registry tag(s) | `<REGISTRY>/lmcache-ascend:hccl-p2p` (also mirrored as `v0.4.4-vllm-ascend-v0.18.0-openeuler` in some deployments) |

---

## 2. Prerequisites

- **Docker** with the legacy builder (this is what Ascend nodes ship). The Dockerfile
  is written to be legacy‑builder‑safe — no heredoc in `RUN` (see STEP 5 note).
- **git** with submodule support.
- **Access to the fork** `matthewygf/LMCache-Ascend`, branch `hccl_host_staging`.
- **Base image available locally** (STEP 0 confirms). If missing, `docker pull
  quay.io/ascend/vllm-ascend:v0.18.0-openeuler` (requires network/proxy).
- **Disk**: the final image is ~17 GB; ensure ≥ 40 GB free for build layers + image.
- **Network**: the build installs `lmcache==0.4.3` and compiles kernels, so the build
  container needs a working pip index. The Dockerfile defaults to the Aliyun mirror
  (`https://mirrors.aliyun.com/pypi/simple`). See STEP 0 for proxy handling.
- **No NPU device is required at build time.** In fact, the build must **never**
  `import vllm`/`import torch` (see STEP 0 and §11) — there is no NPU in the builder.

---

## 3. STEP 0 — Environment check

Detect the proxy, the pip source, the base image, and confirm the vLLM source path.

### 3.1 Proxy + pip source + base image

```bash
echo "=== proxy ===" && env | grep -i proxy || echo "(none)"
echo "=== pip.conf ===" && cat /etc/pip.conf ~/.pip/pip.conf ~/.config/pip/pip.conf 2>/dev/null || echo "(no host pip.conf)"
echo "=== base images ===" && docker images | grep -i vllm-ascend
```

Interpret the output:

- **`(none)`** under `proxy` → you are on a **direct‑internet** node. Use the
  *direct* build in STEP 5 (no proxy build‑args).
- A line like `http_proxy=http://127.0.0.1:3128` → you are on a **loopback‑proxy**
  node. Use the *loopback‑proxy* build in STEP 5 and set `<PROXY_URL>` accordingly.
- **`(no host pip.conf)`** → fine; the Dockerfile bakes the Aliyun index as its
  default `PIP_INDEX_URL`, which works from inside the build container.
- The `docker images` line should show
  `quay.io/ascend/vllm-ascend  v0.18.0-openeuler  bab0bb869c9c`. Confirm the short id
  is **`bab0bb869c9c`**:

```bash
docker image inspect quay.io/ascend/vllm-ascend:v0.18.0-openeuler --format '{{.Id}}'
# expected: sha256:bab0bb869c9c291f75472ee545a25825352ea83ed8c7dece028c3aad7a507155
```

### 3.2 Confirm the vLLM source path

The base image keeps vLLM at `/vllm-workspace/vllm/vllm`, **not** in
`site-packages`. Confirm it with the guarded probe below.

> **Why the guard:** there is no NPU in the builder. A plain `import vllm`/`import
> torch` triggers `torch_npu`, which loads `libascend_hal.so` and crashes with
> `cannot open shared object file`. `TORCH_DEVICE_BACKEND_AUTOLOAD=0` suppresses the
> *autoload* path enough for `import vllm` to report its own directory. (It does
> **not** stop vllm-ascend's *explicit* `import torch_npu` — which is exactly why the
> real build never imports vllm/torch and uses fixed paths + `pip show` instead.)

> **Does `TORCH_DEVICE_BACKEND_AUTOLOAD=0` disable NPU at runtime? No.** The `ENV`
> persists into the runtime container, but it does **not** affect actual NPU
> functionality. In torch 2.9 this flag only controls the *implicit* autoload that
> `import torch` performs via the `torch.backends` entry point
> (`torch_npu -> torch_npu:_autoload`). vLLM‑Ascend registers the NPU backend anyway
> because its runtime code **explicitly `import torch_npu`** in 30+ modules on the hot
> path (e.g. `vllm_ascend/attention/attention_v1.py`, `ops/rotary_embedding.py`,
> `ops/fused_moe/*`, `device/device_op.py`). An explicit `import torch_npu` performs
> the same backend registration as the autoload, so with `vllm_ascend` loaded (which
> every serving path does) the NPU is fully available. NPU in this image comes entirely
> from the out‑of‑tree `torch_npu` (base `torch` is a `+cpu` build); the flag only
> changes *how* torch_npu gets imported (explicitly vs. implicitly), not *whether* it
> works. Edge case: code that does a bare `import torch; torch.npu...` **without**
> importing `torch_npu`/`vllm_ascend` would not see the NPU under this flag — add
> `import torch_npu` for such standalone probes.

```bash
docker run --rm --entrypoint bash quay.io/ascend/vllm-ascend:v0.18.0-openeuler \
  -c 'TORCH_DEVICE_BACKEND_AUTOLOAD=0 python -c "import vllm,os;print(os.path.dirname(vllm.__file__))"' \
  2>&1 | tail -5
```

Expected output (the last line):

```
/vllm-workspace/vllm/vllm
```

If this prints a different path, edit the `F=` line in `docker/patch-vllm.sh`
(STEP 4) to match `<that path>/distributed/utils.py`.

---

## 4. STEP 1 — Get the source

Clone the fork, check out the exact reproducible commit, make the release tag
visible, sync submodules, and verify the `kvcache-ops` pin.

```bash
cd ~
# Clone the FORK (not upstream). Drop the proxy env if you are direct-internet.
git clone https://github.com/matthewygf/LMCache-Ascend.git LMCache-Ascend-p2p
cd LMCache-Ascend-p2p

# If 'origin' happens to be upstream (LMCache/LMCache-Ascend) instead of the fork,
# add the fork explicitly:
#   git remote add fork https://github.com/matthewygf/LMCache-Ascend.git
#   git fetch fork
git remote -v
```

Check out the exact commit and make the `v0.4.3` tag visible (this is required for
the version string — see §10 and pitfall in §11):

```bash
git fetch --tags origin
# The v0.4.3 tag lives on upstream. Add upstream and fetch its tags so
# setuptools_scm can compute 0.4.4.dev27 (guess-next-dev from v0.4.3 + 27 commits):
git remote add upstream https://github.com/LMCache/LMCache-Ascend.git 2>/dev/null || true
git fetch --tags upstream

git checkout 5a577dfed0248ba9e32f324574dd7c9b28dcd130
echo "HEAD=$(git rev-parse HEAD)"
echo "describe=$(git describe --tags --long)"
```

Expected:

```
HEAD=5a577dfed0248ba9e32f324574dd7c9b28dcd130
describe=v0.4.3-27-g5a577df
```

Sync submodules and verify `kvcache-ops`:

```bash
git submodule update --init --recursive
KV=$(git -C third_party/kvcache-ops rev-parse HEAD)
echo "kvcache-ops=$KV"
[ "${KV:0:7}" = "579190d" ] && echo "kvcache-ops OK" || echo "kvcache-ops MISMATCH"
```

Expected:

```
kvcache-ops=579190dfc7acf288352ec48808d93b23eb8e2e9a
kvcache-ops OK
```

> The two LMCache patch inputs referenced by the Dockerfile already ship in the fork:
> `docker/lmcache-controller.diff` and `docker/vllm-utils.diff`. Confirm:
> ```bash
> ls -l docker/lmcache-controller.diff docker/vllm-utils.diff
> ```

---

## 5. STEP 2–4 — Repo files

You create/adjust three things in the fork checkout: `docker/patch-vllm.sh`,
`docker/Dockerfile.p2p`, and one `.dockerignore` exception. All three are shown in
full below, byte‑for‑byte, with the command to create each.

### 5.1 `docker/patch-vllm.sh`

**Why a separate script (not inline in the Dockerfile):** the legacy Docker builder
does **not** support heredoc (`<<EOF`) inside a `RUN`. The vLLM patch needs a Python
heredoc (`python3 - <<'PYEOF'`), so the logic must live in a file the `RUN` calls.

**What it does:** (a) `sed`‑inserts `from vllm.platforms import CpuArchEnum, Platform`
right after the `init_logger` import; (b) does an **exact‑block** Python replace of the
`USE_SCHED_YIELD = ...` assignment, guarded by an `assert` so the build fails loudly if
the vLLM source ever changes; (c) two trailing `grep -q` checks so the `RUN` returns
non‑zero if either edit did not land.

Full content:

```bash
#!/bin/bash
set -euo pipefail
F=/vllm-workspace/vllm/vllm/distributed/utils.py    # adjust if STEP 0 differed
echo ">>> patch vLLM @ $F"
if grep -q CpuArchEnum "$F"; then
    echo "already patched"
else
    sed -i '/^from vllm.logger import init_logger$/a from vllm.platforms import CpuArchEnum, Platform' "$F"
    python3 - "$F" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
old = """USE_SCHED_YIELD = (sys.version_info[:3] >= (3, 11, 1)) or (
    sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8
)"""
new = """USE_SCHED_YIELD = (
    (sys.version_info[:3] >= (3, 11, 1))
    or (sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8)
) and Platform.get_cpu_architecture() != CpuArchEnum.ARM"""
assert old in s, "USE_SCHED_YIELD block not found — vLLM source changed, STOP and inspect"
open(p, "w").write(s.replace(old, new, 1))
print("USE_SCHED_YIELD patched")
PYEOF
fi
grep -q CpuArchEnum "$F"
grep -q "get_cpu_architecture() != CpuArchEnum.ARM" "$F"
```

Create it and make it executable:

```bash
cat > docker/patch-vllm.sh <<'SCRIPT'
#!/bin/bash
set -euo pipefail
F=/vllm-workspace/vllm/vllm/distributed/utils.py    # adjust if STEP 0 differed
echo ">>> patch vLLM @ $F"
if grep -q CpuArchEnum "$F"; then
    echo "already patched"
else
    sed -i '/^from vllm.logger import init_logger$/a from vllm.platforms import CpuArchEnum, Platform' "$F"
    python3 - "$F" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
old = """USE_SCHED_YIELD = (sys.version_info[:3] >= (3, 11, 1)) or (
    sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8
)"""
new = """USE_SCHED_YIELD = (
    (sys.version_info[:3] >= (3, 11, 1))
    or (sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8)
) and Platform.get_cpu_architecture() != CpuArchEnum.ARM"""
assert old in s, "USE_SCHED_YIELD block not found — vLLM source changed, STOP and inspect"
open(p, "w").write(s.replace(old, new, 1))
print("USE_SCHED_YIELD patched")
PYEOF
fi
grep -q CpuArchEnum "$F"
grep -q "get_cpu_architecture() != CpuArchEnum.ARM" "$F"
SCRIPT
chmod +x docker/patch-vllm.sh
bash -n docker/patch-vllm.sh && echo "syntax OK"
```

### 5.2 `docker/Dockerfile.p2p`

Full content (verbatim):

```dockerfile
ARG BASE_IMAGE=quay.io/ascend/vllm-ascend:v0.18.0-openeuler
FROM ${BASE_IMAGE}

ARG http_proxy
ARG https_proxy
ARG no_proxy

ARG PIP_INDEX_URL="https://mirrors.aliyun.com/pypi/simple"
ARG PIP_TRUSTED_HOST="mirrors.aliyun.com"

ENV SOC_VERSION=Ascend
ENV LMCACHE_TRACK_USAGE=false
ENV PIP_NO_CACHE_DIR=1
ENV TORCH_DEVICE_BACKEND_AUTOLOAD=0

RUN printf "[global]\nindex-url = %s\ntrusted-host = %s\n" "$PIP_INDEX_URL" "$PIP_TRUSTED_HOST" > /etc/pip.conf

WORKDIR /workspace
COPY . /workspace/LMCache-Ascend/

# 1) LMCache 0.4.3 wheel + controller patch (locate via pip show, never import)
RUN NO_CUDA_EXT=1 pip install lmcache==0.4.3 && \
    LMC="$(pip show lmcache | awk '/^Location:/{print $2}')" && \
    echo ">>> patch LMCache @ $LMC" && cd "$LMC" && \
    git apply -p1 --verbose /workspace/LMCache-Ascend/docker/lmcache-controller.diff && \
    grep -q target_worker_id "$LMC/lmcache/v1/cache_controller/utils.py"

# 2) vLLM sched_yield patch via script (baseline mismatch => cannot git apply)
RUN bash /workspace/LMCache-Ascend/docker/patch-vllm.sh

# 2b) OPTIONAL: only for cross-instance P2P / disagg-prefill
# RUN cd /vllm-workspace/vllm-ascend && \
#     git apply -p1 --verbose /workspace/LMCache-Ascend/docker/vllm-sched.diff

# 3) Compile + editable install LMCache-Ascend (kvcache-ops C++)
RUN cd /workspace/LMCache-Ascend && \
    export CPLUS_INCLUDE_PATH=/usr/include/c++/12:/usr/include/c++/12/$(uname -i)-openEuler-linux:$CPLUS_INCLUDE_PATH && \
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/Ascend/ascend-toolkit/latest/$(uname -i)-linux/devlib && \
    pip install -v --no-build-isolation -e .

CMD ["/bin/bash"]
```

Create it:

```bash
cat > docker/Dockerfile.p2p <<'DOCKERFILE'
ARG BASE_IMAGE=quay.io/ascend/vllm-ascend:v0.18.0-openeuler
FROM ${BASE_IMAGE}

ARG http_proxy
ARG https_proxy
ARG no_proxy

ARG PIP_INDEX_URL="https://mirrors.aliyun.com/pypi/simple"
ARG PIP_TRUSTED_HOST="mirrors.aliyun.com"

ENV SOC_VERSION=Ascend
ENV LMCACHE_TRACK_USAGE=false
ENV PIP_NO_CACHE_DIR=1
ENV TORCH_DEVICE_BACKEND_AUTOLOAD=0

RUN printf "[global]\nindex-url = %s\ntrusted-host = %s\n" "$PIP_INDEX_URL" "$PIP_TRUSTED_HOST" > /etc/pip.conf

WORKDIR /workspace
COPY . /workspace/LMCache-Ascend/

# 1) LMCache 0.4.3 wheel + controller patch (locate via pip show, never import)
RUN NO_CUDA_EXT=1 pip install lmcache==0.4.3 && \
    LMC="$(pip show lmcache | awk '/^Location:/{print $2}')" && \
    echo ">>> patch LMCache @ $LMC" && cd "$LMC" && \
    git apply -p1 --verbose /workspace/LMCache-Ascend/docker/lmcache-controller.diff && \
    grep -q target_worker_id "$LMC/lmcache/v1/cache_controller/utils.py"

# 2) vLLM sched_yield patch via script (baseline mismatch => cannot git apply)
RUN bash /workspace/LMCache-Ascend/docker/patch-vllm.sh

# 2b) OPTIONAL: only for cross-instance P2P / disagg-prefill
# RUN cd /vllm-workspace/vllm-ascend && \
#     git apply -p1 --verbose /workspace/LMCache-Ascend/docker/vllm-sched.diff

# 3) Compile + editable install LMCache-Ascend (kvcache-ops C++)
RUN cd /workspace/LMCache-Ascend && \
    export CPLUS_INCLUDE_PATH=/usr/include/c++/12:/usr/include/c++/12/$(uname -i)-openEuler-linux:$CPLUS_INCLUDE_PATH && \
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/Ascend/ascend-toolkit/latest/$(uname -i)-linux/devlib && \
    pip install -v --no-build-isolation -e .

CMD ["/bin/bash"]
DOCKERFILE
```

### 5.3 `.dockerignore` exception

The fork's `.dockerignore` starts with:

```
docker/Dockerfile*
docker/*.sh
```

The `docker/*.sh` line would exclude **`patch-vllm.sh`** from the build context, so
STEP 5's `RUN bash /workspace/LMCache-Ascend/docker/patch-vllm.sh` would fail with
`No such file or directory`. Add an explicit exception so the file survives the
`COPY . /workspace/LMCache-Ascend/`:

```bash
grep -q '!docker/patch-vllm.sh' .dockerignore || printf '\n!docker/patch-vllm.sh\n' >> .dockerignore
cat .dockerignore
```

Expected (the relevant lines):

```
docker/Dockerfile*
docker/*.sh
!docker/patch-vllm.sh
```

> Note: `Dockerfile.p2p` itself does not need a `.dockerignore` exception — it is
> passed with `-f` and is not required inside the build context.

---

## 6. STEP 5 — Build

Run from the fork checkout root (`~/LMCache-Ascend-p2p`). Choose the variant that
matches STEP 0.

### 6.1 Direct‑internet node (no proxy) — the source‑of‑truth build

`--network=host` is kept (harmless on a direct node), and proxy build‑args are
**dropped**. `env -u http_proxy ...` strips any stray proxy vars from the client env.

```bash
cd ~/LMCache-Ascend-p2p
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
  docker build --network=host \
  -f docker/Dockerfile.p2p \
  -t lmcache-ascend:hccl-p2p .
```

### 6.2 Loopback‑proxy node

Some nodes reach the internet only through a proxy on `<PROXY_URL>` (e.g.
`http://127.0.0.1:3128`). **Key subtlety:** a build container has its **own network
namespace**, so `127.0.0.1` inside the build is *not* the host's proxy. Two things are
required together:

1. `--network=host` so the build container shares the host's loopback (making
   `127.0.0.1:3128` actually reach the host proxy), and
2. passing `http_proxy`/`https_proxy`/`no_proxy` as **build‑args** (the Dockerfile
   declares matching `ARG`s) so pip/git inside each `RUN` use the proxy.

```bash
cd ~/LMCache-Ascend-p2p
docker build --network=host \
  --build-arg http_proxy=<PROXY_URL> \
  --build-arg https_proxy=<PROXY_URL> \
  --build-arg no_proxy=127.0.0.1,localhost,<REGISTRY> \
  -f docker/Dockerfile.p2p \
  -t lmcache-ascend:hccl-p2p .
```

### 6.3 `--build-arg BASE_IMAGE` vs the default

The Dockerfile defaults `BASE_IMAGE` to `quay.io/ascend/vllm-ascend:v0.18.0-openeuler`
(short id `bab0bb869c9c`, confirmed in STEP 0). **Do not pass `--build-arg BASE_IMAGE`**
unless your node's base image is retagged under a different name/registry — in which
case pass the exact ref you confirmed in STEP 0, e.g.
`--build-arg BASE_IMAGE=<REGISTRY>/vllm-ascend:v0.18.0-openeuler`.

### 6.4 What success looks like, per RUN

- **RUN 1 (LMCache patch)** — `git apply --verbose` lists the two patched files, and
  the trailing `grep -q target_worker_id` passes silently (any failure aborts the
  build):
  ```
  Checking patch lmcache/v1/cache_controller/controllers/kv_controller.py...
  Checking patch lmcache/v1/cache_controller/utils.py...
  Applied patch lmcache/v1/cache_controller/controllers/kv_controller.py cleanly.
  Applied patch lmcache/v1/cache_controller/utils.py cleanly.
  ```
- **RUN 2 (vLLM patch)** — `patch-vllm.sh` prints:
  ```
  >>> patch vLLM @ /vllm-workspace/vllm/vllm/distributed/utils.py
  USE_SCHED_YIELD patched
  ```
- **RUN 3 (compile kvcache-ops + editable install)** — CMake/ninja build finishing
  with lines like `[100%] Built target ...`, then:
  ```
  Successfully installed lmcache-ascend-0.4.4.dev27
  ```
  The final line of a successful build is the image being tagged, e.g.
  `Successfully tagged lmcache-ascend:hccl-p2p` (or `naming to ...` on BuildKit).

> If RUN 3 reports `Successfully installed lmcache-ascend-0.1.devNNN` instead of
> `0.4.4.dev27`, the `v0.4.3` tag was not visible to setuptools_scm inside the build
> context — go back to STEP 1 and `git fetch --tags` (see §10 / §11).

---

## 7. STEP 6 — Verify

Two blocks: static patch/compile checks, then the runtime ARM behavior check. All are
run against the built image (no NPU needed).

### 7.1 Static patch + compile + versions

```bash
docker run --rm --entrypoint bash lmcache-ascend:hccl-p2p -c '
  grep -q target_worker_id $(pip show lmcache | awk "/^Location:/{print \$2}")/lmcache/v1/cache_controller/utils.py && echo "LMCache patch OK" || echo "LMCache FAIL"
  grep -q CpuArchEnum /vllm-workspace/vllm/vllm/distributed/utils.py && echo "vLLM import OK" || echo "vLLM import FAIL"
  grep -q "get_cpu_architecture() != CpuArchEnum.ARM" /vllm-workspace/vllm/vllm/distributed/utils.py && echo "vLLM expr OK" || echo "vLLM expr FAIL"
  python3 -m py_compile /vllm-workspace/vllm/vllm/distributed/utils.py && echo "py_compile OK" || echo "py_compile FAIL"
  echo "lmcache wheel: $(pip show lmcache | awk "/^Version:/{print \$2}")"
  echo "lmcache-ascend: $(pip show lmcache-ascend | awk "/^Version:/{print \$2}")"
'
```

Expected output:

```
LMCache patch OK
vLLM import OK
vLLM expr OK
py_compile OK
lmcache wheel: 0.4.3
lmcache-ascend: 0.4.4.dev27
```

### 7.2 Runtime ARM check → `USE_SCHED_YIELD=False`

This reproduces vLLM's own logic and confirms that on aarch64 the patched expression
evaluates to `False` (i.e. the image will use `time.sleep(0)`, not `sched_yield`).

```bash
docker run --rm --entrypoint bash lmcache-ascend:hccl-p2p -c '
  python3 -c "
import sys, platform
is_arm = platform.machine().lower() in (\"aarch64\",\"arm64\")
print(\"machine =\", platform.machine())
print(\"USE_SCHED_YIELD =\", (((sys.version_info[:3]>=(3,11,1)) or (sys.version_info[:2]==(3,10) and sys.version_info[2]>=8)) and not is_arm))
"'
```

Expected output:

```
machine = aarch64
USE_SCHED_YIELD = False
```

### 7.3 PASS criteria

The build is a valid reproduction **iff all** of the following hold:

- `LMCache patch OK`, `vLLM import OK`, `vLLM expr OK`, `py_compile OK`
- `lmcache wheel: 0.4.3`
- `lmcache-ascend: 0.4.4.dev27`
- `machine = aarch64` **and** `USE_SCHED_YIELD = False`

| item | expected | source of truth |
|---|---|---|
| source commit | `5a577df` (`v0.4.3-27`) | STEP 1 `git describe` |
| kvcache-ops | `579190d…` | STEP 1 submodule check |
| base image id | `bab0bb869c9c` | STEP 0 inspect |
| lmcache wheel | `0.4.3` | 7.1 |
| lmcache-ascend | `0.4.4.dev27` | 7.1 |
| LMCache patch | OK | 7.1 |
| vLLM patch | OK (expr + py_compile + `USE_SCHED_YIELD=False`) | 7.1 + 7.2 |

---

## 8. STEP 7 — Push & distribute

Replace `<REGISTRY>` with your local registry host:port (the source‑of‑truth build
used `127.0.0.1:32000`, mirrored on `reg.local:32000`).

### 8.1 Tag + push

```bash
docker tag lmcache-ascend:hccl-p2p <REGISTRY>/lmcache-ascend:hccl-p2p
docker push <REGISTRY>/lmcache-ascend:hccl-p2p 2>&1 | tail -5
```

The push is ~17 GB, so it takes a while. Success ends with a `digest: sha256:...` line.

### 8.2 Verify the tag is in the registry

```bash
curl -s http://<REGISTRY>/v2/lmcache-ascend/tags/list; echo
```

Expected JSON (order may vary; `hccl-p2p` must be present):

```json
{"name":"lmcache-ascend","tags":["v0.4.4-vllm-ascend-v0.18.0-openeuler","hccl-p2p"]}
```

### 8.3 Verify containerd can self‑pull (plain‑http)

Nodes running Kubernetes/containerd should be able to pull directly — no per‑node
import needed — provided containerd is configured for the plain‑http local registry:

```bash
sudo ctr -n k8s.io images pull --plain-http <REGISTRY>/lmcache-ascend:hccl-p2p 2>&1 | tail -4
```

Success ends with layers unpacking for arm64 and a `done` / `unpacking ... done` line,
e.g.:

```
...
unpacking linux/arm64/v8 sha256:...
done: ...s
```

### 8.4 Fallback: `docker save` + `ctr import` for nodes not on the registry

If a node cannot reach `<REGISTRY>`, transfer the image as a tarball and import it into
containerd's `k8s.io` namespace directly:

```bash
# On the build node:
docker save lmcache-ascend:hccl-p2p -o lmcache-ascend-hccl-p2p.tar
#   scp lmcache-ascend-hccl-p2p.tar <target-node>:/tmp/

# On the target node:
sudo ctr -n k8s.io images import /tmp/lmcache-ascend-hccl-p2p.tar
sudo ctr -n k8s.io images ls | grep lmcache-ascend
```

---

## 9. Optional — `vllm-sched.diff` (Step 2b)

`docker/vllm-sched.diff` patches **vllm-ascend's** `AscendScheduler` to skip a request
when the KV connector cannot determine the number of externally matched tokens. It is
**DISABLED by default** (the `RUN` in the Dockerfile is commented out). You only need it
for **cross‑instance P2P / disaggregated‑prefill** setups; the default single‑instance
P2P image does not require it.

Full content of `docker/vllm-sched.diff`:

```diff
diff --git a/vllm_ascend/core/scheduler.py b/vllm_ascend/core/scheduler.py
index cc6822f..88c22e9 100644
--- a/vllm_ascend/core/scheduler.py
+++ b/vllm_ascend/core/scheduler.py
@@ -150,6 +150,14 @@ class AscendScheduler(Scheduler):
                         self.connector.get_num_new_matched_tokens(
                             request, num_new_local_computed_tokens))
 
+                    # TODO: newly added, also present in vllm scheduler
+                    if num_external_computed_tokens is None:
+                        # The request cannot be scheduled because
+                        # the KVConnector couldn't determine
+                        # the number of matched tokens.
+                        skip_cur_request()
+                        continue
+
                 # Total computed tokens (local + external).
                 num_computed_tokens = (num_new_local_computed_tokens +
                                        num_external_computed_tokens)
```

To enable, uncomment the Step 2b block in `docker/Dockerfile.p2p`:

```dockerfile
# 2b) OPTIONAL: only for cross-instance P2P / disagg-prefill
RUN cd /vllm-workspace/vllm-ascend && \
    git apply -p1 --verbose /workspace/LMCache-Ascend/docker/vllm-sched.diff
```

Then rebuild (STEP 5) and re‑verify (STEP 6). Confirm the extra hunk landed:

```bash
docker run --rm --entrypoint bash lmcache-ascend:hccl-p2p -c \
  'grep -n "KVConnector couldn.t determine" /vllm-workspace/vllm-ascend/vllm_ascend/core/scheduler.py'
```

---

## 10. Reproducibility reference (checklist)

Pin every input; a divergence in any one of these changes the artifact.

- [ ] **Fork/branch**: `matthewygf/LMCache-Ascend`, branch `hccl_host_staging`.
- [ ] **Commit**: `5a577dfed0248ba9e32f324574dd7c9b28dcd130`
      (`git describe = v0.4.3-27-g5a577df`).
- [ ] **Submodule**: `third_party/kvcache-ops @ 579190dfc7acf288352ec48808d93b23eb8e2e9a`.
- [ ] **Base image**: `quay.io/ascend/vllm-ascend:v0.18.0-openeuler`, short id
      `bab0bb869c9c`.
- [ ] **lmcache wheel**: `0.4.3` (installed, then patched).
- [ ] **Patch 1**: `docker/lmcache-controller.diff` → LMCache site‑packages via
      `git apply` (adds `target_worker_id`).
- [ ] **Patch 2**: `docker/vllm-utils.diff` applied as *intent* via
      `docker/patch-vllm.sh` (ARM `USE_SCHED_YIELD` fix; `git apply` does **not** work
      against the newer base — see §11).
- [ ] **Patch 3 (optional)**: `docker/vllm-sched.diff` — disabled by default (§9).
- [ ] **kvcache-ops C++** compiled + installed editable (RUN 3).
- [ ] **`.git` + tags requirement**: `COPY . /workspace/LMCache-Ascend/` includes
      `.git`, which setuptools_scm reads to compute the version. **The `v0.4.3` tag
      must be present** in that `.git` so the version resolves to `0.4.4.dev27`
      (guess‑next‑dev from `v0.4.3` + 27 commits). No `v0.4.4` tag exists. A fresh fork
      clone without tags produces `0.1.devNNN` instead → run `git fetch --tags`
      (STEP 1) before building.
- [ ] **Final tag**: `lmcache-ascend:hccl-p2p`.

Version derivation, spelled out: there is **no `v0.4.4` tag**. `git describe` gives
`v0.4.3-27-g5a577df`; setuptools_scm's *guess-next-dev* bumps the patch component of
the last tag (`0.4.3 → 0.4.4`) and appends `.dev<distance>` where distance is the
number of commits since the tag (`27`) → **`0.4.4.dev27`**.

---

## 11. Troubleshooting

| # | Symptom (exact error / wrong output) | Root cause | Fix |
|---|---|---|---|
| 1 | pip/git in the build hang or fail with connection/timeout on a proxied node; or work on the host but not in the build | The build container has its **own** network namespace, so `127.0.0.1:3128` is not the host proxy; build‑args also aren't set | Build with `--network=host` **and** pass `--build-arg http_proxy=<PROXY_URL> --build-arg https_proxy=<PROXY_URL> --build-arg no_proxy=...` (§6.2). On a direct node, drop the proxy args; keep `--network=host` (harmless). |
| 2 | `libascend_hal.so: cannot open shared object file` during build when locating paths | `import vllm`/`import torch` pulls in `torch_npu`, which loads Ascend HAL — but there is no NPU in the builder | Never import vllm/torch during build. Use the fixed path `/vllm-workspace/vllm/vllm` and `pip show`. `ENV TORCH_DEVICE_BACKEND_AUTOLOAD=0` is a safeguard but does **not** stop vllm-ascend's explicit `import torch_npu`. For the STEP 0 probe, keep `TORCH_DEVICE_BACKEND_AUTOLOAD=0`. (This flag does **not** disable NPU at runtime — vLLM‑Ascend explicitly imports torch_npu; see §3 "Does `TORCH_DEVICE_BACKEND_AUTOLOAD=0` disable NPU at runtime?".) |
| 3 | `docker build` fails parsing a `RUN` with `<<` / heredoc: `unexpected token` / `/bin/sh: bad substitution` | Legacy Docker builder does not support heredoc inside `RUN` | Keep the Python heredoc in the separate `docker/patch-vllm.sh`; the `RUN` only calls `bash .../patch-vllm.sh`. |
| 4 | RUN 2 fails: `bash: /workspace/LMCache-Ascend/docker/patch-vllm.sh: No such file or directory` | `.dockerignore` line `docker/*.sh` excludes the script from the build context | Add the exception `!docker/patch-vllm.sh` to `.dockerignore` (§5.3). |
| 5 | RUN 3 prints `Successfully installed lmcache-ascend-0.1.devNNN` instead of `0.4.4.dev27` | The build context's `.git` has no `v0.4.3` tag, so setuptools_scm counts from the repo root | `git fetch --tags` (add `upstream` and fetch if needed) so `v0.4.3` is visible, then rebuild — `COPY .` includes `.git`, so the tag becomes visible to scm (§4, §10). |
| 6 | `patch-vllm.sh` aborts: `AssertionError: USE_SCHED_YIELD block not found — vLLM source changed, STOP and inspect` | The vLLM `USE_SCHED_YIELD = ...` block in the base image no longer matches the exact `old` string | The base image's vLLM changed. Inspect `/vllm-workspace/vllm/vllm/distributed/utils.py`, update the `old`/`new` blocks in `patch-vllm.sh` to match, and re‑confirm the two trailing `grep -q` checks pass. Do **not** silently skip the patch. |
| 7 | Trying `git apply docker/vllm-utils.diff` fails: `patch does not apply` / context mismatch around `get_tcp_uri` | The base image's vLLM is newer than the diff's baseline (imports refactored to `from vllm.utils.network_utils import get_tcp_uri`), so the diff's context lines no longer match | Do not `git apply vllm-utils.diff`. Apply the fix as **intent** via `docker/patch-vllm.sh` (RUN 2), which is resilient to the import refactor. |
| 8 | `ctr ... pull` fails with TLS/`http: server gave HTTP response to HTTPS client` | containerd defaulting to HTTPS for a plain‑http local registry | Pass `--plain-http` (§8.3), and ensure containerd's `certs.d/<REGISTRY>/hosts.toml` allows the local registry. Otherwise use the `docker save` + `ctr import` fallback (§8.4). |
| 9 | A reviewer insists the **`lmcache` patch must be an editable install** | Misconception (see §1 "Install method"): editable is *not* required — the default build patches the wheel's `.py` in site‑packages in place, which is what Python imports, verified live. Editable is only needed if you patch a *separate* `lmcache` source checkout. | Keep the default (wheel + in‑place `git apply`) — it is verified. **Only if** an editable `lmcache` is a hard requirement, replace RUN 1 with the equivalent below (patch the source, then `pip install -e .`). Re‑run §7; results are identical. |

Equivalent editable variant for RUN 1 (only if explicitly required — **not** the default):

```dockerfile
# ALTERNATIVE to RUN 1: editable lmcache 0.4.3 carrying the controller patch
RUN git clone --depth 1 --branch v0.4.3 https://github.com/LMCache/LMCache.git /opt/lmcache-src && \
    cd /opt/lmcache-src && \
    git apply -p1 --verbose /workspace/LMCache-Ascend/docker/lmcache-controller.diff && \
    grep -q target_worker_id /opt/lmcache-src/lmcache/v1/cache_controller/utils.py && \
    NO_CUDA_EXT=1 pip install --no-build-isolation -e .
```

> Trade‑offs of the editable variant: it builds `lmcache` from source (slower, needs the
> `v0.4.3` tag reachable on the LMCache repo) and the running import path becomes
> `/opt/lmcache-src/...` instead of site‑packages. The default in‑place method is simpler
> and is the one that produced the verified `lmcache-ascend:hccl-p2p` image.

---

### Appendix — `docker/lmcache-controller.diff` (reference)

Applied in RUN 1 with `git apply -p1` into the installed `lmcache` in site‑packages.
It adds `target_worker_id` plumbing to the P2P controller lookup:

```diff
diff --git a/lmcache/v1/cache_controller/controllers/kv_controller.py b/lmcache/v1/cache_controller/controllers/kv_controller.py
index 0b3a749..cf3266a 100644
--- a/lmcache/v1/cache_controller/controllers/kv_controller.py
+++ b/lmcache/v1/cache_controller/controllers/kv_controller.py
@@ -268,12 +268,13 @@ class KVController:
         :return: A BatchedP2PLookupRetMsg containing the lookup results.
         """
         hashes = msg.hashes
+        worker_id = msg.worker_id
         if not hashes:
             return BatchedP2PLookupRetMsg(layout_info=[("", "", 0, "")])
 
         # Single lookup to get all needed info (optimized path)
         result = self.registry.find_kv_with_worker_info(
-            hashes[0], exclude_instance_id=msg.instance_id
+            hashes[0], exclude_instance_id=msg.instance_id, target_worker_id=worker_id
         )
         if result is None:
             return BatchedP2PLookupRetMsg(layout_info=[("", "", 0, "")])
diff --git a/lmcache/v1/cache_controller/utils.py b/lmcache/v1/cache_controller/utils.py
index c54dc1d..82b82a7 100644
--- a/lmcache/v1/cache_controller/utils.py
+++ b/lmcache/v1/cache_controller/utils.py
@@ -251,6 +251,24 @@ class InstanceNode:
                 )
         return None
 
+    def find_worker_key(
+        self, key: int, target_worker_id: int
+    ) -> Optional[tuple[KVChunkInfo, Optional[str], set[int]]]:
+        """
+        Find a key in the given worker within this instance.
+        Returns: (KVChunkInfo, peer_init_url, keys) if found, None otherwise.
+        """
+        worker_node = self.get_worker(target_worker_id)
+        if worker_node and (result := worker_node.find_key(key)):
+            # Fill in the instance_id in KVChunkInfo
+            kv_info, peer_init_url, keys = result
+            return (
+                KVChunkInfo(self.instance_id, target_worker_id, kv_info.location),
+                peer_init_url,
+                keys,
+            )
+        return None
+
     def find_key_simple(self, key: int) -> Optional[KVChunkInfo]:
         """
         Find a key in any worker within this instance, returning only KVChunkInfo.
@@ -485,6 +503,7 @@ class RegistryTree:
     def find_kv_with_worker_info(
         self,
         key: int,
+        target_worker_id: int,
         exclude_instance_id: Optional[str] = None,
     ) -> Optional[tuple[KVChunkInfo, Optional[str], set[int]]]:
         """
@@ -497,7 +516,7 @@ class RegistryTree:
         for instance_id, instance_node in list(self.instances.items()):
             if exclude_instance_id is not None and instance_id == exclude_instance_id:
                 continue
-            result = instance_node.find_key(key)
+            result = instance_node.find_worker_key(key, target_worker_id=target_worker_id)
             if result is not None:
                 return result
         return None
```

### Appendix — `docker/vllm-utils.diff` (reference, NOT `git apply`‑able here)

This is the *intended* vLLM change. It is shown for reference only — the build applies
its intent via `docker/patch-vllm.sh` because the base image's vLLM baseline differs
(the `get_tcp_uri` import moved to `vllm.utils.network_utils`), which makes
`git apply` fail (see §11 row 7).

```diff
diff --git a/vllm/distributed/utils.py b/vllm/distributed/utils.py
index 67f7164..2dc24d6 100644
--- a/vllm/distributed/utils.py
+++ b/vllm/distributed/utils.py
@@ -26,6 +26,7 @@ from torch.distributed.rendezvous import rendezvous
 
 import vllm.envs as envs
 from vllm.logger import init_logger
+from vllm.platforms import CpuArchEnum, Platform
 from vllm.utils import get_tcp_uri, is_torch_equal_or_newer
 
 logger = init_logger(__name__)
@@ -33,9 +34,15 @@ logger = init_logger(__name__)
 # We prefer to use os.sched_yield as it results in tighter polling loops,
 # measured to be around 3e-7 seconds. However on earlier versions of Python
 # os.sched_yield() does not release the GIL, so we fall back to time.sleep(0)
-USE_SCHED_YIELD = ((sys.version_info[:3] >= (3, 11, 1))
-                   or (sys.version_info[:2] == (3, 10)
-                       and sys.version_info[2] >= 8))
+#
+# On Arm systems, os.sched_yield does not take effect, causing the GIL
+# (Global Interpreter Lock) to remain unrelinquished and resulting in CPU bound
+# issues. we should making the process execute time.sleep(0) instead to release
+# the GIL.
+USE_SCHED_YIELD = (
+    (sys.version_info[:3] >= (3, 11, 1))
+    or (sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8)
+) and Platform.get_cpu_architecture() != CpuArchEnum.ARM
 
 
 def sched_yield():
```

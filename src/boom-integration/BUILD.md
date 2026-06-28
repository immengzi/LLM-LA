# Building BooM Gateway — Steps That Worked

Environment: Huawei Cloud master node, no direct internet, corporate MITM
proxy at `172.18.100.92:8080`.

---

## Step 1 — Install Rust (one-time)

```bash
export https_proxy=http://peulerosweb:EulerOS_123@172.18.100.92:8080
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
```

Verify:

```bash
rustc --version    # should show 1.85+
cargo --version
```

---

## Step 2 — Configure Cargo proxy + crate mirror (one-time)

Create `~/.cargo/config.toml`:

```bash
cat > ~/.cargo/config.toml << 'EOF'
[source.crates-io]
replace-with = "rsproxy-sparse"

[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"

[http]
proxy = "http://peulerosweb:EulerOS_123@172.18.100.92:8080"
EOF
```

This tells cargo to:
- Use rsproxy.cn instead of crates.io (faster from China / Huawei Cloud)
- Route all HTTP traffic through the corporate proxy

---

## Step 3 — Build the binary

```bash
cd /path/to/BooMGateway-main
cargo build --release -p boom-main
```

First build takes ~10-20 minutes (downloads + compiles ~200 crates).
Subsequent builds take seconds (only recompiles changed code).

The binary lands at `target/release/boom-gateway`.

---

## Step 4 — Build the Docker image

Copy the Dockerfile into the BooM Gateway repo root (if not already there):

```bash
cp /path/to/microservice/boom-integration/Dockerfile /path/to/BooMGateway-main/
```

Build and push:

```bash
cd /path/to/BooMGateway-main
docker build -t reg.local:32000/boom-gateway:latest .
docker push reg.local:32000/boom-gateway:latest
```

This is fast (~seconds) — no compilation inside Docker, it just copies the
pre-built binary into an openeuler base image.

---

## What didn't work (and why)

### Multi-stage Docker build with rust:bookworm

Failed because:
1. `apt-get` couldn't reach `deb.debian.org` — no internet from inside Docker
2. After adding proxy env vars, cargo couldn't reach `rsproxy.cn` — the
   corporate MITM proxy presents a self-signed certificate that cargo's
   internal HTTP client (libcurl) rejects
3. Cargo has no config key to disable SSL verification (`ssl-verify` is not
   a recognized key)

### Solution: host build + minimal Docker image

Build on the host where Rust + proxy are properly configured, then package
the binary in a minimal Docker image. This bypasses all Docker networking
and SSL issues.

---

## Rebuilding after code changes

If BooM Gateway source code is updated:

```bash
cd /path/to/BooMGateway-main
cargo build --release -p boom-main         # incremental, fast
docker build -t reg.local:32000/boom-gateway:latest .
docker push reg.local:32000/boom-gateway:latest
```

Then restart the boom-proxy pod:

```bash
kubectl rollout restart deployment/boom-proxy -n vllm
```

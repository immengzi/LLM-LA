# Docker Pull Fails Behind SSL-Inspecting Proxy

## Symptom

Docker image pulls fail with:

```
error pulling image configuration: download failed after attempts=6:
tls: failed to verify certificate: x509: certificate signed by unknown authority
```

HTTP traffic works fine (`curl http://...`) but `docker pull` always retries and fails.

---

## Root Cause

Docker pulls happen in **two separate phases**, each contacting a different host:

| Phase | Host | Purpose |
|---|---|---|
| 1 | `registry-1.docker.io` | Fetch manifest/metadata (Amazon TLS cert — not intercepted) |
| 2 | `r2.cloudflarestorage.com` | Download layer blobs (Cloudflare R2 CDN — **intercepted**) |

A corporate proxy doing SSL inspection substitutes its own TLS certificate for the R2 traffic. Docker does not trust this certificate, so every layer download fails — even though the manifest phase succeeds silently.

The error message misleadingly implies `docker.io` is the problem. The actual failing request in the original error log points at:

```
docker-images-prod.6aa30f8b08e16409b46e0173d6de2f56.r2.cloudflarestorage.com
```

---

## Environment

- OS: openEuler / EulerOS (RPM-based, no `update-ca-certificates`)
- Proxy: `http://<user>:<pass>@172.18.100.92:8080`
- Docker configured with `HTTP_PROXY` / `HTTPS_PROXY` env via systemd drop-in

---

## Fix

### Step 1 — Extract the proxy CA cert from R2 traffic

```bash
openssl s_client -connect r2.cloudflarestorage.com:443 \
  -proxy 172.18.100.92:8080 -showcerts 2>/dev/null \
  | awk '/BEGIN CERTIFICATE/{c=""} {c=c"\n"$0} /END CERTIFICATE/{last=c} END{print last}' \
  > /tmp/r2-ca.crt

# verify it has content
cat /tmp/r2-ca.crt
```

The `awk` command extracts only the **last** cert in the chain, which is the root CA — the one that needs to be trusted.

### Step 2 — Install into system trust store

```bash
cp /tmp/r2-ca.crt /etc/pki/ca-trust/source/anchors/r2-ca.crt
update-ca-trust
```

### Step 3 — Install into Docker per-registry trust store

Docker checks `/etc/docker/certs.d/<hostname>/ca.crt` for each registry it contacts. Add the cert for both the base domain and the specific R2 bucket hostname:

```python
# save as /tmp/fix_docker_certs.py and run with python3
import os, shutil

src = '/tmp/r2-ca.crt'
dirs = [
    'r2.cloudflarestorage.com',
    'docker-images-prod.6aa30f8b08e16409b46e0173d6de2f56.r2.cloudflarestorage.com',
]
for d in dirs:
    path = '/etc/docker/certs.d/' + d
    os.makedirs(path, exist_ok=True)
    shutil.copy(src, path + '/ca.crt')
    print('done:', path)
```

```bash
python3 /tmp/fix_docker_certs.py
```

> **Note:** Use a script file rather than copy-pasting Python inline. Some terminals/chat interfaces mangle domain names (e.g. rendering `registry-1.docker.io` as a hyperlink), which corrupts inline commands.

### Step 4 — Configure Docker daemon proxy

```bash
mkdir -p /etc/systemd/system/docker.service.d
```

Write the following to `/etc/systemd/system/docker.service.d/proxy.conf` using vim (do not paste — terminals may mangle the content):

```ini
[Service]
Environment="HTTP_PROXY=http://<user>:<pass>@172.18.100.92:8080"
Environment="HTTPS_PROXY=http://<user>:<pass>@172.18.100.92:8080"
Environment="NO_PROXY=localhost,127.0.0.1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
```

### Step 5 — Restart Docker and verify

```bash
systemctl daemon-reload
systemctl restart docker
docker pull hello-world
```

Expected output:

```
latest: Pulling from library/hello-world
58dee6a49ef1: Pull complete
Digest: sha256:...
Status: Downloaded newer image for hello-world:latest
```

---

## Why Not Just Trust registry-1.docker.io?

The proxy does **not** intercept `registry-1.docker.io` — it uses the real Amazon certificate. Placing the proxy CA cert under `/etc/docker/certs.d/registry-1.docker.io/` has no effect on the actual failure. The blob downloads go directly to Cloudflare R2, which is a separate TLS connection.

---

## Applies To Nodes

This fix must be applied on **every node** that runs Docker and pulls images through the proxy — not just the master. For a Kubernetes cluster, repeat Steps 1–5 on each worker node.

---

## Summary

```
docker pull
  └── GET manifest   → registry-1.docker.io   (Amazon cert, proxy passes through) ✓
  └── GET blobs      → r2.cloudflarestorage.com (proxy intercepts, substitutes own cert) ✗
                                                  ↑ fix: trust the proxy CA for this host
```

---

# Docker Push to Private Registry Hangs Behind Corporate Proxy

## Symptom

`docker push` to an internal registry hangs indefinitely with no error:

```
The push refers to repository [reg.local:32000/kv-router]
db7915c6b563: Retrying in 1 second
ae1535207980: Retrying in 1 second
...
```

## Root Cause

The Docker daemon has its own proxy configuration in `/etc/systemd/system/docker.service.d/proxy.conf`, independent of the shell environment. If the corporate proxy is set there without excluding internal registries, **all** Docker traffic — including pushes to private registries — gets routed through the corporate proxy, which cannot reach internal hostnames like `reg.local`.

Setting `no_proxy=reg.local` inline in the shell does **not** help because that only affects the current shell process. The Docker daemon is a separate systemd service that reads its own environment from the proxy.conf drop-in.

## Diagnostic

Verify the registry is reachable when bypassing the proxy:

```bash
# should return 200 OK with {}
curl --noproxy reg.local http://reg.local:32000/v2/

# should list all repositories
curl --noproxy reg.local http://reg.local:32000/v2/_catalog
```

If both return correctly, the registry is healthy and the proxy is the problem.

Verify the proxy env is what Docker is using:

```bash
cat /etc/systemd/system/docker.service.d/proxy.conf
```

## Fix

Add `reg.local` to the `NO_PROXY` list in the Docker daemon proxy config:

```bash
sed -i 's/NO_PROXY=localhost,127.0.0.1,/NO_PROXY=localhost,127.0.0.1,reg.local,/' \
  /etc/systemd/system/docker.service.d/proxy.conf

# verify
cat /etc/systemd/system/docker.service.d/proxy.conf

systemctl daemon-reload && systemctl restart docker
docker push reg.local:32000/kv-router:latest
```

The resulting `NO_PROXY` line should look like:

```
Environment="NO_PROXY=localhost,127.0.0.1,reg.local,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
```

## Notes

- This fix must be applied on every cluster node that pushes or pulls from `reg.local`
- Any other internal hostnames (e.g. NFS server, internal DNS names) should also be added to `NO_PROXY` to avoid similar hangs
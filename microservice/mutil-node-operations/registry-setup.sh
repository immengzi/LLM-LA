#!/bin/bash
set -euo pipefail

REGISTRY_PORT=32000
REGISTRY_NAME="registry"
REGISTRY_DATA="/mnt/nvme1/registry"

echo "=== [1/4] Preflight ==="
echo "Host: $(hostname)"
echo "Registry port: ${REGISTRY_PORT}"
echo "Registry data: ${REGISTRY_DATA}"
echo

echo "=== [2/4] Start (or replace) standalone Docker registry on this host ==="
sudo mkdir -p "${REGISTRY_DATA}"

# Remove only the existing container named "registry" (does not affect other containers)
docker rm -f "${REGISTRY_NAME}" >/dev/null 2>&1 || true

docker run -d \
  --restart=always \
  -p "${REGISTRY_PORT}:5000" \
  -v "${REGISTRY_DATA}:/var/lib/registry" \
  --name "${REGISTRY_NAME}" \
  registry:2

echo "Registry container started."
echo

echo "=== [3/4] Verify registry is listening + responding locally ==="
sudo ss -lntp | grep ":${REGISTRY_PORT} " || {
  echo "ERROR: Nothing is listening on port ${REGISTRY_PORT}."
  exit 1
}

curl -s "http://localhost:${REGISTRY_PORT}/v2/" >/dev/null || {
  echo "ERROR: Registry API not responding on http://localhost:${REGISTRY_PORT}/v2/"
  exit 1
}

echo "Local registry API OK: http://localhost:${REGISTRY_PORT}/v2/"
echo "Local catalog (may be empty):"
curl -s "http://localhost:${REGISTRY_PORT}/v2/_catalog" || true
echo
echo

echo "=== [4/4] Notes / Optional steps (commented) ==="

cat <<'EOF'

# ------------------------------------------------------------------------------------
# OPTIONAL: Only needed if Docker on THIS machine does NOT already trust reg.local:32000
#           You already verified via:
#             docker info | grep -i 'Insecure Registries' -A5
#           If reg.local:32000 and/or 7.242.102.243:32000 are listed there, SKIP this.
# ------------------------------------------------------------------------------------
# sudo mkdir -p /etc/docker
# sudo cp -a /etc/docker/daemon.json "/etc/docker/daemon.json.bak.$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true
#
# cat <<JSON | sudo tee /etc/docker/daemon.json >/dev/null
# {
#   "insecure-registries": ["reg.local:32000", "7.242.102.243:32000"]
# }
# JSON
#
# sudo systemctl restart docker

# ------------------------------------------------------------------------------------
# OPTIONAL: If you want all nodes to use reg.local -> 7.242.102.243 consistently,
#           update /etc/hosts on ALL machines (master + workers).
# ------------------------------------------------------------------------------------
# echo "7.242.102.243 reg.local" | sudo tee -a /etc/hosts
# getent hosts reg.local

# ------------------------------------------------------------------------------------
# OPTIONAL: Quick remote check from another node:
#   nc -vz reg.local 32000
#   curl -s http://reg.local:32000/v2/ && echo
# ------------------------------------------------------------------------------------

EOF

echo "Done."
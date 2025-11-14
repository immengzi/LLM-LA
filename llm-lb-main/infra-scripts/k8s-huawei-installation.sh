#!/usr/bin/env bash
set -euo pipefail

########################################
# Configuration
########################################

# Proxy settings
PROXY_HOST="192.168.0.100"
PROXY_PORT="3128"
PROXY="http://${PROXY_HOST}:${PROXY_PORT}"

# Cluster network ranges
POD_CIDR="10.1.0.0/16"
SVC_CIDR="10.152.183.0/24"

# Additional domains/IPs to bypass the proxy
EXTRA_NO_PROXY="*.huawei.com,*.inhuawei.com"

# MicroK8s channel to install
M8S_CHANNEL="1.30/stable"

# Enable GPU stack (requires NVIDIA driver already installed on host)
ENABLE_GPU="true"

# Test imagesge'w'i
HELLO_IMAGE="hello-world"
CUDA_IMAGE="nvidia/cuda:12.2.0-runtime-ubuntu22.04"

########################################
# Derived settings
########################################

NODE_IP="$(hostname -I | awk '{print $1}')"
NO_PROXY_LIST="127.0.0.1,localhost,${NODE_IP},${POD_CIDR},${SVC_CIDR}"
if [[ -n "${EXTRA_NO_PROXY}" ]]; then
  NO_PROXY_LIST="${NO_PROXY_LIST},${EXTRA_NO_PROXY}"
fi

echo "Proxy: ${PROXY}"
echo "No Proxy: ${NO_PROXY_LIST}"
echo "Node IP: ${NODE_IP}"
echo "MicroK8s channel: ${M8S_CHANNEL}"
echo "NPU enable: ${ENABLE_GPU}"

########################################
# Optional: install corporate CA if needed
########################################
# Place the CA cert at /tmp/corp-ca.crt and uncomment this block
# if [[ -f /tmp/corp-ca.crt ]]; then
#   echo "Installing corporate CA..."
#   sudo cp /tmp/corp-ca.crt /usr/local/share/ca-certificates/corp-proxy.crt
#   sudo update-ca-certificates
#   sudo systemctl restart snapd
# fi

########################################
# Step 1: Configure snap proxy before installing MicroK8s
########################################
echo "Setting snap proxy..."
sudo snap set system proxy.http="${PROXY}"
sudo snap set system proxy.https="${PROXY}"

########################################
# Step 2: Remove any existing MicroK8s
########################################
echo "Removing old MicroK8s if present..."
sudo microk8s stop || true
sudo snap remove microk8s --purge || true
sudo rm -rf /var/snap/microk8s /root/snap/microk8s ~/.microk8s || true

########################################
# Step 3: Install MicroK8s
########################################
echo "Installing MicroK8s..."
sudo snap install microk8s --classic --channel="${M8S_CHANNEL}"

########################################
# Step 4: Configure MicroK8s containerd proxy
########################################
echo "Configuring MicroK8s containerd environment..."
sudo mkdir -p /var/snap/microk8s/current/args
sudo tee /var/snap/microk8s/current/args/containerd-env >/dev/null <<EOF
HTTP_PROXY=${PROXY}
HTTPS_PROXY=${PROXY}
NO_PROXY=${NO_PROXY_LIST}
EOF

########################################
# Step 5: Add current user to microk8s group
########################################
echo "Adding ${USER} to microk8s group..."
sudo usermod -aG microk8s "$USER" || true
sudo mkdir -p ~/.kube
sudo chown -R "$USER":"$USER" ~/.kube || true

########################################
# Step 6: Restart MicroK8s and wait
########################################
echo "Restarting MicroK8s..."
sudo microk8s stop
sudo systemctl restart snap.microk8s.daemon-containerd
sudo microk8s start
sudo microk8s status --wait-ready

########################################
# Step 7: Enable core addons
########################################
echo "Enabling core addons (dns, flannel, storage)..."
sudo microk8s enable dns
sudo microk8s enable flannel
sudo microk8s enable storage
sudo microk8s status --wait-ready

########################################
# Step 8: Optional GPU setup
########################################
if [[ "${ENABLE_GPU}" == "true" ]]; then
  echo "Preparing NVIDIA runtime..."
  if ! command -v nvidia-ctk >/dev/null 2>&1; then
    echo "Installing NVIDIA container toolkit..."
    distribution=$(. /etc/os-release; echo ${ID}${VERSION_ID})
    curl -s -L https://nvidia.github.io/libnvidia-container/gpgkey \
      | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
    curl -s -L https://nvidia.github.io/libnvidia-container/${distribution}/libnvidia-container.list \
      | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
      | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null
    sudo apt-get update
    sudo apt-get install -y nvidia-container-toolkit
  fi

  echo "Configuring NVIDIA runtime for containerd..."
  sudo nvidia-ctk runtime configure --runtime=containerd

  echo "Restarting MicroK8s..."
  sudo systemctl restart snap.microk8s.daemon-containerd
  sudo microk8s stop
  sudo microk8s start
  sudo microk8s status --wait-ready

  echo "Enabling GPU operator..."
  sudo microk8s enable gpu
fi

########################################
# Step 9: Test deployments
########################################
echo "Cluster info:"
sudo microk8s kubectl get nodes -o wide
sudo microk8s kubectl get pods -A | head -n 20 || true

echo "Running hello-world test..."
sudo microk8s kubectl delete pod hello-world --ignore-not-found
sudo microk8s kubectl run hello-world --image="${HELLO_IMAGE}" --restart=Never
sudo microk8s kubectl wait --for=condition=Ready=False --timeout=30s pod/hello-world || true
sudo microk8s kubectl logs hello-world || true

if [[ "${ENABLE_GPU}" == "true" ]]; then
  echo "Running GPU test..."
  sudo microk8s kubectl delete pod gpu-test --ignore-not-found
  sudo microk8s kubectl run gpu-test \
    --image="${CUDA_IMAGE}" \
    --restart=Never --command -- nvidia-smi
  for i in {1..18}; do
    PHASE="$(sudo microk8s kubectl get pod gpu-test -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown)"
    [[ "${PHASE}" == "Running" || "${PHASE}" == "Succeeded" || "${PHASE}" == "Failed" ]] && break
    sleep 5
  done
  sudo microk8s kubectl logs gpu-test || true
fi

########################################
# Step 10: Group permissions for current session
########################################
if id -nG "$USER" | grep -qw microk8s; then
  echo "Applying group permissions for this session..."
  newgrp microk8s <<'EOS'
microk8s kubectl get nodes
EOS
else
  echo "Log out and back in (or run 'newgrp microk8s') to use microk8s without sudo."
fi

echo "Installation and testing complete."
if [[ "${ENABLE_GPU}" == "true" ]]; then
  echo "GPU operator has been enabled and tested."
fi

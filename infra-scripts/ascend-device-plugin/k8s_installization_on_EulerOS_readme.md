# Kubernetes Deployment Guide (EulerOS 2.0 SP10 + Docker 24.0.7 + k8s 1.23.1)

> This document is for single-node Kubernetes cluster deployment, based on **EulerOS 2.0 (SP10)**, Docker version 24.0.7, and Kubernetes version 1.23.1.

## Set up yum repo

The yum repo needs to be set up for k8s installation. `EulerOS.repo` and `kubernetes.repo` have to be set up. In theory only the EulerOS yum repo is needed, but the OpenEuler repo has more resources so it's recommended to have it too. The compatible version of OpenEuler for the corresponding EulerOS can be found online.

The following setup is for 184 machine with the EulerOS 2.0 (SP10), for other EulerOS versions please find the corresponding repo in this link:  
https://mirrors.tools.huawei.com/mirrorDetail/5ea633e18192505e39b1c1e5?mirrorName=euler&catalog=os  

For other OpenEuler versions please find the corresponding repo in this link:  
https://mirrors.tools.huawei.com/openeuler/

> **Tips**: All yum-related commands must be run with `sudo`.

- EulerOS.repo
```ini
[base]
name=EulerOS-2.0SP10 base
baseurl=http://mirrors.tools.huawei.com/euler/2.10/os/aarch64/
enabled=1
gpgcheck=1
gpgkey=https://mirrors.tools.huawei.com/euler/2.10/os/RPM-GPG-KEY-EulerOS
```

- OpenEuler.repo
```ini
[openEuler-everything]
name=openEuler-everything
baseurl=https://mirrors.tools.huawei.com/openeuler/openEuler-24.09/everything/aarch64/
enabled=1
gpgcheck=1
gpgkey=https://mirrors.tools.huawei.com/openeuler/openEuler-24.09/everything/aarch64/RPM-GPG-KEY-openEuler

[openEuler-EPOL]
name=openEuler-epol
baseurl=https://mirrors.tools.huawei.com/openeuler/openEuler-24.09/EPOL/main/aarch64/
enabled=1
gpgcheck=1

[openEuler-update]
name=openEuler-update
baseurl=https://mirrors.tools.huawei.com/openeuler/openEuler-24.09/update/aarch64/
enabled=1
gpgcheck=1
```

- K8s repo
```ini
[kubernetes] 
name=Kubernetes 
baseurl=https://mirrors.huaweicloud.com/kubernetes/yum/repos/kubernetes-el7-aarch64/ 
enabled=1 
gpgcheck=1 
repo_gpgcheck=0 
gpgkey=https://mirrors.huaweicloud.com/kubernetes/yum/doc/yum-key.gpg https://mirrors.huaweicloud.com/kubernetes/yum/doc/rpm-package-key.gpg 
```

- Setup steps (take the kubernetes.repo as example):
- Create the kubernetes.repo file in the /etc/yum.repos.d/ directory:
```bash
touch /etc/yum.repos.d/kubernetes.repo
```

- 0pen the kubernetes.repo file:
```bash
vi /etc/yum.repos.d/kubernetes.repo
```

- Press "i" to enter edit mode and add the following content:
```ini
[kubernetes] 
name=Kubernetes 
baseurl=https://mirrors.huaweicloud.com/kubernetes/yum/repos/kubernetes-el7-aarch64/ 
enabled=1 
gpgcheck=1 
repo_gpgcheck=0 
gpgkey=https://mirrors.huaweicloud.com/kubernetes/yum/doc/yum-key.gpg https://mirrors.huaweicloud.com/kubernetes/yum/doc/rpm-package-key.gpg
```

- Run:
```bash
yum makecache
```

## Install and configure K8s nodes
- Set the hostname for each node
    - Management Node:

```bash
hostnamectl set-hostname master
```
    Note: Other nodes' names and /etc/hosts files are not modified in this case since it's single-node implementation.

- Turn off the firewall:
```bash
systemctl stop firewalld && systemctl disable firewalld
```
- Modify Docker JSON:
(In this case, Docker is already installed, so only the following "exec-opts" needs to be added to /etc/docker/daemon.json.)
```bash
cat <<EOF > /etc/docker/daemon.json
{
        "exec-opts": ["native.cgroupdriver=systemd"]
}
EOF
```

- The file content should be as follows:
```json
{
        "default-runtime": "ascend",
        "default-shm-size": "8G",
        "runtimes": {
                "ascend": {
                        "path": "/usr/local/Ascend/Ascend-Docker-Runtime/ascend-docker-runtime",
                        "runtimeArgs": []
                }
        },
        "registry-mirrors": ["http://registry-cbu.huawei.com"],
        "insecure-registries": ["http://registry-cbu.huawei.com"],
        "bip": "192.168.1.1/24",
        "default-address-pools": [
                {"base": "174.0.0.0/8", "size": 24}
        ],
        "exec-opts": ["native.cgroupdriver=systemd"],
        "experimental": true,
        "data-root": "/mnt/storage1/docker-data"
}
```
```bash
systemctl daemon-reload
```

```bash
systemctl restart docker
```
> **Alert**: This step requires restarting Docker. Please coordinate with other users first!!!

- Enable the NET.BRIDGE.BRIDGE-NF-CALL-IPTABLES kernel option:
```bash
sysctl -w net.bridge.bridge-nf-call-iptables=1
```
- Disable the enabled swap partition:
```bash
swapoff -a
```
> **Note**: So far this step was not done on 184:
```bash
Open /etc/fstab:
vi /etc/fstab
Press "i" to enter edit mode, and comment out the swap line:
# /dev/mapper/openeuler-swap none                    swap    defaults        0 0
```
- Install k8s:
```bash
yum install -y kubelet kubeadm kubectl kubernetes-cni
```
- Check installation:
```bash
rpm -qa | grep kubelet
rpm -qa | grep kubeadm
rpm -qa | grep kubectl
rpm -qa | grep kubernetes-cni
```
- Set iptables:
```bash
echo "net.bridge.bridge-nf-call-iptables=1" > /etc/sysctl.d/k8s.conf
```
- Enable kubelet:
```bash
systemctl enable kubelet
```
- Set NTP
    - Install the chrony component on all nodes and set the time zone, which is configured as the Asia time zone in this document:
```bash
yum install -y chrony
timedatectl set-timezone Asia/Shanghai
```

Set the management node as the NTP server node:
Edit /etc/chrony.conf:
```bash
vi /etc/chrony.conf
```
Add:
```bash
allow 192.168.1.0/16
```
> **Note**: In this tutorial, this part is set up but not sure if correct — needs double-check.

Add:
```bash
local stratum 10
```
> **Note**: In this tutorial, this part is set up but not sure if correct — needs double-check.

- Configure the compute node as an NTP client:
Edit /etc/chrony.conf:
```bash
vi /etc/chrony.conf
```
Comment out:
```bash
# pool pool.ntp.org iburst
```
Note: In this tutorial, this part is set up but not sure if correct — needs double-check.

Add:
```bash
server master iburst
```
Note: In this tutorial, this part is set up but not sure if correct — needs double-check.

- Start the NTP service:
    - On all nodes:
```bash
systemctl enable chronyd.service
systemctl start chronyd.service
systemctl status chronyd.service
```
- Verify time synchronization:
```bash
chronyc sources
```
- View the list of images and use it as a basis to match the Docker image version that needs to be downloaded:
```bash
kubeadm config images list
```
- Download images from DockerHub:
(I have incorporated this section from the official document, but I was unable to confirm the sourcing for the images. I recommend we double-check their origin)
```bash
docker pull k8s.gcr.io/kube-apiserver:v1.23.1 
docker pull k8s.gcr.io/kube-controller-manager:v1.23.1 
docker pull k8s.gcr.io/kube-scheduler:v1.23.1 
docker pull k8s.gcr.io/kube-proxy:v1.23.1 
docker pull k8s.gcr.io/pause:3.6 
docker pull k8s.gcr.io/etcd:3.5.1-0 
docker pull k8s.gcr.io/coredns/coredns:v1.8.6
```
- Check all the images:
```bash
docker images | grep k8s
```
- Deploying a K8s Cluster
    - Configuring the Management Node
    Optional: Check if a proxy is configured.
    If a proxy is already configured, delete it to avoid timeout failures during kubeadm init initialization. (I didn’t encounter timeout errors, so I skipped proxy setup.)

    Check if a proxy is configured:
```bash
env | grep -E "http_proxy|https_proxy|no_proxy"
```
    If the command returns results, a proxy is configured.

    - Delete the proxy:
```bash
export -n http_proxy
export -n https_proxy
export -n no_proxy
```
- Create the resolv.conf file:
```bash
touch /etc/resolv.conf
```
- Execute the cluster initialization command on the management node:
```bash
kubeadm init --pod-network-cidr=10.244.0.0/16 --kubernetes-version v1.23.1
```
> **Note**: If you see the following message, initialization succeeded:
Your Kubernetes control-plane has initialized successfully! ...

## Configure the cluster:
```bash
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
export KUBECONFIG=/etc/kubernetes/admin.conf
```
- View cluster node information on the management node:
```bash
kubectl get nodes
```
- Check the kubelet service status on both the management node and the compute node:
```bash
systemctl status kubelet
```
> **Note**: You should see Active: active (running).

- Add the Flannel network plugin to the management node to resolve network communication issues between Pods on various host nodes:
    - Download the Flannel network plugin configuration file:
```bash
wget --no-check-certificate https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml
```
- Modify the kube-flannel.yml file to configure resources:
(Add the following under container: section:)
```json
        resources:
          requests:
            cpu: "100m"
            memory: "50Mi"
          limits:
            cpu: "200m"
            memory: "100Mi"
```

- Install Flannel:
```bash
kubectl apply -f kube-flannel.yml
```
- Check nodes status:
```bash
kubectl get nodes
```
- Check pods status:
```bash
kubectl get pod -A
```
## Operation and Verification
- Verifying the Deployment Results of the K8s Cluster
- Create a file named nginx_deploy.yaml in the management node:
```bash
vi nginx_deploy.yaml
```
- Add the following content:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.14.2
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 80
```
- Create the Nginx pod:
```bash
kubectl create -f nginx_deploy.yaml
```
- Check pod:
```
kubectl get pod --all-namespaces -o wide
```
> **Tip**: K8s only allows adding pods when the disk usage is less than 85%. If the pod status is Pending and the event shows disk-pressure, clean your disk first.

- The official k8s documentation is here:https://www.hikunpeng.com/document/detail/zh/kunpengcpfs/ecosystemEnable/Kubernetes/kunpengk8s_04_0007.html

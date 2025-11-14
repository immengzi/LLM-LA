# Ascend device plugin installization 

## Overview

- This is a tutorical of ascend device plugin installization on single node, with NPU 910B4 chips. Before you start to install this, you should have the CANN software stack, Ascend Docker Runtime, and Kubernetes installed on the host machine. The link to the offical installization document is: https://www.hiascend.com/document/detail/zh/mindcluster/70rc1/clustersched/dlug/dlug_installation_001.html

- Note: You may find rescource of ascend device plugin on W3 with name of MindCluster and MindX DL, they are the same product with different name. 

- To check whether your NPU chip is supported by the MindCluster, you need to check the corrosponding product model given the NPU chip type. For example, the 184 machine use NPU 910B4, which is the Atlas800I A2 product model and supported by the MindCluster. You can quickly find the product model of your NPU chip from this link: https://3ms.huawei.com/km/groups/3296493/blogs/details/17793100

## Content

- [Build Ascend device plugin Image](#build-ascend-device-plugin-image)
- [Build Ascend device plugin](#Build-Ascend-device-plugin)
- [Assign NPU to pod](#Assign-NPU-to-pod)


## Build Ascend device plugin Image

- To build the image, the software package for the cluster scheduling component needs to be installed. You can find the correct version of the software package based on the version of your CANN , Pytorch and CPU architacheture from this link: 
  https://www.hiascend.com/developer/download/community
  For example on the 184 machine, you can download the Ascend-mindxdl-device-plugin_7.1.RC1_linux-aarch64.zip

- Unzip the software package, check whether the nodes used to build the cluster scheduling component image contain the following base images:
  * docker pull ubuntu:18.04 
  * docker pull arm64v8/alpine:latest (Optional, only if you need to install Volcano)

- Enter the component extraction directory and execute the docker build command to create the image.
```bash
docker build --no-cache -t ascend-k8sdeviceplugin:{tag} ./
```
(Note: Change the tag based on your MindCluster version, for example v7.1.RC1)

Tips: In the docker file the default ubuntu:18.04 keeps give me the following error: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64). Can solve this problem by changing the ubuntu version to ubuntu:24.04 in the docker file (image id of arm64v8/ubuntu (Tag: 24.04): 9d45648b4030).


## Build Ascend device plugin
- Execute the following command in the corresponding YAML path on the K8s management node to start the Ascend Device Plugin.
```bash
kubectl apply -f device-plugin-910-v7.1.RC1.yaml 
```
- Execute the following command at any node to check if the component has started successfully.
```bash
kubectl get pod -n kube-system
```
  If you see the following content then the installization is successed:
```bash
NAME                                        READY   STATUS    RESTARTS   AGE
ascend-device-plugin-daemonset-d5ctz        1/1     Running   0          11s
```
- Since this tutorial is emplementing K8s and Ascend Device Plugin on single node, when you run the following command to check the status of ascend-device-plugin-daemonset:
```bash
kubectl get daemonset ascend-device-plugin-daemonset -n kube-system
```
You may see the following infomation:
```bash
NAME                             DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR                     AGE
ascend-device-plugin-daemonset   0         0         0       0            0           accelerator=huawei-Ascend910   2d20h
```
This is because the ascend-device-plugin-daemonset set the NODE SELECTOR to be accelerator=huawei-Ascend910, and so far we haven't specify this label on any node. Thus, we need to manually add this label to the node so that the DaemonSet can recognize it.

First find the node you want to label:
```bash
kubectl get nodes
```
Label this node:
```bash
kubectl label node <node name> accelerator=huawei-Ascend910
```
Now when you check the status of ascend-device-plugin-daemonset you should be able to see:
```bash
NAME                             DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR                     AGE
ascend-device-plugin-daemonset   1         1         1       1            1           accelerator=huawei-Ascend910   2d20h
```
When you run the following command:
```bash
kubectl describe node master
```
You can see in the capacity and allocatable section, the huawei.com/Ascend910: 8.

## Assign NPU to pod
You can specify the NPU rescource by adding the following content under -container in the yaml file:
```yaml
resources:
  limits:
    huawei.com/Ascend910: 2  # maximum 2 NPU
  requests:
    huawei.com/Ascend910: 2  # request 2 NPU
```
The example yaml file with a test python script can be found in the test_pod folder.








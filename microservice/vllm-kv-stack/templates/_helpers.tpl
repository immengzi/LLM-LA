{{- define "vllmkv.sidecarMode" -}}
{{- if eq (lower .Values.router.mode) "pull" -}}
pull
{{- else -}}
push
{{- end -}}
{{- end -}}

{{- define "vllmkv.kedaMinReplicas" -}}
{{- if .Values.autoscaling.minReplicaCount -}}
{{- .Values.autoscaling.minReplicaCount -}}
{{- else -}}
{{- .Values.replicas.vllm -}}
{{- end -}}
{{- end -}}

{{/*
Strip an existing registry from an image reference, if present.

Examples:
- "redis:7-alpine" -> "redis:7-alpine"
- "kv-router:latest" -> "kv-router:latest"
- "quay.io/ascend/vllm-ascend:v0.11.0rc0" -> "ascend/vllm-ascend:v0.11.0rc0"
- "7.242.102.243:32000/kv-sidecar:latest" -> "kv-sidecar:latest"
*/}}
{{- define "vllmkv.stripRegistry" -}}
{{- $img := . | trim -}}
{{- $parts := splitList "/" $img -}}
{{- if lt (len $parts) 2 -}}
{{- $img -}}
{{- else -}}
  {{- $first := index $parts 0 -}}
  {{- $hasRegistry := or (contains "." $first) (contains ":" $first) (eq $first "localhost") -}}
  {{- if $hasRegistry -}}
    {{- join "/" (slice $parts 1) -}}
  {{- else -}}
    {{- $img -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Force all images to use .Values.global.imageRegistry if set, even if image already has a registry.
*/}}
{{- define "vllmkv.image" -}}
{{- $root := index . 0 -}}
{{- $img := index . 1 -}}
{{- $reg := default "" $root.Values.global.imageRegistry | trimSuffix "/" -}}
{{- if $reg -}}
{{- printf "%s/%s" $reg (include "vllmkv.stripRegistry" $img) -}}
{{- else -}}
{{- $img -}}
{{- end -}}
{{- end -}}

{{/*
Generate comma-separated device list for ASCEND_RT_VISIBLE_DEVICES.
Examples:
- tp=1 -> "0"
- tp=2 -> "0,1"
- tp=4 -> "0,1,2,3"
*/}}
{{- define "vllmkv.tpDevices" -}}
{{- $tp := .tp | int -}}
{{- $devices := "" -}}
{{- range $i := until $tp -}}
  {{- if gt $i 0 -}}
    {{- $devices = printf "%s,%d" $devices $i -}}
  {{- else -}}
    {{- $devices = printf "0" -}}
  {{- end -}}
{{- end -}}
{{- $devices -}}
{{- end -}}

{{/*
Ascend NPU driver host volumes — shared by all pods that need NPU access.
Centralised here to avoid repeating 5 hostPath entries in every Deployment.
*/}}
{{- define "vllmkv.ascendDriverVolumes" -}}
- name: dcmi-volume
  hostPath:
    path: /usr/local/dcmi
    type: Directory
- name: npu-smi-volume
  hostPath:
    path: /usr/local/bin/npu-smi
    type: File
- name: ascend-driver-lib64-volume
  hostPath:
    path: /usr/local/Ascend/driver/lib64/
    type: Directory
- name: version-info-volume
  hostPath:
    path: /usr/local/Ascend/driver/version.info
    type: File
- name: ascend-install-info-volume
  hostPath:
    path: /etc/ascend_install.info
    type: File
{{- end -}}

{{/*
Ascend NPU driver volumeMounts — pairs with ascendDriverVolumes above.
*/}}
{{- define "vllmkv.ascendDriverMounts" -}}
- name: dcmi-volume
  mountPath: /usr/local/dcmi
- name: npu-smi-volume
  mountPath: /usr/local/bin/npu-smi
- name: ascend-driver-lib64-volume
  mountPath: /usr/local/Ascend/driver/lib64/
- name: version-info-volume
  mountPath: /usr/local/Ascend/driver/version.info
- name: ascend-install-info-volume
  mountPath: /etc/ascend_install.info
{{- end -}}
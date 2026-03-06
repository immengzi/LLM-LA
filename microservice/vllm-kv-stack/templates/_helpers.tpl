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

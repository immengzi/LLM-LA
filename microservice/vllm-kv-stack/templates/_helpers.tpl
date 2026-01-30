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

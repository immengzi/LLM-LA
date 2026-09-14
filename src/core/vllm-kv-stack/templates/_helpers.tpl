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
Per-model autoscaling helpers (used by 60-keda-scaledobject.yaml).
All take a dict: { "root": $, "model": <model dict> }.
The model dict matches the entries synthesized in 40-vllm-unified.yaml, so
"name", "servedModelName", "replicas", and "dataParallel" are available.
*/}}

{{/*
vllmkv.modelAutoscaled — emits "true" when this model should get a ScaledObject.
A model is autoscaled when autoscaling is globally enabled AND the model is not
explicitly opted out via autoscaling.perModel.<name>.enabled: false.
*/}}
{{- define "vllmkv.modelAutoscaled" -}}
{{- $root := .root -}}
{{- $m := .model -}}
{{- $as := $root.Values.autoscaling | default dict -}}
{{- if $as.enabled -}}
  {{- $pm := get ($as.perModel | default dict) ($m.name | toString) -}}
  {{- if not (kindIs "map" $pm) }}{{- $pm = dict -}}{{- end -}}
  {{- if hasKey $pm "enabled" -}}
    {{- if $pm.enabled -}}true{{- end -}}
  {{- else -}}
    true
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
vllmkv.modelBaseReplicas — the model's configured replica count, matching the
fallback logic in 40-vllm-unified.yaml (DP groups for LeaderWorkerSet).
*/}}
{{- define "vllmkv.modelBaseReplicas" -}}
{{- $m := .model -}}
{{- $dp := $m.dataParallel | default dict -}}
{{- if $dp.enabled -}}
{{- $dp.groups | default ($m.replicas | default 1) -}}
{{- else -}}
{{- $m.replicas | default 1 -}}
{{- end -}}
{{- end -}}

{{/*
vllmkv.modelMinReplicas — KEDA minReplicaCount for this model:
per-model override → global autoscaling.minReplicaCount → model base replicas.
*/}}
{{- define "vllmkv.modelMinReplicas" -}}
{{- $root := .root -}}
{{- $m := .model -}}
{{- $as := $root.Values.autoscaling | default dict -}}
{{- $pm := get ($as.perModel | default dict) ($m.name | toString) -}}
{{- if not (kindIs "map" $pm) }}{{- $pm = dict -}}{{- end -}}
{{- if $pm.minReplicaCount -}}
{{- $pm.minReplicaCount -}}
{{- else if $as.minReplicaCount -}}
{{- $as.minReplicaCount -}}
{{- else -}}
{{- include "vllmkv.modelBaseReplicas" (dict "model" $m) -}}
{{- end -}}
{{- end -}}

{{/*
vllmkv.modelMaxReplicas — per-model override → global maxReplicaCount.
*/}}
{{- define "vllmkv.modelMaxReplicas" -}}
{{- $root := .root -}}
{{- $m := .model -}}
{{- $as := $root.Values.autoscaling | default dict -}}
{{- $pm := get ($as.perModel | default dict) ($m.name | toString) -}}
{{- if not (kindIs "map" $pm) }}{{- $pm = dict -}}{{- end -}}
{{- if $pm.maxReplicaCount -}}
{{- $pm.maxReplicaCount -}}
{{- else -}}
{{- $as.maxReplicaCount | default 16 -}}
{{- end -}}
{{- end -}}

{{/*
vllmkv.modelSignal — effective scaling signal ("queue" | "vllm" | "sglang") for a model.
*/}}
{{- define "vllmkv.modelSignal" -}}
{{- $root := .root -}}
{{- $m := .model -}}
{{- $as := $root.Values.autoscaling | default dict -}}
{{- $pm := get ($as.perModel | default dict) ($m.name | toString) -}}
{{- if not (kindIs "map" $pm) }}{{- $pm = dict -}}{{- end -}}
{{- if $pm.signal -}}
{{- lower $pm.signal -}}
{{- else -}}
{{- lower ($as.signal | default "queue") -}}
{{- end -}}
{{- end -}}

{{/*
vllmkv.kedaThreshold — KEDA trigger threshold for a model (signal-aware).
*/}}
{{- define "vllmkv.kedaThreshold" -}}
{{- $root := .root -}}
{{- $m := .model -}}
{{- $as := $root.Values.autoscaling | default dict -}}
{{- $pm := get ($as.perModel | default dict) ($m.name | toString) -}}
{{- if not (kindIs "map" $pm) }}{{- $pm = dict -}}{{- end -}}
{{- $sig := include "vllmkv.modelSignal" (dict "root" $root "model" $m) -}}
{{- $engine := include "vllmkv.engineType" (dict "root" $root "model" $m) | trim -}}
{{- if $pm.threshold -}}
{{- $pm.threshold -}}
{{- else if or (eq $sig "sglang") (and (eq $sig "vllm") (eq $engine "sglang")) -}}
{{- $as.sglangThreshold | default $as.vllmThreshold | default "0.8" -}}
{{- else if eq $sig "vllm" -}}
{{- $as.vllmThreshold | default "0.8" -}}
{{- else -}}
{{- $as.threshold | default "16" -}}
{{- end -}}
{{- end -}}

{{/*
vllmkv.kedaQuery — PromQL query for a model's ScaledObject trigger.
Precedence: per-model query → signal-derived query.
  queue → additive per-model router gauge (router_central_queue_length_by_model)
  vllm   → vLLM engine load (gpu KV-cache usage), router-independent
  sglang → SGLang token usage, router-independent
*/}}
{{- define "vllmkv.kedaQuery" -}}
{{- $root := .root -}}
{{- $m := .model -}}
{{- $as := $root.Values.autoscaling | default dict -}}
{{- $pm := get ($as.perModel | default dict) ($m.name | toString) -}}
{{- if not (kindIs "map" $pm) }}{{- $pm = dict -}}{{- end -}}
{{- $ns := $root.Release.Namespace -}}
{{- $served := $m.servedModelName | default $m.name -}}
{{- $sig := include "vllmkv.modelSignal" (dict "root" $root "model" $m) -}}
{{- $engine := include "vllmkv.engineType" (dict "root" $root "model" $m) | trim -}}
{{- if $pm.query -}}
{{- $pm.query -}}
{{- else if or (eq $sig "sglang") (and (eq $sig "vllm") (eq $engine "sglang")) -}}
{{- if $as.sglangQuery -}}
{{- $as.sglangQuery -}}
{{- else -}}
{{- printf "max({__name__=~\"sglang(:|_)token_usage\",namespace=%q,model_name=%q}) or vector(0)" $ns $served -}}
{{- end -}}
{{- else if eq $sig "vllm" -}}
{{- if $as.vllmQuery -}}
{{- $as.vllmQuery -}}
{{- else -}}
{{- printf "max(vllm:gpu_cache_usage_perc{model_name=\"%s\"})" $served -}}
{{- end -}}
{{- else if $as.prometheusQuery -}}
{{- $as.prometheusQuery -}}
{{- else -}}
{{- printf "max(router_central_queue_length_by_model{namespace=\"%s\",model=\"%s\"})" $ns $served -}}
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
Resolve the inference engine for a model. Per-model selection wins over the
global selector; the historical default remains vllm.
Context: dict with keys "root" and "model".
*/}}
{{- define "vllmkv.engineType" -}}
{{- lower (default (default "vllm" .root.Values.engine.type) .model.engine) -}}
{{- end -}}

{{/*
vllmkv.modelResourceName — Deployment/Service/app identity for a model.

Historically every engine workload was named vllm-<model>. That leaked the
vLLM product name into SGLang pods (vllm-qwen while running SGLang). Resource
names are now <engine>-<model> (vllm-qwen / sglang-qwen) so kubectl, Services,
KEDA targets, and Redis-facing pod names match the serving engine.
Context: dict with keys "root" and "model" (model.name required).
*/}}
{{- define "vllmkv.modelResourceName" -}}
{{- $engine := include "vllmkv.engineType" . | trim -}}
{{- printf "%s-%s" $engine (.model.name | toString) -}}
{{- end -}}

{{/* Resolve health and metrics contracts without changing vLLM defaults. */}}
{{- define "vllmkv.engineHealthPath" -}}
{{- $profile := get (.root.Values.engineProfiles | default dict) .engine | default dict -}}
{{- $engineValues := get .root.Values .engine | default dict -}}
{{- $modelValues := get .model .engine | default dict -}}
{{- $path := $modelValues.healthPath | default $engineValues.healthPath | default $profile.healthPath | default "/health" -}}
{{- if eq (trimSuffix "/" $path) "/health_generate" -}}/health{{- else -}}{{ $path }}{{- end -}}
{{- end -}}

{{- define "vllmkv.engineReadinessPath" -}}
{{- $profile := get (.root.Values.engineProfiles | default dict) .engine | default dict -}}
{{- $engineValues := get .root.Values .engine | default dict -}}
{{- $modelValues := get .model .engine | default dict -}}
{{- $ready := $modelValues.readinessPath | default $engineValues.readinessPath | default $profile.readinessPath -}}
{{- if $ready -}}{{ $ready }}{{- else -}}{{ include "vllmkv.engineHealthPath" . }}{{- end -}}
{{- end -}}

{{- define "vllmkv.engineMetricsPath" -}}
{{- $profile := get (.root.Values.engineProfiles | default dict) .engine | default dict -}}
{{- $engineValues := get .root.Values .engine | default dict -}}
{{- $modelValues := get .model .engine | default dict -}}
{{- $modelValues.metricsPath | default $engineValues.metricsPath | default $profile.metricsPath | default "/metrics" -}}
{{- end -}}

{{- define "vllmkv.engineMetricsPort" -}}
{{- $profile := get (.root.Values.engineProfiles | default dict) .engine | default dict -}}
{{- $engineValues := get .root.Values .engine | default dict -}}
{{- $modelValues := get .model .engine | default dict -}}
{{- $modelValues.metricsPort | default $engineValues.metricsPort | default $profile.metricsPort | default 8200 -}}
{{- end -}}

{{/*
Select the engine image while retaining the per-model override.
Context: dict with keys "root", "model", and "engine".
*/}}
{{- define "vllmkv.engineImage" -}}
{{- if .model.image -}}
{{- .model.image -}}
{{- else if eq .engine "sglang" -}}
{{- include "vllmkv.image" (list .root (default "lmsysorg/sglang:v0.5.15-cu129" .root.Values.images.sglang)) -}}
{{- else -}}
{{- include "vllmkv.image" (list .root .root.Values.images.vllm) -}}
{{- end -}}
{{- end -}}

{{/*
SGLang launch command for the supported non-DP Deployment profile.
Context: dict with keys root, model, servedModelName, tp.
*/}}
{{- define "vllmkv.sglangLaunchCommand" -}}
{{- $sv := .model.sglang | default dict -}}
{{- $gv := .root.Values.sglang | default dict -}}
{{- $kv := $gv.kvEvents | default dict -}}
exec python -m sglang.launch_server \
  --model-path /model \
  --served-model-name {{ .servedModelName }} \
  --host 0.0.0.0 \
  --port 8200 \
  --tp-size {{ .tp }} \
  --page-size {{ $sv.pageSize | default $gv.pageSize | default 16 }} \
  --mem-fraction-static {{ $sv.memFractionStatic | default $gv.memFractionStatic | default 0.9 }} \
  --enable-metrics \
{{- if or $sv.trustRemoteCode $gv.trustRemoteCode }}
  --trust-remote-code \
{{- end }}
{{- $tcp := $sv.toolCallParser | default $gv.toolCallParser -}}
{{- if $tcp }}
  --tool-call-parser {{ $tcp }} \
{{- end }}
{{- $rp := $sv.reasoningParser | default $gv.reasoningParser -}}
{{- if $rp }}
  --reasoning-parser {{ $rp }} \
{{- end }}
  --kv-events-config '{"publisher":"zmq","endpoint":"tcp://*:{{ $kv.port | default 5557 }}","replay_endpoint":"tcp://*:{{ $kv.replayPort | default 5558 }}","topic":"{{ $kv.topic | default "kv@" }}'"${POD_NAME}"'@{{ .servedModelName }}"}' \
{{- range ($sv.extraArgs | default $gv.extraArgs | default list) }}
  {{ . }} \
{{- end }}
  --log-level {{ $sv.logLevel | default $gv.logLevel | default "info" }}
{{- end -}}

{{/*
Engine-neutral sidecar aliases. VLLM_* variables remain alongside these for
backward compatibility with existing images.
Context: dict with keys engine and servedModelName.
*/}}
{{- define "vllmkv.inferenceSidecarEnv" -}}
- name: INFERENCE_ENGINE
  value: {{ .engine | quote }}
- name: INFERENCE_URL
  value: "http://127.0.0.1:8200"
- name: INFERENCE_HOST
  value: "127.0.0.1"
- name: INFERENCE_HEALTH_PATH
  value: {{ .healthPath | default "/health" | quote }}
{{- if and .readinessPath (ne .readinessPath .healthPath) }}
- name: INFERENCE_READINESS_PATH
  value: {{ .readinessPath | quote }}
{{- end }}
- name: INFERENCE_TIMEOUT_S
  value: "7200.0"
- name: KV_EVENT_PORT
  value: {{ .kvEventPort | default 5557 | quote }}
- name: KV_EVENT_REPLAY_PORT
  value: {{ .kvEventReplayPort | default 5558 | quote }}
- name: KV_EVENT_TOPIC
  value: {{ .kvEventTopic | default "kv@" | quote }}
{{- if eq .engine "sglang" }}
- name: KV_EVENT_EXPECTED_PAGE_SIZE
  value: {{ .expectedPageSize | default 16 | quote }}
- name: KV_EVENT_DISCOVERY_ENABLED
  value: "true"
- name: KV_EVENT_DISCOVERY_TIMEOUT_S
  value: {{ .kvEventDiscoveryTimeoutS | default 2.0 | quote }}
{{- end }}
{{- end -}}

{{/*
Select the router image based on .Values.serviceImpl ("python" | "go").
Context: the root $ context.
*/}}
{{- define "vllmkv.routerImage" -}}
{{- $root := . -}}
{{- if eq (lower (default "python" $root.Values.serviceImpl)) "go" -}}
{{- include "vllmkv.image" (list $root (default "kv-router-go:latest" $root.Values.images.routerGo)) -}}
{{- else -}}
{{- include "vllmkv.image" (list $root $root.Values.images.router) -}}
{{- end -}}
{{- end -}}

{{/*
Select the sidecar image based on .Values.serviceImpl ("python" | "go").
Context: the root $ context.
*/}}
{{- define "vllmkv.sidecarImage" -}}
{{- $root := . -}}
{{- if eq (lower (default "python" $root.Values.serviceImpl)) "go" -}}
{{- include "vllmkv.image" (list $root (default "kv-sidecar-go:latest" $root.Values.images.sidecarGo)) -}}
{{- else -}}
{{- include "vllmkv.image" (list $root $root.Values.images.sidecar) -}}
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
Hardware backend switch. Selects the accelerator vendor for the vLLM engine.
Set .Values.hardware to "nvidia" for NVIDIA GPUs; anything else (default
"ascend") keeps the historical Ascend NPU behaviour byte-for-byte. Typically
set per cluster via helm.values.hardware in configs/clusters.yaml.
Returns the non-empty string "true" for NVIDIA and "" (falsey) otherwise, so it
is safe to use directly in `if include "vllmkv.isNvidia" $`.
*/}}
{{- define "vllmkv.isNvidia" -}}
{{- if eq (lower (default "ascend" .Values.hardware)) "nvidia" -}}true{{- end -}}
{{- end -}}

{{/*
Kubernetes extended-resource name for the active hardware backend:
NVIDIA GPUs use the well-known "nvidia.com/gpu"; other backends request the
resource named by accelerator.resourceName. The public chart default is a
generic placeholder; real values are supplied by the deployment overlay.
Context: the root $ context.
*/}}
{{- define "vllmkv.acceleratorResource" -}}
{{- if include "vllmkv.isNvidia" . -}}nvidia.com/gpu{{- else -}}{{ .Values.accelerator.resourceName | default "accelerator.example.com/device" }}{{- end -}}
{{- end -}}

{{/*
Ascend NPU driver host volumes — shared by all pods that need NPU access.
Centralised here to avoid repeating 5 hostPath entries in every Deployment.
No-op when hardware=nvidia so DP/LeaderWorkerSet and other callers inherit the
same hardware switch as the standard Deployment path.
*/}}
{{- define "vllmkv.ascendDriverVolumes" -}}
{{- if not (include "vllmkv.isNvidia" .) -}}
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
{{- end -}}

{{/*
Ascend NPU driver volumeMounts — pairs with ascendDriverVolumes above.
No-op when hardware=nvidia (see ascendDriverVolumes).
*/}}
{{- define "vllmkv.ascendDriverMounts" -}}
{{- if not (include "vllmkv.isNvidia" .) -}}
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
{{- end -}}

{{/*
Common vLLM CLI flags shared by all deployment modes.
Context: dict with keys "mv" (per-model overrides), "gv" (global .Values.vllm), "batch" (batch size).
Each flag line ends with ' \' for bash continuation.
*/}}
{{- define "vllmkv.vllmRuntimeFlags" -}}
--dtype {{ .mv.dtype | default .gv.dtype | default "auto" }} \
--kv-cache-dtype {{ .mv.kvCacheDtype | default .gv.kvCacheDtype | default "auto" }} \
{{- $cpuOff := .mv.cpuOffloadGb | default .gv.cpuOffloadGb -}}
{{- if $cpuOff }}
--cpu-offload-gb {{ $cpuOff }} \
{{- end }}
--max-num-seqs {{ .batch }} \
{{- $epc := .mv.enablePrefixCaching | default .gv.enablePrefixCaching -}}
{{- if $epc }}
--prefix-caching-hash-algo sha256_cbor \
{{- else }}
--no-enable-prefix-caching \
{{- end -}}
{{- $eptd := .mv.enablePromptTokensDetails | default .gv.enablePromptTokensDetails -}}
{{- if $eptd }}
--enable-prompt-tokens-details \
{{- end -}}
{{- $gpuMem := .mv.gpuMemoryUtilization | default .gv.gpuMemoryUtilization -}}
{{- if $gpuMem }}
--gpu-memory-utilization {{ $gpuMem }} \
{{- end -}}
{{- $quant := .mv.quantization | default .gv.quantization -}}
{{- if $quant }}
--quantization {{ $quant }} \
{{- end -}}
{{- $eep := .mv.enableExpertParallel | default .gv.enableExpertParallel -}}
{{- if $eep }}
--enable-expert-parallel \
{{- end -}}
{{- $mvCC := .mv.compilationConfig | default nil -}}
{{- $gvCC := .gv.compilationConfig | default nil -}}
{{- if or $mvCC $gvCC -}}
{{- $cc := $mvCC | default $gvCC }}
--compilation-config '{"cudagraph_mode": "{{ $cc.cudagraphMode | default "FULL_DECODE_ONLY" }}"}' \
{{- end -}}
{{- if or .mv.trustRemoteCode .gv.trustRemoteCode }}
--trust-remote-code \
{{- end -}}
{{- $mnbt := .mv.maxNumBatchedTokens | default .gv.maxNumBatchedTokens -}}
{{- if $mnbt }}
--max-num-batched-tokens {{ $mnbt }} \
{{- end -}}
{{- $seed := .mv.seed | default .gv.seed -}}
{{- if $seed }}
--seed {{ $seed }} \
{{- end -}}
{{- $mml := .mv.maxModelLen | default .gv.maxModelLen -}}
{{- if $mml }}
--max-model-len {{ $mml }} \
{{- end -}}
{{- $mvAC := .mv.additionalConfig | default nil -}}
{{- $gvAC := .gv.additionalConfig | default nil -}}
{{- if or $mvAC $gvAC -}}
{{- $ac := $mvAC | default $gvAC -}}
{{- if $ac.multistreamOverlapSharedExpert }}
--additional-config '{"multistream_overlap_shared_expert": {{ $ac.multistreamOverlapSharedExpert }}}' \
{{- end -}}
{{- end -}}
{{- $mvSC := .mv.speculativeConfig | default nil -}}
{{- $gvSC := .gv.speculativeConfig | default nil -}}
{{- if or $mvSC $gvSC -}}
{{- $sc := $mvSC | default $gvSC -}}
{{- if $sc.numSpeculativeTokens }}
--speculative-config '{"num_speculative_tokens": {{ $sc.numSpeculativeTokens }}, "method": "{{ $sc.method }}"}' \
{{- end -}}
{{- end -}}
{{- $tcp := .mv.toolCallParser | default .gv.toolCallParser -}}
{{- if $tcp }}
--tool-call-parser {{ $tcp }} \
--enable-auto-tool-choice \
{{- end -}}
{{- $rp := .mv.reasoningParser | default .gv.reasoningParser -}}
{{- if $rp }}
--reasoning-parser {{ $rp }} \
{{- end -}}
{{- $schedCls := .mv.schedulerCls | default .gv.schedulerCls -}}
{{- if $schedCls }}
--scheduler-cls {{ $schedCls }} \
{{- end -}}
{{- $mlec := .mv.modelLoaderExtraConfig | default .gv.modelLoaderExtraConfig -}}
{{- if $mlec }}
--model-loader-extra-config '{{ $mlec }}' \
{{- end -}}
{{- end -}}

{{/*
Common env vars for the vLLM container.
Context: dict with keys "tp" (tensor parallel size), "root" (the root $ context).
*/}}
{{- define "vllmkv.vllmBaseEnv" -}}
- name: HF_HUB_OFFLINE
  value: "1"
- name: TRANSFORMERS_OFFLINE
  value: "1"
- name: HF_HUB_DISABLE_TELEMETRY
  value: "1"
- name: PYTHONHASHSEED
  value: "0"
- name: TOKENIZERS_PARALLELISM
  value: "false"
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: VLLM_USE_V1
  value: "1"
{{- if not (include "vllmkv.isNvidia" .root) }}
- name: ASCEND_RT_VISIBLE_DEVICES
  value: {{ include "vllmkv.tpDevices" (dict "tp" .tp) | quote }}
- name: HCCL_OP_EXPANSION_MODE
  value: "AIV"
- name: PYTORCH_NPU_ALLOC_CONF
  value: "expandable_segments:True"
- name: ASCEND_BUFFER_POOL
  value: {{ .root.Values.vllm.ascendBufferPool | default "4:8" | quote }}
- name: ASCEND_USE_SHORT_CONNECTION
  value: {{ .root.Values.vllm.ascendUseShortConnection | default "1" | quote }}
- name: ASCEND_CONNECT_TIMEOUT
  value: {{ .root.Values.vllm.ascendConnectTimeout | default "2000" | quote }}
{{- if .root.Values.vllm.ascendEnableFlashcomm1 }}
- name: VLLM_ASCEND_ENABLE_FLASHCOMM1
  value: "1"
{{- end }}
{{- if or .root.Values.mooncake.enabled .root.Values.lmcache.enabled }}
- name: OMP_PROC_BIND
  value: "false"
{{- end }}
{{- end }}
{{- end -}}

{{/* Minimal engine environment for SGLang; intentionally excludes vLLM/Ascend knobs. */}}
{{- define "vllmkv.sglangBaseEnv" -}}
- name: HF_HUB_OFFLINE
  value: "1"
- name: TRANSFORMERS_OFFLINE
  value: "1"
- name: HF_HUB_DISABLE_TELEMETRY
  value: "1"
- name: PYTHONHASHSEED
  value: "0"
- name: TOKENIZERS_PARALLELISM
  value: "false"
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
{{- end -}}

{{/*
NIC auto-detection script for Mooncake / HCCL.
Caller must have NODE_IP available as a shell variable.
*/}}
{{- define "vllmkv.nicDetectScript" -}}
local_ip="${POD_IP:-${NODE_IP}}"
nic_name=$(python3 -c "
import os, socket, struct, fcntl
local_ip = '${local_ip}'
SIOCGIFADDR = 0x8915
for iface in os.listdir('/sys/class/net/'):
    if iface == 'lo': continue
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ifreq = struct.pack('16sH14s', iface.encode(), socket.AF_INET, b'')
        res = fcntl.ioctl(sock.fileno(), SIOCGIFADDR, ifreq)
        ip = socket.inet_ntoa(res[20:24])
        sock.close()
        if ip == local_ip:
            print(iface)
            break
    except: pass
")
if [ -z "$nic_name" ]; then
  nic_name=eth0
  echo "[nic-detect] no iface matched local_ip=${local_ip}, fallback to ${nic_name}"
fi
{{- end -}}

{{/*
Inline Python Prometheus thread exporter for vLLM.
Caller must set VLLM_PID and POD_NAME shell variables before including.
*/}}
{{- define "vllmkv.threadExporter" -}}
{{ include "vllmkv.threadExporterOn" (dict "port" 9101) }}
{{- end -}}

{{/*
Inline Python Prometheus thread exporter on a caller-chosen port (per-card P/D
pods run one exporter per engine container and must not collide).
Context: dict "port". Caller must set VLLM_PID and POD_NAME shell variables.
*/}}
{{- define "vllmkv.threadExporterOn" -}}
VLLM_PID="$VLLM_PID" POD_NAME="$POD_NAME" python - <<'PY' &
import os, time
from http.server import BaseHTTPRequestHandler, HTTPServer
PID = int(os.environ["VLLM_PID"])
POD = os.environ.get("POD_NAME", "unknown")
PORT = {{ .port | int }}
def threads(pid):
    try: return len(os.listdir(f"/proc/{pid}/task"))
    except: return None
for _ in range(120):
    if os.path.exists(f"/proc/{PID}"): break
    time.sleep(0.25)
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404); self.end_headers(); return
        n = threads(PID)
        body = 'vllm_threads{pod="%s"} %d\n' % (POD, n) if n else "# pid not visible\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(body.encode())
    def log_message(self, *a): pass
HTTPServer(("0.0.0.0", PORT), H).serve_forever()
PY
{{- end -}}

{{/*
Sleep-mode vLLM base env for per-card P/D warm standby.
Same knobs as vllmkv.vllmBaseEnv, minus PYTORCH_NPU_ALLOC_CONF
(CaMemAllocator rejects expandable_segments under sleep-mode, SIGABRT) plus the
sleep-mode compatibility vars (VLLM_SERVER_DEV_MODE=1 to expose /sleep,
VLLM_WORKER_MULTIPROC_METHOD=spawn, VLLM_ASCEND_ENABLE_NZ=0 because wake_up
refuses FRACTAL_NZ). Context: dict "tp", "root" (the root $).
*/}}
{{- define "vllmkv.vllmSleepBaseEnv" -}}
- name: HF_HUB_OFFLINE
  value: "1"
- name: TRANSFORMERS_OFFLINE
  value: "1"
- name: HF_HUB_DISABLE_TELEMETRY
  value: "1"
- name: PYTHONHASHSEED
  value: "0"
- name: TOKENIZERS_PARALLELISM
  value: "false"
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: VLLM_USE_V1
  value: "1"
{{- if not (include "vllmkv.isNvidia" .root) }}
- name: ASCEND_RT_VISIBLE_DEVICES
  value: {{ include "vllmkv.tpDevices" (dict "tp" .tp) | quote }}
- name: HCCL_OP_EXPANSION_MODE
  value: "AIV"
- name: ASCEND_BUFFER_POOL
  value: {{ .root.Values.vllm.ascendBufferPool | default "4:8" | quote }}
- name: ASCEND_USE_SHORT_CONNECTION
  value: {{ .root.Values.vllm.ascendUseShortConnection | default "1" | quote }}
- name: ASCEND_CONNECT_TIMEOUT
  value: {{ .root.Values.vllm.ascendConnectTimeout | default "2000" | quote }}
- name: VLLM_SERVER_DEV_MODE
  value: "1"
- name: VLLM_WORKER_MULTIPROC_METHOD
  value: "spawn"
- name: VLLM_ASCEND_ENABLE_NZ
  value: "0"
{{- if or .root.Values.mooncake.enabled .root.Values.lmcache.enabled }}
- name: OMP_PROC_BIND
  value: "false"
{{- end }}
{{- end }}
{{- end -}}

{{/*
Device visibility for dual-engine P/D pods. The Ascend device plugin
(presetVirtualDevice) renumbers the allocated physical cards to 0..N-1 inside
the container, and ASCEND_RT_VISIBLE_DEVICES from the chart/plugin already
matches that view. Overriding it with physical IDs from the device plugin's
real-device annotation (accelerator.realDeviceAnnotation) is a second mapping
that fails with aclInit 107001 (Invalid device ID) on any pod whose physical
cards are not 0..N-1 (e.g. 5,4/3,2).
*/}}
{{- define "vllmkv.timeshareNpuDeviceScript" -}}
# The device-plugin writes {{ .Values.accelerator.realDeviceAnnotation }} after
# pod creation; the downwardAPI file can lag the container start. Wait for it
# so the engine never falls back to the hardcoded card list.
for ((_i=0; _i<120; _i++)); do
  for _ann in /etc/pod-annotations/ascend-alloc /etc/pod-annotations/ascend-real; do
    [ -s "${_ann}" ] && break 2
  done
  sleep 1
done
for _ann in /etc/pod-annotations/ascend-alloc /etc/pod-annotations/ascend-real; do
  [ -s "${_ann}" ] || continue
  ascend_real="$(cat "${_ann}" 2>/dev/null || true)"
  [ -n "${ascend_real}" ] || continue
  # MUST sort ascending: torch_npu aclInit fails with 107001 (Invalid device ID)
  # when ASCEND_RT_VISIBLE_DEVICES is in descending order
  # ("5,4"/"4,2" -> device_count()=0; "4,5"/"2,4" -> ok).
  # Prefer explicit "<vendor>-<n>" tokens (e.g. "Ascend910-4,Ascend910-7" or
  # "Device-4,Device-7"). A plain all-digits fallback would also capture the
  # vendor model number (the "910" in "Ascend910-4"), so only use it when the
  # annotation has no "<vendor>-<n>" tokens at all.
  dev="$(printf '%s\n' "${ascend_real}" | tr ',' '\n' | sed -nE 's/^.*-([0-9]+)$/\1/p' | sort -n | paste -sd, - || true)"
  if [ -z "${dev}" ]; then
    dev="$(printf '%s\n' "${ascend_real}" | grep -oE '[0-9]+' | sort -n | paste -sd, - || true)"
  fi
  if [ -n "${dev}" ]; then
    export ASCEND_RT_VISIBLE_DEVICES="${dev}"
    echo "[device-env] ASCEND_RT_VISIBLE_DEVICES=${dev} (from ${_ann}, sorted)"
    break
  fi
done
unset _ann
{{- end -}}

{{/*
Startup mutual-exclusion gate for either engine in a dual-engine P/D pod.
Exactly one engine may own the cards at any time.

- decode (`waitPeer=true`): boot order — never start before the prefill engine
  has been healthy once; then sleep prefill and wait for `is_sleeping=true`.
- prefill (`waitPeer=false`): restart safety — if decode is loading or awake,
  wait for it to be healthy, sleep it, and only then start; if decode is not
  running (port closed, initial boot or crashed container), start immediately.

This covers container restarts: a restarted engine must never initialise while
its peer is still loading or awake on the same cards (CaMem OOM -> native
`corrupted size vs. prev_size` crash loop). Context: dict "port" (peer http
port), "level" (sleep level), "waitPeer" (bool).
*/}}
{{- define "vllmkv.timeshareStartupGateScript" -}}
python3 - <<'PY'
import json
import socket
import time
import urllib.request

peer_port = {{ .port | int }}
peer = "http://127.0.0.1:{}".format(peer_port)
level = {{ .level | default 1 | int }}
wait_peer = {{ ternary "True" "False" (.waitPeer | default false) }}


def peer_open() -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        sock.connect(("127.0.0.1", peer_port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


# Phase 1: wait until the peer is healthy (we can sleep it).
started = time.monotonic()
while time.monotonic() - started < 600:
    if peer_open():
        try:
            with urllib.request.urlopen(peer + "/health", timeout=5) as resp:
                if resp.status == 200:
                    print(f"[startup-gate] peer {peer} healthy, will sleep it")
                    break
        except Exception:
            pass
    elif not wait_peer:
        # prefill: peer not running (initial boot or crashed container) — safe
        # to start now; decode must keep waiting for prefill's /health.
        print(f"[startup-gate] peer {peer} not running, safe to start")
        break
    time.sleep(5)
else:
    print(f"[startup-gate] WARN: peer {peer} never became healthy in 600s, "
          "proceeding anyway")

if peer_open():
    request = urllib.request.Request(
        peer + "/sleep",
        data=json.dumps({"level": level}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as resp:
        resp.read()

    for _ in range(120):
        try:
            with urllib.request.urlopen(peer + "/is_sleeping", timeout=5) as resp:
                if json.loads(resp.read()).get("is_sleeping") is True:
                    break
        except Exception:
            pass
        time.sleep(2)
else:
    print(f"[startup-gate] peer {peer} not running, skipping sleep handshake")

PY
{{- end -}}

{{/*
Pre-warm pool bootstrap: sleep the caller's own engine after it becomes
healthy, so the card joins the pool with BOTH engines asleep (poolBootSleep).
The rebalancer's first reconcile is then a drain-free wake of exactly the
target role engines. Context: dict "port" (own http port), "level" (sleep
level).
*/}}
{{- define "vllmkv.poolBootSleepScript" -}}
# poolBootSleep: sleep own engine so the card joins the pre-warm pool
python3 - <<'PY'
import json
import time
import urllib.request

self_url = "http://127.0.0.1:{{ .port }}"
level = {{ .level | default 1 | int }}

for _ in range(600):
    try:
        with urllib.request.urlopen(self_url + "/health", timeout=5) as resp:
            if resp.status == 200:
                break
    except Exception:
        pass
    time.sleep(5)

request = urllib.request.Request(
    self_url + "/sleep",
    data=json.dumps({"level": level}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=120) as resp:
    resp.read()

for _ in range(120):
    try:
        with urllib.request.urlopen(self_url + "/is_sleeping", timeout=5) as resp:
            if json.loads(resp.read()).get("is_sleeping") is True:
                break
    except Exception:
        pass
    time.sleep(2)
PY
{{- end -}}

{{/* ------------------------------------------------------------------ *
 * dyn-pd warmstandby helpers
 *
 * Both engines of a warm-standby pod run on the SAME NPUs, so the port plan
 * and the sleep/wake overlay are validated here, at render time, instead of
 * after a role flip (where a bad plan shows up as a woken engine that can
 * never bind its NPU-adapter port: EI0020 Bind_IP_Port -> error_codes=[-800]
 * -> recompute tail). See DELIVERY.md ("2026-09-14 multi-process port plan").
 * ------------------------------------------------------------------ */}}

{{/* Parse "start-end" into "start end" (validated). Context: the range string. */}}
{{- define "vllmkv.parsePortRange" -}}
{{/* printf coerces: a --set value without a dash arrives as int64, and a type
     error here would mask the real problem ("that is not a range"). */}}
{{- $raw := printf "%v" . -}}
{{- $parts := splitList "-" $raw -}}
{{- if ne (len $parts) 2 -}}
{{- fail (printf "port range %q must look like <start>-<end>" $raw) -}}
{{- end -}}
{{- $start := index $parts 0 -}}
{{- $end := index $parts 1 -}}
{{- if or (not (regexMatch "^[0-9]+$" $start)) (not (regexMatch "^[0-9]+$" $end)) -}}
{{- fail (printf "port range %q must contain two integers (got %q-%q)" $raw $start $end) -}}
{{- end -}}
{{- if gt (int $start) (int $end) -}}
{{- fail (printf "port range %q has start > end" $raw) -}}
{{- end -}}
{{- printf "%d %d" (int $start) (int $end) -}}
{{- end -}}

{{/* Validate the warmstandby port plan. Context: dict modelName/prefill/decode. */}}
{{- define "vllmkv.validateWarmStandbyPorts" -}}
{{- $modelName := .modelName -}}
{{- $prefill := .prefill | default dict -}}
{{- $decode := .decode | default dict -}}
{{- $pfHixl := $prefill.hixlListenPort | default 0 -}}
{{- $dcHixl := $decode.hixlListenPort | default 0 -}}
{{- if or (not $pfHixl) (not $dcHixl) -}}
{{- fail (printf "warmstandby model %q: hixlListenPort must be set for BOTH roles (prefill=%v decode=%v). Every engine of a warm-standby pod shares the pod's NPUs, so each role needs its OWN HIXL/NPU-adapter listen port (rendered as ASCEND_GLOBAL_RESOURCE_CONFIG comm_resource_config.listen_port). Pick two disjoint, non-reserved ports, e.g. prefill 16700 / decode 16800, and set them under prefillDecode.{prefill,decode}.hixlListenPort" $modelName $pfHixl $dcHixl) -}}
{{- end -}}
{{- range $role, $port := (dict "prefill" $pfHixl "decode" $dcHixl) -}}
{{- if not (regexMatch "^[0-9]+$" (printf "%v" $port)) -}}
{{- fail (printf "warmstandby model %q: %s.hixlListenPort=%v is not an integer" $modelName $role $port) -}}
{{- end -}}
{{- $p := int $port -}}
{{- if or (lt $p 1024) (gt $p 65520) -}}
{{- fail (printf "warmstandby model %q: %s.hixlListenPort=%d is outside the usable range 1024-65520" $modelName $role $p) -}}
{{- end -}}
{{- if or (eq $p 16666) (eq $p 16667) -}}
{{- fail (printf "warmstandby model %q: %s.hixlListenPort=%d is a CANN reserved port (16666-16667)" $modelName $role $p) -}}
{{- end -}}
{{- end -}}
{{- if eq (int $pfHixl) (int $dcHixl) -}}
{{- fail (printf "warmstandby model %q: prefill.hixlListenPort and decode.hixlListenPort must differ (both %d); the two engines share the pod's NPUs" $modelName (int $pfHixl)) -}}
{{- end -}}
{{- range $family := list "hcclHostSocketPortRange" "hcclSocketPortRange" -}}
{{- $pfRange := index $prefill $family -}}
{{- $dcRange := index $decode $family -}}
{{- if or (not $pfRange) (not $dcRange) -}}
{{- fail (printf "warmstandby model %q: %s must be set for BOTH roles so the two engines use disjoint socket ranges (prefill=%v decode=%v). Pick two wide ranges away from the CANN defaults 16666-16667 and from HCCL_IF_BASE_PORT's 16-port block, e.g. prefill host 62000-62050 / npu 63000-63050, decode host 64000-64050 / npu 65000-65050" $modelName $family $pfRange $dcRange) -}}
{{- end -}}
{{- $bounds := dict -}}
{{- range $role, $raw := (dict "prefill" $pfRange "decode" $dcRange) -}}
{{- $parsed := splitList " " (include "vllmkv.parsePortRange" $raw) -}}
{{- $_ := set $bounds $role (dict "start" (int (index $parsed 0)) "end" (int (index $parsed 1))) -}}
{{- end -}}
{{- $pf := index $bounds "prefill" -}}
{{- $dc := index $bounds "decode" -}}
{{- if and (le (int $pf.start) (int $dc.end)) (le (int $dc.start) (int $pf.end)) -}}
{{- fail (printf "warmstandby model %q: prefill.%s=%s overlaps decode.%s=%s; the two engines share the pod's NPUs and cannot overlap (unlike the old single-engine layout)" $modelName $family $pfRange $family $dcRange) -}}
{{- end -}}
{{- range $role, $raw := (dict "prefill" $pfRange "decode" $dcRange) -}}
{{- $parsed := splitList " " (include "vllmkv.parsePortRange" $raw) -}}
{{- $lo := int (index $parsed 0) -}}
{{- $hi := int (index $parsed 1) -}}
{{- if or (eq $lo 16666) (eq $hi 16666) (and (lt $lo 16667) (gt $hi 16666)) -}}
{{- fail (printf "warmstandby model %q: %s.%s=%s covers the CANN reserved ports 16666-16667; move the range (e.g. 62000-62050 / 64000-65050 style blocks)" $modelName $role $family $raw) -}}
{{- end -}}
{{- if and (ge (int $pfHixl) $lo) (le (int $pfHixl) $hi) (eq $role "prefill") -}}
{{- fail (printf "warmstandby model %q: prefill.hixlListenPort=%d falls inside prefill.%s=%s" $modelName (int $pfHixl) $family $raw) -}}
{{- end -}}
{{- if and (ge (int $dcHixl) $lo) (le (int $dcHixl) $hi) (eq $role "decode") -}}
{{- fail (printf "warmstandby model %q: decode.hixlListenPort=%d falls inside decode.%s=%s" $modelName (int $dcHixl) $family $raw) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Validate vllm.sleepOverlay.files. Context: the root chart context ($). */}}
{{- define "vllmkv.validateOverlayFiles" -}}
{{- $files := .Values.vllm.sleepOverlay.files | default dict -}}
{{- if not $files -}}
{{- fail "vllm.sleepOverlay.enabled requires vllm.sleepOverlay.files (one entry per patched upstream file: key = ConfigMap key/mount subPath, path = target inside the container, content = file body). The patch package ships overlay.json + make-overlay-command.py which generate both the values fragment and the --set-file flags; or set vllm.sleepOverlay.enabled=false and deploy an image built with the patch" -}}
{{- end -}}
{{- range $name, $f := $files -}}
{{- if or (not $f.key) (not $f.path) (not $f.content) -}}
{{- fail (printf "vllm.sleepOverlay.files.%s needs key, path and content; inject the body with --set-file vllm.sleepOverlay.files.%s.content=<file> (see the patch package helper)" $name $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

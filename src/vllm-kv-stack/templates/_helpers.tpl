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
vllmkv.modelSignal — effective scaling signal ("queue" | "vllm") for a model.
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
{{- if $pm.threshold -}}
{{- $pm.threshold -}}
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
  vllm  → vLLM engine load (gpu KV-cache usage), router-independent
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
{{- if $pm.query -}}
{{- $pm.query -}}
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
- name: ASCEND_RT_VISIBLE_DEVICES
  value: {{ include "vllmkv.tpDevices" (dict "tp" .tp) | quote }}
- name: HCCL_OP_EXPANSION_MODE
  value: "AIV"
- name: VLLM_USE_V1
  value: "1"
- name: PYTORCH_NPU_ALLOC_CONF
  value: "expandable_segments:True"
- name: ASCEND_BUFFER_POOL
  value: {{ .root.Values.vllm.ascendBufferPool | default "4:8" | quote }}
{{- if .root.Values.vllm.ascendEnableFlashcomm1 }}
- name: VLLM_ASCEND_ENABLE_FLASHCOMM1
  value: "1"
{{- end }}
{{- if or .root.Values.mooncake.enabled .root.Values.lmcache.enabled }}
- name: OMP_PROC_BIND
  value: "false"
{{- end }}
{{- end -}}

{{/*
NIC auto-detection script for Mooncake / HCCL.
Caller must have NODE_IP available as a shell variable.
*/}}
{{- define "vllmkv.nicDetectScript" -}}
local_ip="${NODE_IP}"
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
{{- end -}}

{{/*
Inline Python Prometheus thread exporter for vLLM.
Caller must set VLLM_PID and POD_NAME shell variables before including.
*/}}
{{- define "vllmkv.threadExporter" -}}
VLLM_PID="$VLLM_PID" POD_NAME="$POD_NAME" python - <<'PY' &
import os, time
from http.server import BaseHTTPRequestHandler, HTTPServer
PID = int(os.environ["VLLM_PID"])
POD = os.environ.get("POD_NAME", "unknown")
PORT = 9101
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
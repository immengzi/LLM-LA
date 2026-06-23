# KV Cache Offload (Mooncake Deployment)

> **Alternate path — not the production default.** This is a standalone prefiller/decoder (P/D) disaggregation lab using vLLM's `MooncakeConnector` with a manually built Mooncake. The **production** Helm stack instead uses the **AscendStoreConnector** (`mooncakestore://...`) wired by the chart — see [Mooncake Helm integration](../mooncake/helm-integration.md) and the [GLM-5 + Mooncake production runbook](../mooncake/glm5-production.md). Don't mix the two connectors.

## Verify Communication Environment

> Same RoCE/NPU preflight as the [multi-node DP prerequisites](../data-parallel-lws.md#2-verify-npu-roce-network); the checks below add P/D-specific steps.

### Single Node Verification
Execute the following commands in sequence. The results must all be `success` and the status must be `UP`:

```bash
# Check the remote switch ports
for i in {0..7}; do hccn_tool -i $i -lldp -g | grep Ifname; done

# Get the link status of the Ethernet ports (UP or DOWN)
for i in {0..7}; do hccn_tool -i $i -link -g ; done

# Check the network health status
for i in {0..7}; do hccn_tool -i $i -net_health -g ; done

# View the network detected IP configuration
for i in {0..7}; do hccn_tool -i $i -netdetect -g ; done

# View gateway configuration
for i in {0..7}; do hccn_tool -i $i -gateway -g ; done
```

## Check NPU Network Configuration
Ensure that the hccn.conf file exists in the environment. If using Docker, mount it into the container.

```bash
cat /etc/hccn.conf
```

## Get NPU IP Addresses
```bash
for i in {0..7}; do hccn_tool -i $i -ip -g;done
```

## Run with Docker
Start a Docker container.
```bash
export IMAGE=m.daocloud.io/quay.io/ascend/vllm-ascend:v0.12.0rc1
export NAME=vllm-ascend 

docker run --rm \
--name $NAME \
--net=host \
-e http_proxy="${http_proxy}" \
-e https_proxy="${https_proxy}" \
-e no_proxy="${no_proxy}" \
--shm-size=1g \
--device /dev/davinci0 \
--device /dev/davinci1 \
--device /dev/davinci2 \
--device /dev/davinci3 \
--device /dev/davinci4 \
--device /dev/davinci5 \
--device /dev/davinci6 \
--device /dev/davinci7 \
--device /dev/davinci_manager \
--device /dev/devmm_svm \
--device /dev/hisi_hdc \
-v /usr/local/dcmi:/usr/local/dcmi \
-v /usr/local/Ascend/driver/tools/hccn_tool:/usr/local/Ascend/driver/tools/hccn_tool \
-v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
-v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/ \
-v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info \
-v /etc/ascend_install.info:/etc/ascend_install.info \
-v /etc/hccn.conf:/etc/hccn.conf \
-v /mnt/sfs_turbo/.cache:/root/.cache \
-v /mnt/nvme1/haiting_jd/DeepSeek-R1-Distill-Qwen-1.5B:/model \
-it $IMAGE bash
```
## Install Mooncake
Mooncake is the serving platform for Kimi, a leading LLM service provided by Moonshot AI. First, we need to obtain the Mooncake project. Refer to the following command:
```bash
git clone -b v0.3.7.post2 --depth 1 https://github.com/kvcache-ai/Mooncake.git
```
Install mpi
```bash
apt-get install mpich libmpich-dev -y
```
Install the relevant dependencies. The installation of Go is not required.

```bash
bash dependencies.sh -y
```
Compile and install
```bash
mkdir build
cd build
cmake .. -DUSE_ASCEND_DIRECT=ON
make -j
make install
```
## Prefiller/Decoder Deployment
Prefiller:
```bash
export HCCL_OP_EXPANSION_MODE=AIV
export GLOO_SOCKET_IFNAME="enp67s0f5"   # Changed from eth0
export TP_SOCKET_IFNAME="enp67s0f5"     # Changed from eth0
export HCCL_SOCKET_IFNAME="enp67s0f5"
export HCCL_IF_IP=7.242.102.243
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10

vllm serve /model \
   --host 0.0.0.0 \
   --port 13700 \
   --no-enable-prefix-caching \
   --tensor-parallel-size 1 \
   --seed 1024 \
   --served-model-name deepseek-r1-distill-qwen-1.5b \
   --max-model-len 40000 \
   --max-num-batched-tokens 40000 \
   --trust-remote-code \
   --gpu-memory-utilization 0.9 \
   --kv-transfer-config \
   '{"kv_connector": "MooncakeConnector",
  "kv_role": "kv_producer",
  "kv_port": "30000",
  "engine_id": "0",
  "kv_connector_module_path": "vllm_ascend.distributed.mooncake_connector",
  "kv_connector_extra_config": {
            "prefill": {
                    "dp_size": 1,
                    "tp_size": 1
             },
             "decode": {
                    "dp_size": 1,
                    "tp_size": 1
             }
      }
  }'

```
Decoder:
```bash
vllm serve /model \
   --host 0.0.0.0 \
   --port 13700 \
   --no-enable-prefix-caching \
   --tensor-parallel-size 1 \
   --seed 1024 \
   --served-model-name deepseek-r1-distill-qwen-1.5b \
   --max-model-len 40000 \
   --max-num-batched-tokens 40000 \
   --trust-remote-code \
   --gpu-memory-utilization 0.9 \
   --kv-transfer-config \
   '{"kv_connector": "MooncakeConnector",
  "kv_role": "kv_producer",
  "kv_port": "30000",
  "engine_id": "0",
  "kv_connector_module_path": "vllm_ascend.distributed.mooncake_connector",
  "kv_connector_extra_config": {
            "prefill": {
                    "dp_size": 1,
                    "tp_size": 1
             },
             "decode": {
                    "dp_size": 1,
                    "tp_size": 1
             }
      }
  }'

```
### Troubleshooting

If you get the error: AttributeError: 'Qwen2Config' object has no attribute 'head_dim' for Qwen series models, run the following command:

```bash
python3 -c "
import os

file_path = '/vllm-workspace/vllm-ascend/vllm_ascend/distributed/mooncake_connector.py'

# The text we want to find (Bad Code)
target_k = 'self.k_head_dim = self.model_config.hf_config.head_dim'
target_v = 'self.v_head_dim = self.model_config.hf_config.head_dim'

# The text we want to insert (Good Code)
fix_k = 'self.k_head_dim = getattr(self.model_config.hf_config, \"head_dim\", self.model_config.hf_config.hidden_size // self.model_config.hf_config.num_attention_heads)'
fix_v = 'self.v_head_dim = getattr(self.model_config.hf_config, \"head_dim\", self.model_config.hf_config.hidden_size // self.model_config.hf_config.num_attention_heads)'

if not os.path.exists(file_path):
    print(f'❌ ERROR: File not found at {file_path}')
    exit(1)

with open(file_path, 'r') as f:
    content = f.read()

changes_made = False

# Fix 1: k_head_dim
if target_k in content:
    content = content.replace(target_k, fix_k)
    print('✅ Fixed k_head_dim bug.')
    changes_made = True
elif 'self.k_head_dim = getattr' in content:
    print('ℹ️  k_head_dim was already fixed.')
else:
    print('⚠️  WARNING: Could not find k_head_dim line to fix.')

# Fix 2: v_head_dim
if target_v in content:
    content = content.replace(target_v, fix_v)
    print('✅ Fixed v_head_dim bug.')
    changes_made = True
elif 'self.v_head_dim = getattr' in content:
    print('ℹ️  v_head_dim was already fixed.')
else:
    print('⚠️  WARNING: Could not find v_head_dim line to fix.')

# Save if needed
if changes_made:
    with open(file_path, 'w') as f:
        f.write(content)
    print('💾 File saved successfully!')
else:
    print('👍 No changes needed.')"

```

## Example Proxy for Deployment
Run a proxy server on the same node with the prefiller service instance. You can get the proxy program in the repository’s examples:
```bash
python load_balance_proxy_server_example.py \
    --host 192.0.0.1 \
    --port 8080 \
    --prefiller-hosts 192.0.0.1 \
    --prefiller-port 13700 \
    --decoder-hosts 192.0.0.1 \
    --decoder-ports 13701

```
## Verification

Check service health using the proxy server endpoint.
```bash
curl http://127.0.0.1:8080/v1/chat/completions \
     -H "Content-Type: application/json" \
     -d '{
        "model": "deepseek-r1-distill-qwen-1.5b",
        "messages": [
            {"role": "user", "content": "Explain quantum entanglement in one sentence."}
        ],
        "max_tokens": 100
    }'
```








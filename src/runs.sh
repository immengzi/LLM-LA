# sleep 20
# python main.py --mode pull-batching --config real_distro_60 --prompts-file short --prompts-limit 400
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_60 --prompts-file short --prompts-limit 400
# sleep 20
# python main.py --mode rr-batching --config real_distro_60 --prompts-file short --prompts-limit 400

# sleep 20
# python main.py --mode pull-batching --config real_distro_61 --prompts-file short --prompts-limit 200
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_61 --prompts-file short --prompts-limit 200
# sleep 20
# python main.py --mode rr-batching --config real_distro_61 --prompts-file short --prompts-limit 200

# sleep 20
# python main.py --mode pull-batching --config real_distro_62 --prompts-file short --prompts-limit 400
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_62 --prompts-file short --prompts-limit 400
# sleep 20
# python main.py --mode rr-batching --config real_distro_62 --prompts-file short --prompts-limit 400

# sleep 20
# python main.py --mode pull-batching --config real_distro_63 --prompts-file short --prompts-limit 200
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_63 --prompts-file short --prompts-limit 200
# sleep 20
# python main.py --mode rr-batching --config real_distro_63 --prompts-file short --prompts-limit 200

# sleep 20
# python main.py --mode pull-batching --config real_distro_64 --prompts-file short --prompts-limit 200
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_64 --prompts-file short --prompts-limit 200
# sleep 20
# python main.py --mode rr-batching --config real_distro_64 --prompts-file short --prompts-limit 200

# sleep 20
# python main.py --mode pull-batching --config real_distro_65 --prompts-file short --prompts-limit 400
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_65 --prompts-file short --prompts-limit 400
# sleep 20

# python main.py --mode rr-batching --config real_distro_65 --prompts-file short --prompts-limit 400

# # sleep 20
# python main.py --mode pull-batching --config real_distro_82 --prompts-file short # --prompts-limit 200
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_82 --prompts-file short # --prompts-limit 200
# sleep 20
# python main.py --mode rr-batching --config real_distro_82 --prompts-file short # --prompts-limit 200

# sleep 20
# python main.py --mode pull-batching --config real_distro_83 --prompts-file short # --prompts-limit 200
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_83 --prompts-file short # --prompts-limit 200
# sleep 20
# python main.py --mode rr-batching --config real_distro_83 --prompts-file short # --prompts-limit 200

# sleep 20
# python main.py --mode pull-batching --config real_distro_60 --prompts-file short --prompts-limit 20
# sleep 20
# python main.py --mode least-queue-batching --config real_distro_60 --prompts-file short --prompts-limit 20
# sleep 20
# python main.py --mode rr-batching --config real_distro_60 --prompts-file short --prompts-limit 20

# #!/usr/bin/env bash
# set -euo pipefail

# Run 1: fixed sizes 8,16,32,64
# python3 batch_size_profile.py --batches "8,16,32,64" --yaml-file vllm-k8s-npu.yaml

# Run 2: sequential sizes 16..32 (hardcoded)
# python3 batch_size_profile.py --batches "32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62,64" --yaml-file vllm-k8s-npu.yaml

# python3 batch_size_profile.py --batches "42,50" --yaml-file vllm-k8s-npu.yaml

# python main.py --mode least-queue-batching --config real_distro_token_length --prompts-file short --prompts-limit 10
# python main.py --mode rr-batching --config real_distro_token_length_false --prompts-file short --prompts-limit 40
# sleep 30


# 191
python main.py --mode pull-batching --config lmsys_real_output_loads1-4 --prompts-limit 1000
sleep 30
python main.py --mode rr-batching --config lmsys_real_output_loads1-4 --prompts-limit 1000
sleep 30
python main.py --mode least-queue-batching --config lmsys_real_output_loads1-4 --prompts-limit 1000
sleep 30

# 192
python main.py --mode pull-batching --config lmsys_real_output_loads1-8 --prompts-limit 1000
sleep 30
python main.py --mode rr-batching --config lmsys_real_output_loads1-8 --prompts-limit 1000
sleep 30
python main.py --mode least-queue-batching --config lmsys_real_output_loads1-8 --prompts-limit 1000
sleep 30

# 193
python main.py --mode pull-batching --config lmsys_real_output_loads1-16 --prompts-limit 1000
sleep 30
python main.py --mode rr-batching --config lmsys_real_output_loads1-16 --prompts-limit 1000
sleep 30
python main.py --mode least-queue-batching --config lmsys_real_output_loads1-16 --prompts-limit 1000
sleep 30

# 194
python main.py --mode pull-batching --config lmsys_real_output_loads1-4 --prompts-limit 10000
sleep 30
python main.py --mode rr-batching --config lmsys_real_output_loads1-4 --prompts-limit 10000
sleep 30
python main.py --mode least-queue-batching --config lmsys_real_output_loads1-4 --prompts-limit 10000
sleep 30

# 195
python main.py --mode pull-batching --config lmsys_real_output_loads1-8 --prompts-limit 10000
sleep 30
python main.py --mode rr-batching --config lmsys_real_output_loads1-8 --prompts-limit 10000
sleep 30
python main.py --mode least-queue-batching --config lmsys_real_output_loads1-8 --prompts-limit 10000
sleep 30

# 196
python main.py --mode pull-batching --config lmsys_real_output_loads1-16 --prompts-limit 10000
sleep 30
python main.py --mode rr-batching --config lmsys_real_output_loads1-16 --prompts-limit 10000
sleep 30
python main.py --mode least-queue-batching --config lmsys_real_output_loads1-16 --prompts-limit 10000
sleep 30
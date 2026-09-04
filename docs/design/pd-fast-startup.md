# xPyD 动态 P/D 转换快速启动实现

## 目标

1. 副本数不变，完成 P/D 角色交换：快速关闭或休眠当前角色实例，再启动另一个角色的实例，避免重建 Pod 与重载权重。
2. 根据 SLO 对副本数进行扩容和缩容。

## 1. sleep/wakeup 机制

### 1.1 原理

sleep/wakeup 是 vLLM 提供的内存回收（降载）机制，不是状态保存。入口是 dev 模式下的 `/sleep` 与 `/wake_up` HTTP 接口（需 `VLLM_SERVER_DEV_MODE=1`），最终落到 `EngineCore.sleep(level)`（[core.py](/Users/mengzi/Develop/infra/vllm/vllm/v1/engine/core.py:761)）→ executor → worker。

vLLM 定义了三档 level：

| level | 行为 |
|---|---|
| 0 | 只暂停调度，请求照收不处理，显存不变 |
| 1 | 权重卸载到 CPU，丢弃 KV cache |
| 2 | 丢弃全部显存（含权重，buffers 先备份到 CPU） |

昇腾侧由 CaMemAllocator（基于 CANN-mem 的 PyTorch 可插拔分配器）实现（[camem.py](/Users/mengzi/Develop/infra/vllm-ascend/vllm_ascend/device_allocator/camem.py:113)）。内存按标签区分：加载模型时权重打 `weights` 标签（[worker.py](/Users/mengzi/Develop/infra/vllm-ascend/vllm_ascend/worker/worker.py:663)），KV cache 打 `kv_cache` 标签（[worker.py](/Users/mengzi/Develop/infra/vllm-ascend/vllm_ascend/worker/worker.py:892)），DeepSeek-V4 indexer attention 的 Hadamard 矩阵打 `sleep_persistent` 标签。

`CaMemAllocator.sleep(offload_tags=("weights",))` 的核心动作（[camem.py](/Users/mengzi/Develop/infra/vllm-ascend/vllm_ascend/device_allocator/camem.py:180)）：

- 带 `offload_tags` 的 tensor（level 1 即 `weights`）：先通过 `acl.rt.memcpy`（即 `aclrtMemcpy`，Device→Host）拷贝到 host pin_memory，再 `unmap_and_release` 释放 HBM。
- 不带 tag 的 tensor（`kv_cache` 等）：直接 `unmap_and_release`，丢弃。
- `sleep_persistent` 的 tensor：保持映射，不动。

wake_up 反向：`create_and_map` 重新映射 HBM，再 Host→Device 拷回。

一句话：**sleep = 把权重搬到 CPU、把 KV 直接丢掉、进程与 host 侧对象结构全保留**。

### 1.2 驻留 HBM 的计算

睡眠后驻留 HBM 由三部分组成：

1. `sleep_persistent` 张量（Hadamard 等 attention 元数据），量级很小。
2. CaMemAllocator 预留的内存池碎片/开销。
3. CANN runtime/context 的常驻开销。

不占 HBM 的部分：

- 权重：level 1 时 D2H 搬到 CPU，占 host RAM，量级为 `模型参数量 × dtype 字节数`（BF16 即 ×2）。
- KV cache：直接释放，占 0。其原本大小为 `num_blocks × block_size × num_layers × num_kv_heads × head_dim × 2(K/V) × dtype 字节`。

所以睡眠态并非"干净退出"：HBM 仍有一小部分常驻（persistent + 分配器池 + runtime context），host RAM 则多了整份权重。

### 1.3 P/D 角色交换流程

同卡双引擎是 sleep 机制为实现"快速切换角色"这一目标而采用的策略：同一张卡先启动 P 引擎，`/sleep` 释放 HBM 后再启动 D 引擎，最后按目标角色 wakeup 其中一个。切换时 drain 代理 → `POST /sleep` 当前活跃实例 → 等 `is_sleeping=true` → `POST /wake_up` 同卡对端 → 等 `/health` → 改 pod label 完成路由切换。关键约束是互斥：当前引擎必须真正睡下（HBM 释放）后才能唤醒对端，否则可能 OOM-kill EngineCore。完整拓扑见 [pd-warm-standby.md](/Users/mengzi/Develop/infra/llm-la/docs/design/pd-warm-standby.md)。

### 1.4 扩缩容流程

初始准备一个预热池：每张卡的 P/D 都启动后 sleep，稳态只有目标数量的引擎 awake。

- 扩容：对池中已 sleep 的目标角色引擎 `wake_up`，改 label 纳入 Service。
- 缩容：对多余引擎 `sleep`，改 label 从 Service 摘除。

### 1.5 同卡双引擎与角色切换的问题及对策

同卡双引擎、PD 分离不同角色带来两个主要问题：

1. **delayed KV free 卡死（根因）**：P（KV producer）异步把 KV 发到 Mooncake，使用延迟释放 `_delayed_free_req_ids`；`finished_sending` 只在 model step 时被消费。sleep 前引擎 quiesce 后不再 step，导致已结束请求的 KV block refcount 无法归零，`reset_prefix_cache`（sleep level ≥1 会触发）返回失败 → `/sleep` 500。上游修复是 vLLM #43433（commit `82536acc54`，v0.22.0 起才有），让引擎 idle 时仍 step 消费 delayed free；部署镜像 `vllm-ascend:v0.18.0` 没有。
2. **HBM 竞争**：睡眠态仍占 HBM（persistent + 池 + context），双引擎同卡需保证互斥、先睡后醒。

对策：

- 移植 #43433（两处：scheduler `has_finished_requests`、core idle-step）到 v0.18.0。
- rebalancer 侧 `/sleep` 退避重试（HTTP 500 / `is_sleeping` 超时），让 producer 在重试间隔消费 delayed free。已实现于 [pd_rebalancer.py](/Users/mengzi/Develop/infra/llm-la/src/core/vllm-kv-stack/files/pd_rebalancer.py) 的 `_sleep_engine`，环境变量 `PD_REBALANCER_SLEEP_RETRIES`（默认 5）、`PD_REBALANCER_SLEEP_BACKOFF_SECONDS`（默认 2）。

### 1.6 优缺点

优点：实现简单，复用 vLLM 现成 `/sleep` `/wake_up`，秒级切换、免重载权重。

缺点：休眠态并非干净退出，仍占用一定 HBM（persistent + 分配器池 + runtime context），host RAM 还多一份权重；切换必须处理 KV 释放时序（delayed-free）。

## 2. suspend/checkpoint 机制

### 2.1 原理

suspend/checkpoint 是 OS/运行时级整进程状态快照：把进程冻结后，将 host 侧（CPU 寄存器、内存页、FD、socket）和 device 侧（NPU/GPU 显存、context、stream）整体落盘，恢复时原样重建（含 KV 和在途计算状态）。

它需要两套机制配合：

- CRIU：负责 host 侧进程状态 dump/restore。
- device snapshot：负责显存。CUDA 是 `cuCheckpointProcessCheckpoint/Restore`，昇腾是 `aclrtSnapShotProcessBackup/Restore`。

单靠 CRIU 不会 dump 显存，单靠 device snapshot 不会 dump CPU 寄存器/内存页/FD，所以必须两者叠加。

### 2.2 CANN API 成熟度

昇腾 `aclrtSnapShotProcessLock/Backup/Restore/Unlock`：

- **试验特性**，文档明确"不支持应用于生产环境"。
- 仅 CANN 9.0+；当前远程 CANN 8.5.2 连头文件声明和 `libascendcl.so` 符号都没有，只有错误码预留（`ACL_ERROR_SNAPSHOT_*` 507905-507909）。
- `pid` 只支持本进程（不支持跨进程），`args` 只支持 NULL。
- 语义是"备份 task 内存/页表"而非 CUDA 那种"dump 全部 GPU memory contents"，与 `cuCheckpointProcessCheckpoint` 不完全同构，不能照搬。

对比 CUDA `cuCheckpointProcessCheckpoint`（Dynamo 用）：成熟、驱动原生，Dynamo 直接通过 `cuda-checkpoint-helper` 调 `lock/checkpoint/restore/unlock`。

### 2.3 Motor 的思路与 MindIE 组件

Motor 的 suspend/resume 是 MindIE 原生引擎能力，vllm/sglang adapter 不暴露，无法被 vllm-ascend 直接复用。

MindIE 家族把"容器快照"做成熟，靠的是编排层而非裸 CANN API：

- grus（MindCluster 组件）：编排 `pause 容器 → runc checkpoint（内部调 CRIU）→ rootfs diff`（[checkpoint.go](/Users/mengzi/Develop/infra/mind-cluster/component/ascend-docker-runtime/runtime/grus/checkpoint.go:195)）。
- npu-plugin：宿主机 `/usr/lib/criu` 下的 CRIU 插件，负责 device 侧 dump（grus 的 dump.log 里检查 `[npu-plugin fini-dump err]`，[checkpoint.go](/Users/mengzi/Develop/infra/mind-cluster/component/ascend-docker-runtime/runtime/grus/checkpoint.go:182)）。

即：**"成熟的容器快照"是 grus + CRIU + npu-plugin + rootfs 这套编排，成熟度在 grus/ascend-docker-runtime 层，而不是用户直接面对的 CANN API**。

### 2.4 vllm-ascend 现状

vllm-ascend 目前只有 sleep/wakeup，没有 suspend。真正的容器快照 suspend/resume 是 PR #14756，针对 v0.23.0，改面极大：涉及 HCCL 进程组重建、KV connector 在途状态、derived weights、ACL graph、attention caches 等，尚未合入，移植到 v0.18.0 代价极高。

## 3. 不重启实例，运行时修改配置

### 3.1 设计思路

P/D 权重完全相同，差异只在启动参数。把启动参数做成可动态配置，切换时不动权重、只重建管理结构，即可免卸载权重、免重新初始化。

### 3.2 可行性

方向部分正确（权重确实不变，这是秒级的关键），但"参数"这个词低估了差异本质。P/D 差异不是几个数值，而是四套不同代码路径：

1. 调度器：P 走 prefill 调度（chunked prefill、大 batch），D 走 decode 调度（逐 token）。
2. connector 角色：P 建 KV send 线程，D 建 KV recv 线程；切换 = 拆线程重建线程，等于 connector 重初始化。
3. KV cache 语义：P 是"待发送缓冲"（`_delayed_free_req_ids`），D 是"待接收缓冲"（`load`），不只是大小不同。
4. 图 capture：P 是 prefill 图，D 是 decode 图，切换要重 capture。

所以"参数动态化"本质是要求引擎支持**热重配置**（runtime reconfiguration），把"权重加载"和"调度器/connector/图/KV cache"解耦、后者可独立热切换——这是 vLLM 当前没有的抽象，工程难度高于 checkpoint/snapshot 的整体快照。

而且它**没解决 delayed-free**：即使热切换了 `kv_role`，P 那批 in-flight 发送的 KV 仍需 drain 或释放，照样撞 block 归零。

结论：方向（权重不用动）对，但落点应是"整体状态快照"或"sleep-fix"，而非"参数热重配置"。

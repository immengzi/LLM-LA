# wake 崩溃 core dump 收集命令清单

> 更新：2026-08-29。目的：给"唤醒沉睡引擎 → `corrupted size vs. prev_size` →
> 进程 abort → k8s 重启 → 冷启动连环崩"定性，把根因钉死在栈帧上，而不是靠猜。
> 触发方式以用户确认的现象为准：**缩容+扩容中间会发生完全崩溃**（最近一次 4 个
> 引擎 / TP=2 全部退出），重试 3 次不恢复，重建 pod 恢复。
>
> 边界：只动 `/workspace/mengzi` 与 `dyn-pd` namespace；主机级改动（sysctl、
> 安装 gdb、在宿主机建目录）属于系统配置，需先列出来得到确认再执行。

## 0. 先做只读盘点（不改任何东西）

```bash
# 1) core_pattern 现状（决定 core 落点）
ssh dynpd 'cat /proc/sys/kernel/core_pattern; cat /proc/sys/kernel/core_uses_pid'

# 2) 主机和容器里有没有 gdb
ssh dynpd 'which gdb || echo no-host-gdb'
ssh dynpd 'export KUBECONFIG=/workspace/mengzi/kubeconfig; \
  kubectl exec -n dyn-pd vllm-qwen-pd-<POD> -c vllm-prefill -- sh -c "which gdb || echo no-container-gdb"'

# 3) vllm worker 进程的 cwd（决定相对 core_pattern 时 core 写到哪里）
ssh dynpd 'export KUBECONFIG=/workspace/mengzi/kubeconfig; \
  kubectl exec -n dyn-pd vllm-qwen-pd-<POD> -c vllm-prefill -- \
  sh -c "for p in \$(pgrep -f vllm.entrypoints.openai); do echo \$p \$(readlink /proc/\$p/cwd); done"'

# 4) chart 现状与磁盘余量
ssh dynpd 'export KUBECONFIG=/workspace/mengzi/kubeconfig; \
  helm ls -n dyn-pd; kubectl get pods -n dyn-pd -o wide; df -h /workspace/mengzi'
```

## 1. 打开 core dump（两条路线，二选一）

### 路线 A（推荐，全部在 dyn-pd 范围内）：hostPath 卷 + ulimit 包装

容器内默认 ulimit -c 是 0，必须显式放开；core 落点必须是**可持久**的路径
（容器内非卷路径在 pod 重启后丢失），所以挂一个 hostPath：

```bash
# 1) 在 k8s-worker1 / k8s-worker2 上各建一次宿主目录（改宿主机，需先确认）
ssh dynpd 'mkdir -p /var/lib/dynpd-cores && chmod 777 /var/lib/dynpd-cores'

# 2) chart 里给 vllm 容器加：
#    - volumeMounts: /cores -> hostPath /var/lib/dynpd-cores
#    - command 包装（在原有 python 命令前）：ulimit -c unlimited && cd /cores && exec ...
#    41-vllm-pd-timeshare.yaml 的引擎容器 command 前插入即可，helm upgrade 到新 rev。

# 3) 验证已生效（rev 部署完成后）
ssh dynpd 'export KUBECONFIG=/workspace/mengzi/kubeconfig; \
  kubectl exec -n dyn-pd vllm-qwen-pd-<POD> -c vllm-prefill -- sh -c "ulimit -c; ls -la /cores"'
```

### 路线 B（主机级，需授权）：改 kernel.core_pattern

```bash
# 只读确认当前值后，若要改：
ssh dynpd 'sysctl -w kernel.core_pattern=/workspace/mengzi/cores/core.%e.%p.%t
           sysctl -w kernel.core_uses_pid=1'
```

注意：若 core_pattern 是 `|/usr/lib/systemd/systemd-coredump` 这类管道形式，容器
内进程的 core 会走 systemd-coredump（宿主侧，容器内不可见），此时优先用路线 A。

## 2. 复现（缩容+扩容，用户确认的触发方式）

```bash
export KUBECONFIG=/workspace/mengzi/kubeconfig

# 1) 暂停 rebalancer，避免它在中途自动翻转干扰复现（记下原副本数，结束后恢复）
kubectl scale deployment dyn-pd-pd-rebalancer -n dyn-pd --replicas=0

# 2) 记录现场基线（每张卡的 used/free、各 pod worker 日志尾部）
ssh dynpd 'npu-smi info > /workspace/mengzi/logs/pre-repro-npu-smi.txt'

# 3) 触发缩容+扩容
kubectl scale deployment vllm-qwen-pd -n dyn-pd --replicas=2
kubectl rollout status deployment/vllm-qwen-pd -n dyn-pd --timeout=300s
kubectl scale deployment vllm-qwen-pd -n dyn-pd --replicas=3
kubectl get pods -n dyn-pd -w   # 观察 CrashLoop / 退出时间戳

# 4) 若一次没崩，等 5-10 分钟再试（"沉睡几小时再唤醒"是历史复现条件之一；
#    也可先对一个引擎 /sleep，挂几小时后再 /wake_up）
```

## 3. 抓取证据（收集脚本）

配套脚本：`scripts/pd-timeshare-phase0/collect_core_evidence.sh`，在远端执行：

```bash
# 把脚本传到远端后运行（或直接 ssh dynpd 'bash -s' < 脚本）
ssh dynpd 'bash -s' < scripts/pd-timeshare-phase0/collect_core_evidence.sh
```

脚本会打包到 `/workspace/mengzi/logs/core-evidence-<时间戳>/`：

- core 文件清单（`/cores`、`/workspace/mengzi/cores`、`/var/lib/dynpd-cores` 递归找）
- gdb 全线程栈：`gdb -batch -ex "thread apply all bt full" <python> <core>`
- 每个 pod 的 prefill/decode 崩溃前日志（含 `--previous` 的上一个容器）
- `[camem] wake_up` 最后几条 remap 日志（与 core 对齐用）
- `/dev/shm` 与 `ipcs -m -s`（semaphore/shared_memory 泄漏计数）
- `dmesg -T` 尾部、`npu-smi info`、崩溃前后 `mem_get_info`

## 4. 现场判定要点

1. **core 有没有、是哪个进程**：EngineCore / Worker_TP0 / Worker_TP1 / pool worker。
2. **崩溃栈里的 glibc 帧**：`_int_free` / `malloc_consolidate` /
   `corrupted size vs. prev_size` 说明堆元数据被踩；重点看谁在 free/malloc。
3. **与 [camem] wake_up 最后一条 remap 对齐**：日志里有 `ptr/tag/size`，能定位是
   哪个 `create_and_map` 触发的（weights 还是 kv_cache）。
4. **冷启动连环崩的 core 同样要**：第一次崩溃的 core 最关键，别被 k8s 重启覆盖
   （core_pattern 带 `%e.%p` 防覆盖，或每轮复现前先归档已有 core）。
5. 把线程栈里的 camem / HCCL / Mooncake / acl 帧摘出来，与
   `docs/internal/pd-sleepfix-patch-ledger.md` 的候选环节（camem 重映射、KV 池
   残留映射、HCCL/Mooncake 重建）一一对照。

## 5. 恢复

```bash
# 删 CrashLoop pod 重建（已确认有效），再恢复 rebalancer
kubectl delete pod -n dyn-pd <crash-pod>
kubectl scale deployment dyn-pd-pd-rebalancer -n dyn-pd --replicas=1
# 验证：3/3 ready、0 重启
kubectl get pods -n dyn-pd
```

## 6. 注意事项

- core 文件可能几十 GiB，收集前先 `df -h`；每轮复现的产物打 tag
  （`core-evidence-<date>-rev<rev>-<pod>`），收集完及时归档。
- 主机级操作（建目录、sysctl、装 gdb）都先列清单确认，不要静默执行。
- 结论区分：core 栈帧 = 代码事实；日志对齐 = 配置与日志证据；其余保持"待验证"。

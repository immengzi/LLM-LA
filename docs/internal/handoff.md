# mengzi dyn-pd 首次交代（handoff）

> 更新：2026-09-03（A 实验执行完毕）。实验 A（sleep 前销毁 mooncake TE，weakref 证实
> `weakref_alive=False referrers=[]`，双 Worker 均确认 C++ freeEngine 已析构）：
> freed 仍 8.19/48.65，与基线一致 → “mooncake TE/peer 连接 retain 物理页”机制证伪。
> 残留指向 CANN/驱动 / CaMem 记账 / ascend_transport(HCCL) pin 层。部署已恢复无 overlay 基座
> （P1,D2 4/4）；实验物在远端 backup/2026-09-03-te-term-a/ 与 logs/dynpd-te-term-a/，
> README 09-03（深夜补）条目已同步。v1 全堆 gc 扫描踩 PydanticUserError，v2/v3 改 holder 注册。

> 更新：2026-09-03（深夜补，#15350 对照 + 历史 clean 期复核）。
> ① 历史 clean 期并非“没服务过真实外部 KV”：09-01 服务过请求的 decode 睡后仅 +8MB、
>    09-02 TP2+DP2 S3 也 clean → “服务过 KV”非充分条件；记录的 clean flip 均非
>    “刚 pull 长 prompt 外部 KV 的 decode 原地 sleep→wake”同进程序列。
> ② PR #15350（v0.27.1）只试过前半段（unregister/reregister，te-fix+camem-diag A/B 无效）；
>    disconnect 段（commit 5054c93）与后续 short-lived 短连接方案（commit 3602a26）均未试过；
>    本镜像 mooncake binding 无 disconnect_all_peers（dir(TransferEngine) 无该 API），
>    直接 backport 5054c93 不可行，需先升级 binding 或走 terminate/re-init 等价路径。
> ③ Mac ~/.ssh 已无 ControlMaster/socket 残留（controlmasters 空目录），hcluster/b0x 与手册一致；
>    后续单会话低频连接（被限流等 60-90s，不复用 master）。
> ④ 已同步远端 README 09-03（深夜）条目（详细证据与建议下一步 a/b/c）。

> 更新：2026-09-03（晚间）。宿主机重启验证 + overlay A/B 均已完成，结论：
> “pkill 驱动残留”与“TE-unregister overlay”两个假设都被排除，基座镜像
> v0.18.0-pd-sleepfix-1 自身即可复现 207001。当前部署已摘除 camem-diag/te-fix
> 两个 overlay（等价 rev 69 行为）。下一步转入代码/状态路径分析（wake 对象
> peer/self、请求后 key 残留、connector reset 顺序），不再改 overlay。
> 连接方式变更：~/.ssh 已禁用 ControlMaster 复用（dynpd 直连；被限流就等）。
> 详见下方“远程访问”与 /workspace/mengzi/README.md 09-03（晚间）条目。

> 更新：2026-09-03（深夜）。状态/代码路径分析结论（只读 + 对照实验）：
> ① 同引擎前后对照（jjnj2，PID 不变，无 overlay）：从未服务外部 KV 时 sleep
> 54.66/2.09 且 wake 200 restored 223/223（干净）；服务 1 次外部 KV 后 sleep
> 8.19/48.65、wake 207001 崩溃 —— 触发条件 = 进程内服务过真实外部 KV。
> ② 物理证据：失败睡眠态 npu-smi 进程仅 ~988MB 但所在卡 HBM 仍占 ~52.9GB
> （aclrtFreePhysical 成功但 ~48GiB 成无主占用）；进程退出后卡回落 4.3GB。
> ③ 排除项：宿主机重启、camem-diag/te-fix overlay（TE unregister）均非根因；
> 上游无该签名直接记录。疑似 CANN/驱动对“被 mooncake/RDMA 触碰过的 huge-page
> 物理块”释放不归还的行为。
> 建议方向：pd_rebalancer 加 sleep 后预检（freed << 池总量 → needs_recreate，
> 避免 wake 崩溃）做产品级兜底；最小 ACL 复现定位驱动层后再决定 issue 归属。

> 更新：2026-09-03（B 修复已部署验证；设备 env 模板修复；wake OOM 根因证据齐备；
> 下一步：宿主机重启验证“pkill 驱动残留”假设）。

## 当前主线与状态（2026-09-03）

目标：单机 TP/DP warm-standby 稳定可用（flip 干净、sleep→wake 不崩），随后更新文档并 forward 提交。

### 已验证通过
- B 修复（`pd_rebalancer.py`：wake 失败→`needs_recreate`、两步标签、wake retry）：
  本地 + 远端 ConfigMap 均已部署；TP=2 上 P1D3→P2D2→P1D2→P2D1→P1D2 四轮翻转干净、restarts 无新增。
- 设备 env 模板修复（`_helpers.tpl` 只提取 `<vendor>-<n>`，去掉 `910` 垃圾项）：
  本地 + 远端 chart + 运行中 deployment 均生效；env 形如 `0,7 / 1,6 / 2,5 / 3,4`。
- KV 链路：长 prompt 走真实外部 KV（master Put/Get 计数 + decode `External prefix cache hit 60-88%`）。
- 修复尝试（均无效，已留档）：#15350 风格 TE unregister/reregister；camem sleep 前 empty_cache + stats。

### 未解问题 A（本次重启要验证）
- 现象：真实外部 KV pull 过的 decode，sleep 后（哪怕 37s）wake 必现
  `aclrtMallocPhysical 207001 OOM` → Worker terminate → EngineCore cancelled → /wake_up 500 →
  api_server 退出时 `corrupted size vs. prev_size` abort。
- 关键证据（A2，06:38 plog，已存 `/tmp/arepro/`）：
  wake 失败为 `halMemCreate drvRetCode=6, size=696254464B(≈664MiB), ErrCode=207001`；
  HUGE_HBM APP `current=12GiB / peak=54.9GiB / alloc=413 free=249`，物理仍空（npu-smi 只剩 ~1GiB/进程）。
- 历史：同签名在 docs/validation/2026-08-27-camem-sleepfix.md 与 README 2026-08-29 已记录
  （“服务过 KV 的引擎 sleep 后 48.65GiB 未释放 + wake 207001 OOM”）；
  8-31~9-02 加 02/03/reset overlay 后曾恢复（54.66GiB 干净释放，14+ 次翻转 OK）。
- 假设：今天 10:07 用户 `pkill -9 "[Vllm]"`（字符类误伤全系统）后进程全新建、但 **NPU 驱动未复位**，
  驱动侧 huge-page/记账残留导致行为退回失败态。验证方法=宿主机重启后同镜像重跑一次
  真 KV→sleep→wake：若恢复 54.66/干净 → 实锤驱动残留；若仍 207001 → 转回代码/状态路径分析。

### 环境与操作速查
- SSH：`ssh dynpd`（183.87.46.77，root 直连；已禁用 ControlMaster 复用，见下方“远程访问”）。
  kubectl 前 `export KUBECONFIG=/workspace/mengzi/kubeconfig`。
- 引擎恢复（8 卡全空后）：`kubectl -n dyn-pd scale deploy vllm-qwen-pd --replicas=4`；
  rebalancer：`scale deploy dyn-pd-pd-rebalancer --replicas=1`；约 7 分钟 Ready。
- 收敛/翻转：exec rebalancer pod → `curl -X POST 127.0.0.1:8081/v1/targets/qwen/propose -d '{"prefill":1,"decode":2,...}'`
  再 `.../commit`（已在执行中时先等完成）。
- 真 KV 请求：`curl http://<vllm-qwen ClusterIP>:8200/v1/chat/completions`，长 prompt(≥1 block)，
  固定 X-Request-Id；确认 decode 日志 `External prefix` >0 且 `Connected to segment`。
- sleep/wake：`curl -X POST http://<podIP>:8201/sleep -d '{"level":1}'` → `.../wake_up`；
  看 decode 日志 `Sleep mode freed X GiB`（干净=54.66/2.1x；失败态=8.19/48.66）与 `[camem]` 行。
- 诊断 overlay 仍在部署上：`dyn-pd-camem-diag`（synchronize/remap INFO/stats）、`dyn-pd-te-fix`（TE tracking）；
  env `ASCEND_GLOBAL_LOG_LEVEL=1`、`ASCEND_SLOG_PRINT_TO_STDOUT=1`（重启后如需可保留以抓 plog）。
- registry `192.168.0.42:32000` 仍 down（router 已改 IfNotPresent）；kubelet `--hostname-override=k8s-worker2` 已持久化。
- 产物：cores `/var/lib/systemd/coredump`（多次，最新 09-03 06:38 三份）；
  plog `/tmp/arepro/`；解压 cores `/workspace/mengzi/logs/cores-wake-20260903/`。
- 本地未提交：`feat/pd-dynamic-rebalance` 上 pd_rebalancer.py + tests + _helpers.tpl +
  docs/design/pd-warm-standby.md；vllm-ascend main camem.py 有诊断改动。

> 更新：2026-08-29（深夜，验证进展）。已上线 03 补丁（camem sleep 空闲块探测 +
> wake_up remap 日志，unmap 默认关闭）并完成首轮远程验证，结论：
> ① **P4 前提证伪**：探测到 pool 空闲块仅 2 块 / 0.02 GiB，不是旧报告说的
>    46.47 GiB；sleep 本来就 freed 54.66 GiB，"空闲块 unmap" 与内存/wake 无关。
> ② **core 定性**：16:41-16:44 的 5 个 core 即"4 引擎退出"现场；Worker_TP 先崩
>    （core truncated），api_server 次生（abort ← malloc_printerr ← free ← exit
>    清理），堆在 exit 前已被踩坏；踩堆者未定位。
> ③ **翻转干净**：P0,D0→P1,D2 applied，`[camem] wake_up restored 223/223`，0 崩溃；
>    缩容+扩容（rebalancer 活跃）复现无崩溃；推理正常。
> ④ 遗留：**长时（小时级）沉睡唤醒崩溃未复现**——已建每日 09:30 定时验证
>    （让引擎沉睡过夜后自动 flip）；Worker core truncated 需改主机 core_pattern
>    （待确认）。工具：宿主机 gdb + /root/.cache/sysroot 符号已就绪。
> 报告：docs/validation/2026-08-29-camem03-verification.md（远端 + 本地双份）。

> 更新：2026-08-29（晚间）。**复核结论修正**：旧报告里的 `8.26/48.66 GiB` 数字
> 在 rev62 现网未复现（三个 pod 每次 sleep 均 freed 54.66 GiB）；期间多次改补丁，
> 报告只能作参考。用户确认的稳定现象：**缩容+扩容中间完全崩溃**（最近一次 TP=2
> 的 4 个引擎全部退出），重试 3 次不恢复，重建 pod 恢复。
>
> 已产出两件套：
> ① core dump 收集命令清单 `docs/internal/core-dump-collection.md` + 远端收集脚本
>    `scripts/pd-timeshare-phase0/collect_core_evidence.sh`（按"缩容+扩容"复现）。
> ② P4 camem 补丁 `patches/vllm/03-camem-sleep-free-block-diagnostics-v0.18.0.patch`
>    （诊断先行，unmap 默认关闭，env 开启），可 `git apply --check` 到 v0.18.0。
>
> 下一步：按 core-dump 清单开 core → 复现一次完整崩溃 → gdb 栈帧 + `[camem]
> wake_up` 最后一条 remap 对齐定性；再用 03 补丁做 A/B（先不开 unmap 跑基线，
> 再看 `[camem] sleep` 是否报出 cached free pool blocks，最后开 env 对比）。

> 更新：2026-08-26。本文件是给新加入的 agent 的首次交代：可直接整段粘贴，或让 agent 先读本文件再开始。权威信息以 /workspace/mengzi 下正式文档为准。

你在协助 mengzi 的 LLM-LA 动态 Prefill/Decode（dyn-pd）验证工作。

## 2026-08-29 更新（P4 sleep 内存实验 + wake 崩溃复现，报告见 docs/validation/2026-08-29-p4-sleep-memory-wake-crash.md）

- **E1（服务过 KV 的 prefill sleep，rev62 部署版 full-reset 生效）**：sleep 成功
  （200、is_sleeping=true、`Successfully reset prefix cache`），日志
  `MooncakeBackend remove_all succeeded` **确实执行了**，但 `Sleep mode freed
  8.26 GiB, 48.66 GiB still in use` 与 rev 49/52 完全一致——**remove_all 不释放
  保留内存**。
- **保留量构成**：prefill 启动日志 `Available KV cache memory: 46.47 GiB` ≈ 保留
  48.66 GiB → 保留主体是 **camem pool 的 KV 池（free 后块仍映射）**，与 store
  pin 无关。soft/full 取舍因此与内存释放**解耦**：默认 soft（保留远端 KV）不会
  让内存更差。
- **wake 崩溃复现（新证据）**：唤醒沉睡 5h 的 decode → `Call to wake_up method
  failed: cancelled` → EngineDeadError → glibc **`corrupted size vs. prev_size`**
  → 进程 abort → k8s 重启 → **冷启动连环崩**（`Engine core initialization
  failed` + 同款 heap corruption，泄漏 semaphore/shared_memory）→ 删 Pod 重建恢复。
- **对照**：rebalancer 翻转时唤醒**刚 sleep 几分钟**的 prefill **成功**
  （P0,D3→P1,D2 applied，无崩溃）→ 崩溃与"沉睡时长/状态"相关，不是 wake 本身
  必然崩溃；根因仍待 core dump（8-27 遗留问题）。
- **清理结论修正**：48.66 GiB 的主战场在 **camem pool 空闲块 unmap（P4 补丁）**，
  store 侧 reset 级联（P3 软 reset）只解决账本与 pin，不是内存主因。

## 2026-08-29 更新（启动并发判定实验 D，结论已写入验证报告）

- **校准**：rev 56 部署的引擎初始化锁是**节点级真锁**。`cache-volume` 为 hostPath
  `/root/.cache`（不是 emptyDir），6 个引擎拿锁时间戳严格串行（8-28 11:24:07 →
  12:01:58）。此前"锁是 pod 级、跨 Pod 只有 BOOT_SLOT 错峰"的描述与实际部署不符。
- **D 修正实验**：`BOOT_SLOT=0` + per-pod 锁（保留同卡 device-share 串行、放开跨
  Pod），连续 4 次冷启动（helm rev 57、59 + 2×scale 0→3）全部 3/3、2/2、0 重启；
  3 个 prefill 并发初始化（拿锁差 ≤1s、startup 完成差 ≤8s）无崩溃。
- **结论**：跨卡并发 init **不触发** CANN 驱动竞态；8-26 触发面是同卡 device-share
  并发（启动门 + per-pod 锁已覆盖）。
- **最终配置（rev 62）**：删除 BOOT_SLOT 错峰；引擎初始化锁改为 **per-pod**
  （`/root/.cache/dynpd-engine-init-${POD_NAME}.lock`）；冷启动 ~6.5 分钟、3/3、
  2/2、0 重启（rev 62 实测）。节点级 hostPath 锁从生产默认移除，作为可选保险保留
  在注释与备份中。
- 报告：`/workspace/mengzi/docs/validation/2026-08-29-boot-concurrency-d.md`；
  备份：`backup/2026-08-29-boot-concurrency-d/`；helm rev 57-62。

## 第一步：先读这些文件（按顺序）
1. /workspace/mengzi/README.md —— 工作区规则 + 工作记录（最新在上），项目状态以这里为准
2. /workspace/mengzi/ENV-NOTES.md —— 环境速查（集群、凭据、registry、模型）
3. /workspace/mengzi/docs/environment.md —— 红线与验证纪律，必须遵守
4. /workspace/mengzi/dyn-pd-runbook.md —— 部署与验证手册
5. /workspace/mengzi/docs/validation/ —— 验证报告（最新：2026-08-26-multi-tp-warmstandby.md）
6. /workspace/mengzi/codex-memory/llm-la-dynpd-digest.md —— Codex 记忆摘录（背景结论、常见坑）

Mac 端对应：/Users/mengzi/Develop/infra/llm-la/docs/internal/dyn-pd-validation-env.md 和 docs/internal/codex-memory/。

## 当前已验证代码（llm-la 仓库，Mac 当前在 feat/pd-dynamic-rebalance）
- feat/pd-dynamic-rebalance @ 7b17e36 —— 特性：per-card dual-engine warm standby（sleep/wake 角色切换）
- fix/dynpd-environment-patches @ 9ef3c23 —— 环境补丁归档
- backup/pd-experiment-records-20260826 —— 验证脚本/实验记录；分支 tip 为 5c05ec1（8-26 多 TP 验证报告），fc35b23 是其下的 "backup: per-card validation scripts and experiment records" 提交；历史里保留了旧版中文设计文档

## 远程访问（重要：直连，不用 ControlMaster 复用）
- Mac ~/.ssh/config：dynpd（183.87.46.77:22，root）与 hcluster/b0x 均已去掉
  ControlMaster 复用（2026-09-03 决定：复用 master 卡死会连带后续连接全挂）。
- 一律 `ssh dynpd '...'` 直连；被 sshd 限流时（表现为 TCP 通但连接长时间无响应/
  connect timeout）不要反复重试，等 60-90 秒再连；不要用 `-o ControlMaster` 复用。
- 长任务分离式运行，不要占住交互会话：
  ssh dynpd 'cd /workspace/mengzi/llm-la/scripts/pd-timeshare-phase0 && setsid nohup bash phase0.sh > phase0-run.log 2>&1 < /dev/null && echo $! > /tmp/phase0.pid'
- 低频轮询日志（间隔 ≥60-90 秒）：ssh dynpd 'tail -n 30 .../phase0-run.log'
- 中断按 PID：ssh dynpd 'kill $(cat /tmp/phase0.pid)'，不要 Ctrl-C 留孤儿进程
- kubectl 前 export KUBECONFIG=/workspace/mengzi/kubeconfig；不要密集打 pod/短连接
- 可选 SSH 隧道 + 本地 kubectl（把 kubeconfig 拷到 Mac）——凭据不外传，除非用户明确同意

## 环境要点
- SSH 别名 dynpd（183.87.46.77:22，root）；工作区 /workspace/mengzi，本机 k8s-worker2 192.168.0.69（hostname 910B3-04）
- 只允许动 /workspace/mengzi 和 namespace dyn-pd；不动他人容器/目录/系统配置；root 不等于集群 admin；kubeconfig 勿外传
- 验证证据：HTTP 200 不算 KV 复用，必须固定 X-Request-Id 贯穿 proxy/prefill/decode 日志与指标

## 当前状态（2026-08-26 核对）
多 TP（TP=2）Warm-Standby 实验：正向转换 P1,D2→P2,D1 已打通；反向转换 P2,D1→P1,D2 必失败（connector 未上报 finished_sending，属 vLLM/connector 层问题）。当前 vllm-qwen-pd 2/3 ready、1 个 CrashLoop pod（已知 CANN 初始化偶发崩溃）；rebalancer/proxy/router/redis/mooncake 正常。

## 代码风格（必须遵守）
1. 已有文件：与原有注释风格保持一致（注释密度、语言）
2. 新增文件：与同类型文件的注释风格保持一致
3. 实验记录可以用中文；但可能被提交进代码仓库的内容默认用英文，除非已有提交在类似位置明确使用了大量中文
4. 实验/验证记录不要加入特性分支的提交（放 backup 分支或工作区文档）

## 工作约定
- 每次完成工作，在 README.md 工作记录最上方插入带时间戳的新条目
- 验证报告放 docs/validation/，日志放 logs/，配置放 configs/
- 结论区分：代码事实 / 配置与日志证据 / 推断 / 待验证
- 本地→远端模板同步不要覆盖环境补丁；模板改动必须走 helm upgrade
- 发现新的关键环境信息，写回 ENV-NOTES.md / README.md，不要只留在会话里

# dyn-pd sleepfix 补丁账本（overlay ↔ 归属补丁对照）

> 更新：2026-08-31（补丁归位）。**01/02/03 补丁已从 llm-la 仓库移出**，归档到
> 各自仓库的本地分支（**只提交、未推送**）：
> - vllm `wip/dynpd-sleepfix-v0.18.0` @ `8257269a52`：01（#43433）
> - vllm-ascend `wip/dynpd-sleepfix-v0.18.0` @ `65c78d09e`：02 + 03 + README
> llm-la 的 `patches/vllm/` 目录已清空；补丁不再以任何形式进入 LA 仓库。

> 更新：2026-08-29（晚间复核）。**报告数字不可全信**：期间多次打补丁又删除、
> 也改过旧补丁，`8.26/48.66 GiB` 在 rev62 现网未复现——当前三个 pod 每次 sleep
> 均记录 `[camem] sleep start: active=223 total=54.66 GiB per_tag={weights:
> 7.97, kv_cache: 46.69}` 且 freed 54.66 GiB。用户确认的稳定现象是：**缩容+扩容
> 中间会发生完全崩溃**（最近一次 TP=2 的 4 个引擎全部退出），重试 3 次不恢复，
> 重建 pod 恢复。P4 因此改为"诊断先行 + 默认关闭 unmap"的 03 补丁，先让下次复现
> 拿到可信数据。

> 更新：2026-08-29。用途：迁往 vllm-ascend / vllm backport 分支的清单。
> 基线：vllm `v0.18.0` = `bcf2be9612`；vllm-ascend `v0.18.0` = `c5959ec19`。
> 数据来源：rev62 部署版 ConfigMap overlay（`backup/2026-08-29-pre-sync-overlays/files/`）
> 与基线逐文件 `diff -u` 得到；不是凭记忆。
>
> 基线已进容器核对（2026-08-29）：pip `vllm 0.18.0+empty` / `vllm_ascend 0.18.0`；
> 未覆盖文件 sha256 与 v0.18.0 tag 一致；判别文件（rc1 与正式版有差异的
> `vllm/v1/attention/backends/mla/cutlass_mla.py`、`vllm-ascend/setup.py`）哈希
> 均匹配正式版、不匹配 rc1 → **排除 rc1 基线**。

## 对照表

| # | overlay 文件（挂载路径） | 归属仓库 / 基线路径 | diff 内容（修复） | 状态 | 验证证据 | 迁移去向 |
|---|---|---|---|---|---|---|
| 1 | `vllm_scheduler.py` → `vllm/v1/core/sched/scheduler.py` | vllm @ v0.18.0 | ① `has_finished_requests` 统计队列外残留请求（#43433）；② `reset_connector_cache` 无 connector 时 no-op 返回 True（#42694 配套 tweak） | ① **已在镜像**（01-fix-43433）；② **仅 overlay** | ① rev49 sleep 500 修复；② 随 reset 级联部署 | ① 保留在镜像，补丁归档 vllm `wip/dynpd-sleepfix-v0.18.0`；② 随 P3 软 reset 决定是否入 vllm 补丁 |
| 2 | `vllm_core.py` → `vllm/v1/engine/core.py` | vllm @ v0.18.0 | ① `_process_engine_step` GIL 让出改判 `has_requests()`（#43433）；② `_reset_caches(reset_connector=True)` 级联（#42694 语义） | ① **已在镜像**；② **仅 overlay** | ① rev49；② P4 实验确认 remove_all 确实执行 | ① 保留；② 重设计为 soft（P3），默认保留远端 KV |
| 3 | `camem_sleep_fix.py` → `vllm_ascend/device_allocator/camem.py` | vllm-ascend @ v0.18.0 | ① `gc.collect()+torch.npu.empty_cache()`（#7709）；② `wake_up` try/except 回滚（#34600） | **仅 overlay** | ① 保留 #7709 语义；② #34600 在 Ascend 不可达（C++ terminate 绕过 Python except） | 由 03 补丁替换（诊断 + 默认关闭 unmap；不再带 #34600），归档 vllm-ascend `wip/dynpd-sleepfix-v0.18.0` |
| 4 | `ascend_store_connector.py` → `vllm_ascend/.../ascend_store_connector.py` | vllm-ascend @ v0.18.0 | `reset_cache()`：scheduler 侧清 `load_specs/_request_trackers/_unfinished_requests/_preempted_req_ids` + `client.reset()` RPC + LookupKeyServer RESET_MSG handler | **仅 overlay** | 随 reset 级联部署（P4 实验确认 remove_all 执行） | P3 软 reset：保留清账本，remove_all 改 soft 语义 |
| 5 | `pool_scheduler.py` → `vllm_ascend/.../pool_scheduler.py` | vllm-ascend @ v0.18.0 | `LookupKeyClient.reset()`（RESET_MSG → worker `reset_store()`） | **仅 overlay** | 同 #4 | 随 P3 软 reset |
| 6 | `pool_worker.py` → `vllm_ascend/.../pool_worker.py` | vllm-ascend @ v0.18.0 | `reset_store()`：`request_queue.join()`（依赖 #7 的 finally）+ `m_store.remove_all()` | **仅 overlay** | P4 实验：join/remove_all 正常执行，但不释放保留内存 | P3 软 reset 核心：join 保留；remove_all 是否保留待定（P4 结论：与内存无关，可改 soft） |
| 7 | `kv_transfer.py` → `vllm_ascend/.../kv_transfer.py` | vllm-ascend @ v0.18.0 | `SendingThread._handle_request` 包 try/finally，`dec_stored_request + task_done()` 无条件执行（#43742 镜像） | **仅 overlay** | **已验证**：rev52+ freed 8.19→54.66 GiB；`patch -p1 --dry-run` 可应用 v0.18.0 | **转正入镜像**（P2，补丁已归档 vllm-ascend `wip/dynpd-sleepfix-v0.18.0`） |
| 8 | `mooncake_backend_patched.py` → `vllm_ascend/.../backend/mooncake_backend.py` | vllm-ascend @ v0.18.0 | ① `put()` 传 `preferred_segments=[self.local_seg]`（本地 segment 写，03 dynpd 补丁）；② `remove_all()` 方法（后端支持 reset 级联） | **仅 overlay** | ① **已验证**：rev≥37 修复 sleep/wake 后 TRANSFER_FAIL -800；② 同 #6 | ① **转正**（环境必需，建议入 vllm-ascend 或 chart files）；② 随 P3 |

## 非 overlay 的配套项

| 项 | 归属 | 状态 |
|---|---|---|
| 01-fix-43433（scheduler + core 的 #43433 部分） | vllm，镜像构建期 patch | **已在镜像**（llm-la `patches/vllm/01-...`，PR 分支内） |
| P1 有界 drain + assert（idle 回调超时 + 残留清单） | vllm | 未开始 |
| P3 软 connector reset（soft 默认，保留远端 KV） | vllm + vllm-ascend | 设计已定，未实现 |
| P4 camem pool 空闲块 unmap | vllm-ascend | 补丁已写并归档 vllm-ascend `wip/dynpd-sleepfix-v0.18.0`（默认关闭，env `VLLM_ASCEND_CAMEM_SLEEP_UNMAP_FREE_BLOCKS=1` 开启；含 sleep/wake 诊断日志）。**远程已验证**：pool 空闲块仅 0.02 GiB，前提证伪；unmap 保持默认关闭 |
| D rebalancer 预检/事后校验 + 幂等恢复 | llm-la（自有代码） | 未开始 |

## 迁移目标（建议）

- vllm-ascend fork 建 backport 分支（如 `feat/backport-pd-sleepfix-v0.18`）：放 #4/#5/#6/#7/#8 的补丁文件 + README（来源/验证证据/应用方式）。
- vllm 侧（#1②、#2②、P1）：若做 P3/P1，走 vllm 的 backport 分支或上游 PR。
- camem（#3）：不迁移，丢弃。
- 镜像构建：从上述分支取补丁构建，打可识别 tag（`v0.18.0-pd-sleepfix-N`），chart 引用 tag、摘 overlay。

## 已知纠正

- 早前判断"部署版 scheduler 无 no-op-True tweak"有误：rev62 部署版**包含**该 tweak（#1②），本次 diff 为准。
- P4 实验结论修正了 reset 级联的目标：`remove_all` 不释放保留内存，内存主因在 camem pool；因此 soft/full 与内存解耦，soft 可安全默认。

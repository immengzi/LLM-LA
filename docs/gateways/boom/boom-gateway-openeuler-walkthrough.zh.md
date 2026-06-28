# openEuler BooM Gateway 走读

源码仓库：`/home/haiting/llm-la/boom-gateway`  
上游：`https://gitcode.com/openeuler/gateway`  
分支：`master`  
HEAD：`6e76f6b !34 feat(rewrite): strip Claude Code attribution block from /v1/messages`

本文只记录当前 `openeuler/gateway` 对齐后的 BooM Gateway 实现，包括仓库结构、语义转换、Claude Code 处理、KVC-aware 路由和集成注意事项。

## 仓库概览

当前 gateway 是一个 Rust workspace：

```text
boom-gateway/
  Cargo.toml
  Cargo.lock
  README.md
  ARCH.md
  CLAUDE.md
  CONFIG_EXAMPLE.md
  DESCRIPTOR.md
  config.example.yaml
  docs/
    kvc-aware-design.md
    kvc-aware-event-reporting-research.md
    vip-header-forwarding-design.md
  boom-gateway/
    boom-core/
    boom-config/
    boom-auth/
    boom-provider/
    boom-limiter/
    boom-routing/
    boom-audit/
    boom-flowcontrol/
    boom-main/
    boom-dashboard/
    boom-promptlog/
    boom-kvindex/
  misc/
    LB/
```

根 workspace 成员列在 `/home/haiting/llm-la/boom-gateway/Cargo.toml` 中：

- `boom-gateway/boom-core`
- `boom-gateway/boom-config`
- `boom-gateway/boom-auth`
- `boom-gateway/boom-provider`
- `boom-gateway/boom-limiter`
- `boom-gateway/boom-routing`
- `boom-gateway/boom-audit`
- `boom-gateway/boom-flowcontrol`
- `boom-gateway/boom-main`
- `boom-gateway/boom-dashboard`
- `boom-gateway/boom-promptlog`
- `boom-gateway/boom-kvindex`

关键能力：

- Rust 原生 KVC-aware 子系统位于 `boom-gateway/boom-kvindex`。
- Gateway 直接订阅 vLLM ZMQ KV events。
- Gateway 构建内存中的 token-prefix trie，不依赖 Redis sidecar 状态。
- Gateway 使用 `tokenizers` 和 `minijinja` 在 Rust 中对请求做 tokenization。
- KVC 子系统支持热重载；当 KVC 配置未变化时，会保留 trie 和 subscriber。
- `boom-main/src/rewrite.rs` 中有 handler 级别的 Claude Code attribution stripping。
- 当 KVC hit ratio 低于阈值时，OpenAI-compatible provider 可以注入 `vllm_xargs.kv_cache_report_mode=full`。

## 基于路由的客户端识别

路由注册在 `boom-gateway/boom-main/src/main.rs` 中：

- `/v1/chat/completions` -> `routes::chat_completions`，line 131。
- `/v1/messages` -> `routes::messages`，line 132。
- `/v1/completions` -> `routes::completions`，line 135。
- OpenAI-compatible alias routes `/chat/completions`、`/completions`、`/models` 位于 lines 136-140。
- 内部 KVC debug route `/internal/kv-index` 位于 lines 153-155。
- 没有不带 `/v1` 的 `/messages` alias；Anthropic-shaped traffic 必须使用 `/v1/messages`。

当前实现没有找到基于以下内容的 client detection：

- `User-Agent`
- `x-client-*`
- `Claude-Code` 字符串匹配
- `OpenCode` 字符串匹配
- `Codex` 字符串匹配

语义协议由 path 和 request type 决定：

- Claude Code / Anthropic-compatible 客户端走 `POST /v1/messages`。
- OpenAI-compatible 客户端走 `POST /v1/chat/completions` 或 alias route。
- Legacy completions 走 `POST /v1/completions`。
- 没有 Codex-specific route、header parser 或 client enum。如果 Codex 使用 OpenAI endpoint，会被当作 OpenAI-compatible 处理。

Auth header compatibility 比 client detection 更宽：

- `RequiredAuth` 在 `boom-main/src/extractor.rs` 中接受 `Authorization: Bearer ...`、Anthropic-style `x-api-key` 和 Azure-style `api-key`。
- `allowed_routes` 存在于 DB/auth model 中，但当前 route handlers 没有 enforce 它。换句话说，per-key route restrictions 看起来会被存储和加载，但在请求处理路径中还没有生效。

## 通用内部类型

核心类型位于 `boom-gateway/boom-core/src/types.rs`。

OpenAI-compatible 类型：

- `MessageRole` lines 11-18。
- `MessageContent` lines 20-26。
- `ContentPart::Reasoning` lines 41-45。
- `Message` lines 55-71。
- `ChatCompletionRequest` lines 127-187。
- `Tool` 和 `ToolFunction` lines 273-286。
- `ChatCompletionResponse` lines 292-306。
- `StreamDelta` lines 365-377。

值得注意的字段：

- `Message.reasoning_content` 在 lines 66-70 接受 alias `reasoning`。
- `ChatCompletionRequest.extra` 在 lines 166-171 接受未知字段，但跳过 serialization。
- `ChatCompletionRequest.gateway_headers` 在 lines 172-178 是 internal-only。
- `ChatCompletionRequest.kv_cache_report_full` 在 lines 179-186 是 internal-only。
- 在当前 `master` tree 中没有找到 `chat_template_kwargs` 或 `clear_thinking` 字段。

## Claude Code / Anthropic Messages 生命周期

Claude Code 预期发送 Anthropic Messages API traffic：

```text
Claude Code
  -> POST /v1/messages
  -> routes::messages()
  -> optional strip_cc_attribution_anthropic()
  -> anthropic_request_to_openai()
  -> KVC tokenization + provider selection
  -> provider.chat/chat_stream(OpenAI internal request)
  -> upstream OpenAI-compatible vLLM / Anthropic / other provider
  -> response converted back to Anthropic shape
```

Handler 位于 `boom-gateway/boom-main/src/routes.rs`：

- `messages()` 从 line 1432 开始。
- 它在 line 1437 接受 `Json(mut req): Json<AnthropicMessagesRequest>`。
- 它在 lines 1445-1456、conversion 前剥离 Claude Code attribution。
- 它在 line 1458 通过 `anthropic_request_to_openai(&req)` 转换为 OpenAI internal request。
- 它在 lines 1492-1495 resolve model。
- 它在 lines 1540-1555 通过 `TokenizerPool::tokenize_anthropic()` 做 tokenization。
- 它在 lines 1557-1567 通过 `select_provider_with_prefix()` 选择 provider。
- 它在 lines 1577-1582 设置 `openai_req.kv_cache_report_full`。
- 它在 line 1599 附加 gateway headers。
- Streaming 从 lines 1757 onward 使用 `sse_stream_from_anthropic_chat_stream()`。
- Non-streaming 在 line 1722 通过 `openai_response_to_anthropic(&response)` 把 OpenAI response 转回 Anthropic shape。

## Claude Code Attribution / `cch` 处理

Claude Code attribution stripping 是 gateway rewrite，不在外部 router 中。

配置：

- `boom-config/src/lib.rs` lines 388-407 中的 `router_settings.strip_claude_code_attribution`。
- 默认值：`false`。
- 注释说明：仅在把 Claude Code 路由到非 Anthropic backend 时启用，因为 stripping 可能影响官方 Anthropic API 行为。

实现：

- `boom-gateway/boom-main/src/rewrite.rs`。
- `ATTRIBUTION_PREFIX` 在 lines 7-12 是 `x-anthropic-billing-header`。
- `strip_cc_attribution_anthropic()` 从 line 23 开始。
- 它在 lines 26-32 移除 text 以该 prefix 开头的 top-level system text blocks。
- 它在 lines 34-48 移除 nested `role="system"` message text blocks。
- String-form system 明确保持不变。
- User-role messages 不会被扫描。

这个逻辑的目标不是全请求无差别删除 `x-anthropic-billing-header` 字符串，而是覆盖 Claude Code 可能放 attribution 的 system 语义位置。Claude Code v2.1.36+ 会注入变化的 `cch=` attribution 字段；删除整个 attribution block 可以恢复稳定 token prefix，避免破坏 KVC-aware routing。

在 `routes.rs` 中，stripping 位于 `anthropic_request_to_openai()` 之前，也位于 prompt-log capture 之前，位置是 lines 1445-1456。`/v1/chat/completions` 不受影响。

## Anthropic -> OpenAI 转换

转换逻辑位于 `boom-gateway/boom-core/src/anthropic.rs`。

`anthropic_request_to_openai()` lines 9-107：

- Top-level `system` 在 lines 13-26 变成一个 `MessageRole::System` message。
- `user` messages 在 line 31 调用 `convert_user_message()`。
- `assistant` messages 在 line 32 调用 `convert_assistant_message()`。
- Anthropic `tools[].input_schema` 在 lines 48-60 变成 OpenAI `tools[].function.parameters`。
- `stop_sequences` 在 lines 62-69 变成 OpenAI `stop`。
- Anthropic `thinking`、`metadata` 和 `extra` 在 lines 71-81 被复制到 `extra`。
- 返回的 `ChatCompletionRequest` 在 lines 103-105 初始化 internal `gateway_headers` 为空，并设置 `kv_cache_report_full=false`。

User content conversion：

- `convert_user_message()` 大约从 line 575 开始。
- `tool_result` blocks 变成带有 `tool_call_id` 的 OpenAI `role=tool` messages。
- Error tool results 通过给 text 加 `[ERROR]` 前缀来编码。

Assistant content conversion：

- `convert_assistant_message()` 大约从 line 685 开始。
- `tool_use` blocks 变成 OpenAI `tool_calls`；input 会被 JSON-stringified 到 `function.arguments`。
- `thinking` blocks 会作为 internal reasoning content 传递下去。

## OpenAI-Compatible 生命周期

OpenAI-compatible clients，包括可能的 OpenCode/Codex 用法，遵循：

```text
Client
  -> POST /v1/chat/completions
  -> routes::chat_completions()
  -> KVC tokenization with OpenAI messages + tools
  -> select_provider_with_prefix()
  -> provider.chat/chat_stream()
  -> upstream response returned as OpenAI-compatible JSON/SSE
```

Handler：

- `chat_completions()` 从 `routes.rs` line 220 开始。
- 共享的 `chat_completions_inner()` 从 line 231 开始。
- 它在 lines 275-278 resolve content-aware/hybrid model。
- 它在 lines 324-338 使用 `TokenizerPool::tokenize_openai()` 做 tokenization。
- 它在 lines 340-350 使用 prefix tokens 选择 provider。
- 它在 lines 360-369 设置 `req.kv_cache_report_full`。
- Non-streaming 在 lines 504-510 把 internal reasoning parts 规范化为 OpenAI `reasoning_content`。

Legacy completions：

- `completions()` 从 lines 540-549 开始。
- 它调用 `CompletionRequest.into_chat_request()`，然后复用 `chat_completions_inner()`。
- 这个转换在 `types.rs` lines 224-263 中把 prompt 包装成单个 user message。

不支持的 OpenAI endpoints：

- `/v1/embeddings`、`/v1/audio/speech`、`/v1/audio/transcriptions`、`/v1/moderations` 会通过 routes lines 638-652 返回 `NotSupported`。

## Provider Mapping 和 Model Names

配置格式位于 `boom-config/src/lib.rs`。

Model deployment：

- `ModelEntry` lines 167-184 包含 `model_name`、`litellm_params`、`model_info`、`flow_control`、`serve_not_match`、`enabled`。
- `ProviderParams` lines 196-226 包含 `model`、`api_key`、`api_base`、timeout、headers 等。
- `ProviderParams::resolve_provider_and_model()` lines 228-241 解析 `provider/model-id`。
- Auto-detection 位于 lines 248-294。

Aliases：

- `ModelGroupAlias` lines 324-354。
- `router_settings.model_group_alias` lines 361-363。

Router resolution：

- `boom-routing/src/router.rs`
- `resolve_model()` lines 79-83。
- `resolve_model_name()` lines 85-96。
- `resolve_request_model()` lines 98-117 应用 optional hybrid router，然后应用 aliases。
- `select_provider_with_prefix()` lines 128-140 resolve candidates，并把 token ids 委托给 policy。
- `resolve_candidates()` lines 142-165 尝试 exact model、alias target，然后尝试 wildcard `*`。

Provider creation：

- `boom-provider/src/lib.rs`
- `create_provider()` 从 line 20 开始。
- OpenAI-compatible provider families 包括 `openai`、`hosted_vllm`、`vllm`、`ollama`、`deepseek` 等，位于 lines 35-79。
- `anthropic`、`azure`、`gemini`、`bedrock` 在 lines 80-121 有独立 provider adapters。

KVC worker identity：

- `kv_worker_id_from_api_base()` lines 177-207 从 `api_base` host 派生 worker id。
- `OpenAIProvider::new()` 在 `boom-provider/src/openai.rs` lines 19-36 存储这个值。
- `OpenAIProvider::kv_worker_id()` 在 lines 237-239 返回它。

需要注意的映射链：

```text
client model
  -> alias / hybrid router resolved_model
  -> DeploymentStore candidate providers for resolved_model
  -> provider actual model from litellm_params.model
  -> OpenAIProvider rewrites request model to actual provider model
  -> upstream vLLM receives that model
```

如果 `api_base` host 和 ZMQ topic worker id 不匹配，KVC-aware selection 可能退化或无法选中匹配项。

## KVC-Aware 高层流程

```text
Client
  -> Boom Gateway route handler
  -> optional protocol rewrite / Anthropic -> OpenAI conversion
  -> TokenizerPool tokenizes request for resolved model
  -> Router.select_provider_with_prefix()
  -> KvcAwarePolicy queries TokenPrefixIndex
  -> selected Provider sends request to upstream vLLM/OpenAI-compatible endpoint

In background:
vLLM ZMQ PUB
  -> boom-kvindex subscriber
  -> vllm_event parser
  -> GatewayKvEvent
  -> TokenPrefixIndex trie
```

重要文件：

- `boom-gateway/boom-main/src/routes.rs`
- `boom-gateway/boom-main/src/state.rs`
- `boom-gateway/boom-config/src/lib.rs`
- `boom-gateway/boom-routing/src/policy/kvc_aware.rs`
- `boom-gateway/boom-routing/src/router.rs`
- `boom-gateway/boom-kvindex/src/tokenizer.rs`
- `boom-gateway/boom-kvindex/src/subscriber.rs`
- `boom-gateway/boom-kvindex/src/vllm_event.rs`
- `boom-gateway/boom-kvindex/src/backend/token_prefix.rs`
- `docs/kvc-aware-design.md`

## KVC 配置开关

配置位于 `boom-gateway/boom-config/src/lib.rs`。

`RouterSettings`：

- `schedule_policy` / alias `routing_strategy` 位于 lines 356-360。设置 `schedule_policy: kvc_aware` 即启用 KVC-aware。
- `kvc_aware` 位于 lines 379-381。
- `strip_claude_code_attribution` 位于 lines 388-407。

`KvcAwareSettings` 位于 lines 410-446：

- `block_size`：token block size，默认 `16`。
- `cache_weight`：默认 `0.5`。
- `load_weight`：默认 `0.2`。
- `tier_weight`：默认 `0.3`。
- `tokenizer_dir`：包含 `{model}/tokenizer.json` 的目录。
- `zmq_endpoints`：vLLM ZMQ PUB endpoints。
- `zmq_topic_prefix`：默认 `kv@`。
- `max_blocks`：默认 `500000`。
- `full_report_hit_threshold`：默认 `0.8`。

`KvcAwareSettings::validate()` 在 lines 464-480 拒绝无效的 `full_report_hit_threshold`。设计文档在 `docs/kvc-aware-design.md` lines 159-201 有完整示例。

## KVC 启动与热重载

`AppState` 在 `boom-gateway/boom-main/src/state.rs` 中持有 KVC-aware 状态：

- `kv_index` 在 lines 66-77 可热替换。
- `tokenizer_pool` 在 lines 78-79 可热替换。
- `kv_subscriber_handle` 和 `kv_shutdown_tx` 位于 lines 80-86。

Startup：

- `from_config()` 在 lines 159-166 调用 `build_kvc_subsystems()`。
- 它在 line 166 使用这个 index 创建 routing policy。

构建子系统：

- `build_kvc_subsystems()` 从 line 398 开始。
- 如果 `schedule_policy != "kvc_aware"`，它在 lines 401-404 返回 `(None, None)`。
- 它在 lines 406-413 创建 `TokenPrefixIndex::new()`。
- 如果 `tokenizer_dir` 存在，它在 lines 414-423 初始化 `TokenizerPool`。

Subscriber：

- `spawn_kv_subscriber()` 从 line 448 开始。
- 如果 KVC-aware 禁用或没有配置 endpoints，它会 no-op。
- 它在 lines 461-464 把 `zmq_endpoints` 和 `zmq_topic_prefix` 传入 `KvSubscriberConfig`。

Hot reload：

- `reload()` 在 lines 291-317 比较 KVC signature。
- 如果 KVC-relevant config 未变，它会保留 trie 和 subscriber。
- 如果发生变化，它会在 lines 321-335 停止旧 subscriber，重建空 index/pool，并启动新 subscriber。
- `full_report_hit_threshold` 被刻意排除，因为它在 routing time 读取，不需要重建 trie。

Shutdown：

- `boom-main/src/main.rs` 在 lines 106-110 的进程 shutdown 期间发送 `kv_shutdown_tx`。

## Tokenization

Tokenization 位于 `boom-gateway/boom-kvindex/src/tokenizer.rs`。

Assets：

- `ModelAssets` lines 14-23 包含 tokenizer、chat template、BOS/EOS。
- `TokenizerPool` lines 25-33 缓存 per-model assets。
- 文件从 `{tokenizer_dir}/{model}/` 加载。
- `tokenizer.json` 在 lines 51-64 是必需的。
- `tokenizer_config.json` 和 optional `chat_template.jinja` 由 `load_tokenizer_config()` 在 lines 185-243 加载。

OpenAI tokenization：

- `tokenize_openai()` 从 line 92 开始。
- 它在 lines 103-110 使用 messages 和 tools render chat template。
- 它在 lines 115-121 使用 `tokenizer.encode(text, false)` 编码。

Anthropic tokenization：

- `tokenize_anthropic()` 从 line 134 开始。
- 它在 lines 145-153 把 top-level string system prompt 合并成一个 `role=system` message。
- 它在 lines 155-165 render，且不传 `tools` 参数。

Template rendering：

- `render_chat_template()` 从 line 276 开始。
- 它使用 `minijinja`。
- 它在 lines 289-293 设置 `trim_blocks=true` 和 `lstrip_blocks=true`，以匹配 vLLM/Jinja2 行为。
- 它在 lines 295-354 实现 `tojson`、`strip`、`split`、`startswith` 等 compatibility filters 和 functions。
- 它在 lines 365-376 预先把 `tools` serialize 成 JSON strings，以在 minijinja `BTreeMap` 行为下保持 serde insertion order。
- `python_compat_json()` 在 lines 455-493 添加 Python `json.dumps` 风格的 `:` 和 `,` 周围空格。

重要限制：

- 当前 `tokenize_anthropic()` 只在 top-level `system` 为 string form 时传入。Claude Code 注入的 attribution block 是 blocks-form；如果启用开关，它会在 tokenization 前被单独剥离。
- 对于 tools，请求会被转换为 OpenAI 后再转发，但 `/v1/messages` 的 KVC tokenization 当前使用 `tokenize_anthropic()` 处理 Anthropic body，而不是处理转换后的 OpenAI body。

## ZMQ Event Ingestion

Subscriber 位于 `boom-gateway/boom-kvindex/src/subscriber.rs`。

- `KvSubscriberConfig` lines 14-20 包含 endpoints 和 topic prefix。
- `spawn_kv_subscriber()` lines 27-37 启动 background task。
- `run_subscriber()` lines 39-94 为每个 endpoint 创建一个 SUB stream，并通过 `select_all` merge。
- 它在 lines 49-53 使用 `topic_prefix` 订阅。
- `handle_message()` lines 96-197 处理 multipart frames。

预期 frame 形状：

```text
[topic, seq, msgpack_payload]
```

`handle_message()`：

- 在 lines 105-108 通过 `parse_topic()` 解析 topic。
- 在 lines 110-116 读取 sequence number。
- 在 line 120 通过 `parse_vllm_batch()` 解析 msgpack payload。
- 在 lines 122-172 记录 event counts。
- 在 lines 174-186 转换成 `GatewayKvEvent` 并 apply 到 index。

## vLLM Event Parsing

Parser 位于 `boom-gateway/boom-kvindex/src/vllm_event.rs`。

Types：

- `VllmKvEvent::BlockStored` lines 14-28。
- `VllmKvEvent::BlockRemoved` lines 29-34。
- `VllmKvEvent::AllBlocksCleared` lines 35-36。
- `VllmEventBatch` lines 39-48。

Parsing：

- `parse_vllm_batch()` lines 50-88 解析 top-level msgpack array `[ts, events, data_parallel_rank?]`。
- `parse_vllm_event()` lines 90-114 dispatch msgspec `tag=True` event arrays。
- `parse_block_stored()` lines 116-147 读取 `block_hashes`、`parent_block_hash`、`token_ids`、`block_size`、`medium` 等。
- `parse_hash_array()` lines 166-185 使用 `as_u64()`，并 fallback 到 signed reinterpretation，以处理高于 `i64::MAX` 的 hashes。
- `parse_topic()` lines 198-209 期望 `kv@{worker_id}@{model}`。
- `medium_to_tier()` lines 211-222 把 `None/gpu -> Gpu`、`cpu -> Cpu`、`disk/ssd -> Ssd`、其他 -> `Remote`。

转换为内部事件：

- `vllm_batch_to_gateway_events()` 从 line 230 开始。
- `BlockStored` 在 lines 239-301 fan out 成每个 block 一个 `GatewayKvEvent::Store`。
- Parent chain 会在一个 multi-block event 内重建：第一个 block 使用 event `parent_block_hash`，后续 blocks 指向前一个 block，位于 lines 271-288。
- `BlockRemoved` 在 lines 318-328 变成 `GatewayKvEvent::EvictBlocks`。
- `AllBlocksCleared` 在 lines 330-335 变成 `GatewayKvEvent::Remove`。

重要前提：

- 单个 `BlockStored` event 被假设包含来自同一个 sequence 的连续 stored blocks。这在代码 lines 271-283 有记录；wire format 没有 sequence id 可以验证这一点。

## Token Prefix Trie

Trie 实现位于 `boom-gateway/boom-kvindex/src/backend/token_prefix.rs`。

数据结构：

- `TrieNode` lines 10-22：
  - `children: HashMap<u64, Box<TrieNode>>`
  - `workers: HashMap<String, StorageTier>`
- `TokenPrefixIndex` lines 34-67：
  - `tries`
  - `block_parent`
  - `reverse_children`
  - `block_trie_key`
  - `hash_to_tokens`
  - `loads`
  - `lru_queue`
  - `block_size`、score weights、`temp_trie_path`

Trie key：

- `hash_block_tokens()` lines 69-89 对 `u32` token ids 的 little-endian bytes 计算 `xxhash3_64`。
- 这不是 vLLM 的 block hash。它是用于 token block 的紧凑 trie edge key。
- Request lookup 会按 `TokenPrefixIndex.block_size` 切分 incoming token ids，并用同样方式 hash 每个 block，所以 gateway `block_size` 必须匹配 vLLM `--block-size`。

Parent/child handling：

- `build_path()` lines 131-170 从 vLLM parent hashes 重建 root-to-block trie path。
- `find_children()` lines 173-188 使用 `reverse_children` 做 O(1) parent-to-children lookup。
- `reverse_detach()` lines 190-213 保持 reverse index 同步。
- `reinsert_block_tree()` lines 302-366 在 parent 到达后重新定位 delayed orphan children。

Eviction：

- `build_evict_paths()` lines 215-300 增量计算受影响的 trie paths。
- Tier-aware eviction 会避免在 block 从 GPU 移除但仍保留在 CPU/SSD 时删除它；见 `check_worker_tier()` lines 412-429 附近注释。

Matching：

- `KvIndexBackend::find_matches()` 在这个文件中实现，并由 `KvcAwarePolicy` 调用。
- Policy 期望结果按 combined score 排序，并使用第一个 match。
- Scoring fields 在 `KvcAwarePolicy` lines 94-104 中记录。

Scoring formula 来自 `docs/kvc-aware-design.md` lines 144-153：

```text
combined_score = cache_weight * hit_ratio
               + tier_weight * tier_score
               + load_weight * load_score
```

`hit_ratio = matched_blocks / total_request_blocks`。

`LoadMetrics` 存在于 internal event type 中，但当前 ZMQ path 不发出 load events。实践中，`load_score` 通常会 fallback 到默认值，直到其他来源更新 worker load。

## 路由决策

Routing policy 位于 `boom-gateway/boom-routing/src/policy/kvc_aware.rs`。

`KvcAwarePolicy`：

- 持有 `kv_index`、`InFlightTracker` 和 optional flow-control queue info，位于 lines 12-19。
- `select()` 在没有 tokens 时，在 lines 36-46 fallback 到 lowest-load。
- `select_with_context()` 从 line 48 开始。

重要分支：

- 单个 candidate 会在 lines 60-67 跳过 KVC lookup。
- Empty token ids 在 lines 69-74 fallback 到 lowest-load。
- Worker ids 在 lines 76-83 从各 provider 的 `kv_worker_id()` 取得。
- 如果不存在 worker ids，在 lines 85-89 fallback。
- line 92 调用 `kv_index.find_matches(model, token_ids, &worker_ids)`。
- Best match 会被 log，并在 lines 94-117 通过 `kv_worker_id()` 映射回 provider。
- No match 在 lines 121-130 fallback 到 lowest-load。

Provider worker identity：

- `boom-provider/src/lib.rs` 中的 `kv_worker_id_from_api_base()` 位于 lines 177-207。
- 它会从 `api_base` 剥离 scheme、port 和 path，例如 `http://10.0.0.5:8000/v1 -> 10.0.0.5`。
- `OpenAIProvider::new()` 在 `boom-provider/src/openai.rs` lines 19-36 存储这个值。
- `OpenAIProvider::kv_worker_id()` 在 lines 237-239 返回它。

Identity requirement：

- vLLM ZMQ topic worker id 必须等于 gateway provider `kv_worker_id`，当前也就是 `api_base` 的 host 部分。
- 如果 vLLM 发布的是 pod name，但 `api_base` host 是 service name 或 VIP，KVC-aware matching 就无法选中目标 provider。
- 在这个实现里，`model_info.id` 不是 KVC worker identity。

## Request Path 和 Full KV Report

OpenAI `/v1/chat/completions`：

- Handler 从 `boom-main/src/routes.rs` lines 220-238 开始。
- Model 在 lines 275-278 通过 `resolve_request_model()` resolve。
- Request tokenization 在 lines 324-338 通过 `TokenizerPool::tokenize_openai()` 完成。
- Provider 在 lines 340-350 使用 prefix tokens 选择。
- `kv_cache_report_full` 在 lines 360-369 使用 `need_full_kv_report()` 设置。

Anthropic `/v1/messages`：

- Handler 从 lines 1432-1438 开始。
- Optional Claude Code attribution stripping 在 conversion 前发生，位于 lines 1445-1456。
- `anthropic_request_to_openai()` 在 line 1458 发生。
- Model 在 lines 1492-1495 resolve。
- Request tokenization 在 lines 1540-1555 通过 `TokenizerPool::tokenize_anthropic()` 完成。
- Provider selection 位于 lines 1557-1567。
- `openai_req.kv_cache_report_full` 在 lines 1577-1582 设置。

Full report decision：

- `need_full_kv_report()` 位于 `routes.rs` lines 26-51。
- KVC disabled -> false。
- KVC enabled -> `kv_hit_ratio < full_report_hit_threshold`。
- 默认阈值是 `0.8`。

Wire injection：

- `ChatCompletionRequest.kv_cache_report_full` 是 `boom-core/src/types.rs` lines 179-186 中的 internal serde-skipped field。
- `OpenAIProvider::build_request()` 在 `boom-provider/src/openai.rs` lines 39-42 读取它。
- 如果为 true，它会注入：

```json
{
  "vllm_xargs": {
    "kv_cache_report_mode": "full"
  }
}
```

位于 `openai.rs` lines 60-68。

## OpenAI Provider 和 Streaming

OpenAI-compatible provider 位于 `boom-provider/src/openai.rs`。

Request building：

- `OpenAIProvider::build_request()` 从 line 39 开始。
- 它在 line 41 读取 internal `kv_cache_report_full`。
- 它在 lines 43-44 把 `req.model` 重写成实际 provider model。
- 它在 lines 45-55 把 internal `ContentPart::Reasoning` 转成 text，以兼容 upstream。
- 它在 line 58 serialize request。
- 如果 `kv_cache_report_full` 为 true，它会注入 `vllm_xargs.kv_cache_report_mode=full`。

Streaming：

- `chat_stream()` 从 line 122 开始。
- 它在 lines 126-133 强制 `stream=true` 和 `stream_options.include_usage=true`。
- 它在 lines 163-222 把 SSE `data:` chunks 解析为 `ChatStreamChunk`。
- 它在 lines 186-189 把原始 SSE chunk text 存入 `chunk.raw_data`。
- OpenAI client SSE path 会在 `routes.rs` 中 buffer fragmented tool-call arguments，并在 finish chunk 前 flush，所以 clients 会在 `finish_reason` 之前看到完整 tool JSON。

## Anthropic Response Conversion

转换逻辑位于 `boom-core/src/anthropic.rs`。

Non-streaming：

- `openai_response_to_anthropic()` 从 line 113 开始。
- Top-level `reasoning_content` 在 lines 117-126 变成 `thinking` block。
- Text/parts 在 lines 128-154 转换。
- OpenAI `tool_calls` 在 lines 156-167 变成 Anthropic `tool_use`。
- Usage 在 lines 189-194 也映射 cache fields。

Streaming：

- `AnthropicStreamTranscoder` 从 line 208 开始。
- `transcode()` 从 line 273 开始。
- 第一个 chunk 在 lines 298-324 发出 `message_start`。
- `delta.reasoning_content` 在 lines 326-363 变成 Anthropic `thinking_delta`。
- `delta.content` 在 lines 365-403 变成 `text_delta`。
- `delta.tool_calls` 在 lines 405-464 打开 tool_use blocks 并 buffer JSON arguments。
- Finish 在 lines 466-508 关闭打开的 blocks 并 flush `input_json_delta`。
- Stop events 会通过 `pending_stop_reason` 延迟到 usage 可用时再发出。

`routes.rs` 从 lines 1757 onward 使用 `sse_stream_from_anthropic_chat_stream()`。

## Tool / Function Call 语义

Request direction：

- Anthropic `tools[].input_schema` -> OpenAI `ToolFunction.parameters`，位于 `anthropic.rs` lines 48-60。
- Anthropic assistant `tool_use` -> OpenAI assistant `tool_calls`。
- Anthropic user `tool_result` -> OpenAI `role=tool`。

Response direction：

- OpenAI response `tool_calls` -> `openai_response_to_anthropic()` 中的 Anthropic `tool_use` blocks。
- OpenAI streaming `delta.tool_calls` -> 通过 `AnthropicStreamTranscoder` 变成 Anthropic streaming `tool_use` + `input_json_delta`。

KV impact：

- Tool schemas 和 tool-call history 会影响 chat template rendering。
- 在 OpenAI path 中，`TokenizerPool::tokenize_openai()` 接收 `messages` 和 `tools`，并在 minijinja render 前预先 serialize tools，以保持顺序。
- 在 Anthropic path 中，`TokenizerPool::tokenize_anthropic()` 当前接收 Anthropic messages 和 string system，不接收转换后的 OpenAI tools。这是 Claude Code 使用内置 tools 时需要重点验证的区域。

## KVC 与语义转换的耦合点

KVC-aware routing 在 `routes.rs` 中和 semantic translation 耦合。

OpenAI path：

- 在 lines 324-338，model resolution 后对 `req.messages` + `req.tools` 做 tokenization。
- 在 lines 340-350，按 token-prefix match 选择 provider。
- 在 lines 360-369，设置 full report mode。

Anthropic path：

- 在 lines 1445-1456，先剥离 Claude Code attribution。
- 在 line 1458，转换为 OpenAI internal request。
- 在 lines 1540-1555，对 Anthropic request 做 tokenization。
- 在 lines 1557-1567，选择 provider。
- 在 lines 1577-1582，在转换后的 OpenAI request 上设置 full report mode。

这意味着 semantic conversion 会通过两种方式影响 KVC：

- 它选择 resolved model，进而决定 tokenizer directory 和 provider candidates。
- 它影响用于 prefix matching 的 token sequence。

为了获得最佳 KVC 正确性，tokenization path 必须匹配最终 upstream request 的 chat-template input。OpenAI path 更接近这一点，因为它 tokenizes 的是同一个会发送到 downstream 的 OpenAI request shape。Anthropic path 需要针对 Claude Code tools 和 block-form system prompts 做仔细测试。

## 分客户端摘要

### Claude Code

```text
Claude Code -> /v1/messages
  -> strip x-anthropic-billing-header block if enabled
  -> convert Anthropic to internal OpenAI request
  -> tokenize Anthropic body for KVC
  -> select provider
  -> set kv_cache_report_full on OpenAI request
  -> call upstream provider
  -> convert OpenAI response/SSE back to Anthropic
```

重要开关：

```yaml
router_settings:
  strip_claude_code_attribution: true
```

仅在不转发到官方 Anthropic 时使用。

### OpenCode

仓库 README 说 `/v1/messages` 兼容 Claude Code / opencode。如果 OpenCode 使用 Anthropic Messages，它会走和 Claude Code 相同的路径。

如果 OpenCode 使用 OpenAI Chat Completions，它会走 OpenAI path。没有特殊的 OpenCode code path。

### Codex

没有找到 Codex-specific code。如果 Codex 使用 OpenAI Chat Completions 或 legacy Completions，它会走 OpenAI-compatible paths。如果它需要 `/v1/responses`，当前 gateway 不暴露这个 route。

## Runtime DB Migrations

Runtime DB migrations 由 `boom-dashboard/src/migrations.rs` 通过 `run_migrations()` 编排，并在 startup 时从 `boom-main/src/state.rs` 调用。

Migration path 涉及的 tables/DDL：

- 来自 `boom-audit` 的 `boom_request_log`，以及新增 columns，例如 `key_alias`、`deployment_id`、`model_name`、`client_ip`、`ttft_ms`。
- 来自 `boom-routing` 的 `boom_model_deployment`，以及 `deployment_id`、`quota_count_ratio`、`auto_disabled`、queue/context limits 等字段。
- 来自 `boom-routing` 的 `boom_model_alias`。
- 来自 `boom-limiter` 的 `boom_rate_limit_state`、`boom_key_plan_assignment` 和 `boom_rate_limit_plan`。
- Dashboard migrations 中的 `boom_config`、`boom_team_table` 和 `boom_verification_token`。

重要 caveat：`boom_verification_token.allowed_routes` 会被 migrated 和 auth code loaded，但当前没有在 request handlers 中看到 enforcement。

## Practical Requirements

要让 KVC-aware routing 端到端工作：

- 设置 `router_settings.routing_strategy: kvc_aware`。
- 配置 `router_settings.kvc_aware.block_size`，使其匹配 vLLM `--block-size`。
- 配置 `tokenizer_dir`，包含 per-model tokenizer files。
- 为所有 vLLM workers 配置 `zmq_endpoints`。
- 确保 ZMQ topic 是 `kv@{worker_id}@{model}`，或者调整代码/配置。
- 确保 ZMQ topic 中的 `worker_id` 等于从 `api_base` host 派生的 provider `kv_worker_id()`。
- 对 Claude Code -> non-Anthropic backend，启用 `strip_claude_code_attribution: true`。
- 如果使用 `vllm_xargs` full-report injection，确保 provider 是 OpenAI-compatible。

## 主要风险 / 未决问题

- `tokenize_anthropic()` 不接收 Anthropic `tools`；对于 `/v1/messages`，tool schemas 会影响 downstream OpenAI conversion，但不会影响当前 Anthropic tokenization path。这可能需要针对 Claude Code tool-heavy requests 做验证。
- `tokenize_anthropic()` 只把 string-form top-level system 提升到 messages 中；除了 stripped/converted flow，block-form system content 不会被合并。Claude Code attribution 已处理，但其他 block-form system content 可能需要更仔细验证。
- Worker id alignment 很严格；如果 `api_base` 使用 service/VIP，而 ZMQ 发布 pod IP 或 pod name，就可能出问题。
- `vllm_xargs` 只由 OpenAI-compatible provider path 注入。Non-OpenAI providers 不会收到这个 full-report hint，除非另行实现。
- `serde_json` 编译时启用了 `preserve_order`，这降低但没有完全消除 tokenization 顺序问题。

## 上游设计文档偏差

`boom-gateway/docs/kvc-aware-design.md` 很有用，但和当前 checkout 的代码有几点不同：

- 设计文档把 trie keys 描述为原始 `Vec<u32>` token blocks；实现使用 `u64` `xxhash3_64` trie edges。
- 设计文档说 `model_info.id` 必须匹配 vLLM worker identity；实现使用从 `litellm_params.api_base` 提取的 host。
- 设计文档暗示 reload 时会广泛重建 trie；实现会在 KVC config signature 未变时保留 trie 和 subscriber。
- `load_weight` 是可配置的，但在当前 ZMQ path 没有 `LoadMetrics` events 的情况下，load score 通常是 fallback 常量。

## 集成前检查

- 验证 Claude Code 2.1.150 是否确实发送匹配 `strip_cc_attribution_anthropic()` 的 block-form `x-anthropic-billing-header`。
- 决定 Claude Code requests 是否应该在 Anthropic->OpenAI conversion 后做 KVC tokenization，而不是使用 `tokenize_anthropic()`。
- 验证 MiniMax/Qwen tokenizer templates 在 minijinja preprocessing 下是否正确 render。
- 验证 vLLM ZMQ topic worker id 是否等于 provider `api_base` host。
- 验证 upstream vLLM 是否接受 `vllm_xargs.kv_cache_report_mode=full`。
- 在最终 Boom entrypoint 和 model names 确认后，为 BZ local `MiniMax-M2.7` / `Qwen3.5-122B-A10B` model aliases 添加配置示例。

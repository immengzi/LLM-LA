# Results Log

Structured, per-entry migration of the legacy `analysis-notebooks/logs` journal. The original `logs` file is kept untouched as the source of truth until this is verified.

Add new entries with `new_entry.py` (see `template.md`).


## 01-push-vs-pull-strategy

| Series | Entry | Exp IDs | Summary |
|---|---|---|---|
| 0 | [`000-check-the-pull-method-under-capped-input-output.md`](01-push-vs-pull-strategy/000-check-the-pull-method-under-capped-input-output.md) | 1, 2, 3, 4 | With bounded prefill and decode, batching efficiency dominates. Pull consistently wins latency and t |
| 1 | [`001-check-the-pull-method-under-none-capped-on-input.md`](01-push-vs-pull-strategy/001-check-the-pull-method-under-none-capped-on-input.md) | 5, 6, 7, 8 | Once decode dominates, early admission helps. Push methods improve TTFT and TPOT by starting work so |
| 2 | [`002-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/002-check-the-pull-method-under-none-capped-input-an.md) | 9, 10, 11, 12 | Unbounded prefills expose scheduling trade-offs, Pull stabilizes mean latency and TPOT by pacing hea |
| 3 | [`003-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/003-check-the-pull-method-under-none-capped-input-an.md) | 13, 14, 15, 16 | At very low load, scheduling policy is irrelevant. GPUs are idle, queues are empty, and all methods  |
| 4 | [`004-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/004-check-the-pull-method-under-none-capped-input-an.md) | 17, 18, 19, 20 | Light load favors responsiveness: push improves TTFT, while Pull already begins to reduce tail ampli |
| 5 | [`005-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/005-check-the-pull-method-under-none-capped-input-an.md) | 21, 22, 23, 24 | Near saturation, the core trade-off appears: Pull protects tail latency via admission control, while |
| 6 | [`006-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/006-check-the-pull-method-under-none-capped-input-an.md) | 25, 26, 27, 28 | At sustained high load, speculative admission breaks down. Pull clearly dominates latency and tails  |
| 7 | [`007-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/007-check-the-pull-method-under-none-capped-input-an.md) | 29, 30, 31, 32 | Under heavy decode pressure, push maximizes utilization and TTFT but worsens TPOT stability. Pull sa |
| 8 | [`008-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/008-check-the-pull-method-under-none-capped-input-an.md) | 33, 34, 35, 36 | At extreme load, push optimizes throughput and responsiveness but amplifies tail risk. Pull remains  |
| 9 | [`009-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/009-check-the-pull-method-under-none-capped-input-an.md) | 37, 38, 39, 40 | Burst arrivals favor controlled batching. Pull absorbs bursts cleanly, while push reacts faster but  |
| 10 | [`010-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/010-check-the-pull-method-under-none-capped-input-an.md) | 41, 42, 43, 44 | Results are consistent, Pull is latency-stable, push is responsiveness-biased, and Least-Queue is br |
| 11 | [`011-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/011-check-the-pull-method-under-none-capped-input-an.md) | 45, 46, 47, 48 | Results are consistent, Pull is latency-stable, push is responsiveness-biased, and Least-Queue is br |
| 12 | [`012-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/012-check-the-pull-method-under-none-capped-input-an.md) | 49, 50, 51, 52 | pull is better but as there are many lost requests no conclusion can be infered |
| 13 | [`013-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/013-check-the-pull-method-under-none-capped-input-an.md) | 53, 54, 55, 56 | pull is better but as there are many lost requests no conclusion can be infered. |
| 14 | [`014-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/014-check-the-pull-method-under-none-capped-input-an.md) | 57, 58, 59, 60 | pull slightly better but since it isn't under saturation point like 13 and 14 which seems to be the  |
| 15 | [`015-check-the-pull-method-under-none-capped-input-an.md`](01-push-vs-pull-strategy/015-check-the-pull-method-under-none-capped-input-an.md) | 61, 62, 63, 64 | pull slightly better but since it isn't under saturation point like 13 and 14 which seems to be the  |
| 16 | [`016-first-eperiment-on-10000-requests.md`](01-push-vs-pull-strategy/016-first-eperiment-on-10000-requests.md) | 65, 66, 67, 68 | very good resutls in terms of the tail and mean latency of the requests but many lost requests and p |
| 17 | [`017-sanity-check-on-200-requests.md`](01-push-vs-pull-strategy/017-sanity-check-on-200-requests.md) | 69, 70, 71, 72 | good results but not as good 10000 experiment, our approach is much better for long running requests |
| 18 | [`018-sanity-check-on-200-requests-with-dump.md`](01-push-vs-pull-strategy/018-sanity-check-on-200-requests-with-dump.md) | 73, 74, 75, 76 | good results but not as good 10000 experiment, our approach is much better for long running requests |
| 19 | [`019-repeat-on-10000-with-fixes-on-the-sidecar-tcp.md`](01-push-vs-pull-strategy/019-repeat-on-10000-with-fixes-on-the-sidecar-tcp.md) | 77, 78, 79, 80 | good results but still many lost requests |
| 20 | [`020-repeat-on-200-with-fixes-on-the-sidecar-tcp.md`](01-push-vs-pull-strategy/020-repeat-on-200-with-fixes-on-the-sidecar-tcp.md) | 81, 82, 83, 84 | sanity check, good but not representative due to short experiment |
| 21 | [`021-series-21.md`](01-push-vs-pull-strategy/021-series-21.md) | 85, 86, 87, 88 | — |
| 22 | [`022-series-22.md`](01-push-vs-pull-strategy/022-series-22.md) | 89, 90, 91, 92 | — |
| 23 | [`023-10000-on-rps-1.md`](01-push-vs-pull-strategy/023-10000-on-rps-1.md) | 93, 94, 95, 96 | marginal benefit on not saturated workloads |
| 24 | [`024-10000-on-rps-2.md`](01-push-vs-pull-strategy/024-10000-on-rps-2.md) | 97, 98, 99, 100 | marginal benefit on not saturated workloads |
| 25 | [`025-10000-on-rps-3.md`](01-push-vs-pull-strategy/025-10000-on-rps-3.md) | 101, 102, 103, 104 | consistent best near saturation excelent performance |
| 26 | [`026-10000-on-rps-4.md`](01-push-vs-pull-strategy/026-10000-on-rps-4.md) | 105, 106, 107, 108 | no conclusion as too many lost request from 5000 requests |
| 27 | [`027-10000-on-rps-5.md`](01-push-vs-pull-strategy/027-10000-on-rps-5.md) | 109, 110, 111, 112 | no conclusion as too many lost request from 2500 requests |
| 28 | [`028-10000-on-rps-6.md`](01-push-vs-pull-strategy/028-10000-on-rps-6.md) | 113, 114, 115, 116 | no conclusion as too many lost request from 1250 requests |
| 29 | [`029-200-on-rps-6-for-debugging-the-new-experriments.md`](01-push-vs-pull-strategy/029-200-on-rps-6-for-debugging-the-new-experriments.md) | 117, 118, 119, 120 | Not important |
| 30 | [`030-repeat-of-10000-after-adding-the-zmq-on-the-clie.md`](01-push-vs-pull-strategy/030-repeat-of-10000-after-adding-the-zmq-on-the-clie.md) | 121, 122, 123, 124 | lots of lost requests, no deduction |
| 31 | [`031-repeat-of-25000-after-adding-the-zmq-on-the-clie.md`](01-push-vs-pull-strategy/031-repeat-of-25000-after-adding-the-zmq-on-the-clie.md) | 125, 126, 127, 128 | lots of lost requests, no deduction |
| 32 | [`032-just-checking-the-changes-on-both-client-router.md`](01-push-vs-pull-strategy/032-just-checking-the-changes-on-both-client-router.md) | 129, 130, 131, 132 | no conclusion as it is a short experiment but also no lost requests so looks good |
| 33 | [`033-10000-requests-experiment-on-rps-1.md`](01-push-vs-pull-strategy/033-10000-requests-experiment-on-rps-1.md) | 133, 134, 135, 136 | under low and not saturated load there is almost no difference in the methods |
| 34 | [`034-10000-requests-experiment-on-rps-2.md`](01-push-vs-pull-strategy/034-10000-requests-experiment-on-rps-2.md) | 137, 138, 139, 140 | under low and not saturated load there is almost no difference in the methods |
| 35 | [`035-10000-requests-experiment-on-rps-3.md`](01-push-vs-pull-strategy/035-10000-requests-experiment-on-rps-3.md) | 141, 142, 143, 144 | 3 seems to be a magic number that shows the best performance |
| 36 | [`036-10000-requests-experiment-on-rps-4.md`](01-push-vs-pull-strategy/036-10000-requests-experiment-on-rps-4.md) | 145, 146, 147, 148 | After 3 we seem to starrt getting diminishing return. Almost no improvement in mean and lower improv |
| 37 | [`037-10000-requests-experiment-on-rps-5.md`](01-push-vs-pull-strategy/037-10000-requests-experiment-on-rps-5.md) | 149, 150, 151, 152 | After 3 we seem to starrt getting diminishing return. Almost no improvement in mean and lower improv |
| 38 | [`038-10000-requests-experiment-on-rps-6.md`](01-push-vs-pull-strategy/038-10000-requests-experiment-on-rps-6.md) | 153, 154, 155, 156 | After 3 we seem to starrt getting diminishing return. Almost no improvement in mean and lower improv |
| 39 | [`039-25000-requests-experiment-on-rps-3-repeating-the.md`](01-push-vs-pull-strategy/039-25000-requests-experiment-on-rps-3-repeating-the.md) | 157, 158, 159, 160 | 3 seems to be a magic number that shows the best performance |
| 40 | [`040-40000-requests-experiment-on-rps-3-repeating-the.md`](01-push-vs-pull-strategy/040-40000-requests-experiment-on-rps-3-repeating-the.md) | 161, 162, 163, 164 | 3 seems to be a magic number that shows the best performance |
| 41 | [`041-40000-requests-experiment-on-rps-4-repeating-the.md`](01-push-vs-pull-strategy/041-40000-requests-experiment-on-rps-4-repeating-the.md) | 165, 166, 167, 168 | Since the servers became over saturated then the queuing latency is now so high that the difference  |
| 42 | [`042-40000-requests-experiment-on-rps-5-repeating-the.md`](01-push-vs-pull-strategy/042-40000-requests-experiment-on-rps-5-repeating-the.md) | 169, 170, 171, 172 | Since the servers became over saturated then the queuing latency is now so high that the difference  |
| 43 | [`043-40000-requests-experiment-on-rps-6-repeating-the.md`](01-push-vs-pull-strategy/043-40000-requests-experiment-on-rps-6-repeating-the.md) | 173, 174, 175, 176 | Since the servers became over saturated then the queuing latency is now so high that the difference  |
| 44 | [`044-debugging-16-servers-on-moderate-load.md`](01-push-vs-pull-strategy/044-debugging-16-servers-on-moderate-load.md) | 177, 178, 179, 180 | all good and seems working well |
| 45 | [`045-series-45.md`](01-push-vs-pull-strategy/045-series-45.md) | 181, 182, 183, 184 | TODO |
| 46 | [`046-series-46.md`](01-push-vs-pull-strategy/046-series-46.md) | 185, 186, 187, 188 | TODO |
| 47 | [`047-series-47.md`](01-push-vs-pull-strategy/047-series-47.md) | 189, 190, 191, 192 | TODO |
| 48 | [`048-series-48.md`](01-push-vs-pull-strategy/048-series-48.md) | 193, 194, 195, 196, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253 | TODO |

## 02-boom-reproduce

| Series | Entry | Exp IDs | Summary |
|---|---|---|---|
| 1 | [`000-check-the-multi-turn-feature.md`](02-boom-reproduce/000-check-the-multi-turn-feature.md) | — | it works |
| 2 | [`001-check-the-multi-turn-feature.md`](02-boom-reproduce/001-check-the-multi-turn-feature.md) | — | it works |
| 3 | [`002-stability-test-with-single-turn.md`](02-boom-reproduce/002-stability-test-with-single-turn.md) | — | stable |
| 4 | [`003-stability-test.md`](02-boom-reproduce/003-stability-test.md) | — | it is stable |
| 5-8 | [`004-reproducing-series-35.md`](02-boom-reproduce/004-reproducing-series-35.md) | 5, 6, 7, 8 | reproducable with very good results |
| 9-12 | [`005-reproducing-series-35-exp-141-144.md`](02-boom-reproduce/005-reproducing-series-35-exp-141-144.md) | 9, 10, 11, 12 | Good imporvement but not impressevie |
| 13-16 | [`006-reproduing-old-series-35.md`](02-boom-reproduce/006-reproduing-old-series-35.md) | 13, 14, 15, 16 | Good Results *** |
| 17-20 | [`007-check-streaming.md`](02-boom-reproduce/007-check-streaming.md) | 17, 18, 19, 20 | Seems not stremaing |
| 21-24 | [`008-check-streaming.md`](02-boom-reproduce/008-check-streaming.md) | 21, 22, 23, 24 | Seems not to be streaming |

## 03-bz-kv-soak

| Series | Entry | Exp IDs | Summary |
|---|---|---|---|
| 1-2 | [`000-us-boom-vs-boom-only-kv-soak-tonight-placeholders.md`](03-bz-kv-soak/000-us-boom-vs-boom-only-kv-soak-tonight-placeholders.md) | TBD | Tonight placeholders: us+boom vs boom-only KV soak on BZ. Analyse in `prefix-kv-drop-root-cause.ipynb`. |

## 04-bz-fair-highload

| Series | Entry | Exp IDs | Summary |
|---|---|---|---|
| 1-2 | [`000-fair-pull-a-b-at-96-users-prefix-tonight-placeholders.md`](04-bz-fair-highload/000-fair-pull-a-b-at-96-users-prefix-tonight-placeholders.md) | TBD | Tonight placeholders: fair OFF vs ON at 96 users (prefix). Analyse in `claude-strategy-comparison.ipynb`. |

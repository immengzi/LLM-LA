# llmla_sidecarless — 2 instances

- Users: `20`  Spawn rate: `10`  Run time: `15s`
- Model: `served-model`  Mock latency ms: `0`

| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| POST | /chat/completions | 7 | 23 | 51 | 9.51 | 25.95 |
| Custom | Gateway Overhead Duration (ms) | 7 | 23 | 51 | 9.51 | 25.95 |
| POST | Aggregated | 7 | 23 | 51 | 9.51 | 51.89 |

# CodeFlowBench Experiment Documentation

## Dataset Overview

**Source:** [WaterWang-001/CodeFlowBench-2505](https://huggingface.co/datasets/WaterWang-001/CodeFlowBench-2505) (HuggingFace)

**Total examples:** 5,258 competitive programming problems

### Structure

Each example contains:
- `problem-id`: Unique problem identifier (e.g., "2060G")
- `title`: Problem title (e.g., "G. Bugged Sort")
- `problem-description`: Full problem statement in markdown
- `input`: Input format specification
- `output`: Output format specification
- `solutions`: List of reference solutions with `content`
- `subproblems`: Decomposed sub-tasks (for multi-turn evaluation)
- `overall-turns`: Number of independent subproblems to solve
- `overall-depth`: Dependency chain depth

### Input Token Length Statistics (problem-description + input)

| Statistic | Value (tokens) |
|-----------|----------------|
| Min | 5 |
| Max | 1,464 |
| Mean | 397 |
| P50 (Median) | 381 |
| P75 | 495 |
| P90 | 613 |
| P95 | 688 |
| P99 | 884 |

*Tokenization: GPT-2 tokenizer (cl100k_base equivalent)*

### Multi-Turn Structure

CodeFlowBench decomposes problems into subproblems with dependencies:

| `overall-turns` | Count | % |
|-----------------|-------|---|
| 1 | 1,488 | 28.3% |
| 2 | 2,158 | 41.0% |
| 3 | 990 | 18.8% |
| 4 | 402 | 7.6% |
| 5+ | 220 | 4.2% |

Each subproblem has:
- `name`: Function name (e.g., `get_right_out`, `solve`)
- `depth`: Dependency level (0 = top-level, depends on others)
- `dependencies`: List of subproblem names that must be solved first
- `statement`: Description of what the subproblem does

**Example (3-turn problem "H. Coffee Break"):**
1. `get_right_out` (depth=2, no deps) - calculate right-moving students
2. `get_left_out` (depth=1, depends on `get_right_out`) - calculate left-moving students
3. `solve` (depth=0, depends on both) - combine results

---

## Experiment Setup

### Current Configuration

The experiment uses **backend: boom** to route directly to vLLM pods.

Key settings in `configs/4-2-template-boom-claude-glm.yaml`:
- `prompt_source: "codeflow"` - Uses CodeFlowBench dataset
- `hf_lmsys.dataset_name: "./data/codeflowbench"` - Local Arrow files
- `hf_lmsys.tokenizer_name: "zai-org/GLM-5"` - For token counting
- `generation.max_tokens: 1024` - Output token limit
- `generation.temperature: 0.0` - Deterministic output

### Token Filtering
- `min_input_tokens: 256` - Filter out very short problems
- `max_input_tokens: 10000000` - No upper limit

### Load Pattern
- `det` (deterministic) pattern with `rate_rps: 0.3`
- `step_schedule: "0:5,30:15,60:3"` - Varying request rates

---

## Running the Experiment

### Prerequisites

1. Ensure vLLM pods are running and accessible at `http://7.216.57.215:30401`
2. Ensure the Boom service is running
3. Local dataset at `./data/codeflowbench` (Arrow files)

### Basic Run

```bash
cd /mnt/code/llm-lb/microservice
python main.py --config 4-2-template-boom-claude-glm
```

### Override Parameters

```bash
# Run with custom number of requests
python main.py --config 4-2-template-boom-claude-glm --n 1000

# Or use an inline override
```

### Output

Results are saved to `/mnt/data/experiments/<id>/`:
- `config.json` - Effective configuration used
- `config_used.yaml` - YAML file snapshot
- `logs.json` - Per-request JSON lines (NDJSON)
- `run_summary.json` - Machine-readable summary
- `endpoint_tokens.json` - Token usage statistics

---

## Multi-Turn Evaluation

### Current Status: ENABLED

Multi-turn evaluation is now **enabled** in the experiment configuration.

### How It Works

With `multi_turn: true`, each problem with multiple subproblems is evaluated as a multi-turn conversation:

1. **Subproblems are sorted by depth** (descending) — highest depth first
   - Example: depth=2 → depth=1 → depth=0
   - This ensures dependencies are solved in the correct order

2. **Each subproblem becomes a separate turn**:
   - Turn 1 (user): Problem description + first subproblem statement
   - Turn 1 (assistant): Model generates code for first subproblem
   - Turn 2 (user): Previous code as context + next subproblem statement
   - Turn 2 (assistant): Model generates code for second subproblem
   - ... and so on

3. **Context is passed between turns** — later turns see code from earlier turns

### Conversation Structure

```
Turn 1 (user): [Problem Description] + [Subproblem 0 statement]
Turn 1 (assistant): [Model generates code for subproblem 0]

Turn 2 (user): [Previous code] + [Subproblem 1 statement]
Turn 2 (assistant): [Model generates code for subproblem 1]

...

Turn N (assistant): [Model generates final solve function]
```

### Implementation Notes

- `_iter_codeflowbench_conversations` in `prompts.py` builds multi-turn conversations
- `HFLmsysConfig.multi_turn` field enables this feature
- Ground truth solutions only exist for the final `solve` function (turn N)
- Intermediate subproblem outputs are model-generated (no ground truth for evaluation)

---

## Quick Reference

| Question | Answer |
|----------|--------|
| Where is the data? | `./data/codeflowbench/` (Arrow format) |
| How many examples? | 5,258 |
| Is multi-turn enabled? | **Yes** (`multi_turn: true` in config) |
| How to disable multi-turn? | Set `multi_turn: false` or remove from config |
| Default max output tokens? | 1024 |
| Default rate? | 0.3 RPS |
"""
SWE-bench Verified + LangGraph Agent Workflow
=============================================
Dataset : princeton-nlp/SWE-bench_Verified  (500 instances, test split)
Local   : loaded from Arrow produced by download_swebench.py
Workflow: Generator → Reviewer → Fixer  (3 LLM rounds per instance)

Usage
-----
    python swebench_agent.py \
        --local_dir /mnt/nvme1/haiting_jd/datasets/SWE-bench \
        --n 3 --difficulty "<15 min fix"
"""

import argparse
import difflib
import json
import re
import textwrap
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, TypedDict

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph

# ---------------------------------------------------------------------------
# vLLM client
# ---------------------------------------------------------------------------
VLLM_BASE_URL  = "http://localhost:10000/v1"
MODEL_NAME     = "qwen3-8b"
MAX_ITERATIONS = 10

DIFFICULTY_LEVELS = ["<15 min fix", "15 min - 1 hour", "1-4 hours", ">4 hours"]


def make_llm(temperature: float = 0.15) -> ChatOpenAI:
    return ChatOpenAI(
        base_url=VLLM_BASE_URL,
        api_key="EMPTY",
        model=MODEL_NAME,
        temperature=temperature,
        max_tokens=2048,
        streaming=False,
    )


# ===========================================================================
# LangGraph state — must be TypedDict so LangGraph can read keys correctly
# ===========================================================================

class AgentState(TypedDict):
    messages:     list[BaseMessage]
    stage:        str
    iterations:   int
    final_output: str


# ===========================================================================
# Data model
# ===========================================================================

@dataclass
class SWEInstance:
    instance_id:              str
    repo:                     str
    problem_statement:        str
    hints_text:               str
    gold_patch:               str
    test_patch:               str
    base_commit:              str
    created_at:               str
    fail_to_pass:             list[str]
    pass_to_pass:             list[str]
    difficulty:               str
    environment_setup_commit: str


def _row_to_instance(row: dict) -> SWEInstance:
    def _parse_list(value) -> list[str]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else []
            except (json.JSONDecodeError, ValueError):
                return []
        return []

    return SWEInstance(
        instance_id=              row.get("instance_id", ""),
        repo=                     row.get("repo", ""),
        problem_statement=        row.get("problem_statement", ""),
        hints_text=               row.get("hints_text", ""),
        gold_patch=               row.get("patch", ""),
        test_patch=               row.get("test_patch", ""),
        base_commit=              row.get("base_commit", ""),
        created_at=               row.get("created_at", ""),
        fail_to_pass=             _parse_list(row.get("FAIL_TO_PASS", "[]")),
        pass_to_pass=             _parse_list(row.get("PASS_TO_PASS", "[]")),
        difficulty=               row.get("difficulty", ""),
        environment_setup_commit= row.get("environment_setup_commit", ""),
    )


# ===========================================================================
# Local dataset loader
# ===========================================================================

def load_local_instances(
    local_dir:         str,
    n:                 int            = 5,
    repo_filter:       Optional[str]  = None,
    instance_id:       Optional[str]  = None,
    difficulty_filter: Optional[str]  = None,
) -> list[SWEInstance]:
    base = Path(local_dir)
    if not base.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {base.resolve()}\n"
            f"Run:  python download_swebench.py --output {local_dir}"
        )

    meta_path = base / "meta.json"
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
        print(f"Dataset : {meta.get('dataset_name')}")
        print(f"Total   : {meta.get('n_instances')} instances")
        print(f"Difficulty distribution: {meta.get('difficulty_counts')}")

    # Arrow load (state.json is written by save_to_disk into the root dir)
    if (base / "state.json").exists():
        try:
            from datasets import load_from_disk
            print(f"\nLoading Arrow dataset from {base} …")
            ds = load_from_disk(str(base))

            if instance_id:
                ds = ds.filter(lambda x: x["instance_id"] == instance_id)
            elif repo_filter:
                ds = ds.filter(lambda x: repo_filter in x["repo"])

            if difficulty_filter:
                ds = ds.filter(lambda x: x["difficulty"] == difficulty_filter)

            instances = [_row_to_instance(dict(row)) for row in ds][:n]
            print(f"  ✓ {len(instances)} instance(s) loaded from Arrow.")
            _print_difficulty_summary(instances)
            return instances

        except Exception as exc:
            print(f"[WARN] Arrow load failed ({exc}), falling back to JSONL.")

    # JSONL fallback
    jsonl_path = base / "instances.jsonl"
    if jsonl_path.exists():
        print(f"Loading JSONL from {jsonl_path} …")
        instances: list[SWEInstance] = []
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if instance_id and row.get("instance_id") != instance_id:
                    continue
                if repo_filter and repo_filter not in row.get("repo", ""):
                    continue
                if difficulty_filter and row.get("difficulty") != difficulty_filter:
                    continue
                instances.append(_row_to_instance(row))
                if len(instances) >= n:
                    break
        print(f"  ✓ {len(instances)} instance(s) loaded from JSONL.")
        _print_difficulty_summary(instances)
        return instances

    raise FileNotFoundError(
        f"No loadable data in {base.resolve()}.\n"
        f"Run:  python download_swebench.py --output {local_dir}"
    )


def _print_difficulty_summary(instances: list[SWEInstance]) -> None:
    if not instances:
        return
    dist = Counter(i.difficulty for i in instances)
    ordered = {k: dist[k] for k in DIFFICULTY_LEVELS if k in dist}
    print(f"  Difficulty breakdown: {ordered}")


# ===========================================================================
# LangGraph: Generator → Reviewer → Fixer
# ===========================================================================

GENERATOR_SYSTEM = SystemMessage(content=textwrap.dedent("""\
    You are an expert Python software engineer solving real GitHub issues.
    You will be given a problem statement and optional hints from issue comments.

    Output a unified diff patch in this exact format:
    --- a/path/to/file.py
    +++ b/path/to/file.py
    @@ -N,M +N,M @@
     context line
    -removed line
    +added line

    Rules:
    - Modify only what is needed. Do not touch unrelated code.
    - Include 3 lines of context around each change.
    - Output ONLY the patch — no explanation, no markdown fences.
"""))

REVIEWER_SYSTEM = SystemMessage(content=textwrap.dedent("""\
    You are a senior Python code reviewer. Given a patch for a GitHub issue:
    1. Correctness  — does the logic actually resolve the issue?
    2. Edge cases   — inputs or states the patch might miss?
    3. Diff format  — is the unified diff syntactically valid?
    4. Regressions  — could this break existing behaviour?
    5. Style        — PEP8, naming, type hints?
    Give numbered comments. End with LGTM or NEEDS_FIX:<count>.
"""))

FIXER_SYSTEM = SystemMessage(content=textwrap.dedent("""\
    You are a senior Python engineer producing the final production-ready patch.
    You receive: the original issue, a first-attempt patch, and review comments.
    Output a corrected unified diff that addresses EVERY review comment.
    Output ONLY the patch — no explanation.
"""))


def _print_stage(name: str, content: str, max_chars: int = 900) -> None:
    preview = content[:max_chars] + ("…" if len(content) > max_chars else "")
    print(f"\n{'─'*60}\n[{name}]\n{preview}\n")


def build_swebench_graph() -> StateGraph:
    llm = make_llm()

    def generator_node(state: AgentState) -> AgentState:
        response = llm.invoke([GENERATOR_SYSTEM] + state["messages"])
        _print_stage("GENERATOR", response.content)
        return AgentState(
            messages     = state["messages"] + [response],
            stage        = "review",
            iterations   = state["iterations"] + 1,
            final_output = "",
        )

    def reviewer_node(state: AgentState) -> AgentState:
        prompt = HumanMessage(
            content=f"[REVIEWER] Review this patch:\n\n{state['messages'][-1].content}"
        )
        response = llm.invoke([REVIEWER_SYSTEM] + state["messages"] + [prompt])
        _print_stage("REVIEWER", response.content)
        return AgentState(
            messages     = state["messages"] + [prompt, response],
            stage        = "fix",
            iterations   = state["iterations"] + 1,
            final_output = "",
        )

    def fixer_node(state: AgentState) -> AgentState:
        recent  = state["messages"][-6:]
        context = "\n\n".join(
            f"[{'USER' if isinstance(m, HumanMessage) else 'AGENT'}]\n{m.content}"
            for m in recent
        )
        prompt = HumanMessage(
            content=(
                "[FIXER] Produce the corrected final patch addressing all review comments.\n\n"
                f"Context:\n{context}"
            )
        )
        response = llm.invoke([FIXER_SYSTEM] + state["messages"] + [prompt])
        _print_stage("FIXER", response.content)
        return AgentState(
            messages     = state["messages"] + [prompt, response],
            stage        = "done",
            iterations   = state["iterations"] + 1,
            final_output = response.content,
        )

    def router(state: AgentState) -> Literal["review", "fix", "done"]:
        if state["iterations"] >= MAX_ITERATIONS:
            return "done"
        s = state.get("stage", "done")
        return s if s in ("review", "fix") else "done"

    g = StateGraph(AgentState)
    g.add_node("generator", generator_node)
    g.add_node("review",    reviewer_node)
    g.add_node("fix",       fixer_node)

    g.set_entry_point("generator")
    g.add_conditional_edges("generator", router, {"review": "review", "done": END})
    g.add_conditional_edges("review",    router, {"fix": "fix",       "done": END})
    g.add_edge("fix", END)

    return g.compile()


def run_instance(instance: SWEInstance) -> str:
    print(f"\n{'='*60}")
    print(f"Instance   : {instance.instance_id}")
    print(f"Repo       : {instance.repo}")
    print(f"Difficulty : {instance.difficulty}")
    print(f"Created    : {instance.created_at}")
    print(f"F2P tests  : {len(instance.fail_to_pass)}")
    print(f"{'='*60}")

    hint_block = (
        f"\n\nHints from issue comments:\n{instance.hints_text}"
        if instance.hints_text.strip() else ""
    )
    f2p_block = (
        f"\n\nThese tests must go FAIL → PASS after your patch:\n"
        + json.dumps(instance.fail_to_pass, indent=2)
        if instance.fail_to_pass else ""
    )

    initial_prompt = HumanMessage(content=textwrap.dedent(f"""\
        Repository  : {instance.repo}
        Instance ID : {instance.instance_id}
        Difficulty  : {instance.difficulty}
        Commit      : {instance.base_commit}

        === PROBLEM STATEMENT ===
        {instance.problem_statement}{hint_block}{f2p_block}

        === TASK ===
        Generate a unified diff patch that resolves the issue above.
    """))

    # Construct initial state as a plain dict matching AgentState TypedDict
    initial_state: AgentState = {
        "messages":     [initial_prompt],
        "stage":        "review",
        "iterations":   0,
        "final_output": "",
    }

    graph  = build_swebench_graph()
    result = graph.invoke(initial_state)
    return result.get("final_output") or result["messages"][-1].content


# ===========================================================================
# Lightweight patch scoring  (no Docker required)
# ===========================================================================

@dataclass
class PatchScore:
    instance_id:      str
    difficulty:       str
    edit_similarity:  float
    file_coverage:    float
    keyword_hit_rate: float
    has_diff_format:  bool
    composite:        float


_STOPWORDS = {
    "def", "class", "self", "return", "if", "else", "elif", "for", "in",
    "import", "from", "and", "or", "not", "True", "False", "None", "pass",
    "with", "as", "try", "except", "raise", "while", "break", "continue",
    "lambda", "yield", "del", "global", "assert",
}


def _tokens(text: str) -> set[str]:
    return {
        t for t in re.findall(r"[A-Za-z_]\w*", text)
        if len(t) > 2 and t not in _STOPWORDS
    }


def _modified_files(patch: str) -> set[str]:
    files: set[str] = set()
    for line in patch.splitlines():
        m = re.match(r"^(?:\+\+\+|---)\s+(?:[ab]/)?(.+?)(?:\s.*)?$", line)
        if m:
            p = m.group(1).strip()
            if p != "/dev/null":
                files.add(p)
    return files


def score_patch(instance: SWEInstance, agent_patch: str) -> PatchScore:
    gold  = instance.gold_patch
    agent = agent_patch

    g_lines  = [l for l in gold.splitlines()  if l.strip()]
    a_lines  = [l for l in agent.splitlines() if l.strip()]
    edit_sim = difflib.SequenceMatcher(None, g_lines, a_lines).ratio()

    gfiles = _modified_files(gold)
    agent_lower = agent.lower()
    if gfiles:
        covered  = sum(
            1 for f in gfiles
            if any(seg.lower() in agent_lower for seg in f.split("/") if len(seg) > 3)
        )
        file_cov = covered / len(gfiles)
    else:
        file_cov = 1.0

    g_tok  = _tokens(gold)
    a_tok  = _tokens(agent)
    kw_hit = len(g_tok & a_tok) / len(g_tok) if g_tok else 0.0

    has_diff = bool(re.search(r"^@@\s+-\d+", agent, re.MULTILINE))

    composite = round(
        0.40 * edit_sim +
        0.25 * file_cov +
        0.25 * kw_hit   +
        0.10 * float(has_diff),
        4,
    )
    return PatchScore(
        instance_id      = instance.instance_id,
        difficulty       = instance.difficulty,
        edit_similarity  = round(edit_sim, 4),
        file_coverage    = round(file_cov, 4),
        keyword_hit_rate = round(kw_hit,   4),
        has_diff_format  = has_diff,
        composite        = composite,
    )


# ===========================================================================
# Main evaluation loop
# ===========================================================================

@dataclass
class InstanceResult:
    instance_id:        str
    repo:               str
    difficulty:         str
    score:              PatchScore
    agent_patch:        str
    gold_patch:         str
    fail_to_pass_tests: list[str]
    timestamp:          str


def run_eval(
    local_dir:         str,
    n:                 int            = 3,
    repo_filter:       Optional[str]  = None,
    instance_id:       Optional[str]  = None,
    difficulty_filter: Optional[str]  = None,
    output_path:       str            = "swebench_results.json",
) -> list[InstanceResult]:

    instances = load_local_instances(
        local_dir         = local_dir,
        n                 = n,
        repo_filter       = repo_filter,
        instance_id       = instance_id,
        difficulty_filter = difficulty_filter,
    )

    if not instances:
        print("[WARN] No instances matched the filter. Exiting.")
        return []

    results: list[InstanceResult] = []

    for i, inst in enumerate(instances):
        print(f"\n\n{'#'*60}")
        print(f"# Instance {i+1}/{len(instances)}: {inst.instance_id}")
        print(f"{'#'*60}")

        try:
            agent_patch = run_instance(inst)
            score       = score_patch(inst, agent_patch)

            results.append(InstanceResult(
                instance_id        = inst.instance_id,
                repo               = inst.repo,
                difficulty         = inst.difficulty,
                score              = score,
                agent_patch        = agent_patch,
                gold_patch         = inst.gold_patch,
                fail_to_pass_tests = inst.fail_to_pass,
                timestamp          = datetime.utcnow().isoformat(),
            ))

            print(f"\n📊 Scores  [{inst.instance_id}]  difficulty={inst.difficulty}")
            print(f"   edit_similarity  : {score.edit_similarity:.3f}")
            print(f"   file_coverage    : {score.file_coverage:.3f}")
            print(f"   keyword_hit_rate : {score.keyword_hit_rate:.3f}")
            print(f"   has_diff_format  : {score.has_diff_format}")
            print(f"   ── composite     : {score.composite:.3f}")

        except Exception as exc:
            import traceback
            print(f"[ERROR] {inst.instance_id}: {exc}")
            traceback.print_exc()          # ← full traceback so next error is obvious

    if results:
        n_ok  = len(results)
        avg_c = sum(r.score.composite        for r in results) / n_ok
        avg_e = sum(r.score.edit_similarity  for r in results) / n_ok
        avg_k = sum(r.score.keyword_hit_rate for r in results) / n_ok

        by_diff: dict[str, list[float]] = {}
        for r in results:
            by_diff.setdefault(r.difficulty, []).append(r.score.composite)

        print(f"\n\n{'='*60}")
        print(f"SUMMARY  ({n_ok} instance(s)  |  model: {MODEL_NAME})")
        print(f"  avg composite        : {avg_c:.3f}")
        print(f"  avg edit_similarity  : {avg_e:.3f}")
        print(f"  avg keyword_hit_rate : {avg_k:.3f}")
        print(f"  per-difficulty composite averages:")
        for level in DIFFICULTY_LEVELS:
            if level in by_diff:
                scores = by_diff[level]
                print(f"    {level:20s}: {sum(scores)/len(scores):.3f}  (n={len(scores)})")
        print(f"{'='*60}")

    output = {
        "model":         MODEL_NAME,
        "dataset":       "princeton-nlp/SWE-bench_Verified (local)",
        "local_dir":     str(Path(local_dir).resolve()),
        "n_instances":   len(results),
        "avg_composite": round(
            sum(r.score.composite for r in results) / max(len(results), 1), 4
        ),
        "timestamp": datetime.utcnow().isoformat(),
        "instances": [
            {
                "instance_id":        r.instance_id,
                "repo":               r.repo,
                "difficulty":         r.difficulty,
                "scores":             asdict(r.score),
                "fail_to_pass_tests": r.fail_to_pass_tests,
                "agent_patch":        r.agent_patch,
                "gold_patch":         r.gold_patch,
                "timestamp":          r.timestamp,
            }
            for r in results
        ],
    }
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n✅ Results written → {output_path}")

    return results


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="SWE-bench Verified + LangGraph agent (local dataset)"
    )
    parser.add_argument("--local_dir",    required=True)
    parser.add_argument("--n",            type=int, default=3)
    parser.add_argument("--repo",         default=None)
    parser.add_argument("--instance_id",  default=None)
    parser.add_argument("--difficulty",   default=None, choices=DIFFICULTY_LEVELS)
    parser.add_argument("--output",       default="swebench_results.json")
    args = parser.parse_args()

    run_eval(
        local_dir         = args.local_dir,
        n                 = args.n,
        repo_filter       = args.repo,
        instance_id       = args.instance_id,
        difficulty_filter = args.difficulty,
        output_path       = args.output,
    )


if __name__ == "__main__":
    main()

# prompts.py
# Prompt sources: file-based JSON and HF LMSYS dataset.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Iterator, Any
import json
import os

from config import HFLmsysConfig


@dataclass
class ConversationTurn:
    role: str            # "user" or "assistant"
    content: str
    output_tokens: int = 0  # tokenizer length of the assistant reply (0 for user turns)


@dataclass
class Conversation:
    turns: List[ConversationTurn] = field(default_factory=list)

    @property
    def num_rounds(self) -> int:
        return sum(1 for t in self.turns if t.role == "user")


def load_replay_output_lengths(logs_path: str, n: int) -> List[int]:
    """
    Read per-request completion_tokens from a previous experiment's logs.json.

    Returns a list of length n, ordered by the original request idx.
    Requests without a valid completion_tokens (errors, lost) are assigned
    the median of successful values so every slot has a usable length.
    """
    from pathlib import Path
    import statistics

    p = Path(logs_path)
    if not p.is_file():
        raise FileNotFoundError(
            f"replay_output_lengths_from: file not found: {logs_path}"
        )

    by_idx: dict[int, int] = {}
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            idx = rec.get("idx")
            ct = rec.get("completion_tokens")
            if idx is not None and ct is not None and not rec.get("send_failed"):
                try:
                    by_idx[int(idx)] = int(ct)
                except (ValueError, TypeError):
                    continue

    if not by_idx:
        raise ValueError(
            f"replay_output_lengths_from: no valid completion_tokens "
            f"found in {logs_path}"
        )

    successful_vals = list(by_idx.values())
    fallback = int(statistics.median(successful_vals))

    lengths = [by_idx.get(i, fallback) for i in range(n)]

    present = sum(1 for i in range(n) if i in by_idx)
    missing = n - present
    if missing > 0:
        print(
            f"[replay] WARNING: {missing}/{n} requests missing completion_tokens "
            f"in {logs_path}; using median={fallback} as fallback"
        )

    print(
        f"[replay] Loaded {present} output lengths from {logs_path}: "
        f"min={min(lengths)}, max={max(lengths)}, "
        f"avg={sum(lengths)/len(lengths):.0f}, median={fallback}"
    )
    return lengths


def load_prompts_from_file(path: str, variant: str, n: int) -> List[str]:
    """
    Load prompts from a JSON file with keys: short, medium, long.
    Repeat the selected variant n times.
    """
    with open(path, "r") as f:
        data = json.load(f)

    for key in ("short", "medium", "long"):
        if key not in data:
            raise ValueError(f"JSON file '{path}' is missing required key: '{key}'")

    prompt = str(data[variant])
    return [prompt for _ in range(n)]


def _iter_lmsys_pairs(
    cfg: HFLmsysConfig,
    max_n: int,
) -> Iterator[Tuple[str, int]]:
    """
    Minimal LMSYS loader:

    - Uses cfg.dataset_name / cfg.split via datasets.load_dataset or load_from_disk
    - Tokenizer from cfg.tokenizer_name
    - Yields (prompt, out_tokens) pairs
    - Filters by input token len (min/max)
    - Repeats each example cfg.repeat_each times
    """
    try:
        from datasets import load_dataset, load_from_disk  # type: ignore
        from transformers import AutoTokenizer, PreTrainedTokenizerFast  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "hf-lmsys prompt_source requires 'datasets' and 'transformers' packages. "
            "Install them via `pip install datasets transformers`."
        ) from e

    # Tokenizer
    tokenizer_name = cfg.tokenizer_name
    if isinstance(tokenizer_name, str) and tokenizer_name and tokenizer_name.strip():
        if os.path.isdir(tokenizer_name):
            print(f"[LMSYS] Using local tokenizer path: {tokenizer_name}")
            tok = PreTrainedTokenizerFast.from_pretrained(
                tokenizer_name,
                local_files_only=True,
                extra_special_tokens={},
            )
        else:
            print(f"[LMSYS] Loading tokenizer from HF Hub: {tokenizer_name}")
            tok = PreTrainedTokenizerFast.from_pretrained(
                tokenizer_name,
                extra_special_tokens={},
            )
    else:
        raise ValueError("hf_lmsys.tokenizer_name must be provided for hf-lmsys mode")

    try:
        tok.model_max_length = int(1e9)
    except Exception:
        pass

    # Dataset
    dataset_name = cfg.dataset_name
    split = cfg.split
    if isinstance(dataset_name, str) and os.path.isdir(dataset_name):
        print(f"[LMSYS] Using local dataset path: {dataset_name}")
        try:
            ds = load_from_disk(dataset_name)
        except Exception as e:
            raise RuntimeError(f"Failed to load local dataset at {dataset_name}: {e}") from e
        if split:
            print(f"[LMSYS] NOTE: local dataset loaded; 'split={split}' is not enforced here.")
    else:
        print(
            f"[LMSYS] Loading from HF Hub: {dataset_name}:{split} (streaming={cfg.streaming})"
        )
        ds = load_dataset(dataset_name, split=split, streaming=cfg.streaming)

    def _to_int_or_none(x: Any) -> Optional[int]:
        try:
            return int(x) if x is not None else None
        except Exception:
            return None

    min_input_tokens = _to_int_or_none(cfg.min_input_tokens)
    max_input_tokens = _to_int_or_none(cfg.max_input_tokens)
    min_output_tokens = _to_int_or_none(getattr(cfg, "min_output_tokens", None))
    max_output_tokens = _to_int_or_none(getattr(cfg, "max_output_tokens", None))
    repeat_each = _to_int_or_none(cfg.repeat_each) or 1
    if repeat_each < 1:
        repeat_each = 1

    if min_input_tokens is not None or max_input_tokens is not None:
        print(
            "[LMSYS] Input token filter: "
            f"min_input={min_input_tokens}, max_input={max_input_tokens}"
        )
    if min_output_tokens is not None or max_output_tokens is not None:
        print(
            "[LMSYS] Output token filter: "
            f"min_output={min_output_tokens}, max_output={max_output_tokens}"
        )
    if repeat_each != 1:
        print(f"[LMSYS] Repetition enabled: repeat_each={repeat_each} per example")

    def _role(t: dict) -> str:
        return (t.get("from") or t.get("role") or "").lower()

    def _text(t: dict) -> str:
        return (t.get("value") or t.get("content") or "").strip()

    def _extract_pair(ex: dict) -> Optional[Tuple[str, str]]:
        conv = None
        for k in ("conversations", "conversation", "conversation_a"):
            if k in ex and isinstance(ex[k], list) and ex[k]:
                conv = ex[k]
                break
        if not isinstance(conv, list) or len(conv) < 2:
            return None

        last_user_idx = None
        for i, t in enumerate(conv):
            if _role(t) in ("human", "user"):
                last_user_idx = i
        if last_user_idx is None:
            return None

        for j in range(last_user_idx + 1, len(conv)):
            if _role(conv[j]) in ("gpt", "assistant", "bot"):
                u, a = _text(conv[last_user_idx]), _text(conv[j])
                if u and a:
                    return u, a
        return None

    yielded = 0
    for i, ex in enumerate(ds):
        if yielded >= max_n:
            break

        pair = _extract_pair(ex)
        if not pair:
            continue

        prompt, reply = pair
        in_len = len(tok.encode(prompt, add_special_tokens=False))
        out_len = len(tok.encode(reply, add_special_tokens=False))

        if min_input_tokens is not None and in_len < min_input_tokens:
            continue
        if max_input_tokens is not None and in_len > max_input_tokens:
            continue
        if min_output_tokens is not None and out_len < min_output_tokens:
            continue
        if max_output_tokens is not None and out_len > max_output_tokens:
            continue

        for _ in range(repeat_each):
            if yielded >= max_n:
                break
            yield prompt, out_len
            yielded += 1

    print(
        f"[LMSYS] Done. Yielded {yielded} prompts "
        f"(max_n={max_n}, repeat_each={repeat_each})."
    )


def build_prompts_from_lmsys(
    cfg: HFLmsysConfig, n: int
) -> List[Tuple[str, int]]:
    """
    Returns list of (prompt, output_tokens) tuples.
    output_tokens is the tokenized length of the real assistant reply
    from the dataset — use it for per-request min_tokens/max_tokens.
    """
    import random as _random

    seed = getattr(cfg, "seed", None)

    if seed is not None:
        pool_size = max(n, n * 2)
        pool: List[Tuple[str, int]] = []
        for prompt, out_len in _iter_lmsys_pairs(cfg, max_n=pool_size):
            pool.append((prompt, out_len))
            if len(pool) >= pool_size:
                break

        rng = _random.Random(seed)
        rng.shuffle(pool)
        result = pool[:n]
        print(f"[LMSYS] Seeded selection: seed={seed}, pool={len(pool)}, selected={len(result)}")
    else:
        result = []
        for prompt, out_len in _iter_lmsys_pairs(cfg, max_n=n):
            result.append((prompt, out_len))
            if len(result) >= n:
                break

    return result


def _iter_lmsys_conversations(
    cfg: HFLmsysConfig,
    max_n: int,
    min_rounds: int = 2,
) -> Iterator[Conversation]:
    """
    Yields Conversation objects from the LMSYS dataset.

    Only yields conversations with at least min_rounds user turns.
    Reuses the same tokenizer/dataset loading logic as _iter_lmsys_pairs.
    """
    try:
        from datasets import load_dataset, load_from_disk  # type: ignore
        from transformers import PreTrainedTokenizerFast  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "hf-lmsys prompt_source requires 'datasets' and 'transformers' packages."
        ) from e

    tokenizer_name = cfg.tokenizer_name
    if isinstance(tokenizer_name, str) and tokenizer_name and tokenizer_name.strip():
        if os.path.isdir(tokenizer_name):
            tok = PreTrainedTokenizerFast.from_pretrained(
                tokenizer_name, local_files_only=True, extra_special_tokens={},
            )
        else:
            tok = PreTrainedTokenizerFast.from_pretrained(
                tokenizer_name, extra_special_tokens={},
            )
    else:
        raise ValueError("hf_lmsys.tokenizer_name must be provided for hf-lmsys mode")

    try:
        tok.model_max_length = int(1e9)
    except Exception:
        pass

    dataset_name = cfg.dataset_name
    split = cfg.split
    if isinstance(dataset_name, str) and os.path.isdir(dataset_name):
        try:
            from datasets import load_from_disk
            ds = load_from_disk(dataset_name)
        except Exception as e:
            raise RuntimeError(f"Failed to load local dataset at {dataset_name}: {e}") from e
    else:
        from datasets import load_dataset
        ds = load_dataset(dataset_name, split=split, streaming=cfg.streaming)

    def _role(t: dict) -> str:
        return (t.get("from") or t.get("role") or "").lower()

    def _text(t: dict) -> str:
        return (t.get("value") or t.get("content") or "").strip()

    def _normalize_role(r: str) -> Optional[str]:
        if r in ("human", "user"):
            return "user"
        if r in ("gpt", "assistant", "bot"):
            return "assistant"
        return None

    def _to_int_or_none(x: Any) -> Optional[int]:
        try:
            return int(x) if x is not None else None
        except Exception:
            return None

    min_input_tokens = _to_int_or_none(cfg.min_input_tokens)
    max_input_tokens = _to_int_or_none(cfg.max_input_tokens)
    min_output_tokens = _to_int_or_none(getattr(cfg, "min_output_tokens", None))
    max_output_tokens = _to_int_or_none(getattr(cfg, "max_output_tokens", None))

    yielded = 0
    for ex in ds:
        if yielded >= max_n:
            break

        conv_raw = None
        for k in ("conversations", "conversation", "conversation_a"):
            if k in ex and isinstance(ex[k], list) and ex[k]:
                conv_raw = ex[k]
                break
        if not isinstance(conv_raw, list) or len(conv_raw) < 2:
            continue

        turns: List[ConversationTurn] = []
        valid = True
        for t in conv_raw:
            role = _normalize_role(_role(t))
            text = _text(t)
            if role is None or not text:
                continue
            if turns and turns[-1].role == role:
                continue
            out_tok = 0
            if role == "assistant":
                out_tok = len(tok.encode(text, add_special_tokens=False))
            elif role == "user":
                in_len = len(tok.encode(text, add_special_tokens=False))
                if min_input_tokens is not None and in_len < min_input_tokens:
                    valid = False
                    break
                if max_input_tokens is not None and in_len > max_input_tokens:
                    valid = False
                    break
            turns.append(ConversationTurn(role=role, content=text, output_tokens=out_tok))

        if not valid:
            continue

        if turns and turns[0].role != "user":
            turns = turns[1:]
        if turns and turns[-1].role != "assistant":
            turns = turns[:-1]
        if len(turns) < 2:
            continue

        conv = Conversation(turns=turns)
        if conv.num_rounds < min_rounds:
            continue

        if min_output_tokens is not None or max_output_tokens is not None:
            skip = False
            for ct in conv.turns:
                if ct.role == "assistant":
                    if min_output_tokens is not None and ct.output_tokens < min_output_tokens:
                        skip = True
                        break
                    if max_output_tokens is not None and ct.output_tokens > max_output_tokens:
                        skip = True
                        break
            if skip:
                continue

        yield conv
        yielded += 1

    print(
        f"[LMSYS] Multi-turn: yielded {yielded} conversations "
        f"(min_rounds={min_rounds}, max_n={max_n})"
    )


def build_conversations_from_lmsys(
    cfg: HFLmsysConfig, n: int, min_rounds: int = 2,
) -> List[Conversation]:
    """
    Returns a list of Conversation objects from the LMSYS dataset.
    Only includes conversations with >= min_rounds user turns.
    """
    import random as _random

    seed = getattr(cfg, "seed", None)

    if seed is not None:
        pool_size = max(n, n * 2)
        pool: List[Conversation] = list(_iter_lmsys_conversations(cfg, max_n=pool_size, min_rounds=min_rounds))
        rng = _random.Random(seed)
        rng.shuffle(pool)
        result = pool[:n]
        print(f"[LMSYS] Multi-turn seeded: seed={seed}, pool={len(pool)}, selected={len(result)}")
    else:
        result = []
        for conv in _iter_lmsys_conversations(cfg, max_n=n, min_rounds=min_rounds):
            result.append(conv)
            if len(result) >= n:
                break

    return result


def load_replay_conversation_lengths(
    logs_path: str, n_conversations: int,
) -> List[List[int]]:
    """
    Read per-turn completion_tokens from a previous multi-turn experiment's logs.json.

    Returns a list of length n_conversations, where each element is a list of
    per-turn output token counts ordered by turn_idx.
    """
    from pathlib import Path
    import statistics

    p = Path(logs_path)
    if not p.is_file():
        raise FileNotFoundError(f"replay: file not found: {logs_path}")

    by_conv: dict[int, dict[int, int]] = {}
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            conv_id = rec.get("conversation_id")
            turn_idx = rec.get("turn_idx")
            ct = rec.get("completion_tokens")
            if conv_id is not None and turn_idx is not None and ct is not None and not rec.get("send_failed"):
                try:
                    by_conv.setdefault(int(conv_id), {})[int(turn_idx)] = int(ct)
                except (ValueError, TypeError):
                    continue

    if not by_conv:
        raise ValueError(f"replay: no valid multi-turn completion_tokens in {logs_path}")

    all_vals = [v for turns in by_conv.values() for v in turns.values()]
    fallback = int(statistics.median(all_vals))

    result: List[List[int]] = []
    for cid in range(n_conversations):
        if cid in by_conv:
            turns = by_conv[cid]
            max_turn = max(turns.keys()) + 1
            result.append([turns.get(t, fallback) for t in range(max_turn)])
        else:
            result.append([fallback])

    present = sum(1 for cid in range(n_conversations) if cid in by_conv)
    missing = n_conversations - present
    if missing > 0:
        print(
            f"[replay] WARNING: {missing}/{n_conversations} conversations missing "
            f"in {logs_path}; using median={fallback} as fallback"
        )

    total_turns = sum(len(t) for t in result)
    print(
        f"[replay] Multi-turn: loaded {present} conversations ({total_turns} total turns) "
        f"from {logs_path}"
    )
    return result

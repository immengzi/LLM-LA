# prompts.py
# Prompt sources: file-based JSON and HF LMSYS dataset.

from __future__ import annotations

from typing import List, Optional, Tuple, Iterator, Any
import json
import os

from config import HFLmsysConfig


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
    repeat_each = _to_int_or_none(cfg.repeat_each) or 1
    if repeat_each < 1:
        repeat_each = 1

    if min_input_tokens is not None or max_input_tokens is not None:
        print(
            "[LMSYS] Input token filter: "
            f"min_input={min_input_tokens}, max_input={max_input_tokens}"
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

        for _ in range(repeat_each):
            if yielded >= max_n:
                break
            yield prompt, out_len
            yielded += 1

    print(
        f"[LMSYS] Done. Yielded {yielded} prompts "
        f"(max_n={max_n}, repeat_each={repeat_each})."
    )


def build_prompts_from_lmsys(cfg: HFLmsysConfig, n: int) -> List[str]:
    prompts: List[str] = []
    for prompt, _out_len in _iter_lmsys_pairs(cfg, max_n=n):
        prompts.append(prompt)
        if len(prompts) >= n:
            break
    return prompts

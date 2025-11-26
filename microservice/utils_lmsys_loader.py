# -*- coding: utf-8 -*-
"""
Config-aware LMSYS loader with unified local/hub logic.
Yields (prompt, out_tokens_estimated_from_reply) pairs for testing schedulers.

- If HF_DATASET_NAME points to a local path, load_from_disk() is used.
- Otherwise, it’s treated as a Hugging Face dataset ID.
- Same rule applies to HF_TOKENIZER_NAME (local path vs. hub model).

Supports:
    - input-token filtering:
        LMSYS_MIN_INPUT_TOKENS
        LMSYS_MAX_INPUT_TOKENS
    - per-example repetition:
        LMSYS_REPEAT_EACH
"""

import os
import sys
from datasets import load_dataset, load_from_disk
from transformers import AutoTokenizer
from config import get_config


def _load_tokenizer(tokenizer_name: str):
    if isinstance(tokenizer_name, str) and os.path.isdir(tokenizer_name):
        print(f"[TOK] ✅ Using local tokenizer path: {tokenizer_name}")
        tok = AutoTokenizer.from_pretrained(tokenizer_name, local_files_only=True)
    else:
        print(f"[TOK] 🌐 Loading tokenizer from HF Hub: {tokenizer_name}")
        tok = AutoTokenizer.from_pretrained(tokenizer_name)

    try:
        tok.model_max_length = int(1e9)
    except Exception:
        pass
    return tok


def _load_dataset(dataset_name: str, split: str, streaming: bool):
    if isinstance(dataset_name, str) and os.path.isdir(dataset_name):
        print(f"[DATASET] ✅ Using local dataset path: {dataset_name}")
        try:
            return load_from_disk(dataset_name)
        except Exception as e:
            print(f"[DATASET] ⚠️ Failed to load local dataset ({e}), falling back to HF Hub.")

    print(
        f"[DATASET] 🌐 Loading from Hugging Face Hub: "
        f"{dataset_name}:{split} (streaming={streaming})"
    )
    return load_dataset(dataset_name, split=split, streaming=streaming)


def iter_lmsys_pairs(
    dataset_name: str = None,
    split: str = None,
    tokenizer_name: str = None,
    max_n: int = 10000,
    streaming: bool = None,
    log_interval: int = 500,
    verbose: bool = False,
    *,
    progress: bool = False,
    progress_desc: str = "LMSYS",
):
    cfg = get_config()

    dataset_name = dataset_name or getattr(cfg, "HF_DATASET_NAME", "lmsys/lmsys-chat-1m")
    split = split or getattr(cfg, "HF_DATASET_SPLIT", "train")
    tokenizer_name = tokenizer_name or getattr(cfg, "HF_TOKENIZER_NAME", "gpt2")
    if streaming is None:
        streaming = bool(getattr(cfg, "HF_STREAMING", False))

    print(f"[LMSYS] Preparing dataset='{dataset_name}:{split}', tokenizer='{tokenizer_name}'")

    tok = _load_tokenizer(tokenizer_name)
    ds = _load_dataset(dataset_name, split, streaming)

    def _to_int_or_none(x):
        if x is None:
            return None
        try:
            return int(x)
        except Exception:
            return None

    min_input_tokens = _to_int_or_none(getattr(cfg, "LMSYS_MIN_INPUT_TOKENS", None))
    max_input_tokens = _to_int_or_none(getattr(cfg, "LMSYS_MAX_INPUT_TOKENS", None))

    if min_input_tokens is not None or max_input_tokens is not None:
        print(
            "[LMSYS] Input token filter: "
            f"min_input={min_input_tokens}, max_input={max_input_tokens}"
        )

    repeat_each = _to_int_or_none(getattr(cfg, "LMSYS_REPEAT_EACH", 1)) or 1
    if repeat_each < 1:
        repeat_each = 1
    if repeat_each != 1:
        print(f"[LMSYS] Repetition enabled: repeat_each={repeat_each} per logical example")

    use_bar = bool(progress)
    pbar = None
    if use_bar:
        try:
            from tqdm.auto import tqdm
            total = None
            if not streaming:
                try:
                    total = len(ds)
                except Exception:
                    total = None
            if total is None:
                total = max_n if isinstance(max_n, int) and max_n > 0 else None
            pbar = tqdm(total=total, desc=progress_desc, unit="ex")
        except Exception:
            pbar = None
            use_bar = False

    def _role(t):
        return (t.get("from") or t.get("role") or "").lower()

    def _text(t):
        return (t.get("value") or t.get("content") or "").strip()

    def _extract_pair_like_notebook(ex):
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

        pair = _extract_pair_like_notebook(ex)
        if not pair:
            continue

        prompt, reply = pair

        in_len = len(tok.encode(prompt, add_special_tokens=False))
        out_len = len(tok.encode(reply, add_special_tokens=False))

        if min_input_tokens is not None and in_len < min_input_tokens:
            if verbose:
                print(
                    f"[LMSYS] Skip idx={i}: in_len={in_len} < min_input={min_input_tokens}"
                )
            continue

        if max_input_tokens is not None and in_len > max_input_tokens:
            if verbose:
                print(
                    f"[LMSYS] Skip idx={i}: in_len={in_len} > max_input={max_input_tokens}"
                )
            continue

        for r in range(repeat_each):
            if yielded >= max_n:
                break

            if verbose:
                rep_info = f" (rep {r+1}/{repeat_each})" if repeat_each > 1 else ""
                print(
                    f"\n[LMSYS] Example #{yielded+1}{rep_info}\n"
                    f"Input  ({in_len} tokens):\n{prompt}\n"
                    f"{'-'*40}\n"
                    f"Output ({out_len} tokens):\n{reply}\n"
                    f"{'='*80}\n"
                )

            yield prompt, out_len
            yielded += 1

            if use_bar and pbar is not None:
                pbar.update(1)
            elif not verbose and yielded % log_interval == 0:
                sys.stdout.write(f"\r[LMSYS] Processed {yielded} examples so far...")
                sys.stdout.flush()

    if pbar is not None:
        pbar.close()

    print(
        f"\n[LMSYS] Done. Yielded {yielded} requests "
        f"(max_n={max_n}, repeat_each={repeat_each})."
    )

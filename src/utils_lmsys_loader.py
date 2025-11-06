# -*- coding: utf-8 -*-
"""
Config-aware LMSYS loader with unified local/hub logic.
Yields (prompt, out_tokens_estimated_from_reply) pairs for testing schedulers.

- If HF_DATASET_NAME points to a local path, load_from_disk() is used.
- Otherwise, it’s treated as a Hugging Face dataset ID.
- Same rule applies to HF_TOKENIZER_NAME (local path vs. hub model).
"""

import os
import sys
from datasets import load_dataset, load_from_disk
from transformers import AutoTokenizer
from config import get_config


def _load_tokenizer(cfg):
    """Load tokenizer from either a local folder or the HF Hub."""
    name = getattr(cfg, "HF_TOKENIZER_NAME", "gpt2")

    if isinstance(name, str) and os.path.isdir(name):
        print(f"[TOK] ✅ Using local tokenizer path: {name}")
        tok = AutoTokenizer.from_pretrained(name, local_files_only=True)
    else:
        print(f"[TOK] 🌐 Loading tokenizer from HF Hub: {name}")
        tok = AutoTokenizer.from_pretrained(name)

    try:
        tok.model_max_length = int(1e9)
    except Exception:
        pass
    return tok


def _load_dataset(cfg):
    """Load dataset from local path or HF Hub, depending on HF_DATASET_NAME value."""
    name = getattr(cfg, "HF_DATASET_NAME", "lmsys/lmsys-chat-1m")
    split = getattr(cfg, "HF_DATASET_SPLIT", "train")
    streaming = bool(getattr(cfg, "HF_STREAMING", False))

    # Detect local directory path
    if isinstance(name, str) and os.path.isdir(name):
        print(f"[DATASET] ✅ Using local dataset path: {name}")
        try:
            return load_from_disk(name)
        except Exception as e:
            print(f"[DATASET] ⚠️ Failed to load local dataset ({e}), falling back to HF Hub.")

    # Otherwise treat as HF dataset repo id
    print(f"[DATASET] 🌐 Loading from Hugging Face Hub: {name}:{split} (streaming={streaming})")
    return load_dataset(name, split=split, streaming=streaming)


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
    """
    Stream or iterate LMSYS dataset and yield (prompt, reply_len_tokens).
    Automatically respects local paths and fallback to hub when needed.
    """

    cfg = get_config()

    # Apply overrides or config defaults
    dataset_name = dataset_name or getattr(cfg, "HF_DATASET_NAME", "lmsys/lmsys-chat-1m")
    split = split or getattr(cfg, "HF_DATASET_SPLIT", "train")
    tokenizer_name = tokenizer_name or getattr(cfg, "HF_TOKENIZER_NAME", "gpt2")
    if streaming is None:
        streaming = bool(getattr(cfg, "HF_STREAMING", False))

    print(f"[LMSYS] Preparing dataset='{dataset_name}:{split}', tokenizer='{tokenizer_name}'")

    tok = _load_tokenizer(cfg)
    ds = _load_dataset(cfg)

    # optional progress bar
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
        """Extract last user → next assistant pair, as in notebook Cell 2."""
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
        pair = _extract_pair_like_notebook(ex)
        if not pair:
            continue

        prompt, reply = pair
        in_len = len(tok.encode(prompt, add_special_tokens=False))
        out_len = len(tok.encode(reply, add_special_tokens=False))

        if verbose:
            print(
                f"\n[LMSYS] Example #{yielded+1}\n"
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

        if yielded >= max_n:
            break

    if pbar is not None:
        pbar.close()

    print(f"\n[LMSYS] Done. Yielded {yielded} (max_n={max_n}).")

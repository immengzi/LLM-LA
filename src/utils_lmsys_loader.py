# -*- coding: utf-8 -*-
"""
Minimal on-the-fly LMSYS loader with live logs.
Yields (prompt, out_tokens_estimated_from_reply) pairs for testing schedulers.

NEW:
- progress: bool = False   # show a tqdm progress bar during prepopulation
- progress_desc: str       # optional label for the bar
"""

import sys
from datasets import load_dataset
from transformers import AutoTokenizer

def iter_lmsys_pairs(
    dataset_name: str = "lmsys/lmsys-chat-1m",
    split: str = "train",
    tokenizer_name: str = "gpt2",
    max_n: int = 10000,
    streaming: bool = True,
    log_interval: int = 500,
    verbose: bool = False,
    *,
    progress: bool = False,
    progress_desc: str = "LMSYS",
):
    """
    Stream LMSYS dataset and yield (prompt, reply_len_tokens).
    Prints lightweight logs as it progresses.

    Args:
        dataset_name: HF dataset repo id
        split: dataset split
        tokenizer_name: HF tokenizer for token counting
        max_n: cap on yielded pairs
        streaming: use streaming loader (no local full materialization)
        log_interval: how often to print a simple counter when not verbose and no tqdm
        verbose: print each example (prompt + reply lengths)
        progress: if True, show a tqdm progress bar (best for prepopulation)
        progress_desc: label text for the progress bar
    """
    print(f"[LMSYS] Loading dataset '{dataset_name}:{split}' (streaming={streaming})...")
    tok = AutoTokenizer.from_pretrained(tokenizer_name)
    try:
        # Avoid model_max_length warnings when only counting tokens
        tok.model_max_length = int(1e9)
    except Exception:
        pass

    ds = load_dataset(dataset_name, split=split, streaming=streaming)

    # --- progress bar wiring (optional) ---
    # If we can detect total (non-streaming), use it; otherwise use max_n as the bar total.
    use_bar = bool(progress)
    pbar = None
    if use_bar:
        try:
            from tqdm.auto import tqdm  # type: ignore
            total = None
            if not streaming:
                try:
                    # Non-streaming datasets typically expose __len__
                    total = len(ds)  # may raise if not supported
                except Exception:
                    total = None
            if total is None:
                total = max_n if isinstance(max_n, int) and max_n > 0 else None
            pbar = tqdm(total=total, desc=progress_desc, unit="ex")
        except Exception:
            # tqdm not available; we’ll fall back to simple prints below
            pbar = None
            use_bar = False

    yielded = 0
    for i, ex in enumerate(ds):
        conv = ex.get("conversation") or ex.get("conversation_a")
        if not isinstance(conv, list) or len(conv) < 2:
            continue

        u0 = conv[0].get("content", "").strip() if isinstance(conv[0], dict) else ""
        r1 = conv[1].get("content", "").strip() if isinstance(conv[1], dict) else ""
        if not u0 or not r1:
            continue

        in_len = len(tok.encode(u0, add_special_tokens=False))
        out_len = len(tok.encode(r1, add_special_tokens=False))

        if verbose:
            print(
                f"\n[LMSYS] Example #{yielded+1}\n"
                f"Input  ({in_len} tokens):\n{u0}\n"
                f"{'-'*40}\n"
                f"Output ({out_len} tokens):\n{r1}\n"
                f"{'='*80}\n"
            )

        yield u0, out_len
        yielded += 1

        # progress feedback
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

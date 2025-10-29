"""
Minimal on-the-fly LMSYS loader with live logs.
Yields (prompt, out_tokens_estimated_from_reply) pairs for testing schedulers.

Keeps the same signature/flow as your original, but:
- Reads LMSYS fields correctly: `conversations` with `{"from": "...", "value": "..."}`.
- Uses the SAME pairing rule as your notebook Cell 2: last user -> next assistant.
- Falls back to your old keys if present (to avoid breaking on other sources).
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
        tok.model_max_length = int(1e9)  # avoid max_length warnings when just counting tokens
    except Exception:
        pass

    ds = load_dataset(dataset_name, split=split, streaming=streaming)

    # --- progress bar wiring (optional) ---
    use_bar = bool(progress)
    pbar = None
    if use_bar:
        try:
            from tqdm.auto import tqdm  # type: ignore
            total = None
            if not streaming:
                try:
                    total = len(ds)  # may raise if not supported
                except Exception:
                    total = None
            if total is None:
                total = max_n if isinstance(max_n, int) and max_n > 0 else None
            pbar = tqdm(total=total, desc=progress_desc, unit="ex")
        except Exception:
            pbar = None
            use_bar = False

    def _role(t):
        # Prefer LMSYS schema; fallback to your old keys if present
        return (t.get("from") or t.get("role") or "").lower()

    def _text(t):
        # Prefer LMSYS `value`; fall back to `content`
        return (t.get("value") or t.get("content") or "").strip()

    def _extract_pair_like_notebook(ex):
        """
        Match Cell 2 behavior:
        - Look for 'conversations' (LMSYS), else fallback to old 'conversation'/'conversation_a'
        - Take the LAST user turn, then the NEXT assistant turn.
        """
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

        # Yield SAME shape as before: (prompt, out_tokens_estimated_from_reply)
        yield prompt, out_len
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

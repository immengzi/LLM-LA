# sim/prompt_source.py
from __future__ import annotations
from typing import Iterator, List, Tuple, Optional
import os

from config import ClientConfig
from prompts import load_prompts_from_file, _iter_lmsys_pairs


def _load_tokenizer(tokenizer_name: str):
    from transformers import AutoTokenizer  # type: ignore
    if os.path.isdir(tokenizer_name):
        tok = AutoTokenizer.from_pretrained(tokenizer_name, local_files_only=True)
    else:
        tok = AutoTokenizer.from_pretrained(tokenizer_name)
    try:
        tok.model_max_length = int(1e9)
    except Exception:
        pass
    return tok


def iter_requests(cfg: ClientConfig) -> Iterator[Tuple[str, int, int]]:
    """
    Yields (prompt, in_tokens, out_tokens).
    Reuses LMSYS prompt extraction logic from prompts._iter_lmsys_pairs().
    """
    n = int(cfg.total_requests)

    if cfg.prompt_source == "file":
        prompts = load_prompts_from_file(cfg.file_prompts.path, cfg.file_prompts.variant, n)
        tok = None
        # If you want true in_tokens here, load a tokenizer. Otherwise keep 0.
        # We'll load tokenizer if hf_lmsys.tokenizer_name looks set.
        tname = getattr(cfg.hf_lmsys, "tokenizer_name", None)
        if isinstance(tname, str) and tname.strip():
            try:
                tok = _load_tokenizer(tname)
            except Exception:
                tok = None

        for p in prompts:
            in_tokens = len(tok.encode(p, add_special_tokens=False)) if tok is not None else 0
            # out_tokens: if target_output_tokens set, use it; else max_tokens
            if cfg.generation.target_output_tokens is not None:
                out_tokens = int(cfg.generation.target_output_tokens)
            else:
                out_tokens = int(cfg.generation.max_tokens)
            yield p, int(in_tokens), int(out_tokens)

    elif cfg.prompt_source == "hf-lmsys":
        tok = _load_tokenizer(cfg.hf_lmsys.tokenizer_name)
        # _iter_lmsys_pairs yields (prompt, out_len) — we compute in_len here with same tokenizer.
        for prompt, out_len in _iter_lmsys_pairs(cfg.hf_lmsys, max_n=n):
            in_len = len(tok.encode(prompt, add_special_tokens=False))
            # if generation sets a target, optionally override LMSYS out_len
            if cfg.generation.target_output_tokens is not None:
                out_tokens = int(cfg.generation.target_output_tokens)
            else:
                out_tokens = int(out_len)
            yield prompt, int(in_len), int(out_tokens)

    else:
        raise ValueError(f"Unknown prompt_source '{cfg.prompt_source}'")


def build_requests(cfg: ClientConfig) -> List[Tuple[str, int, int]]:
    return list(iter_requests(cfg))

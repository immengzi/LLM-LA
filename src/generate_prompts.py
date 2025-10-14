#!/usr/bin/env python3
# make_prompt_buckets_click.py
# Build length-controlled prompt buckets from real datasets with progress bars and streaming.
# Writes both "<name>.json" (prompts only) and "<name>-full.json" (rich records with metadata).
#
# Requirements:
#   pip install datasets transformers ftfy langdetect unidecode tqdm click certifi
#
# This version reads the default output directory from your config:
#   RouterConfig.PROMPTS_FOLDER_PATH (and honors ENV/YAML overrides via get_config()).
# It also:
#   - Defaults to --no-streaming to avoid TLS problems with cdn-lfs in constrained envs
#   - Auto-falls back to non-streaming on TLS/connection errors
#   - Auto-sets SSL_CERT_FILE to certifi.where() if no CA env is configured

import os
import re
import json
import time
import random
import logging
import itertools
from datetime import datetime, timezone
from typing import Iterable, List, Dict, Optional, Tuple, Callable, Any

import click
from tqdm import tqdm
from datasets import load_dataset, IterableDataset, Dataset, DatasetDict
from transformers import AutoTokenizer
from langdetect import detect
from unidecode import unidecode
import ftfy

# Pull defaults from your centralized config
from config import get_config

_CFG = get_config()
DEFAULT_OUT_DIR = _CFG.PROMPTS_FOLDER_PATH  # e.g., /home/saeid/llm-lb/prompts


# ------------------------------------------------------------------------------
# TLS helper: point Python at a modern CA bundle if user hasn't configured one
# ------------------------------------------------------------------------------
def _ensure_certifi_env():
    try:
        if not os.environ.get("SSL_CERT_FILE") and not os.environ.get(
            "REQUESTS_CA_BUNDLE"
        ):
            import certifi

            os.environ["SSL_CERT_FILE"] = certifi.where()
    except Exception:
        # Best effort; safe to ignore if certifi isn't installed
        pass


_ensure_certifi_env()

# ------------------------------------------------------------------------------
# COMMENT-FRIENDLY DEFAULT SOURCES
# You can comment/uncomment individual lines below.
# Each tuple is: (dataset_name_on_hub, extractor_name)
#
# Extractor names available: "oasst", "arena", "dolly", "alpaca"
# ------------------------------------------------------------------------------
SOURCES_DEFAULT: List[Tuple[str, str]] = [
    # ("OpenAssistant/oasst1",                "oasst"),   # OpenAssistant Conversations
    # ("lmsys/chatbot_arena_conversations",   "arena"),   # LMSYS Arena (first user turn)
    # ("databricks/databricks-dolly-15k",     "dolly"),   # Dolly instructions
    # ("tatsu-lab/alpaca",                    "alpaca"),  # Alpaca
    # ("yahma/alpaca-cleaned",                "alpaca"),  # Alpaca cleaned
    ("lmsys/lmsys-chat-1m", "arena"),  # Large; best with streaming
]


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger("prompts")


log = setup_logging()


# ------------------------------------------------------------------------------
# Text utils
# ------------------------------------------------------------------------------
def clean_prompt(s: str) -> str:
    s = ftfy.fix_text(s or "")
    s = s.strip()
    s = unidecode(s)
    s = re.sub(r"^(User|Human|Prompter)\s*:\s*", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def is_english(s: str) -> bool:
    s_clean = re.sub(r"[^\w\s\.\,\?\!\-\’\'\"]+", " ", s).strip()
    if not s_clean:
        return False
    try:
        if len(s_clean) < 20:
            return True  # langdetect is noisy on short texts
        return detect(s_clean) == "en"
    except Exception:
        return True


def unique_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ------------------------------------------------------------------------------
# Extractors
# ------------------------------------------------------------------------------
def extractor_oasst(ex: Dict) -> Iterable[str]:
    role = (ex.get("role") or ex.get("author_role") or ex.get("sender") or "").lower()
    for k in ("text", "content", "message", "prompt"):
        v = ex.get(k)
        if isinstance(v, str) and v.strip():
            if (not role) or role in {"prompter", "user", "human"}:
                yield v


def extractor_arena(ex: Dict) -> Iterable[str]:
    # lmsys: conversation_a / conversation_b each start with a user message
    if isinstance(ex.get("conversation_a"), list):
        for side in ("conversation_a", "conversation_b"):
            lst = ex.get(side) or []
            if lst and isinstance(lst[0], dict):
                yield lst[0].get("content") or ""
        return
    conv = ex.get("conversation") or ex.get("conversation_a")
    if isinstance(conv, list) and conv:
        first = conv[0]
        if isinstance(first, dict):
            yield first.get("content") or ""
        elif isinstance(first, str):
            yield first


def extractor_dolly(ex: Dict) -> Iterable[str]:
    inst = ex.get("instruction")
    if isinstance(inst, str) and inst.strip():
        yield inst


def extractor_alpaca(ex: Dict) -> Iterable[str]:
    inst = (ex.get("instruction") or "").strip()
    inp = (ex.get("input") or "").strip()
    if inst and inp:
        yield f"{inst}\n\nInput: {inp}"
    elif inst:
        yield inst


EXTRACTORS: Dict[str, Callable[[Dict], Iterable[str]]] = {
    "oasst": extractor_oasst,
    "arena": extractor_arena,
    "dolly": extractor_dolly,
    "alpaca": extractor_alpaca,
}


# ------------------------------------------------------------------------------
# Datasets loading helpers
# ------------------------------------------------------------------------------
def _take_n(it: Iterable, n: int) -> Iterable:
    return itertools.islice(it, n)


def _iter_split(split_obj, max_rows: int, pbar_desc: str) -> Iterable[Tuple[int, Dict]]:
    """
    Iterate examples paired with a 0-based row index, capped at max_rows.
    Works for streaming IterableDataset and in-memory Dataset.
    """
    if isinstance(split_obj, IterableDataset):
        for i, ex in enumerate(
            tqdm(_take_n(split_obj, max_rows), desc=pbar_desc, leave=False)
        ):
            yield i, ex
    elif isinstance(split_obj, Dataset):
        count = min(len(split_obj), max_rows)
        for i, ex in enumerate(
            tqdm(
                split_obj.select(range(count)), total=count, desc=pbar_desc, leave=False
            )
        ):
            yield i, ex
    else:
        for i, ex in enumerate(
            tqdm(_take_n(split_obj, max_rows), desc=pbar_desc, leave=False)
        ):
            yield i, ex


def _load_hf_dataset(name: str, streaming: bool) -> Optional[DatasetDict]:
    """
    Robust loader:
      - tries requested mode
      - if streaming True and we hit TLS/connection issues, auto-fallback to non-streaming
      - logs a concise hint about CA bundle env if both modes fail
    """
    # Import here to avoid hard dependency if not needed
    from requests.exceptions import (
        SSLError as ReqSSLError,
        ConnectionError as ReqConnError,
    )
    import urllib3

    # Build a tuple of urllib3 errors we care about
    urllib3_errors = []
    for n in ["SSLError", "MaxRetryError", "NewConnectionError"]:
        if hasattr(urllib3.exceptions, n):
            urllib3_errors.append(getattr(urllib3.exceptions, n))
    urllib3_errors = tuple(urllib3_errors) if urllib3_errors else tuple()

    try:
        return load_dataset(name, streaming=streaming)
    except (ReqSSLError, ReqConnError, urllib3_errors) as e:
        if streaming:
            log.warning(
                f"{name}: streaming failed due to TLS/connection ({e.__class__.__name__}). "
                f"Falling back to non-streaming…"
            )
            try:
                return load_dataset(name, streaming=False)
            except Exception as e2:
                log.error(f"{name}: non-streaming load also failed: {e2}")
                log.error(
                    "Hint: set SSL_CERT_FILE or REQUESTS_CA_BUNDLE to a valid CA bundle; "
                    "or keep --no-streaming (default)."
                )
                return None
        else:
            log.error(f"{name}: load failed ({e.__class__.__name__}): {e}")
            return None
    except Exception as e:
        if streaming:
            log.info(
                f"{name}: streaming failed ({e.__class__.__name__}: {e}); retrying non-streaming…"
            )
            try:
                return load_dataset(name, streaming=False)
            except Exception as e2:
                log.error(f"{name}: non-streaming load also failed: {e2}")
                return None
        log.error(f"{name}: load failed: {e}")
        return None


# ------------------------------------------------------------------------------
# Collection (returns rich records, not just strings)
# ------------------------------------------------------------------------------
def gather_sources(
    sources: List[Tuple[str, str]], max_source_rows: int, streaming: bool
) -> List[Dict[str, Any]]:
    """
    Returns a list of raw records:
      {
        "text": <raw prompt text>,
        "source_dataset": <name>,
        "source_split": <split>,
        "source_row_idx": <int>,
        "raw_excerpt": <trimmed JSON of original example>
      }
    """
    total: List[Dict[str, Any]] = []
    for name, extractor_key in sources:
        extractor = EXTRACTORS.get(extractor_key)
        if extractor is None:
            log.warning(f"Unknown extractor '{extractor_key}' for {name}; skipping.")
            continue

        log.info(f"Loading {name} (requested streaming={streaming})")
        ds = _load_hf_dataset(name, streaming)
        if ds is None:
            log.warning(f"Skipping {name} (failed to load).")
            continue

        for split_name in ds.keys():
            split = ds[split_name]
            log.info(f"  Split: {split_name}")
            added = 0
            for row_idx, ex in _iter_split(
                split, max_source_rows, f"{name}:{split_name}"
            ):
                for p in extractor(ex) or []:
                    if not p:
                        continue
                    try:
                        raw_excerpt = json.dumps(ex, ensure_ascii=False)
                        if len(raw_excerpt) > 1000:
                            raw_excerpt = raw_excerpt[:1000] + " …"
                    except Exception:
                        raw_excerpt = ""
                    total.append(
                        {
                            "text": p,
                            "source_dataset": name,
                            "source_split": split_name,
                            "source_row_idx": row_idx,
                            "raw_excerpt": raw_excerpt,
                        }
                    )
                    added += 1
                if added and (added % 5000 == 0):
                    log.info(
                        f"  {name}:{split_name} added={added} total_collected={len(total)}"
                    )

        log.info(f"{name}: cumulative collected = {len(total)}")
    return total


# ------------------------------------------------------------------------------
# Bucketing helpers (work on rich records)
# ------------------------------------------------------------------------------
def make_tokenizer(name: str):
    return AutoTokenizer.from_pretrained(name)


def count_tokens(tok, s: str) -> int:
    return len(tok.encode(s, add_special_tokens=False))


def filter_clean_records(
    records: List[Dict[str, Any]], min_chars: int, max_chars: int
) -> List[Dict[str, Any]]:
    log.info("Cleaning, language filtering...")
    out: List[Dict[str, Any]] = []
    for rec in tqdm(records, total=len(records), desc="clean"):
        s = clean_prompt(rec["text"])
        if not s:
            continue
        if len(s) < min_chars or len(s) > max_chars:
            continue
        if not is_english(s):
            continue
        new_rec = dict(rec)
        new_rec["prompt"] = s
        del new_rec["text"]
        out.append(new_rec)

    # Deduplicate by normalized prompt while keeping first occurrence & metadata
    log.info("Deduplicating...")
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for r in out:
        p = r["prompt"]
        if p in seen:
            continue
        seen.add(p)
        deduped.append(r)

    log.info(f"After filter & dedupe: {len(deduped)} records")
    return deduped


def bucket_records_by_tokens(
    records: List[Dict[str, Any]],
    tok,
    short_max: int,
    med_min: int,
    med_max: int,
    long_min: int,
    long_max: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    short, med, lng = [], [], []
    log.info("Tokenizing & bucketing by length...")
    for rec in tqdm(records, total=len(records), desc="tokenize"):
        t = count_tokens(tok, rec["prompt"])
        rec["tokens"] = t
        rec["chars"] = len(rec["prompt"])
        if 1 <= t <= short_max:
            short.append(rec)
        elif med_min <= t <= med_max:
            med.append(rec)
        elif long_min <= t <= long_max:
            lng.append(rec)
        # else ignore ultra-long for this run (add XL bucket if desired)
    log.info(f"Bucket sizes: short={len(short)}  medium={len(med)}  long={len(lng)}")
    return short, med, lng


def sample_records(
    lst: List[Dict[str, Any]], n: int, seed: int
) -> List[Dict[str, Any]]:
    if len(lst) <= n:
        return lst
    r = random.Random(seed)
    idxs = list(range(len(lst)))
    r.shuffle(idxs)
    return [lst[i] for i in idxs[:n]]


def write_simple_and_full(
    out_dir: str,
    name: str,
    records: List[Dict[str, Any]],
    bucket: str,
    tokenizer_name: str,
):
    """
    Writes:
      - <name>.json           -> simple list of prompts
      - <name>-full.json      -> rich objects with metadata, response placeholders
    """
    os.makedirs(out_dir, exist_ok=True)

    simple_path = os.path.join(out_dir, f"{name}.json")
    full_path = os.path.join(out_dir, f"{name}-full.json")

    # Simple file (list of prompts)
    with open(simple_path, "w", encoding="utf-8") as f:
        json.dump([r["prompt"] for r in records], f, ensure_ascii=False, indent=2)

    # Full file (list of dicts)
    created_at = datetime.now(timezone.utc).isoformat()
    full_records = []
    for i, r in enumerate(records, 1):
        full_records.append(
            {
                "id": f"{name}-{i:06d}",
                "prompt": r["prompt"],
                "bucket": bucket,
                "tokens": int(r.get("tokens", 0)),
                "chars": int(r.get("chars", 0)),
                "tokenizer": tokenizer_name,
                "source": {
                    "dataset": r.get("source_dataset"),
                    "split": r.get("source_split"),
                    "row_idx": r.get("source_row_idx"),
                },
                "raw_excerpt": r.get("raw_excerpt", ""),
                "request": {  # space for future per-request knobs (max_tokens, temperature, etc.)
                    "params": {}
                },
                "response": None,  # fill later
                "response_meta": {},  # fill later
                "created_at": created_at,
            }
        )

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(full_records, f, ensure_ascii=False, indent=2)

    log.info(f"Wrote {simple_path} ({len(records)} prompts)")
    log.info(f"Wrote {full_path} ({len(records)} records)")


# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--out-dir",
    default=DEFAULT_OUT_DIR,
    show_default=True,
    help="Output directory for JSON files (defaults to RouterConfig.PROMPTS_FOLDER_PATH).",
)
@click.option(
    "--target-per-bucket",
    default=1000,
    show_default=True,
    type=int,
    help="Prompts per bucket.",
)
@click.option(
    "--max-source-rows",
    default=300_000,
    show_default=True,
    type=int,
    help="Max rows per split per dataset.",
)
@click.option(
    "--tokenizer-name",
    default="gpt2",
    show_default=True,
    help="Tokenizer to approximate token counts.",
)
# DEFAULT = NO STREAMING to avoid TLS chain issues on cdn-lfs in constrained envs
@click.option(
    "--streaming/--no-streaming",
    default=False,
    show_default=True,
    help="Use HF streaming when available (may hit corporate TLS/SSL interception).",
)
@click.option(
    "--seed",
    default=13,
    show_default=True,
    type=int,
    help="Random seed for deterministic sampling.",
)
@click.option(
    "--short-max",
    default=12,
    show_default=True,
    type=int,
    help="Max tokens for 'short' bucket.",
)
@click.option(
    "--med-min",
    default=13,
    show_default=True,
    type=int,
    help="Min tokens for 'medium' bucket.",
)
@click.option(
    "--med-max",
    default=32,
    show_default=True,
    type=int,
    help="Max tokens for 'medium' bucket.",
)
@click.option(
    "--long-min",
    default=50,
    show_default=True,
    type=int,
    help="Min tokens for 'long' bucket.",
)
@click.option(
    "--long-max",
    default=130,
    show_default=True,
    type=int,
    help="Max tokens for 'long' bucket.",
)
@click.option(
    "--min-chars",
    default=4,
    show_default=True,
    type=int,
    help="Min characters in a prompt.",
)
@click.option(
    "--max-chars",
    default=4000,
    show_default=True,
    type=int,
    help="Max characters in a prompt.",
)
@click.option(
    "--source",
    "sources_cli",
    multiple=True,
    help=(
        "Repeatable. E.g. --source OpenAssistant/oasst1:oasst "
        "--source databricks/databricks-dolly-15k:dolly . "
        "If omitted, uses SOURCES_DEFAULT (top of file)."
    ),
)
def main(
    out_dir: str,
    target_per_bucket: int,
    max_source_rows: int,
    tokenizer_name: str,
    streaming: bool,
    seed: int,
    short_max: int,
    med_min: int,
    med_max: int,
    long_min: int,
    long_max: int,
    min_chars: int,
    max_chars: int,
    sources_cli: Tuple[str, ...],
):
    random.seed(seed)

    # Parse sources (CLI overrides default list)
    if sources_cli:
        sources: List[Tuple[str, str]] = []
        for spec in sources_cli:
            # format: dataset_name:extractor_key
            if ":" not in spec:
                raise click.BadParameter(
                    f"--source must be dataset:extractor (got '{spec}')"
                )
            ds_name, ext_key = spec.split(":", 1)
            sources.append((ds_name.strip(), ext_key.strip()))
    else:
        sources = list(SOURCES_DEFAULT)

    # Show chosen sources (easy to confirm / comment out)
    log.info("Using sources:")
    for name, ext in sources:
        log.info(f"  - {name:<40} via extractor '{ext}'")

    # Collect (rich records)
    t0 = time.time()
    log.info(f"Collecting raw prompts (streaming={streaming})...")
    raw_records = gather_sources(sources, max_source_rows, streaming)
    log.info(f"Collected raw records: {len(raw_records)}")

    # Clean + filter (preserve metadata)
    cleaned_records = filter_clean_records(raw_records, min_chars, max_chars)

    # Bucketing
    tok = make_tokenizer(tokenizer_name)
    short_recs, med_recs, long_recs = bucket_records_by_tokens(
        cleaned_records, tok, short_max, med_min, med_max, long_min, long_max
    )

    # Sample (deterministic within each bucket)
    short_s = sample_records(short_recs, target_per_bucket, seed)
    med_s = sample_records(med_recs, target_per_bucket, seed)
    long_s = sample_records(long_recs, target_per_bucket, seed)

    # Mix (balanced if possible)
    mix_pool = short_s + med_s + long_s
    random.Random(seed).shuffle(mix_pool)
    mix_s = sample_records(mix_pool, target_per_bucket, seed)

    # Ensure output dir exists (defaults to _CFG.PROMPTS_FOLDER_PATH)
    os.makedirs(out_dir, exist_ok=True)

    # Write simple + full sidecars
    write_simple_and_full(out_dir, "short", short_s, "short", tokenizer_name)
    write_simple_and_full(out_dir, "medium", med_s, "medium", tokenizer_name)
    write_simple_and_full(out_dir, "long", long_s, "long", tokenizer_name)
    write_simple_and_full(out_dir, "mix", mix_s, "mix", tokenizer_name)

    # Also (optional) write the single default file name from config, if you want:
    #   This mirrors your RouterConfig.PROMPTS_FILE (e.g., prompts/prompts.json)
    #   and just writes the 'mix' prompts there for convenience.
    try:
        with open(_CFG.PROMPTS_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump([r["prompt"] for r in mix_s], f, ensure_ascii=False, indent=2)
        log.info(f"Wrote {_CFG.PROMPTS_FILE_PATH} (mirror of mix.json)")
    except Exception as e:
        log.warning(f"Could not write PROMPTS_FILE {_CFG.PROMPTS_FILE_PATH}: {e}")

    log.info(f"Done in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
router_log_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Shared, BooM-proof source of truth for the router's per-request routing
decision (which pod served a request, plus prefix/KV-hit data).

The router records every completed request server-side in its ``/latency_log``
ring buffer, independent of what survives a BooM/proxy hop. This module polls
that endpoint and:

  * appends each new record to ``router_logs.json`` (NDJSON: the router truth);
  * maintains a thread-safe ``req_id -> routing fields`` map for *live*
    best-effort enrichment of the client's ``logs.json``;
  * exposes ``lookup(req_id)`` (handles the ``chatcmpl-`` prefix) and
    ``start()`` / ``stop()``.

Both the load client (``main.py``) and the external observer
(``prod_latency_collector.py``) use this same collector, so they emit identical
artifacts regardless of whether traffic flows through BooM.

Join key: the router stores a bare 32-hex ``rid``; the client stores
``chatcmpl-<rid>`` (OpenAI shim) or the bare hex (``/enqueue``). We match by the
bare hex id.
"""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests


# Fields the router adds to each /latency_log entry that describe the routing
# decision (everything except endpoint, which is normalised separately).
_KV_FIELDS = (
    "kv_hits_len",
    "total_blocks",
    "matched_tokens",
    "kv_hit",
    "affinity_key",
    "block_hashes",
)


def make_router_log_url(router_url: str) -> str:
    """Return the ``/latency_log`` URL for a router base URL.

    Strips any path/query so e.g. ``http://host:30080/v1`` -> ``http://host:30080``.
    """
    u = urlparse(router_url if "://" in router_url else f"http://{router_url}")
    base = urlunparse((u.scheme or "http", u.netloc, "", "", "", ""))
    return f"{base.rstrip('/')}/latency_log"


def normalize_rid(req_id: Optional[str]) -> str:
    """Reduce a client/router request id to the bare hex join key."""
    if not req_id:
        return ""
    rid = str(req_id)
    if rid.startswith("chatcmpl-"):
        rid = rid[len("chatcmpl-"):]
    return rid


def routing_fields_from_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Extract just the enrichment fields (endpoint + kv/prefix) from a ring entry."""
    out: Dict[str, Any] = {}
    ep = entry.get("endpoint")
    if ep is not None:
        out["endpoint_id"] = ep
    for k in _KV_FIELDS:
        if k in entry:
            out[k] = entry[k]
    return out


def router_entry_to_record(entry: Dict[str, Any], idx: int) -> Dict[str, Any]:
    """Map a /latency_log ring entry to a client-shaped log record.

    Mirrors the schema written by load_runner / prod_latency_collector and adds
    the prefix/KV fields when present.
    """
    rid = entry.get("rid", "")
    ts = entry.get("ts", 0)
    e2e_s = (entry.get("e2e_ms") or 0) / 1000.0
    t_start = entry.get("t_start")
    t0 = t_start if t_start is not None else ts

    record: Dict[str, Any] = {
        "idx": idx,
        "req_id": rid,
        "t0_wall": t0,
        "t1_wall": ts,
        "end_to_end_s": e2e_s,
        "model_latency_s": e2e_s,
        "finish_reason": entry.get("finish_reason", "stop"),
        "endpoint_id": entry.get("endpoint"),
        "streaming": entry.get("stream", False),
        "prompt_tokens": entry.get("prompt_tokens", 0),
        "completion_tokens": entry.get("completion_tokens", 0),
    }

    ttft_ms = entry.get("ttft_ms")
    if ttft_ms is not None:
        record["ttft_s"] = ttft_ms / 1000.0
    tpot_ms = entry.get("tpot_avg_ms")
    if tpot_ms is not None:
        record["tpot_avg_s"] = tpot_ms / 1000.0
    model = entry.get("model")
    if model:
        record["model"] = model

    for k in _KV_FIELDS:
        if k in entry:
            record[k] = entry[k]

    return record


class RouterLogCollector:
    """Background poller for the router ``/latency_log`` ring buffer."""

    def __init__(
        self,
        router_url: str,
        out_path: str | Path,
        *,
        poll_interval_s: float = 2.0,
        batch_size: int = 2000,
        dedup_max: int = 50_000,
        request_timeout_s: float = 10.0,
    ):
        self._latency_url = f"{make_router_log_url(router_url)}?last={int(batch_size)}"
        self._out_path = Path(out_path)
        self._poll_interval_s = float(poll_interval_s)
        self._request_timeout_s = float(request_timeout_s)
        self._dedup_max = int(dedup_max)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._lock = threading.Lock()
        self._lookup: Dict[str, Dict[str, Any]] = {}
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self._count = 0
        self._fh = None  # type: Optional[Any]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._out_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._out_path, "a", encoding="utf-8")
        self._thread = threading.Thread(
            target=self._run, name="router-log-collector", daemon=True
        )
        self._thread.start()

    def stop(self, *, final_poll: bool = True) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._poll_interval_s + 5.0)
            self._thread = None
        # One last poll to capture records that completed after the final loop
        # iteration (best-effort; the end-of-run join is still authoritative).
        if final_poll:
            try:
                self._poll_once()
            except Exception:
                pass
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None

    # ------------------------------------------------------------------
    # Live lookup
    # ------------------------------------------------------------------
    def lookup(self, req_id: str) -> Optional[Dict[str, Any]]:
        """Return routing fields for a request id, or None if not seen yet."""
        rid = normalize_rid(req_id)
        if not rid:
            return None
        with self._lock:
            info = self._lookup.get(rid)
            return dict(info) if info else None

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception as e:  # never let polling wedge the run
                print(f"[router-log] poll error: {e}")
            self._stop.wait(self._poll_interval_s)

    def _poll_once(self) -> int:
        resp = requests.get(self._latency_url, timeout=self._request_timeout_s)
        resp.raise_for_status()
        entries: List[Dict[str, Any]] = resp.json()
        return self._ingest(entries)

    def _ingest(self, entries: List[Dict[str, Any]]) -> int:
        new_count = 0
        for entry in entries:
            rid = entry.get("rid", "")
            ts = entry.get("ts", 0)
            dedup_key = f"{rid}:{ts}"
            with self._lock:
                if dedup_key in self._seen:
                    continue
                self._seen[dedup_key] = None
                while len(self._seen) > self._dedup_max:
                    self._seen.popitem(last=False)
                idx = self._count
                self._count += 1
                # latest decision wins for the live lookup map
                if rid:
                    self._lookup[normalize_rid(rid)] = routing_fields_from_entry(entry)

            record = router_entry_to_record(entry, idx)
            self._write(record)
            new_count += 1
        return new_count

    def _write(self, record: Dict[str, Any]) -> None:
        if self._fh is None:
            return
        try:
            line = json.dumps(record, ensure_ascii=False, default=str)
        except Exception:
            return
        with self._lock:
            if self._fh is None:
                return
            try:
                self._fh.write(line + "\n")
                self._fh.flush()
            except Exception:
                pass


class EnrichingLogger:
    """Wraps an ExperimentLogger and live-enriches each record from a collector.

    Duck-typed drop-in: only ``log_request`` is needed by the load runner. The
    underlying logger's lifecycle (open/close) is still owned by the caller.
    Enrichment is best-effort; the end-of-run join is authoritative.
    """

    def __init__(self, base_logger: Any, collector: "RouterLogCollector"):
        self._base = base_logger
        self._collector = collector

    def log_request(self, record: Dict[str, Any]) -> None:
        try:
            fields = self._collector.lookup(record.get("req_id"))
            if fields:
                for k, v in fields.items():
                    # Don't clobber an endpoint the client already resolved;
                    # always add kv/prefix fields the client cannot know.
                    if k == "endpoint_id" and record.get("endpoint_id"):
                        continue
                    record.setdefault(k, v)
        except Exception:
            pass
        self._base.log_request(record)


# ============================================================
# End-of-run authoritative join
# ============================================================

def _read_router_logs(router_logs_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load router_logs.json (NDJSON) into a ``hexrid -> routing fields`` map."""
    by_rid: Dict[str, Dict[str, Any]] = {}
    if not router_logs_path.is_file():
        return by_rid
    with router_logs_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except Exception:
                continue
            rid = normalize_rid(entry.get("req_id") or entry.get("rid"))
            if not rid:
                continue
            fields: Dict[str, Any] = {}
            if entry.get("endpoint_id") is not None:
                fields["endpoint_id"] = entry["endpoint_id"]
            for k in _KV_FIELDS:
                if k in entry:
                    fields[k] = entry[k]
            by_rid[rid] = fields
    return by_rid


def join_logs_with_router(
    logs_path: str | Path,
    router_logs_path: str | Path,
    *,
    overwrite_endpoint: bool = True,
) -> Dict[str, int]:
    """Rewrite ``logs.json`` in place, attaching router endpoint + kv fields.

    Authoritative: covers any record the live path missed (poller lag, ring cap).
    Returns simple stats: ``{matched, total, missing}``.
    """
    logs_path = Path(logs_path)
    router_logs_path = Path(router_logs_path)
    by_rid = _read_router_logs(router_logs_path)
    if not logs_path.is_file():
        return {"matched": 0, "total": 0, "missing": 0}

    tmp_path = logs_path.with_suffix(logs_path.suffix + ".tmp")
    matched = total = missing = 0
    with logs_path.open("r", encoding="utf-8") as fin, \
            tmp_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            total += 1
            try:
                rec = json.loads(line)
            except Exception:
                fout.write(line + "\n")
                continue
            rid = normalize_rid(rec.get("req_id"))
            fields = by_rid.get(rid)
            if fields:
                matched += 1
                for k, v in fields.items():
                    if k == "endpoint_id" and not overwrite_endpoint and rec.get("endpoint_id"):
                        continue
                    rec[k] = v
            else:
                missing += 1
            fout.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    tmp_path.replace(logs_path)
    return {"matched": matched, "total": total, "missing": missing}


def summarize_routing(logs_path: str | Path) -> Dict[str, Any]:
    """Build per-endpoint + affinity-stickiness rollups from joined logs.

    Stickiness groups multi-turn requests by ``conversation_id`` when present,
    else by the router-provided ``affinity_key`` (so prod-observed runs work too).
    """
    logs_path = Path(logs_path)
    per_ep: Dict[str, Dict[str, float]] = {}
    convs: Dict[str, set] = {}
    total = 0
    kv_hits = 0
    kv_known = 0

    if not logs_path.is_file():
        return {}

    with logs_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            total += 1
            ep = rec.get("endpoint_id") or "_unknown_"
            s = per_ep.setdefault(ep, {"requests": 0, "kv_hit": 0, "matched_tokens": 0})
            s["requests"] += 1
            if "kv_hit" in rec:
                kv_known += 1
                if rec.get("kv_hit"):
                    kv_hits += 1
                    s["kv_hit"] += 1
            s["matched_tokens"] += int(rec.get("matched_tokens", 0) or 0)

            group = rec.get("conversation_id") or rec.get("affinity_key")
            if group is not None and ep != "_unknown_":
                convs.setdefault(str(group), set()).add(ep)

    multi = {g: eps for g, eps in convs.items() if g}
    sticky = sum(1 for eps in multi.values() if len(eps) == 1)
    distinct_avg = (
        sum(len(eps) for eps in multi.values()) / len(multi) if multi else float("nan")
    )

    return {
        "total_requests": total,
        "frac_kv_hit": round(kv_hits / kv_known, 4) if kv_known else None,
        "per_endpoint": [
            {
                "endpoint": ep,
                "requests": int(s["requests"]),
                "kv_hit_frac": round(s["kv_hit"] / s["requests"], 4) if s["requests"] else None,
                "matched_tokens": int(s["matched_tokens"]),
            }
            for ep, s in sorted(per_ep.items())
        ],
        "affinity": {
            "groups": len(multi),
            "sticky_groups": sticky,
            "sticky_frac": round(sticky / len(multi), 4) if multi else None,
            "avg_distinct_endpoints": round(distinct_avg, 4) if multi else None,
        },
    }

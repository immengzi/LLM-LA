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
    # Raw /latency_log ring entries carry "endpoint"; already-persisted
    # router_logs.json records carry the renamed "endpoint_id". Accept both so
    # this works for live lookups and the end-of-run content join alike.
    ep = entry.get("endpoint")
    if ep is None:
        ep = entry.get("endpoint_id")
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

    # Passthrough the full request body when the router emitted it
    # (ROUTER_LOG_REQUEST_BODY). Already bounded/truncated server-side.
    if "request_body" in entry:
        record["request_body"] = entry["request_body"]

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
        lean_out_path: Optional[str | Path] = None,
        lean_drop_fields: tuple = ("request_body", "block_hashes"),
    ):
        self._latency_url = f"{make_router_log_url(router_url)}?last={int(batch_size)}"
        self._out_path = Path(out_path)
        # Optional second, LIVE lean stream: same records written to out_path but
        # with the bulky fields (request body + block hashes) stripped. Lets an
        # observer keep a lean logs.json alongside a full logs_full.json without a
        # shutdown rewrite (robust to hard kills). None -> single-stream (default).
        self._lean_out_path = Path(lean_out_path) if lean_out_path else None
        self._lean_drop_fields = tuple(lean_drop_fields)
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
        self._lean_fh = None  # type: Optional[Any]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread is not None:
            return
        self._out_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._out_path, "a", encoding="utf-8")
        if self._lean_out_path is not None:
            self._lean_out_path.parent.mkdir(parents=True, exist_ok=True)
            self._lean_fh = open(self._lean_out_path, "a", encoding="utf-8")
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
        if self._lean_fh is not None:
            try:
                self._lean_fh.close()
            except Exception:
                pass
            self._lean_fh = None

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
        if self._fh is None and self._lean_fh is None:
            return
        try:
            line = json.dumps(record, ensure_ascii=False, default=str)
        except Exception:
            line = None
        lean_line = None
        if self._lean_fh is not None:
            lean_rec = {k: v for k, v in record.items() if k not in self._lean_drop_fields}
            try:
                lean_line = json.dumps(lean_rec, ensure_ascii=False, default=str)
            except Exception:
                lean_line = None
        with self._lock:
            if self._fh is not None and line is not None:
                try:
                    self._fh.write(line + "\n")
                    self._fh.flush()
                except Exception:
                    pass
            if self._lean_fh is not None and lean_line is not None:
                try:
                    self._lean_fh.write(lean_line + "\n")
                    self._lean_fh.flush()
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
    drop_fields: Optional[set] = None,
) -> Dict[str, int]:
    """Rewrite ``logs.json`` in place, attaching router endpoint + kv fields.

    Authoritative: covers any record the live path missed (poller lag, ring cap).
    ``drop_fields`` names router fields to *omit* from ``logs.json`` (e.g.
    ``{"block_hashes"}`` to keep it lean while the full list still rides
    ``logs_full.json``). Returns simple stats: ``{matched, total, missing}``.
    """
    logs_path = Path(logs_path)
    router_logs_path = Path(router_logs_path)
    drop_fields = drop_fields or set()
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
                    if k in drop_fields:
                        continue
                    if k == "endpoint_id" and not overwrite_endpoint and rec.get("endpoint_id"):
                        continue
                    rec[k] = v
            else:
                missing += 1
            # Drop any explicitly-excluded fields so a lean logs.json can omit
            # bulky data (e.g. block_hashes / request_body) that instead rides
            # logs_full.json. Callers that want the full record pass no drop_fields.
            for k in drop_fields:
                rec.pop(k, None)
            fout.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    tmp_path.replace(logs_path)
    return {"matched": matched, "total": total, "missing": missing}


def _read_router_logs_full(router_logs_path: Path) -> Dict[str, Dict[str, Any]]:
    """Like ``_read_router_logs`` but also carries ``request_body`` + block hashes.

    Used to build ``logs_full.json`` -- the superset that keeps the full request
    body and prefix block-hash list the lean ``logs.json`` withholds.
    """
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
            if "request_body" in entry:
                fields["request_body"] = entry["request_body"]
            by_rid[rid] = fields
    return by_rid


def write_logs_full(
    logs_path: str | Path,
    router_logs_path: str | Path,
    full_path: str | Path,
    *,
    overwrite_endpoint: bool = True,
) -> Dict[str, int]:
    """Write ``logs_full.json`` = every ``logs.json`` record PLUS the full request
    body and prefix block-hash list, pulled from the router truth.

    ``logs.json`` is left untouched (lean / body-free); this is its superset
    twin. Requires the router to have emitted bodies (ROUTER_LOG_REQUEST_BODY)
    and hashes (ROUTER_LOG_BLOCK_HASHES) into ``router_logs.json``.
    Returns ``{matched, total, missing}``.
    """
    logs_path = Path(logs_path)
    router_logs_path = Path(router_logs_path)
    full_path = Path(full_path)
    if not logs_path.is_file():
        return {"matched": 0, "total": 0, "missing": 0}
    by_rid = _read_router_logs_full(router_logs_path)

    tmp_path = full_path.with_suffix(full_path.suffix + ".tmp")
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

    tmp_path.replace(full_path)
    return {"matched": matched, "total": total, "missing": missing}


def _last_user_text(entry: Dict[str, Any]) -> Optional[str]:
    """Return the last user-message text from a ring entry's request_body.

    Requires the router to have emitted request bodies (ROUTER_LOG_REQUEST_BODY).
    The router normalises content-block arrays into plain strings, so content is
    usually a str; we still tolerate the list form defensively.
    """
    rb = entry.get("request_body")
    if not isinstance(rb, dict):
        return None
    msgs = rb.get("messages")
    if not isinstance(msgs, list):
        return None
    for m in reversed(msgs):
        if not isinstance(m, dict) or m.get("role") != "user":
            continue
        c = m.get("content")
        if isinstance(c, str):
            return c
        if isinstance(c, list):
            parts = [
                b.get("text", "")
                for b in c
                if isinstance(b, dict) and b.get("type") == "text"
            ]
            return "\n".join(p for p in parts if p)
    return None


def _norm_ws(s: Any) -> str:
    """Whitespace-insensitive normalisation for robust substring matching."""
    return "".join(str(s).split())


# Claude per-turn client fields worth carrying onto the router-truth record as
# "extra info for the claude" (best-effort, attached by content match).
_CLAUDE_EXTRA_FIELDS = (
    "conversation_id",
    "user_id",
    "turn_idx",
    "num_turns",
    "session_id",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
)


def build_claude_logs_from_router(
    logs_path: str | Path,
    router_logs_path: str | Path,
    *,
    client_logs_out: str = "claude_client_logs.json",
    drop_request_body: bool = True,
    core_len: int = 160,
) -> Dict[str, int]:
    """Make the claude ``logs.json`` identical to prod_latency_collector output.

    The claude CLI talks straight to the BooM gateway and never surfaces the
    router request id, so ``logs.json`` is written live with client-side turn
    records only. This rebuilds ``logs.json`` straight from the router
    ``/latency_log`` truth (``router_logs.json``) -- i.e. the exact records
    ``prod_latency_collector.py`` emits, with endpoint + kv_hits/total_blocks/
    matched_tokens/kv_hit/block_hashes. The live client turn records are
    preserved to ``<client_logs_out>`` in the same directory, and the claude
    per-turn extras (conversation_id/user_id/turn_idx/cache tokens/session_id)
    are attached onto the router record by content match where available.

    Request bodies (captured for the content match) are stripped from
    ``logs.json`` by default so it stays as clean as the older client logs; they
    remain in ``router_logs.json``.

    Returns ``{router_records, client_records, extras_attached}``.
    """
    logs_path = Path(logs_path)
    router_logs_path = Path(router_logs_path)

    # 1) Preserve the live client turn records, and index them for extras.
    client_records: List[Dict[str, Any]] = []
    if logs_path.is_file():
        with logs_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    client_records.append(json.loads(line))
                except Exception:
                    pass
    if client_records:
        client_out = logs_path.parent / client_logs_out
        try:
            with client_out.open("w", encoding="utf-8") as fout:
                for rec in client_records:
                    fout.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            print(f"[router-log] WARN: could not write {client_out}: {e}")

    # Index client turns by whitespace-insensitive prompt signature.
    client_index: List[Dict[str, Any]] = []
    for rec in client_records:
        prompt = rec.get("prompt")
        if not prompt:
            continue
        core = _norm_ws(prompt)[:core_len]
        if not core:
            continue
        completion = float(rec.get("actual_send_ts_wall") or 0.0) + float(
            rec.get("end_to_end_s") or 0.0
        )
        client_index.append({
            "core": core,
            "completion": completion,
            "extras": {k: rec[k] for k in _CLAUDE_EXTRA_FIELDS if k in rec},
            "client_wall_s": rec.get("end_to_end_s"),
            "claimed": False,
        })

    # 2) Read router-truth records (already in prod_latency_collector schema).
    router_records: List[Dict[str, Any]] = []
    seen: set = set()
    if router_logs_path.is_file():
        with router_logs_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                rid = e.get("req_id") or e.get("rid") or ""
                dedup = f"{rid}:{e.get('t1_wall') or e.get('ts')}"
                if dedup in seen:
                    continue
                seen.add(dedup)
                router_records.append(e)

    # 3) Attach claude extras onto each router record by content match.
    extras_attached = 0
    for e in router_records:
        lu = _last_user_text(e)
        lu_norm = _norm_ws(lu) if lu else ""
        e["transport"] = "claude"
        if lu_norm and client_index:
            comp = float(e.get("t1_wall") or e.get("ts") or 0.0)
            best = None
            best_dt = None
            for c in client_index:
                if c["claimed"]:
                    continue
                if c["core"] in lu_norm:
                    dt = abs(float(c["completion"]) - comp)
                    if best is None or dt < best_dt:
                        best = c
                        best_dt = dt
            if best is not None:
                best["claimed"] = True
                for k, v in best["extras"].items():
                    e[k] = v
                if best.get("client_wall_s") is not None:
                    e["client_wall_s"] = best["client_wall_s"]
                extras_attached += 1
        if drop_request_body:
            e.pop("request_body", None)

    # 4) Write logs.json = router-truth records (prod_latency_collector style).
    tmp_path = logs_path.with_suffix(logs_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fout:
        for e in router_records:
            fout.write(json.dumps(e, ensure_ascii=False, default=str) + "\n")
    tmp_path.replace(logs_path)

    return {
        "router_records": len(router_records),
        "client_records": len(client_records),
        "extras_attached": extras_attached,
    }


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

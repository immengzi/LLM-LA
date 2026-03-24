#!/usr/bin/env python3
"""
vllm_log_viewer.py — Real-time web dashboard for vllm_monitor JSONL logs.

Serves a live-updating dashboard at http://localhost:9999
showing the latest metrics from all configured instances.

Usage:
    python vllm_log_viewer.py
    python vllm_log_viewer.py --config instances.yaml --port 9999
"""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DEFAULT_CONFIG = Path(__file__).parent / "instances.yaml"
DEFAULT_PORT   = 9999


def load_config(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def read_last_record(jsonl_path: Path) -> Optional[Dict[str, Any]]:
    """Read the last valid JSONL record from a metrics file."""
    if not jsonl_path.exists():
        return None
    try:
        with open(jsonl_path, "rb") as f:
            # Seek backwards to find last non-empty line efficiently
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return None
            buf = b""
            pos = size
            while pos > 0:
                chunk = min(512, pos)
                pos -= chunk
                f.seek(pos)
                buf = f.read(chunk) + buf
                lines = buf.split(b"\n")
                # Try lines from the end
                for line in reversed(lines):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        if rec.get("samples"):
                            return rec
                    except json.JSONDecodeError:
                        continue
                if pos == 0:
                    break
    except OSError:
        return None
    return None


def get_instance_metrics(log_dir: Path, name: str) -> Dict[str, Any]:
    jsonl_path = log_dir / name / "metrics.jsonl"
    rec = read_last_record(jsonl_path)
    if rec is None:
        return {"name": name, "status": "no_data"}

    sample = rec["samples"][0] if rec.get("samples") else {}
    ts = rec.get("ts", "")

    # Age of last record in seconds
    age_sec = None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        age_sec = (datetime.now(tz=timezone.utc) - dt).total_seconds()
    except Exception:
        pass

    def fmt(v, decimals=3):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return round(v, decimals)

    return {
        "name":                    name,
        "status":                  "stale" if (age_sec and age_sec > 30) else "ok",
        "ts":                      ts,
        "age_sec":                 round(age_sec, 1) if age_sec is not None else None,
        "requests_running":        fmt(sample.get("requests_running"), 0),
        "requests_waiting":        fmt(sample.get("requests_waiting"), 0),
        "gpu_kv_cache_usage_frac": fmt(sample.get("gpu_kv_cache_usage_frac"), 3),
        "cpu_kv_cache_usage_frac": fmt(sample.get("cpu_kv_cache_usage_frac"), 3),
        "prefix_cache_hit_rate":   fmt(sample.get("prefix_cache_hit_rate"), 3),
        "prefix_cache_hit_rate_cumulative": fmt(sample.get("prefix_cache_hit_rate_cumulative"), 3),
        "external_prefix_cache_hit_rate": fmt(sample.get("external_prefix_cache_hit_rate"), 3),
        "ttft_avg":                fmt(sample.get("ttft_seconds_avg"), 3),
        "ttft_p99":                fmt(sample.get("ttft_seconds_avg__p99"), 3),
        "e2e_avg":                 fmt(sample.get("e2e_latency_seconds_avg"), 3),
        "e2e_p99":                 fmt(sample.get("e2e_latency_seconds_avg__p99"), 3),
        "queue_avg":               fmt(sample.get("queue_time_seconds_avg"), 3),
        "queue_p99":               fmt(sample.get("queue_time_seconds_avg__p99"), 3),
        "tpot_avg":                fmt(sample.get("tpot_seconds_avg"), 3),
        "tpot_p99":                fmt(sample.get("tpot_seconds_avg__p99"), 3),
        "gen_tokens_per_sec":      fmt(sample.get("gen_tokens_per_sec"), 1),
        "prefill_tokens_per_sec":  fmt(sample.get("prefill_tokens_per_sec"), 1),
        "request_success_per_sec": fmt(sample.get("request_success_per_sec"), 3),
        "preemptions_per_sec":     fmt(sample.get("preemptions_per_sec"), 3),
    }


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>vLLM Monitor</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; font-size: 13px;
       background: #f5f5f5; color: #1a1a1a; }
header { background: #1a1a1a; color: #fff; padding: 12px 20px;
         display: flex; align-items: center; gap: 16px; }
header h1 { font-size: 15px; font-weight: 500; }
#last-updated { font-size: 12px; color: #aaa; margin-left: auto; }
#error-banner { display: none; background: #fdecea; color: #b71c1c;
                padding: 8px 20px; font-size: 12px; }
.grid { display: grid;
        grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
        gap: 12px; padding: 16px; }
.card { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px;
        overflow: hidden; }
.card-header { padding: 10px 14px; border-bottom: 1px solid #f0f0f0;
               display: flex; align-items: center; gap: 8px; }
.card-header .name { font-weight: 500; font-size: 13px; flex: 1;
                     white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.dot-ok     { background: #4caf50; }
.dot-stale  { background: #ff9800; }
.dot-nodata { background: #bdbdbd; }
.age { font-size: 11px; color: #999; }
.card-body { padding: 10px 14px; display: grid;
             grid-template-columns: 1fr 1fr; gap: 4px 12px; }
.row { display: contents; }
.label { color: #757575; font-size: 12px; padding: 2px 0; }
.value { font-size: 12px; font-weight: 500; padding: 2px 0;
         text-align: right; }
.value.warn { color: #e65100; }
.value.good { color: #2e7d32; }
.divider { grid-column: 1 / -1; border-top: 1px solid #f5f5f5;
           margin: 4px 0; }
.section-label { grid-column: 1 / -1; font-size: 11px; color: #bdbdbd;
                 text-transform: uppercase; letter-spacing: 0.05em;
                 padding-top: 4px; }
.bar-wrap { grid-column: 1 / -1; display: flex; align-items: center; gap: 8px; }
.bar-bg { flex: 1; height: 4px; background: #f0f0f0; border-radius: 2px; }
.bar-fill { height: 100%; border-radius: 2px; transition: width 0.4s; }
.bar-label { font-size: 11px; color: #757575; min-width: 36px; text-align: right; }
</style>
</head>
<body>
<header>
  <h1>vLLM Monitor</h1>
  <span id="instance-count" style="font-size:12px;color:#aaa;"></span>
  <span id="last-updated"></span>
</header>
<div id="error-banner"></div>
<div id="grid" class="grid"></div>

<script>
const INSTANCES = __INSTANCES__;
const LOG_DIR   = "__LOG_DIR__";

function pct(v) {
  if (v == null) return "—";
  return (v * 100).toFixed(1) + "%";
}
function ms(v) {
  if (v == null) return "—";
  return (v * 1000).toFixed(0) + " ms";
}
function num(v, dec=1) {
  if (v == null) return "—";
  return (+v).toFixed(dec);
}
function ageStr(s) {
  if (s == null) return "";
  if (s < 60) return s.toFixed(0) + "s ago";
  return (s/60).toFixed(1) + "m ago";
}

function dotClass(m) {
  if (m.status === "no_data") return "dot-nodata";
  if (m.status === "stale")   return "dot-stale";
  return "dot-ok";
}

function bar(pctVal, color) {
  const w = pctVal == null ? 0 : Math.min(100, pctVal * 100);
  return `<div class="bar-wrap">
    <div class="bar-bg"><div class="bar-fill" style="width:${w.toFixed(1)}%;background:${color};"></div></div>
    <span class="bar-label">${pct(pctVal)}</span>
  </div>`;
}

function warnClass(v, warn, bad) {
  if (v == null) return "";
  if (v >= bad)  return "warn";
  if (v <= warn) return "good";
  return "";
}

function renderCard(m) {
  const hitRate = m.prefix_cache_hit_rate ?? m.prefix_cache_hit_rate_cumulative;
  const extHit  = m.external_prefix_cache_hit_rate;

  return `<div class="card" id="card-${m.name}">
  <div class="card-header">
    <div class="dot ${dotClass(m)}"></div>
    <span class="name" title="${m.name}">${m.name}</span>
    <span class="age">${ageStr(m.age_sec)}</span>
  </div>
  ${m.status === "no_data" ? '<div style="padding:12px 14px;color:#bdbdbd;font-size:12px;">no data yet</div>' : `
  <div class="card-body">
    <span class="section-label">queue</span>
    <span class="label">running</span><span class="value">${num(m.requests_running, 0)}</span>
    <span class="label">waiting</span>
    <span class="value ${m.requests_waiting > 5 ? 'warn' : ''}">${num(m.requests_waiting, 0)}</span>

    <div class="divider"></div>
    <span class="section-label">latency</span>
    <span class="label">TTFT avg / p99</span>
    <span class="value">${ms(m.ttft_avg)} / ${ms(m.ttft_p99)}</span>
    <span class="label">E2E avg / p99</span>
    <span class="value">${ms(m.e2e_avg)} / ${ms(m.e2e_p99)}</span>
    <span class="label">queue avg / p99</span>
    <span class="value">${ms(m.queue_avg)} / ${ms(m.queue_p99)}</span>
    <span class="label">TPOT avg / p99</span>
    <span class="value">${ms(m.tpot_avg)} / ${ms(m.tpot_p99)}</span>

    <div class="divider"></div>
    <span class="section-label">kv cache</span>
    ${bar(m.gpu_kv_cache_usage_frac, "#1976d2")}
    ${m.cpu_kv_cache_usage_frac != null ? bar(m.cpu_kv_cache_usage_frac, "#7b1fa2") : ""}

    <div class="divider"></div>
    <span class="section-label">prefix cache</span>
    <span class="label">hit rate</span>
    <span class="value ${warnClass(hitRate, 0.3, 0)}">${pct(hitRate)}</span>
    ${extHit != null ? `<span class="label">ext hit rate</span><span class="value">${pct(extHit)}</span>` : ""}

    <div class="divider"></div>
    <span class="section-label">throughput</span>
    <span class="label">gen tok/s</span>
    <span class="value ${warnClass(m.gen_tokens_per_sec, 50, 0)}">${num(m.gen_tokens_per_sec, 0)}</span>
    <span class="label">prefill tok/s</span>
    <span class="value">${num(m.prefill_tokens_per_sec, 0)}</span>
    <span class="label">success req/s</span>
    <span class="value">${num(m.request_success_per_sec, 3)}</span>
    <span class="label">preemptions/s</span>
    <span class="value ${m.preemptions_per_sec > 0 ? 'warn' : ''}">${num(m.preemptions_per_sec, 3)}</span>
  </div>`}
</div>`;
}

async function refresh() {
  try {
    const resp = await fetch("/api/metrics");
    if (!resp.ok) throw new Error(resp.statusText);
    const data = await resp.json();

    const grid = document.getElementById("grid");
    grid.innerHTML = data.map(renderCard).join("");

    const ok = data.filter(m => m.status === "ok").length;
    document.getElementById("instance-count").textContent =
      `${ok}/${data.length} instances live`;
    document.getElementById("last-updated").textContent =
      "updated " + new Date().toLocaleTimeString();
    document.getElementById("error-banner").style.display = "none";
  } catch (e) {
    document.getElementById("error-banner").textContent = "fetch error: " + e.message;
    document.getElementById("error-banner").style.display = "block";
  }
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    config: dict = {}
    log_dir: Path = Path(".")

    def log_message(self, fmt, *args):
        pass  # suppress access log noise

    def do_GET(self):
        if self.path == "/api/metrics":
            self._serve_metrics()
        elif self.path in ("/", "/index.html"):
            self._serve_html()
        else:
            self.send_error(404)

    def _serve_metrics(self):
        instances = self.config.get("instances", [])
        data = [get_instance_metrics(self.log_dir, inst["name"]) for inst in instances]
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_html(self):
        instances_js = json.dumps([i["name"] for i in self.config.get("instances", [])])
        html = HTML_TEMPLATE \
            .replace("__INSTANCES__", instances_js) \
            .replace("__LOG_DIR__", str(self.log_dir))
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description="vLLM log viewer dashboard")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--port",   type=int,  default=DEFAULT_PORT)
    args = parser.parse_args()

    cfg     = load_config(args.config)
    log_dir = Path(cfg["log_dir"])

    Handler.config  = cfg
    Handler.log_dir = log_dir

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"vLLM log viewer running at  http://localhost:{args.port}")
    print(f"Reading logs from           {log_dir}")
    print(f"Instances                   {len(cfg['instances'])}")
    print("Press Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

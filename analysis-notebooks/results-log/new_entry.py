#!/usr/bin/env python3
"""
new_entry.py
~~~~~~~~~~~~
Scaffold a new results-log entry from template.md.

Picks the next zero-padded index inside a campaign folder, stamps today's
date, sets a campaign-scoped id, and opens a fresh file so every entry stays
in the same format.

Usage:
    # add an entry to an existing campaign
    python new_entry.py --campaign 02-boom-reproduce --title "check multi-turn"

    # start a brand-new campaign folder (created if missing)
    python new_entry.py --campaign 03-fairness --title "fair pull baseline"

    # override the series label / date / exp ids
    python new_entry.py -c 02-boom-reproduce -t "soak" --series 25 \
        --exp-ids 301,302 --date 2026-07-14
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_TEMPLATE = _ROOT / "template.md"


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s or "entry"


def _next_index(campaign_dir: Path) -> int:
    mx = -1
    for p in campaign_dir.glob("[0-9][0-9][0-9]-*.md"):
        try:
            mx = max(mx, int(p.name[:3]))
        except ValueError:
            continue
    return mx + 1


def _campaign_number(campaign: str) -> str:
    m = re.match(r"(\d+)", campaign)
    return m.group(1).zfill(2) if m else "00"


def main() -> None:
    ap = argparse.ArgumentParser(description="Scaffold a new results-log entry.")
    ap.add_argument("-c", "--campaign", required=True,
                    help="Campaign folder name (created if missing), e.g. 02-boom-reproduce")
    ap.add_argument("-t", "--title", required=True, help="Short entry title")
    ap.add_argument("--series", default=None, help="Series label (default: next index)")
    ap.add_argument("--exp-ids", default="", help="Comma-separated experiment dir ids")
    ap.add_argument("--date", default=_dt.date.today().isoformat(), help="YYYY-MM-DD")
    ap.add_argument("--methods", default="", help="Comma-separated methods")
    args = ap.parse_args()

    campaign_dir = _ROOT / args.campaign
    campaign_dir.mkdir(parents=True, exist_ok=True)

    idx = _next_index(campaign_dir)
    series = args.series if args.series is not None else str(idx)
    entry_id = f"c{_campaign_number(args.campaign)}-{idx:03d}"
    fname = f"{idx:03d}-{_slugify(args.title)}.md"
    dest = campaign_dir / fname
    if dest.exists():
        raise SystemExit(f"refusing to overwrite existing {dest}")

    exp_ids = [x.strip() for x in args.exp_ids.split(",") if x.strip()]
    methods = [x.strip() for x in args.methods.split(",") if x.strip()]

    body = _TEMPLATE.read_text(encoding="utf-8")
    body = body.replace("id:            # e.g. c02-005 (campaign-scoped, unique)",
                        f"id: {entry_id}")
    body = body.replace("date:          # YYYY-MM-DD", f"date: {args.date}")
    body = body.replace("campaign:      # folder name, e.g. 02-boom-reproduce",
                        f"campaign: {args.campaign}")
    body = body.replace('series:        # human label, e.g. "5" or "5-8"',
                        f"series: {series}")
    body = body.replace("exp_ids: []    # experiment dir ids referenced, e.g. [241, 242]",
                        f"exp_ids: [{', '.join(exp_ids)}]")
    body = body.replace("methods: []    # e.g. [pull, push-rr, push-random, push-lq]",
                        f"methods: [{', '.join(methods)}]")
    body = body.replace("# Series <N> — <short title>",
                        f"# Series {series} — {args.title}")

    dest.write_text(body, encoding="utf-8")
    print(f"created {dest}")


if __name__ == "__main__":
    main()

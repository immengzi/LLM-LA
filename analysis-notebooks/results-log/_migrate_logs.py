#!/usr/bin/env python3
"""
_migrate_logs.py
~~~~~~~~~~~~~~~~
One-shot, lossless migration of the legacy hand-written journal
``analysis-notebooks/logs`` into the structured ``results-log/`` layout:

    results-log/
      README.md                     # generated index of every entry
      <NN>-<campaign>/
        _preamble.md                # any text before the first "Series" (if any)
        <NNN>-<slug>.md             # one file per Series block, verbatim body

Segmentation rules (kept deliberately simple so nothing is dropped):
  * The journal is split into CAMPAIGNS at the divider line that starts with
    ``*** logs``.
  * Within a campaign, a new ENTRY starts at every line matching ``^Series``.
    Everything up to the next ``Series`` header (including any trailing
    free-form notes) belongs to that entry -> verbatim, lossless.
  * Text before the first ``Series`` header becomes ``_preamble.md``.

The original ``logs`` file is NOT modified. After writing, the script
reconstructs the source from the generated bodies and asserts it matches the
original byte-for-byte (minus the dropped divider lines), so the migration is
provably lossless.

Run:
    python analysis-notebooks/results-log/_migrate_logs.py
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_SRC = _ROOT.parent / "logs"

_SERIES_RE = re.compile(r"^Series\b(.*?):?\s*$")
_DIVIDER_RE = re.compile(r"^\*\*\*\s*logs")
_EXP_RE = re.compile(r"exp(\d+)")


def _slugify(text: str, fallback: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    s = s[:48].strip("-")
    return s or fallback


def _campaigns(lines: list[str]) -> list[tuple[str, list[str]]]:
    """Split the whole file into (name, lines) campaigns at '*** logs' dividers."""
    boundaries = [i for i, ln in enumerate(lines) if _DIVIDER_RE.match(ln)]
    segments: list[tuple[int, int]] = []
    start = 0
    for b in boundaries:
        segments.append((start, b))   # exclude the divider line itself
        start = b + 1
    segments.append((start, len(lines)))

    names = ["01-push-vs-pull-strategy", "02-boom-reproduce",
             "03-more", "04-more", "05-more"]
    out = []
    for idx, (a, b) in enumerate(segments):
        chunk = lines[a:b]
        if not any(s.strip() for s in chunk):
            continue
        name = names[idx] if idx < len(names) else f"{idx + 1:02d}-more"
        out.append((name, chunk))
    return out


def _split_entries(chunk: list[str]) -> tuple[list[str], list[tuple[str, list[str]]]]:
    """Return (preamble_lines, [(series_label, body_lines), ...])."""
    header_idxs = [i for i, ln in enumerate(chunk) if _SERIES_RE.match(ln)]
    if not header_idxs:
        return chunk, []
    preamble = chunk[: header_idxs[0]]
    entries: list[tuple[str, list[str]]] = []
    for n, hi in enumerate(header_idxs):
        end = header_idxs[n + 1] if n + 1 < len(header_idxs) else len(chunk)
        body = chunk[hi:end]
        label = _SERIES_RE.match(chunk[hi]).group(1).strip() or str(n)
        entries.append((label, body))
    return preamble, entries


def _purpose_and_findings(body: list[str]) -> tuple[str, str]:
    purpose = findings = ""
    for ln in body:
        low = ln.strip().lower()
        if low.startswith("purpose:") and not purpose:
            purpose = ln.split(":", 1)[1].strip()
        elif low.startswith("findings:") and not findings:
            findings = ln.split(":", 1)[1].strip()
    return purpose, findings


def main() -> None:
    raw = _SRC.read_text(encoding="utf-8")
    lines = raw.splitlines(keepends=True)

    index_rows: list[dict] = []
    # Track reconstruction to prove losslessness.
    reconstructed: list[str] = []

    campaigns = _campaigns(lines)
    for cname, chunk in campaigns:
        cdir = _ROOT / cname
        cdir.mkdir(parents=True, exist_ok=True)
        preamble, entries = _split_entries(chunk)

        if any(s.strip() for s in preamble):
            (cdir / "_preamble.md").write_text("".join(preamble), encoding="utf-8")
        reconstructed.extend(preamble)

        cnum = re.match(r"(\d+)", cname).group(1).zfill(2)
        for seq, (label, body) in enumerate(entries):
            reconstructed.extend(body)
            purpose, findings = _purpose_and_findings(body)
            exp_ids = sorted({int(x) for ln in body for x in _EXP_RE.findall(ln)})
            title = purpose if purpose and purpose.upper() != "TODO" else f"series {label}"
            slug = _slugify(title, f"series-{label}")
            fname = f"{seq:03d}-{slug}.md"

            front = [
                "---",
                f"id: c{cnum}-{seq:03d}",
                "date:",
                f"campaign: {cname}",
                f'series: "{label}"',
                f"exp_ids: [{', '.join(map(str, exp_ids))}]",
                "methods: []",
                "status: migrated",
                "source: logs",
                "---",
                "",
            ]
            # Body verbatim; add an H1 above for readability (does not alter source).
            heading = f"# Series {label}" + (f" — {title}" if title else "")
            content = "\n".join(front) + heading + "\n\n" + "".join(body)
            (cdir / fname).write_text(content, encoding="utf-8")

            summary = findings or purpose or ""
            index_rows.append({
                "campaign": cname,
                "path": f"{cname}/{fname}",
                "series": label,
                "exp_ids": exp_ids,
                "summary": summary[:100],
            })

    # ---- README index ----
    md = ["# Results Log",
          "",
          "Structured, per-entry migration of the legacy `analysis-notebooks/logs` "
          "journal. The original `logs` file is kept untouched as the source of truth "
          "until this is verified.",
          "",
          "Add new entries with `new_entry.py` (see `template.md`).",
          ""]
    cur = None
    for row in index_rows:
        if row["campaign"] != cur:
            cur = row["campaign"]
            md += ["", f"## {cur}", "",
                   "| Series | Entry | Exp IDs | Summary |",
                   "|---|---|---|---|"]
        exp = ", ".join(map(str, row["exp_ids"])) or "—"
        summ = row["summary"].replace("|", "\\|") or "—"
        md.append(f"| {row['series']} | [`{Path(row['path']).name}`]({row['path']}) | {exp} | {summ} |")
    (_ROOT / "README.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    # ---- losslessness check ----
    # Reconstructed = all campaign chunks concatenated (dividers were dropped).
    src_no_divider = [ln for ln in lines if not _DIVIDER_RE.match(ln)]
    ok = reconstructed == src_no_divider
    print(f"campaigns: {len(campaigns)}  entries: {len(index_rows)}")
    print(f"lossless (bodies == source minus divider lines): {ok}")
    if not ok:
        # Report first mismatch for debugging.
        for i, (a, b) in enumerate(zip(reconstructed, src_no_divider)):
            if a != b:
                print(f"  first diff at reconstructed line {i}:")
                print(f"    got : {a!r}")
                print(f"    want: {b!r}")
                break
        print(f"  len(reconstructed)={len(reconstructed)} len(source-divider)={len(src_no_divider)}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()

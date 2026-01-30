#!/usr/bin/env python3
"""
NAS Log Parser: Level → Components Summary (with special handling for blank components)

Enhancements:
- Robust fallback parsing.
- If component is blank/absent:
   * If level == Debug and message contains "StopwatchScope" (case-insensitive),
     set component = "StopwatchScope".
   * Else set component = "Unknown".

Usage:
    python parse_nas_log_levels.py /path/to/log.txt --out summary.csv --pivot pivot.csv
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

# --- Regexes ---
LINE_RE = re.compile(
    r"""
    ^\s*
    (?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)
    (?:\s+(?P<tz>[+-]\d{2}:\d{2}))?
    \s+\[(?P<level>[^\]]+)\]\s+\[(?P<component>[^\]]*)\]
    (?:\s+(?P<context>(?:\[[^\]]*\]\s*)*))?
    (?P<message>.*)
    $
    """,
    re.VERBOSE,
)

ALT_BRACKETS_RE = re.compile(r"\[([^\]]*)\]")

SEVERITY_ORDER = ["Critical", "Error", "Warning", "Information", "Debug", "Verbose", "Trace", "TRACE"]
SEVERITY_SET = {s.lower() for s in SEVERITY_ORDER}


@dataclass(frozen=True)
class LogRecordLite:
    """Lightweight view over a parsed log line: just level, component, message."""
    level: str
    component: str
    message: str = ""


def _normalize_token(tok: str) -> str:
    return tok.strip()


def _is_level(tok: str) -> bool:
    return _normalize_token(tok).lower() in SEVERITY_SET


def parse_line(line: str) -> Optional[LogRecordLite]:
    """
    Robustly parse a NAS log line to (level, component, message).

    Strategy:
      1) Try strict pattern: timestamp tz [Level] [Component] [context...] message
      2) Fallback: scan ALL [ ... ] tokens; pick the first that looks like a known level,
         then take the NEXT token as the component (may be empty), and use the whole line as message.
      3) If component is empty/blank, apply user rule:
           - If level == Debug and message contains "StopwatchScope" (case-insensitive),
             set component = "StopwatchScope"; else "Unknown".
    """
    m = LINE_RE.search(line)
    if m:
        level = _normalize_token(m.group("level") or "")
        component = _normalize_token(m.group("component") or "")
        message = (m.group("message") or "").strip()

        if not component:
            component = infer_component_from_message(level, message)
        return LogRecordLite(level=level, component=component, message=message)

    # Fallback: scan all bracketed tokens
    tokens = [_normalize_token(t) for t in ALT_BRACKETS_RE.findall(line)]
    level = ""
    component = ""
    message = line.strip()

    # Find first known level and take next as component if present
    for i, tok in enumerate(tokens):
        if _is_level(tok):
            level = tok
            if i + 1 < len(tokens):
                component = tokens[i + 1]
            break

    if not level:
        return None  # can't safely infer level

    if not component:
        component = infer_component_from_message(level, message)

    return LogRecordLite(level=level, component=component, message=message)


def infer_component_from_message(level: str, message: str) -> str:
    """
    Apply user-specified rule for blank components.
    - If Debug level and message mentions 'StopwatchScope' (any case), return 'StopwatchScope'.
    - Otherwise return 'Unknown'.
    """
    if level.lower() == "debug" and "stopwatchscope" in message.lower():
        return "StopwatchScope"
    return "Unknown"


def iter_lines(path: Path) -> Iterator[str]:
    """Yield lines from a file with universal newline handling."""
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield line.rstrip("\n")


def build_level_component_counts(path: Path) -> Dict[str, Counter]:
    """
    Build a mapping: level -> Counter(component -> count).

    Unparseable lines are ignored to be robust to mixed content.
    """
    mapping: Dict[str, Counter] = defaultdict(Counter)
    for line in iter_lines(path):
        rec = parse_line(line)
        if rec is None:
            continue
        mapping[rec.level][rec.component] += 1
    return mapping


def to_rows(mapping: Dict[str, Counter]) -> List[Tuple[str, str, int]]:
    """Flatten mapping to rows: (level, component, count)."""
    rows: List[Tuple[str, str, int]] = []
    for level, ctr in mapping.items():
        for comp, cnt in ctr.items():
            rows.append((level, comp, cnt))
    # Sort by severity, then component, then count desc
    def severity_rank(level: str) -> int:
        return SEVERITY_ORDER.index(level) if level in SEVERITY_ORDER else len(SEVERITY_ORDER)
    rows.sort(key=lambda x: (severity_rank(x[0]), x[1], -x[2]))
    return rows


def write_csv(rows: List[Tuple[str, str, int]], out_path: Path) -> None:
    """Write tidy rows to CSV with header: level,component,count."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["level", "component", "count"])
        w.writerows(rows)


def write_pivot(rows: List[Tuple[str, str, int]], out_path: Path) -> None:
    """
    Write a simple pivot (levels as rows, components as columns) to CSV.

    Implemented without pandas to keep runtime deps minimal.
    """
    levels: List[str] = []
    comps_set = set()
    table: Dict[Tuple[str, str], int] = {}

    for level, comp, cnt in rows:
        if level not in levels:
            levels.append(level)
        comps_set.add(comp)
        table[(level, comp)] = cnt

    comps = sorted(comps_set)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["level"] + list(comps))
        for lvl in levels:
            w.writerow([lvl] + [table.get((lvl, c), 0) for c in comps])


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize NAS log: levels → components.")
    parser.add_argument("logfile", type=Path, help="Path to NAS log file")
    parser.add_argument("--out", type=Path, default=Path("nas_level_components.csv"), help="Output CSV (tidy)")
    parser.add_argument("--pivot", type=Path, default=None, help="Optional pivot CSV (levels rows × components cols)")
    args = parser.parse_args()

    if not args.logfile.exists():
        parser.error(f"Log file not found: {args.logfile}")

    mapping = build_level_component_counts(args.logfile)
    rows = to_rows(mapping)
    write_csv(rows, args.out)
    if args.pivot:
        write_pivot(rows, args.pivot)


if __name__ == "__main__":
    main()

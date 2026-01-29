"""Utility helpers for parsing timestamps, numeric values, and JSON payloads."""

from __future__ import annotations

import re
from datetime import datetime, timezone

ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


# Type alias for JSON-serializable values
JSONValue = str | int | float | bool | None | dict[str, "JSONValue"] | list["JSONValue"]

UNIT_MAP = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}


def strip_ansi(s: str) -> str:
    """Remove ANSI color codes."""
    return ANSI_RE.sub("", s)


def normalize_datetime(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware (UTC).

    Args:
        dt: datetime object (may be naive or aware)

    Returns:
        Timezone-aware datetime in UTC
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    if dt.tzinfo != timezone.utc:
        return dt.astimezone(timezone.utc)
    return dt


def parse_ts(s: str) -> datetime:
    """Parse timestamps and return a timezone-aware UTC datetime.

    Accepts inputs such as '2024-10-25 01:44:23.014 +00:00' or '8/18/2025 10:46:49 AM'.
    """
    from dateutil import parser as dtp

    dt = dtp.parse(s)
    return normalize_datetime(dt)


def to_bytes(value: float, unit: str) -> int | None:
    """Convert a number and unit to bytes. Returns None on error."""
    try:
        factor = UNIT_MAP[unit.upper()]
        return int(round(float(value) * factor))
    except Exception:
        return None


def parse_ms(s: str) -> float | None:
    """Parse strings like '0.96 ms' -> 0.96 (ms)."""
    match = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*ms\s*$", s, re.I)
    return float(match.group(1)) if match else None


def safe_int(s: str) -> int | None:
    """Convert string to int, returning None when conversion fails."""
    try:
        return int(s)
    except Exception:
        return None


def make_json_serializable(obj: JSONValue | datetime | dict | list) -> JSONValue:
    """Recursively convert datetime objects to ISO strings for JSON serialization."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    return obj

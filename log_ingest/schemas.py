from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class LogRecord:
    """A normalized record for generic StorONE log lines."""

    ts: datetime
    level: str
    component: str
    thread: Optional[str] = None
    et: Optional[str] = None
    cg: Optional[str] = None
    v: Optional[str] = None
    s: Optional[str] = None
    message: str = ""
    kv: Dict[str, Any] = field(default_factory=dict)
    source_file: Optional[str] = None


@dataclass
class MetricRecord:
    """Structured metrics parsed from 'latency' style tables or periodic sections.
    Units are normalized:
      - latency_ms: milliseconds (float)
      - size_bytes / io_size_bytes / throughput_bytes_s: integers
    """

    ts: datetime
    metric: str
    fields: Dict[str, Any]
    source_file: Optional[str] = None


@dataclass
class PeriodicSection:
    """Captures parsed 'Periodic Information' sections (as nested dicts)."""

    ts: datetime
    section: str
    data: Dict[str, Any]
    source_file: Optional[str] = None

"""ABOUTME: TimeBucket dataclass for storing time-aligned bucket metrics.

ABOUTME: Extracted to separate module to avoid circular import dependencies.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import pyarrow as pa


@dataclass
class TimeBucket:
    """A fixed time window containing aggregated metrics and events."""

    bucket_id: str
    start_time: datetime
    end_time: datetime

    # Latency metrics (single measurement per 5-min period)
    read_latency_ms: float | None = None
    write_latency_ms: float | None = None
    read_iops: float | None = None
    write_iops: float | None = None
    read_throughput_bps: int | None = None
    write_throughput_bps: int | None = None
    has_spike: bool = False  # Based on threshold

    # System metrics (forward-filled)
    cpu_util: float | None = None
    memory_util: float | None = None
    disk_util: float | None = None
    total_pools: int | None = None
    pools_json: str | None = None  # Pool details

    # Drive counts
    hdd_count: int | None = None
    ssd_count: int | None = None

    # Volume info
    total_volumes: int | None = None
    volumes_json: str | None = None

    # HA status
    ha_state: str | None = None
    is_leader: bool | None = None
    peer_available: bool | None = None

    # Network
    ip_addresses_json: str | None = None  # JSON list of IP addresses

    # Rebuild status
    degraded_volumes: int | None = None
    rebuild_active: bool | None = None

    # Event aggregates
    total_events: int = 0
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    verbose_count: int = 0
    debug_count: int = 0

    # Component counts
    component_counts_json: str | None = None

    # Template storage: {template_id: {template_str, instances: [{var1, var2, ...}]}}
    templates: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert bucket to dictionary with serializable types.

        Returns:
            Dictionary representation with datetime objects as ISO strings
        """
        data = asdict(self)

        # Convert datetime objects to ISO strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()

        return data

    def to_json(self, indent: int | None = 2) -> str:
        """Convert bucket to JSON string.

        Args:
            indent: Number of spaces for indentation (None for compact)

        Returns:
            JSON string representation of the bucket
        """
        return json.dumps(self.to_dict(), indent=indent)


def define_bucket_parquet_schema() -> pa.Schema:
    """Define PyArrow schema for TimeBucket parquet files.

    Returns:
        PyArrow schema with all TimeBucket fields and types
    """
    return pa.schema(
        [
            ("bucket_id", pa.string()),
            ("start_time", pa.timestamp("us")),
            ("end_time", pa.timestamp("us")),
            # Latency metrics
            ("read_latency_ms", pa.float64()),
            ("write_latency_ms", pa.float64()),
            ("read_iops", pa.float64()),
            ("write_iops", pa.float64()),
            ("read_throughput_bps", pa.int64()),
            ("write_throughput_bps", pa.int64()),
            ("has_spike", pa.bool_()),
            # System metrics
            ("cpu_util", pa.float64()),
            ("memory_util", pa.float64()),
            ("disk_util", pa.float64()),
            ("total_pools", pa.int64()),
            ("pools_json", pa.string()),
            ("hdd_count", pa.int64()),
            ("ssd_count", pa.int64()),
            ("total_volumes", pa.int64()),
            ("volumes_json", pa.string()),
            # HA status
            ("ha_state", pa.string()),
            ("is_leader", pa.bool_()),
            ("peer_available", pa.bool_()),
            ("ip_addresses_json", pa.string()),
            # Rebuild status
            ("degraded_volumes", pa.int64()),
            ("rebuild_active", pa.bool_()),
            # Event counts
            ("total_events", pa.int64()),
            ("error_count", pa.int64()),
            ("warning_count", pa.int64()),
            ("info_count", pa.int64()),
            ("verbose_count", pa.int64()),
            ("debug_count", pa.int64()),
            # Component/template data
            ("component_counts_json", pa.string()),
            # Templates: JSON string for now (simpler than nested struct)
            ("templates", pa.string()),
        ]
    )

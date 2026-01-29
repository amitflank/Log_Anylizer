"""ABOUTME: Incremental parquet writer for TimeBucket data with append support.

ABOUTME: Writes bucket data to parquet files incrementally without loading full dataset.
"""

import json
import logging
from dataclasses import asdict
from datetime import timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from log_ingest.bucket_building.time_bucket import TimeBucket, define_bucket_parquet_schema
from log_ingest.utils import make_json_serializable

logger = logging.getLogger(__name__)


class IncrementalParquetWriter:
    """Write TimeBucket data to parquet files incrementally.

    Supports creating new files or appending to existing files without loading
    the entire dataset into memory.
    """

    def __init__(self, output_path: Path | str):
        """Initialize writer with output path and schema.

        Args:
            output_path: Path to output parquet file (Path or string)
        """
        self.output_path = Path(output_path) if isinstance(output_path, str) else output_path
        self.file_exists = self.output_path.exists()
        self.schema = define_bucket_parquet_schema()

    @staticmethod
    def _normalize_timestamp(ts: object) -> object:
        """Normalize datetimes for comparison/storage.

        Parquet timestamps are timezone-naive; some in-memory buckets may be tz-aware.
        We normalize to naive UTC for stable comparisons and storage.
        """
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            return ts.astimezone(timezone.utc).replace(tzinfo=None)
        return ts

    @staticmethod
    def _parse_json_dict(payload: str | None) -> dict[str, Any]:
        if not payload:
            return {}
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
        return {}

    @staticmethod
    def _merge_component_counts(a_json: str | None, b_json: str | None) -> str | None:
        a = IncrementalParquetWriter._parse_json_dict(a_json)
        b = IncrementalParquetWriter._parse_json_dict(b_json)
        merged: dict[str, int] = {}
        for k, v in a.items():
            if isinstance(v, int):
                merged[k] = merged.get(k, 0) + v
        for k, v in b.items():
            if isinstance(v, int):
                merged[k] = merged.get(k, 0) + v
        return json.dumps(merged) if merged else None

    @staticmethod
    def _merge_templates(
        a: dict[str, dict[str, Any]], b: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for src in (a, b):
            for template_id, payload in (src or {}).items():
                if template_id not in merged:
                    merged[template_id] = {
                        "template_str": payload.get("template_str"),
                        "instances": list(payload.get("instances") or []),
                    }
                    continue

                dst = merged[template_id]
                if not dst.get("template_str"):
                    dst["template_str"] = payload.get("template_str")

                dst_instances: list[dict[str, Any]] = list(dst.get("instances") or [])
                dst_instances.extend(list(payload.get("instances") or []))

                # Deduplicate instances (stable + deterministic ordering).
                seen: set[str] = set()
                unique: list[dict[str, Any]] = []
                for inst in dst_instances:
                    key = json.dumps(inst, sort_keys=True, default=str)
                    if key in seen:
                        continue
                    seen.add(key)
                    unique.append(inst)
                unique.sort(
                    key=lambda i: (
                        str(i.get("timestamp", "")),
                        json.dumps(i, sort_keys=True, default=str),
                    )
                )
                dst["instances"] = unique
        return merged

    @staticmethod
    def _merge_time_buckets(existing: TimeBucket, incoming: TimeBucket) -> TimeBucket:
        """Merge two buckets with the same bucket_id into one (in-place on existing)."""
        assert existing.bucket_id == incoming.bucket_id

        # Keep time bounds stable (but be defensive if inputs disagree).
        a_start = IncrementalParquetWriter._normalize_timestamp(existing.start_time)
        b_start = IncrementalParquetWriter._normalize_timestamp(incoming.start_time)
        a_end = IncrementalParquetWriter._normalize_timestamp(existing.end_time)
        b_end = IncrementalParquetWriter._normalize_timestamp(incoming.end_time)

        existing.start_time = min(a_start, b_start)
        existing.end_time = max(a_end, b_end)

        # Event counters are additive.
        existing.total_events += incoming.total_events
        existing.error_count += incoming.error_count
        existing.warning_count += incoming.warning_count
        existing.info_count += incoming.info_count
        existing.verbose_count += incoming.verbose_count
        existing.debug_count += incoming.debug_count

        # Component counts are additive JSON.
        existing.component_counts_json = IncrementalParquetWriter._merge_component_counts(
            existing.component_counts_json, incoming.component_counts_json
        )

        # Templates are additive/unioned.
        existing.templates = IncrementalParquetWriter._merge_templates(
            existing.templates, incoming.templates
        )

        # "Snapshot" metrics: keep the most informative value.
        for field in (
            "read_latency_ms",
            "write_latency_ms",
            "read_iops",
            "write_iops",
            "read_throughput_bps",
            "write_throughput_bps",
            "cpu_util",
            "memory_util",
            "disk_util",
            "total_pools",
            "pools_json",
            "hdd_count",
            "ssd_count",
            "total_volumes",
            "volumes_json",
            "ha_state",
            "is_leader",
            "peer_available",
            "ip_addresses_json",
            "degraded_volumes",
            "rebuild_active",
        ):
            if getattr(existing, field) is None and getattr(incoming, field) is not None:
                setattr(existing, field, getattr(incoming, field))

        existing.has_spike = bool(existing.has_spike or incoming.has_spike)
        return existing

    def _normalize_bucket_timestamps(self, bucket: TimeBucket) -> TimeBucket:
        bucket.start_time = self._normalize_timestamp(bucket.start_time)
        bucket.end_time = self._normalize_timestamp(bucket.end_time)
        return bucket

    def _read_existing_buckets(self) -> dict[str, TimeBucket]:
        """Read existing parquet content and return a dict of merged buckets keyed by bucket_id."""
        if not self.output_path.exists():
            return {}
        table = pq.read_table(self.output_path)
        bucket_fields = set(TimeBucket.__dataclass_fields__)
        merged: dict[str, TimeBucket] = {}
        for row in table.to_pylist():
            templates_payload = row.get("templates")
            if isinstance(templates_payload, str):
                row["templates"] = self._parse_json_dict(templates_payload)
            elif templates_payload is None:
                row["templates"] = {}

            tb_kwargs = {k: v for k, v in row.items() if k in bucket_fields}
            tb = TimeBucket(**tb_kwargs)
            self._normalize_bucket_timestamps(tb)

            if tb.bucket_id in merged:
                self._merge_time_buckets(merged[tb.bucket_id], tb)
            else:
                merged[tb.bucket_id] = tb
        return merged

    def _serialize_buckets(self, buckets: dict[str, TimeBucket]) -> list[dict]:
        """Convert TimeBucket objects to dict records for PyArrow.

        Args:
            buckets: Dictionary mapping bucket_id to TimeBucket

        Returns:
            List of dict records sorted by start_time, with templates as JSON
        """
        records = []
        for bucket in sorted(buckets.values(), key=lambda b: b.start_time):
            record = asdict(bucket)

            # Convert templates dict to JSON string
            templates = record.get("templates")
            if templates:
                templates_serializable = make_json_serializable(templates)
                record["templates"] = json.dumps(templates_serializable)
            else:
                record["templates"] = None

            records.append(record)

        return records

    def append_buckets(self, buckets: dict[str, TimeBucket]) -> int:
        """Write buckets to parquet file, creating new or appending to existing.

        Args:
            buckets: Dictionary mapping bucket_id to TimeBucket

        Returns:
            Number of buckets written
        """
        if not buckets:
            return 0

        merged = self._read_existing_buckets() if self.file_exists else {}
        for bucket_id, bucket in buckets.items():
            self._normalize_bucket_timestamps(bucket)
            if bucket_id in merged:
                self._merge_time_buckets(merged[bucket_id], bucket)
            else:
                merged[bucket_id] = bucket

        records = self._serialize_buckets(merged)
        table = pa.Table.from_pylist(records, schema=self.schema)

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, self.output_path)
        self.file_exists = True

        bucket_count = len(buckets)
        logger.info(f"Wrote {bucket_count} buckets to {self.output_path}")
        return bucket_count

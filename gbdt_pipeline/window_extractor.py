"""Look-back window extraction helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from gbdt_pipeline.exclusion import TemplateExclusion
from gbdt_pipeline.incident_catalog import IncidentTarget
from gbdt_pipeline.label_builder import BucketLabel
from log_ingest.bucket_building.time_bucket import TimeBucket


@dataclass
class WindowMetadata:
    """Metadata describing a look-back window."""

    captured_minutes: int
    requested_minutes: int
    expansions: int


@dataclass
class LookbackWindow:
    """A collection of buckets plus associated metadata."""

    buckets: list[TimeBucket]
    metadata: WindowMetadata


class LookbackWindowExtractor:
    """Materialise look-back windows from a sequence of buckets."""

    BUCKET_MINUTES = 5

    def __init__(
        self,
        buckets: Sequence[TimeBucket],
        exclusion: TemplateExclusion | None = None,
    ) -> None:
        """Store buckets sorted by start time."""
        self._buckets = sorted(buckets, key=lambda bucket: bucket.start_time or datetime.min)
        self._exclusion = exclusion

    def build_window(self, bucket_label: BucketLabel, target: IncidentTarget) -> LookbackWindow:
        """Construct a window ending at the bucket's window_end."""
        if bucket_label.window_end is not None:
            window_end = bucket_label.window_end
        elif bucket_label.start_time is not None:
            window_end = bucket_label.start_time + timedelta(minutes=self.BUCKET_MINUTES)
        else:
            raise ValueError("Bucket label missing start time")

        window_start = window_end - timedelta(minutes=target.initial_lookback_minutes)
        selected = [
            bucket
            for bucket in self._buckets
            if bucket.start_time
            and bucket.end_time
            and bucket.end_time > window_start
            and bucket.start_time <= window_end
        ]

        filtered = [self._prune_bucket(bucket, window_end) for bucket in selected]

        captured_minutes = len(filtered) * self.BUCKET_MINUTES
        metadata = WindowMetadata(
            captured_minutes=captured_minutes,
            requested_minutes=target.initial_lookback_minutes,
            expansions=0,
        )
        return LookbackWindow(filtered, metadata)

    def _prune_bucket(self, bucket: TimeBucket, window_end: datetime) -> TimeBucket:
        """Remove template instances that occur after the window end."""
        if not bucket.templates:
            return bucket

        new_templates: dict[str, dict] = {}
        for template_id, payload in bucket.templates.items():
            if self._exclusion and self._exclusion.matches(template_id):
                continue
            instances = payload.get("instances", [])
            kept = [
                instance
                for instance in instances
                if (timestamp := self._parse_ts(instance)) is None or timestamp < window_end
            ]
            updated_payload = dict(payload)
            updated_payload["instances"] = kept
            new_templates[template_id] = updated_payload

        return replace(bucket, templates=new_templates)

    @staticmethod
    def _parse_ts(instance: dict[str, object]) -> datetime | None:
        """Parse timestamps embedded in template instances."""
        value = instance.get("timestamp") or instance.get("ts")
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

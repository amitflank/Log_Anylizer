"""Build bucket-level labels for incident prediction."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from gbdt_pipeline.incident_catalog import IncidentTarget
from log_ingest.bucket_building.time_bucket import TimeBucket


@dataclass
class BucketLabel:
    """A single labelled bucket used for training or scoring."""

    bucket_id: str
    start_time: datetime | None
    label: int
    symptom_time: datetime | None = None
    window_end: datetime | None = None


@dataclass
class LabeledBuckets:
    """Collection of positive and negative labels."""

    positives: list[BucketLabel] = field(default_factory=list)
    negatives: list[BucketLabel] = field(default_factory=list)

    def all(self) -> Iterable[BucketLabel]:
        """Iterate over all labels."""
        yield from self.positives
        yield from self.negatives


@dataclass
class SymptomInstance:
    """Occurrence of a symptom template within a bucket."""

    bucket_index: int
    timestamp: datetime


@dataclass(frozen=True)
class SymptomFilter:
    """Criteria for filtering symptom template instances.

    A symptom instance must pass both substring and variable filters (AND logic).
    """

    substring: str | None = None
    var_filters: dict[str, str] = field(default_factory=dict)

    def matches(self, instance: dict[str, Any]) -> bool:
        """Return True if the instance satisfies all filter criteria."""
        if self.var_filters:
            for var_name, expected in self.var_filters.items():
                actual = str(instance.get(var_name, "")).strip()
                if actual != expected:
                    return False
        if self.substring:
            blob = " ".join(str(v) for v in instance.values() if v is not None)
            if self.substring.lower() not in blob.lower():
                return False
        return True


class LabelBuilder:
    """Assign bucket-level labels based on symptom templates."""

    def __init__(
        self,
        buckets: Sequence[TimeBucket],
        symptom_filter: SymptomFilter | None = None,
    ) -> None:
        """Store the source buckets in chronological order."""
        self._buckets = list(buckets)
        self._symptom_filter = symptom_filter

    def build_labels(self, target: IncidentTarget) -> LabeledBuckets:
        """Produce positive and negative labels for the provided target."""
        labeled = LabeledBuckets()
        symptom_instances = self._find_symptom_instances(target)

        # Positives are anchored on the bucket *containing* the symptom, but the
        # feature window ends at the symptom timestamp (exclusive). This allows
        # lookback windows to overlap buckets while guaranteeing we don't train
        # on the symptom itself.
        positive_map: dict[int, SymptomInstance] = {}
        for instance in symptom_instances:
            existing = positive_map.get(instance.bucket_index)
            if existing is None or instance.timestamp < existing.timestamp:
                positive_map[instance.bucket_index] = instance

        for bucket_index, instance in positive_map.items():
            bucket = self._buckets[bucket_index]
            labeled.positives.append(
                BucketLabel(
                    bucket_id=bucket.bucket_id,
                    start_time=bucket.start_time,
                    label=1,
                    symptom_time=instance.timestamp,
                    window_end=instance.timestamp,
                )
            )

        symptom_indices = set(positive_map)
        positive_indices = symptom_indices

        for index, bucket in enumerate(self._buckets):
            if index == len(self._buckets) - 1:
                continue
            if index in positive_indices:
                continue
            # Avoid sampling a negative immediately before a positive anchor to reduce
            # ambiguous "near-incident" negatives.
            if (index + 1) in positive_indices:
                continue

            labeled.negatives.append(
                BucketLabel(
                    bucket_id=bucket.bucket_id,
                    start_time=bucket.start_time,
                    label=0,
                    window_end=bucket.end_time,
                )
            )

        return labeled

    def collect_symptom_timestamps(self, target: IncidentTarget) -> list[datetime]:
        """Return all symptom instance timestamps matching the target.

        This is useful for learning episode boundaries (e.g., estimating a gap `g`).
        """
        times: list[datetime] = []
        target_ids = set(target.template_hashes)
        for bucket in self._buckets:
            for template_id, payload in (bucket.templates or {}).items():
                if template_id not in target_ids:
                    continue
                times.extend(self._collect_matching_timestamps(payload))
        return sorted({ts for ts in times if ts is not None})

    @staticmethod
    def collapse_positives_by_gap(
        positives: Sequence[BucketLabel],
        gap_minutes: int,
    ) -> list[BucketLabel]:
        """Collapse bursty positive anchors into episodes using a gap threshold.

        - Episodes are defined by sorted symptom timestamps.
        - A new episode starts when the time since the previous symptom exceeds `gap_minutes`.
        - The representative anchor for an episode is the earliest symptom in that episode.

        Notes:
        - The boundary rule is inclusive: symptoms with Δt <= gap_minutes belong to the same
          episode.
        - Positives with missing symptom_time are ignored.
        """
        gap = max(0, int(gap_minutes))
        candidates = [label for label in positives if label.symptom_time is not None]
        candidates.sort(key=lambda label: label.symptom_time or datetime.min)
        if not candidates or gap <= 0:
            return candidates

        collapsed: list[BucketLabel] = []
        last_time: datetime | None = None
        for label in candidates:
            symptom_time = label.symptom_time
            if symptom_time is None:
                continue
            if last_time is None:
                collapsed.append(label)
                last_time = symptom_time
                continue
            delta_minutes = (symptom_time - last_time).total_seconds() / 60.0
            if delta_minutes > gap:
                collapsed.append(label)
            last_time = symptom_time
        return collapsed

    def _find_symptom_instances(self, target: IncidentTarget) -> list[SymptomInstance]:
        """Identify buckets that contain the target symptom templates."""
        matches: list[SymptomInstance] = []
        target_ids = set(target.template_hashes)

        for index, bucket in enumerate(self._buckets):
            for template_id, payload in (bucket.templates or {}).items():
                if template_id not in target_ids:
                    continue
                timestamps = self._collect_matching_timestamps(payload)
                if not timestamps:
                    continue
                matches.append(SymptomInstance(bucket_index=index, timestamp=min(timestamps)))
        return matches

    def _collect_matching_timestamps(self, payload: dict[str, Any]) -> list[datetime]:
        """Return timestamps from instances that pass the symptom filter."""
        timestamps: list[datetime] = []
        for instance in payload.get("instances", []):
            if self._symptom_filter and not self._symptom_filter.matches(instance):
                continue
            ts = self._parse_timestamp(instance)
            if ts is not None:
                timestamps.append(ts)
        return timestamps

    @staticmethod
    def _parse_timestamp(instance: dict) -> datetime | None:
        """Parse an instance timestamp field if present."""
        value = instance.get("timestamp") or instance.get("ts")
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

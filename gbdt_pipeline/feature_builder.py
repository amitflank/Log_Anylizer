"""Feature engineering utilities for the GBDT pipeline."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from gbdt_pipeline.exclusion import TemplateExclusion
from gbdt_pipeline.label_builder import BucketLabel
from gbdt_pipeline.template_inventory import TemplateSummary
from gbdt_pipeline.window_extractor import LookbackWindow
from log_ingest.bucket_building.time_bucket import TimeBucket

MIN_HISTORY_POINTS = 2


@dataclass
class FeatureConfig:
    """Configuration used when generating features."""

    metric_fields: list[str]
    max_categorical_values: int = 5


@dataclass
class FeatureRow:
    """A single feature vector."""

    bucket_id: str
    timestamp: datetime
    values: dict[str, Any]


class FeatureBuilder:
    """Convert look-back windows into model features."""

    def __init__(
        self,
        config: FeatureConfig,
        summary: TemplateSummary,
        exclusion: TemplateExclusion | None = None,
    ) -> None:
        """Store configuration and template metadata."""
        self._config = config
        self._summary = summary
        self._exclusion = exclusion
        self._allowed_templates: set[str] = set(summary.templates.keys())

    def build_features(self, window: LookbackWindow, label: BucketLabel) -> FeatureRow:
        """Build a feature vector for the provided window."""
        buckets = list(window.buckets)
        if not buckets:
            raise ValueError("Window cannot be empty")

        window_end = label.window_end or label.start_time
        if window_end is None:
            raise ValueError("Bucket label missing window end")

        feature_values: dict[str, Any] = {}
        self._add_metric_features(feature_values, buckets)
        self._apply_template_features(feature_values, buckets, window_end)
        self._apply_variable_features(feature_values, buckets, window_end)

        last_bucket = buckets[-1]
        timestamp = last_bucket.start_time or window_end
        return FeatureRow(
            bucket_id=last_bucket.bucket_id,
            timestamp=timestamp,
            values=feature_values,
        )

    def _add_metric_features(self, features: dict[str, Any], buckets: list[TimeBucket]) -> None:
        """Add metric-derived features to the dictionary."""
        last_bucket = buckets[-1]
        for metric in self._config.metric_fields:
            current = getattr(last_bucket, metric, None)
            history = self._metric_history(buckets, metric)

            features[f"metric_current_{metric}"] = current
            features[f"metric_mean_{metric}"] = self._mean(history)
            features[f"metric_delta_{metric}"] = self._last_delta(history)

    def _metric_history(self, buckets: list[TimeBucket], metric: str) -> list[float]:
        return [
            getattr(bucket, metric)
            for bucket in buckets
            if getattr(bucket, metric, None) is not None
        ]

    @staticmethod
    def _mean(history: list[float]) -> float | None:
        return sum(history) / len(history) if history else None

    @staticmethod
    def _last_delta(history: list[float]) -> float | None:
        if len(history) < MIN_HISTORY_POINTS:
            return None
        return history[-1] - history[-2]

    def _apply_template_features(
        self,
        features: dict[str, Any],
        buckets: list[TimeBucket],
        window_end: datetime,
    ) -> None:
        counts, first_occurrence = self._collect_template_stats(buckets, window_end)
        for template_id, count in counts.items():
            features[f"template_present_{template_id}"] = 1
            features[f"template_count_{template_id}"] = count
            first_ts = first_occurrence.get(template_id)
            features[f"template_lead_minutes_{template_id}"] = (
                (window_end - first_ts).total_seconds() / 60 if first_ts else None
            )

    def _collect_template_stats(
        self,
        buckets: list[TimeBucket],
        window_end: datetime,
    ) -> tuple[Counter[str], dict[str, datetime]]:
        counts: Counter[str] = Counter()
        first_occurrence: dict[str, datetime] = {}
        for bucket in buckets:
            templates = (bucket.templates or {}).items()
            for template_id, payload in templates:
                if self._exclusion and self._exclusion.matches(template_id):
                    continue
                if self._allowed_templates and template_id not in self._allowed_templates:
                    continue
                self._update_template_stats(
                    counts,
                    first_occurrence,
                    template_id,
                    payload,
                    window_end,
                )
        return counts, first_occurrence

    def _update_template_stats(
        self,
        counts: Counter[str],
        first_occurrence: dict[str, datetime],
        template_id: str,
        payload: dict[str, Any],
        window_end: datetime,
    ) -> None:
        instances = payload.get("instances", [])
        counts[template_id] += len(instances) or 1
        first_ts = self._earliest_instance_before(instances, window_end)
        if first_ts is not None and (
            template_id not in first_occurrence or first_ts < first_occurrence[template_id]
        ):
            first_occurrence[template_id] = first_ts

    def _apply_variable_features(
        self,
        features: dict[str, Any],
        buckets: list[TimeBucket],
        window_end: datetime,
    ) -> None:
        summary = self._summary.templates
        value_store = self._collect_variable_values(buckets, window_end)
        for template_id, variables in value_store.items():
            meta = summary.get(template_id)
            if not meta:
                continue
            for var_name, values in variables.items():
                var_type = meta.variable_types.get(var_name, "categorical")
                if var_type == "numeric":
                    self._add_numeric_variable(features, template_id, var_name, values)
                else:
                    self._add_categorical_variable(features, template_id, var_name, values)

    def _collect_variable_values(
        self,
        buckets: list[TimeBucket],
        window_end: datetime,
    ) -> dict[str, dict[str, list[Any]]]:
        store: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
        for bucket in buckets:
            for template_id, payload in (bucket.templates or {}).items():
                if self._exclusion and self._exclusion.matches(template_id):
                    continue
                self._gather_instance_values(store, template_id, payload, window_end)
        return store

    def _gather_instance_values(
        self,
        store: dict[str, dict[str, list[Any]]],
        template_id: str,
        payload: dict[str, Any],
        window_end: datetime,
    ) -> None:
        for instance in payload.get("instances", []):
            timestamp = self._parse_ts(instance)
            if timestamp and timestamp >= window_end:
                continue
            for name, value in instance.items():
                if name in {"timestamp", "ts"}:
                    continue
                store[template_id][name].append(value)

    def _earliest_instance_before(
        self,
        instances: list[dict[str, Any]],
        window_end: datetime,
    ) -> datetime | None:
        earliest: datetime | None = None
        for instance in instances:
            timestamp = self._parse_ts(instance)
            if timestamp is None or timestamp >= window_end:
                continue
            if earliest is None or timestamp < earliest:
                earliest = timestamp
        return earliest

    def _add_numeric_variable(
        self,
        features: dict[str, Any],
        template_id: str,
        var_name: str,
        values: list[Any],
    ) -> None:
        """Aggregate numeric variable values."""
        numeric_values: list[float] = []
        for value in values:
            try:
                numeric_values.append(float(value))
            except (TypeError, ValueError):
                continue
        if not numeric_values:
            return

        mean_key = f"template_{template_id}_{var_name}_mean"
        last_key = f"template_{template_id}_{var_name}_last"
        features[mean_key] = sum(numeric_values) / len(numeric_values)
        features[last_key] = numeric_values[-1]

    def _add_categorical_variable(
        self,
        features: dict[str, Any],
        template_id: str,
        var_name: str,
        values: list[Any],
    ) -> None:
        """Encode categorical variable values as binary indicators."""
        meta = self._summary.templates.get(template_id)
        allowed_values: list[str] = []
        if meta:
            allowed_values = meta.top_values.get(var_name, [])
        if not allowed_values or self._config.max_categorical_values <= 0:
            return

        counts = Counter(str(value) for value in values)
        limit = min(self._config.max_categorical_values, len(allowed_values))
        for value in allowed_values[:limit]:
            if counts.get(value):
                key = f"template_{template_id}_{var_name}={value}"
                features[key] = 1

    @staticmethod
    def _parse_ts(instance: dict[str, Any]) -> datetime | None:
        """Parse timestamp from a template instance."""
        value = instance.get("timestamp") or instance.get("ts")
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

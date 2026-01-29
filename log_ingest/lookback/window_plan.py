"""Core utilities for constructing look-back analysis windows."""
# ruff: noqa

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from log_ingest.cli_commands.bucket_selector import BucketSelector

UTC = timezone.utc


def _ensure_tz(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(slots=True)
class PlannerOptions:
    """Configuration knobs that are optional for building look-back windows."""

    equivalent_templates: Sequence[str] | None = None
    filters: dict[str, Any] | None = None
    anchor_template_id: str | None = None


@dataclass(slots=True)
class IncidentAnchor:
    """Concrete occurrence of an incident template used to seed the window."""

    incident_id: str
    template_id: str
    bucket_id: str
    t_symptom: datetime
    bucket_start: datetime
    bucket_end: datetime
    context: dict[str, Any]


@dataclass(slots=True)
class WindowArtifacts:
    """Group of output files produced for a built look-back window."""

    events_path: Path
    metrics_path: Path
    summary_path: Path
    post_incident_events_path: Path | None = None


class LookBackWindowPlanner:
    """Coordinate construction of incident look-back windows and artifacts."""

    class NoAnchorsFoundError(RuntimeError):
        """Raised when no anchor template occurrences are found."""

    def __init__(
        self,
        bucket_path: Path,
        output_root: Path,
        lead_time_target: int,
        max_lead_time: int,
        options: PlannerOptions | None = None,
    ) -> None:
        """Create a planner with the required storage paths and timing controls."""
        if lead_time_target <= 0:
            raise ValueError("lead_time_target must be positive")
        if max_lead_time < lead_time_target:
            raise ValueError("max_lead_time must be >= lead_time_target")

        self.bucket_path = Path(bucket_path)
        self.output_root = Path(output_root)
        self.lead_time_target = int(lead_time_target)
        self.max_lead_time = int(max_lead_time)
        resolved_options = options or PlannerOptions()
        self.equivalent_templates = list(resolved_options.equivalent_templates or [])
        self.filters = resolved_options.filters or {}
        self._anchor_template_id = resolved_options.anchor_template_id
        self._selector = BucketSelector(self.bucket_path)
        self._bucket_rows: list[dict[str, Any]] | None = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def build_incident_windows(
        self,
        incident_id: str,
        manual_expansion_minutes: int | None = None,
    ) -> WindowArtifacts:
        """Construct positive and control windows for the provided incident."""
        anchor_template = self._determine_anchor_template(incident_id)
        symptom_templates = [anchor_template, *self.equivalent_templates]

        anchors = self._resolve_anchors(
            incident_id=incident_id,
            symptom_templates=symptom_templates,
            anchor_template=anchor_template,
        )
        if not anchors:
            raise self.NoAnchorsFoundError(
                f"No anchors for templates {symptom_templates} with filters {self.filters}"
            )

        primary_anchor = anchors[0]
        base_lead = manual_expansion_minutes or self.lead_time_target
        max_lead = manual_expansion_minutes or self.max_lead_time

        initial_window = self._build_positive_window(primary_anchor, timedelta(minutes=base_lead))
        positive_window, current_lead, expansion_count = self._expand_positive_window(
            anchor=primary_anchor,
            initial_window=initial_window,
            base_lead_minutes=base_lead,
            max_lead_minutes=max_lead,
            manual_expansion_minutes=manual_expansion_minutes,
        )

        controls = self._build_negative_controls(primary_anchor, timedelta(minutes=current_lead))
        coverage_lead_minutes = positive_window["actual_lead_time_minutes"]

        return self._write_artifacts(
            incident_id=incident_id,
            anchor=primary_anchor,
            positive_window=positive_window,
            controls=controls,
            metadata={
                "incident_id": incident_id,
                "lead_time_minutes": base_lead,
                "actual_lead_time_minutes": current_lead,
                "coverage_minutes": coverage_lead_minutes,
                "t_symptom": positive_window["t_symptom"].isoformat(),
                "expansion_count": expansion_count,
                "templates_seen": len(positive_window["templates_seen"]),
                "anchors_analyzed": len(anchors),
                "manual_expansion_override": manual_expansion_minutes,
                "lead_time_used_minutes": current_lead,
            },
        )

    def _expand_positive_window(
        self,
        anchor: IncidentAnchor,
        initial_window: dict[str, Any],
        base_lead_minutes: int,
        max_lead_minutes: int,
        manual_expansion_minutes: int | None,
    ) -> tuple[dict[str, Any], int, int]:
        """Grow the positive window until causal candidates appear or the limit is reached."""
        if manual_expansion_minutes is not None:
            return initial_window, manual_expansion_minutes, 0

        current_lead = base_lead_minutes
        expansion_count = 0
        window = initial_window

        if self._has_causal_candidates(window):
            return window, current_lead, expansion_count

        while current_lead < max_lead_minutes:
            next_lead = min(current_lead * 2, max_lead_minutes)
            if next_lead == current_lead:
                break

            current_lead = next_lead
            window = self._build_positive_window(anchor, timedelta(minutes=current_lead))
            expansion_count += 1

        return window, current_lead, expansion_count

    # ------------------------------------------------------------------ #
    # Anchor resolution
    # ------------------------------------------------------------------ #

    def _determine_anchor_template(self, incident_id: str) -> str:
        if self._anchor_template_id:
            return self._anchor_template_id
        match = re.search(r"(\d+)$", incident_id)
        if not match:
            raise ValueError(
                "Unable to derive anchor template id. Provide anchor_template_id or suffix incident id with digits."
            )
        template_id = match.group(1)
        self._anchor_template_id = template_id
        return template_id

    def _resolve_anchors(
        self,
        incident_id: str,
        symptom_templates: Sequence[str],
        anchor_template: str,
    ) -> list[IncidentAnchor]:
        symptom_set = {str(template_id) for template_id in symptom_templates}
        anchor_template_id = str(anchor_template)
        instances = self._selector.template_instances(
            template_ids=list(symptom_set),
            bucket_filters=self.filters or None,
            include_vars=True,
        )

        grouped: dict[str, list[dict[str, Any]]] = {}
        for inst in instances:
            template_id = str(inst["template_id"])
            if template_id not in symptom_set:
                continue
            grouped.setdefault(inst["bucket_id"], []).append(inst)

        if not grouped:
            return []

        buckets = {row["bucket_id"]: row for row in self._load_bucket_rows()}
        anchors: list[IncidentAnchor] = []
        def _record_sort_key(item: tuple[str, list[dict[str, Any]]]) -> datetime:
            rec = item[1][0]
            return _ensure_tz(rec["start_time"])

        for bucket_id, records in sorted(grouped.items(), key=_record_sort_key):
            bucket = buckets.get(bucket_id)
            if not bucket:
                continue
            t_symptom = min(
                self._coerce_instance_time(
                    rec.get("ts") or rec.get("timestamp"), rec["start_time"]
                )
                for rec in records
            )
            anchor_record = next(
                (
                    rec
                    for rec in records
                    if str(rec["template_id"]) == anchor_template_id
                ),
                None,
            )
            if anchor_record is None:
                continue
            anchors.append(
                IncidentAnchor(
                    incident_id=incident_id,
                    template_id=anchor_record["template_id"],
                    bucket_id=bucket_id,
                    t_symptom=t_symptom,
                    bucket_start=_ensure_tz(bucket["start_time"]),
                    bucket_end=_ensure_tz(bucket["end_time"]),
                    context={"record_count": len(records)},
                )
            )
        return anchors

    # ------------------------------------------------------------------ #
    # Window construction
    # ------------------------------------------------------------------ #

    def _build_positive_window(
        self,
        anchor: IncidentAnchor,
        lead_time: timedelta,
    ) -> dict[str, Any]:
        window_start = anchor.t_symptom - lead_time
        window_end = anchor.t_symptom
        buckets = self._select_buckets(window_start, window_end)

        (
            positive_events,
            post_incident_events,
            candidate_templates,
            templates_seen,
        ) = self._partition_positive_events(anchor, buckets)

        metrics = self._build_slice_metrics(buckets)
        actual_lead_minutes = self._calculate_actual_lead_minutes(anchor, buckets)

        return {
            "events": positive_events,
            "post_incident_events": post_incident_events,
            "metrics": metrics,
            "candidate_templates": candidate_templates,
            "templates_seen": templates_seen,
            "actual_lead_time_minutes": actual_lead_minutes,
            "t_symptom": anchor.t_symptom,
        }

    def _partition_positive_events(
        self,
        anchor: IncidentAnchor,
        buckets: Sequence[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str], set[str]]:
        positive_events: list[dict[str, Any]] = []
        post_incident_events: list[dict[str, Any]] = []
        candidate_templates: set[str] = set()
        templates_seen: set[str] = set()

        for bucket in buckets:
            for event in self._flatten_bucket_events(bucket):
                classification = self._classify_event(anchor.t_symptom, event["event_time"])
                if classification == "pre":
                    positive_events.append(
                        self._annotate_event(event, window_type="positive", phase="pre_incident")
                    )
                    template_id = event["template_id"]
                    templates_seen.add(template_id)
                    if template_id != anchor.template_id and template_id not in self.equivalent_templates:
                        candidate_templates.add(template_id)
                elif classification == "post":
                    post_incident_events.append(
                        self._annotate_event(event, window_type="positive", phase="post_incident")
                    )

        positive_events.sort(key=lambda record: record["event_time"])
        post_incident_events.sort(key=lambda record: record["event_time"])

        return positive_events, post_incident_events, candidate_templates, templates_seen

    @staticmethod
    def _classify_event(anchor_time: datetime, event_time: datetime) -> str:
        if event_time < anchor_time:
            return "pre"
        if event_time > anchor_time:
            return "post"
        return "ignore"

    @staticmethod
    def _annotate_event(event: dict[str, Any], window_type: str, phase: str) -> dict[str, Any]:
        return {**event, "window_type": window_type, "phase": phase}

    @staticmethod
    def _calculate_actual_lead_minutes(
        anchor: IncidentAnchor, buckets: Sequence[dict[str, Any]]
    ) -> int:
        if not buckets:
            return 0
        actual_lead_start = buckets[0]["start_time"]
        delta = anchor.t_symptom - _ensure_tz(actual_lead_start)
        return max(0, int(delta.total_seconds() // 60))

    def _has_causal_candidates(self, window_payload: dict[str, Any]) -> bool:
        return bool(window_payload.get("candidate_templates"))

    def _build_negative_controls(
        self,
        anchor: IncidentAnchor,
        lead_time: timedelta,
    ) -> list[dict[str, Any]]:
        window_start = anchor.t_symptom - lead_time
        window_end = anchor.t_symptom
        buckets = self._select_buckets(window_start, window_end)

        control_buckets = [bucket for bucket in buckets if bucket["bucket_id"] != anchor.bucket_id]
        control_metrics = self._build_slice_metrics(control_buckets)
        control_events = self._collect_control_events(control_buckets)

        return [
            {
                "events": control_events,
                "metrics": control_metrics,
                "bucket_ids": [bucket["bucket_id"] for bucket in control_buckets],
            }
        ]

    def _collect_control_events(self, buckets: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for bucket in buckets:
            for event in self._flatten_bucket_events(bucket):
                events.append(self._annotate_event(event, window_type="control", phase="control"))
        events.sort(key=lambda record: record["event_time"])
        return events

    # ------------------------------------------------------------------ #
    # Data access helpers
    # ------------------------------------------------------------------ #

    def _load_bucket_rows(self) -> list[dict[str, Any]]:
        if self._bucket_rows is not None:
            return self._bucket_rows

        normalised: list[dict[str, Any]] = []
        parquet = pq.ParquetFile(self.bucket_path)
        for batch in parquet.iter_batches():
            for row in batch.to_pylist():
                start_time = _ensure_tz(row.get("start_time"))
                end_time = _ensure_tz(row.get("end_time"))
                templates_payload = row.get("templates")
                if isinstance(templates_payload, str):
                    try:
                        templates_dict = json.loads(templates_payload) or {}
                    except json.JSONDecodeError:
                        templates_dict = {}
                elif isinstance(templates_payload, dict):
                    templates_dict = templates_payload
                else:
                    templates_dict = {}
                row["start_time"] = start_time
                row["end_time"] = end_time
                row["templates"] = templates_dict
                normalised.append(row)

        normalised.sort(key=lambda r: r["start_time"])
        self._bucket_rows = normalised
        return self._bucket_rows

    def _select_buckets(self, window_start: datetime, window_end: datetime) -> list[dict[str, Any]]:
        buckets = []
        for bucket in self._load_bucket_rows():
            if not self._bucket_matches_filters(bucket):
                continue
            if (
                bucket["end_time"]
                and bucket["end_time"] > window_start
                and bucket["start_time"] <= window_end
            ):
                buckets.append(bucket)
        return buckets

    def _bucket_matches_filters(self, bucket: dict[str, Any]) -> bool:
        for key, expected in self.filters.items():
            if bucket.get(key) != expected:
                return False
        return True

    def _flatten_bucket_events(self, bucket: dict[str, Any]) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        templates = bucket.get("templates") or {}
        for template_id, template_info in templates.items():
            template_str = template_info.get("template_str")
            instances = template_info.get("instances") or []
            for index, instance in enumerate(instances):
                event_time = self._coerce_instance_time(
                    instance.get("ts") or instance.get("timestamp"), bucket["start_time"]
                )
                records.append(
                    {
                        "bucket_id": bucket["bucket_id"],
                        "template_id": template_id,
                        "template_str": template_str,
                        "event_time": event_time,
                        "instance_index": index,
                        "cluster_id": bucket.get("cluster_id"),
                        "system_id": bucket.get("system_id"),
                        "metadata": {k: v for k, v in instance.items() if k != "ts"},
                    }
                )
        return records

    @staticmethod
    def _coerce_instance_time(value: Any, fallback: datetime) -> datetime:
        if isinstance(value, datetime):
            return _ensure_tz(value)
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                return _ensure_tz(fallback)
            return _ensure_tz(parsed)
        return _ensure_tz(fallback)

    def _build_slice_metrics(self, buckets: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        slices: list[dict[str, Any]] = []
        prev_totals: dict[str, Any] | None = None
        for bucket in sorted(buckets, key=lambda b: b["start_time"]):
            metrics = {
                "slice_start": bucket["start_time"],
                "slice_end": bucket["end_time"],
                "total_events": bucket.get("total_events", 0) or 0,
                "error_count": bucket.get("error_count", 0) or 0,
                "warning_count": bucket.get("warning_count", 0) or 0,
                "info_count": bucket.get("info_count", 0) or 0,
            }
            if prev_totals is None:
                metrics.update(
                    {
                        "delta_total_events": 0,
                        "delta_error_count": 0,
                        "delta_warning_count": 0,
                        "delta_info_count": 0,
                    }
                )
            else:
                metrics.update(
                    {
                        "delta_total_events": metrics["total_events"] - prev_totals["total_events"],
                        "delta_error_count": metrics["error_count"] - prev_totals["error_count"],
                        "delta_warning_count": metrics["warning_count"] - prev_totals["warning_count"],
                        "delta_info_count": metrics["info_count"] - prev_totals["info_count"],
                    }
                )
            prev_totals = metrics
            slices.append(metrics)
        return slices

    # ------------------------------------------------------------------ #
    # Artifact writing
    # ------------------------------------------------------------------ #

    def _write_artifacts(
        self,
        incident_id: str,
        anchor: IncidentAnchor,
        positive_window: dict[str, Any],
        controls: list[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> WindowArtifacts:
        run_ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        base_dir = self.output_root / incident_id / run_ts
        base_dir.mkdir(parents=True, exist_ok=True)

        events_path = base_dir / "window_events.parquet"
        metrics_path = base_dir / "window_metrics.parquet"
        summary_path = base_dir / "window_summary.json"
        post_path: Path | None = None

        events_rows = list(positive_window["events"])
        for control in controls:
            events_rows.extend(control["events"])

        if events_rows:
            events_table = pa.Table.from_pylist(events_rows)
            pq.write_table(events_table, events_path)
        else:
            events_table = pa.table({"window_type": [], "phase": [], "event_time": []})
            pq.write_table(events_table, events_path)

        metrics_rows = []
        for record in positive_window["metrics"]:
            metrics_rows.append({**record, "window_type": "positive"})
        for control in controls:
            for record in control["metrics"]:
                metrics_rows.append({**record, "window_type": "control"})

        if metrics_rows:
            metrics_table = pa.Table.from_pylist(metrics_rows)
            pq.write_table(metrics_table, metrics_path)
        else:
            metrics_table = pa.table({"window_type": [], "slice_start": [], "slice_end": []})
            pq.write_table(metrics_table, metrics_path)

        post_events = positive_window.get("post_incident_events") or []
        if post_events:
            post_path = base_dir / "post_incident_events.parquet"
            pq.write_table(pa.Table.from_pylist(post_events), post_path)

        with summary_path.open("w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2, default=_json_datetime_encoder)

        return WindowArtifacts(
            events_path=events_path,
            metrics_path=metrics_path,
            summary_path=summary_path,
            post_incident_events_path=post_path,
        )


def _json_datetime_encoder(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, math.nan.__class__):
        return None
    raise TypeError(f"Object of type {type(value)!r} is not JSON serialisable")

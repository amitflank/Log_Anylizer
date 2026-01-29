"""Utilities for serialising look-back windows for downstream analysis."""
# ruff: noqa: I001

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from itertools import combinations
from pathlib import Path
from typing import Literal, TypedDict
import pickle
import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from gbdt_pipeline.exclusion import TemplateExclusion
from gbdt_pipeline.incident_catalog import IncidentTarget
from gbdt_pipeline.label_builder import BucketLabel, LabelBuilder
from gbdt_pipeline.window_extractor import (
    LookbackWindow,
    LookbackWindowExtractor,
)
from log_ingest.bucket_building.time_bucket import TimeBucket

class EventRecord(TypedDict):
    """Single template instance captured within a bucket."""

    template_id: str
    timestamp: datetime


class BucketSnapshot(TypedDict, total=False):
    """Captured view of a single bucket inside a window."""

    bucket_id: str
    start: datetime
    end: datetime
    features: dict
    events: list[EventRecord]
    shap: dict | None


class WindowSnapshot(TypedDict):
    """Captured view of a look-back window."""

    bucket_id: str
    label: int
    window_start: datetime | None
    window_end: datetime | None
    buckets: list[BucketSnapshot]


@dataclass(frozen=True)
class WindowSnapshotArtifacts:
    """Locations of persisted window snapshot data."""

    root: Path
    bucket_path: Path
    event_path: Path
    window_path: Path
    window_ids: tuple[str, ...]


class WindowSnapshotStore:
    """Write window snapshots to disk incrementally to avoid holding everything in memory."""

    def __init__(self, directory: Path) -> None:
        """Initialise writers for window snapshot outputs."""
        self._directory = directory
        self._directory.mkdir(parents=True, exist_ok=True)
        self._bucket_path = directory / "window_buckets.parquet"
        self._event_path = directory / "window_events.parquet"
        self._window_path = directory / "window_index.parquet"
        self._bucket_writer: pq.ParquetWriter | None = None
        self._event_writer: pq.ParquetWriter | None = None
        self._window_writer: pq.ParquetWriter | None = None
        self._window_ids: list[str] = []
        self._id_counter = 0

    @property
    def artifacts(self) -> WindowSnapshotArtifacts | None:
        """Return artifact locations if any windows have been written."""
        if not self._window_ids:
            return None
        return WindowSnapshotArtifacts(
            root=self._directory,
            bucket_path=self._bucket_path,
            event_path=self._event_path,
            window_path=self._window_path,
            window_ids=tuple(self._window_ids),
        )

    def append_window(self, label: BucketLabel, window: LookbackWindow) -> str:
        """Persist a single window and return its allocated identifier."""
        window_id = self._allocate_window_id(label.bucket_id)
        buckets = window.buckets
        if not buckets:
            return window_id

        bucket_rows: list[dict] = []
        event_rows: list[dict] = []
        for index, bucket in enumerate(buckets):
            bucket_rows.append(
                {
                    "window_id": window_id,
                    "bucket_index": index,
                    "bucket_id": bucket.bucket_id,
                    "start": _to_iso(bucket.start_time),
                    "end": _to_iso(bucket.end_time),
                    "features": pickle.dumps(asdict(bucket)),
                }
            )
            for event in _collect_events(bucket):
                event_rows.append(
                    {
                        "window_id": window_id,
                        "bucket_id": bucket.bucket_id,
                        "template_id": event["template_id"],
                        "timestamp": _to_iso(event["timestamp"]),
                    }
                )

        window_row = {
            "window_id": window_id,
            "bucket_id": label.bucket_id,
            "label": label.label,
            "window_start": _to_iso(buckets[0].start_time),
            "window_end": _to_iso(buckets[-1].end_time),
        }

        self._write_bucket_rows(bucket_rows)
        self._write_event_rows(event_rows)
        self._write_window_row(window_row)
        self._window_ids.append(window_id)
        return window_id

    def close(self) -> WindowSnapshotArtifacts | None:
        """Close writers and return artifact locations."""
        if self._bucket_writer:
            self._bucket_writer.close()
            self._bucket_writer = None
        if self._event_writer:
            self._event_writer.close()
            self._event_writer = None
        if self._window_writer:
            self._window_writer.close()
            self._window_writer = None
        return self.artifacts

    def _allocate_window_id(self, bucket_id: str | None) -> str:
        anchor = bucket_id or "window"
        identifier = f"{anchor}_{self._id_counter:06d}"
        self._id_counter += 1
        return identifier

    def _write_bucket_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        schema = pa.schema(
            [
                ("window_id", pa.string()),
                ("bucket_index", pa.int32()),
                ("bucket_id", pa.string()),
                ("start", pa.string()),
                ("end", pa.string()),
                ("features", pa.binary()),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        if self._bucket_writer is None:
            self._bucket_writer = pq.ParquetWriter(self._bucket_path, schema)
        self._bucket_writer.write_table(table)

    def _write_event_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        schema = pa.schema(
            [
                ("window_id", pa.string()),
                ("bucket_id", pa.string()),
                ("template_id", pa.string()),
                ("timestamp", pa.string()),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        if self._event_writer is None:
            self._event_writer = pq.ParquetWriter(self._event_path, schema)
        self._event_writer.write_table(table)

    def _write_window_row(self, row: dict) -> None:
        schema = pa.schema(
            [
                ("window_id", pa.string()),
                ("bucket_id", pa.string()),
                ("label", pa.int32()),
                ("window_start", pa.string()),
                ("window_end", pa.string()),
            ]
        )
        table = pa.Table.from_pylist([row], schema=schema)
        if self._window_writer is None:
            self._window_writer = pq.ParquetWriter(self._window_path, schema)
        self._window_writer.write_table(table)

@dataclass(frozen=True)
class NegativeWindowStrategy:
    """Configuration controlling how negative windows are selected."""

    mode: Literal["time-matched", "template-matched", "none"] = "time-matched"
    time_tolerance_minutes: int = 60
    max_negatives_per_positive: int = 1
    skip_adjacency: bool = True
    symptom_templates: tuple[str, ...] = ()
    max_total_negatives: int | None = None


def build_window_snapshots(
    window_records: Iterable[tuple[BucketLabel, LookbackWindow]],
    store: WindowSnapshotStore,
) -> list[str]:
    """Persist look-back windows using the provided snapshot store."""
    window_ids: list[str] = []
    for label, window in window_records:
        window_ids.append(store.append_window(label, window))
    return window_ids


def _collect_events(bucket: TimeBucket) -> list[EventRecord]:
    records: list[EventRecord] = []
    templates = bucket.templates or {}
    for template_id, payload in templates.items():
        for instance in payload.get("instances", []) or []:
            timestamp = _parse_instance_timestamp(instance)
            if timestamp is None:
                continue
            records.append(
                EventRecord(
                    template_id=str(template_id),
                    timestamp=timestamp,
                )
            )
    records.sort(key=lambda record: record["timestamp"])
    return records


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _from_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _parse_instance_timestamp(instance: dict) -> datetime | None:
    value = instance.get("timestamp") or instance.get("ts")
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def attach_shap_to_snapshots(
    snapshots: list[WindowSnapshot],
    shap_rows: Iterable[dict],
) -> None:
    """Attach SHAP contribution vectors to buckets by bucket_id."""
    shap_by_bucket: dict[str, dict] = {}
    for row in shap_rows:
        bucket_id = row.get("bucket_id")
        contributions = row.get("contributions")
        if not bucket_id or not isinstance(contributions, dict):
            continue
        shap_by_bucket[str(bucket_id)] = contributions

    for snapshot in snapshots:
        for bucket in snapshot["buckets"]:
            contrib = shap_by_bucket.get(bucket["bucket_id"])
            if contrib is not None:
                bucket["shap"] = contrib


def summarise_window(
    window: WindowSnapshot,
    feature_cols: list[str],
) -> dict[str, float]:
    """Compute mean SHAP value per feature for the provided window."""
    bucket_count = len(window["buckets"])
    if bucket_count == 0:
        return {f"shap_mean::{feature}": 0.0 for feature in feature_cols}

    totals: dict[str, float] = {feature: 0.0 for feature in feature_cols}
    for bucket in window["buckets"]:
        contributions = bucket.get("shap") or {}
        for feature in feature_cols:
            totals[feature] += float(contributions.get(feature, 0.0))

    return {
        f"shap_mean::{feature}": totals[feature] / bucket_count
        for feature in feature_cols
    }


def build_binary_itemset(aggregates: dict[str, float], threshold: float) -> set[str]:
    """Convert mean SHAP aggregates into a binary itemset based on a threshold."""
    items: set[str] = set()
    prefix = "shap_mean::"
    for feature, value in aggregates.items():
        if not feature.startswith(prefix):
            continue
        if value >= threshold:
            items.add(feature.removeprefix(prefix))
    return items


def compute_window_summaries(
    artifacts: WindowSnapshotArtifacts,
    shap_path: Path,
    feature_cols: list[str],
) -> list[dict[str, float]]:
    """Compute mean SHAP per feature for each stored window."""
    bucket_map, bucket_counts = _load_bucket_map_and_counts(artifacts.bucket_path)
    totals = _accumulate_shap_totals(bucket_map, shap_path, feature_cols)
    return _totals_to_means(totals, bucket_counts, feature_cols, artifacts.window_ids)


def compute_interaction_summaries(
    artifacts: WindowSnapshotArtifacts,
    interaction_batches: Iterable[tuple[Sequence[str], np.ndarray, list[str]]],
) -> list[dict[tuple[str, str], dict[str, float]]]:
    """Compute mean signed and absolute interaction strength per feature pair for each window."""
    bucket_map, _ = _load_bucket_map_and_counts(artifacts.bucket_path)
    pair_totals: dict[str, dict[tuple[str, str], dict[str, float]]] = defaultdict(dict)

    for bucket_ids, interactions, feature_names in interaction_batches:
        if not _valid_interaction_batch(bucket_ids, interactions):
            continue
        _accumulate_interaction_batch(
            bucket_ids=bucket_ids,
            interactions=interactions,
            feature_names=feature_names,
            bucket_map=bucket_map,
            pair_totals=pair_totals,
        )

    return _finalise_interaction_summaries(pair_totals, artifacts.window_ids)


def extract_top_interactions(
    interaction_summaries: list[dict[tuple[str, str], dict[str, float]]],
    top_k: int = 10,
) -> list[dict]:
    """Extract strongest pairwise interactions across all windows."""
    aggregate: dict[tuple[str, str], dict[str, float]] = {}
    for summary in interaction_summaries:
        for pair, stats in summary.items():
            count = stats.get("count", 0)
            if count == 0:
                continue
            agg = aggregate.setdefault(
                pair, {"sum_abs": 0.0, "sum_signed": 0.0, "total_count": 0, "windows": 0}
            )
            agg["sum_abs"] += stats["mean_abs"] * count
            agg["sum_signed"] += stats["mean_signed"] * count
            agg["total_count"] += count
            agg["windows"] += 1

    ranked = sorted(
        aggregate.items(),
        key=lambda item: abs(item[1]["sum_abs"] / item[1]["total_count"])
        if item[1]["total_count"]
        else 0.0,
        reverse=True,
    )[:top_k]

    results: list[dict] = []
    for (feature_a, feature_b), stats in ranked:
        total = stats["total_count"] or 1
        results.append(
            {
                "feature_a": feature_a,
                "feature_b": feature_b,
                "mean_abs_interaction": stats["sum_abs"] / total,
                "mean_signed_interaction": stats["sum_signed"] / total,
                "windows_with_interaction": stats["windows"],
            }
        )
    return results


def _valid_interaction_batch(bucket_ids: Sequence[str], interactions: np.ndarray) -> bool:
    return interactions is not None and len(bucket_ids) == len(interactions)


def _pair_indices(size: int) -> list[tuple[int, int]]:
    return [(i, j) for i in range(size) for j in range(i + 1, size)]


def _accumulate_interaction_batch(
    bucket_ids: Sequence[str],
    interactions: np.ndarray,
    feature_names: list[str],
    bucket_map: dict[str, list[str]],
    pair_totals: dict[str, dict[tuple[str, str], dict[str, float]]],
) -> None:
    pair_indices = _pair_indices(len(feature_names))
    for row_idx, bucket_id in enumerate(bucket_ids):
        window_ids = bucket_map.get(bucket_id, [])
        if not window_ids:
            continue
        matrix = interactions[row_idx]
        _accumulate_interaction_row(
            matrix, feature_names, window_ids, pair_indices, pair_totals
        )


def _accumulate_interaction_row(
    matrix: np.ndarray,
    feature_names: list[str],
    window_ids: Sequence[str],
    pair_indices: list[tuple[int, int]],
    pair_totals: dict[str, dict[tuple[str, str], dict[str, float]]],
) -> None:
    for i, j in pair_indices:
        pair_key = (feature_names[i], feature_names[j])
        value = float(matrix[i, j])
        for window_id in window_ids:
            _update_pair_totals(pair_totals, window_id, pair_key, value)


def _update_pair_totals(
    pair_totals: dict[str, dict[tuple[str, str], dict[str, float]]],
    window_id: str,
    pair_key: tuple[str, str],
    value: float,
) -> None:
    stats = pair_totals.setdefault(window_id, {}).setdefault(
        pair_key, {"sum_signed": 0.0, "sum_abs": 0.0, "count": 0}
    )
    stats["sum_signed"] += value
    stats["sum_abs"] += abs(value)
    stats["count"] += 1


def _finalise_interaction_summaries(
    pair_totals: dict[str, dict[tuple[str, str], dict[str, float]]],
    window_ids: Sequence[str],
) -> list[dict[tuple[str, str], dict[str, float]]]:
    summaries: list[dict[tuple[str, str], dict[str, float]]] = []
    for window_id in window_ids:
        window_pairs = {
            pair: _summarise_pair(stats) for pair, stats in pair_totals.get(window_id, {}).items()
        }
        summaries.append(window_pairs)
    return summaries


def _summarise_pair(stats: dict[str, float]) -> dict[str, float]:
    count = stats["count"] or 1
    return {
        "mean_signed": stats["sum_signed"] / count,
        "mean_abs": stats["sum_abs"] / count,
        "count": stats["count"],
    }


def _load_bucket_map_and_counts(bucket_path: Path) -> tuple[dict[str, list[str]], dict[str, int]]:
    bucket_map: dict[str, list[str]] = defaultdict(list)
    counts: dict[str, int] = defaultdict(int)
    parquet = pq.ParquetFile(bucket_path)
    for batch in parquet.iter_batches(columns=["window_id", "bucket_id"]):
        for record in batch.to_pylist():
            bucket_map[record["bucket_id"]].append(record["window_id"])
            counts[record["window_id"]] += 1
    return bucket_map, counts


def _accumulate_shap_totals(
    bucket_map: dict[str, list[str]],
    shap_path: Path,
    feature_cols: list[str],
) -> dict[str, list[float]]:
    totals: dict[str, list[float]] = defaultdict(lambda: [0.0] * len(feature_cols))
    feature_index = {feature: idx for idx, feature in enumerate(feature_cols)}
    parquet = pq.ParquetFile(shap_path)
    for batch in parquet.iter_batches(columns=["bucket_id", "contributions"]):
        for record in batch.to_pylist():
            window_ids = bucket_map.get(record["bucket_id"], [])
            updates = _build_feature_updates(record["contributions"], feature_index)
            _apply_updates_to_windows(totals, window_ids, updates)
    return totals


def _build_feature_updates(
    contributions_json: str, feature_index: dict[str, int]
) -> list[tuple[int, float]]:
    updates: list[tuple[int, float]] = []
    contributions = json.loads(contributions_json) if contributions_json else {}
    for feature, value in contributions.items():
        idx = feature_index.get(feature)
        if idx is not None:
            updates.append((idx, float(value)))
    return updates


def _apply_updates_to_windows(
    totals: dict[str, list[float]], window_ids: list[str], updates: list[tuple[int, float]]
) -> None:
    for window_id in window_ids:
        window_totals = totals[window_id]
        for idx, value in updates:
            window_totals[idx] += value


def _totals_to_means(
    totals: dict[str, list[float]],
    counts: dict[str, int],
    feature_cols: list[str],
    window_ids: tuple[str, ...],
) -> list[dict[str, float]]:
    summaries: list[dict[str, float]] = []
    feature_index = {feature: idx for idx, feature in enumerate(feature_cols)}
    for window_id in window_ids:
        window_totals = totals.get(window_id, [0.0] * len(feature_cols))
        count = counts.get(window_id, 0) or 1
        summary = {
            f"shap_mean::{feature}": window_totals[idx] / count
            for feature, idx in feature_index.items()
        }
        summaries.append(summary)
    return summaries


def load_event_sequences(artifacts: WindowSnapshotArtifacts) -> list[list[tuple[datetime, str]]]:
    """Load ordered template sequences for each window from persisted events."""
    sequences: dict[str, list[tuple[datetime, str]]] = {
        window_id: [] for window_id in artifacts.window_ids
    }
    if artifacts.event_path.exists():
        event_file = pq.ParquetFile(artifacts.event_path)
        for batch in event_file.iter_batches():
            for record in batch.to_pylist():
                window_id = record["window_id"]
                sequences.setdefault(window_id, []).append(
                    (_from_iso(record["timestamp"]), record["template_id"])
                )
    ordered: list[list[tuple[datetime, str]]] = []
    for window_id in artifacts.window_ids:
        events = sequences.get(window_id, [])
        events.sort(key=lambda item: item[0])
        ordered.append(events)
    return ordered


def load_window_snapshots(artifacts: WindowSnapshotArtifacts) -> list[WindowSnapshot]:
    """Reconstruct WindowSnapshot objects from the on-disk representation (testing/debug)."""
    if not artifacts.bucket_path.exists() or not artifacts.window_path.exists():
        return []

    window_records = {
        row["window_id"]: row
        for row in pq.read_table(artifacts.window_path).to_pylist()
    }
    buckets_by_window: dict[str, list[tuple[int, BucketSnapshot]]] = defaultdict(list)
    bucket_table = pq.read_table(artifacts.bucket_path)
    for record in bucket_table.to_pylist():
        bucket_snapshot: BucketSnapshot = {
            "bucket_id": record["bucket_id"],
            "start": _from_iso(record["start"]),
            "end": _from_iso(record["end"]),
            "features": pickle.loads(record["features"]),
            "events": [],
            "shap": None,
        }
        buckets_by_window[record["window_id"]].append(
            (record["bucket_index"], bucket_snapshot)
        )

    events_by_bucket: dict[str, list[EventRecord]] = defaultdict(list)
    if artifacts.event_path.exists():
        event_table = pq.read_table(artifacts.event_path)
        for record in event_table.to_pylist():
            events_by_bucket[record["bucket_id"]].append(
                EventRecord(
                    template_id=str(record["template_id"]),
                    timestamp=_from_iso(record["timestamp"]),
                )
            )
    for bucket_list in buckets_by_window.values():
        for _, snapshot in bucket_list:
            snapshot["events"] = sorted(
                events_by_bucket.get(snapshot["bucket_id"], []),
                key=lambda item: item["timestamp"],
            )

    snapshots: list[WindowSnapshot] = []
    for window_id in artifacts.window_ids:
        info = window_records.get(window_id)
        if info is None:
            continue
        ordered_buckets = [
            bucket
            for _, bucket in sorted(
                buckets_by_window.get(window_id, []), key=lambda item: item[0]
            )
        ]
        snapshots.append(
            WindowSnapshot(
                bucket_id=info["bucket_id"],
                label=int(info["label"]),
                window_start=_from_iso(info["window_start"]),
                window_end=_from_iso(info["window_end"]),
                buckets=ordered_buckets,
            )
        )
    return snapshots


def select_negative_windows(
    buckets: Sequence[TimeBucket],
    target: IncidentTarget,
    exclusion: TemplateExclusion | None,
    strategy: NegativeWindowStrategy,
    episode_gap_minutes: int | None = None,
) -> list[tuple[BucketLabel, LookbackWindow]]:
    """Select negative look-back windows according to the provided strategy."""
    if strategy.mode == "none" or strategy.max_negatives_per_positive <= 0:
        return []

    label_builder = LabelBuilder(buckets)
    labeled = label_builder.build_labels(target)
    extractor = LookbackWindowExtractor(buckets, exclusion=exclusion)

    positives = labeled.positives
    if episode_gap_minutes is not None:
        positives = LabelBuilder.collapse_positives_by_gap(
            positives, gap_minutes=int(episode_gap_minutes)
        )
    positive_pairs = _build_window_pairs(extractor, positives, target)
    negative_pairs = _build_window_pairs(extractor, labeled.negatives, target)

    if not negative_pairs:
        return []

    if strategy.mode == "time-matched":
        return _select_time_matched_negatives(
            positive_pairs,
            negative_pairs,
            strategy,
            lookback_minutes=target.initial_lookback_minutes,
        )
    if strategy.mode == "template-matched":
        return _select_template_matched_negatives(
            positive_pairs,
            negative_pairs,
            strategy,
            lookback_minutes=target.initial_lookback_minutes,
        )
    return []


def _build_window_pairs(
    extractor: LookbackWindowExtractor,
    labels: list[BucketLabel],
    target: IncidentTarget,
) -> list[tuple[BucketLabel, LookbackWindow]]:
    pairs: list[tuple[BucketLabel, LookbackWindow]] = []
    seen: set[str] = set()
    for label in labels:
        key = _label_bucket_key(label)
        if key in seen:
            continue
        try:
            window = extractor.build_window(label, target)
        except ValueError:
            continue
        if not window.buckets:
            continue
        pairs.append((label, window))
        seen.add(key)
    return pairs


def _select_time_matched_negatives(
    positives: list[tuple[BucketLabel, LookbackWindow]],
    negatives: list[tuple[BucketLabel, LookbackWindow]],
    strategy: NegativeWindowStrategy,
    lookback_minutes: int,
) -> list[tuple[BucketLabel, LookbackWindow]]:
    max_total = _compute_max_total(strategy, len(positives))
    if max_total == 0:
        return []

    all_positive_ends = _collect_positive_ends(positives)
    state = _TimeMatchState(strategy, max_total, all_positive_ends, lookback_minutes)
    return state.select_from(positives, negatives)


def _compute_max_total(strategy: NegativeWindowStrategy, num_positives: int) -> int:
    """Compute the maximum total negatives to select."""
    max_total = strategy.max_total_negatives
    if max_total is None:
        max_total = strategy.max_negatives_per_positive * num_positives
    return max(0, max_total)


def _collect_positive_ends(positives: list[tuple[BucketLabel, LookbackWindow]]) -> list[datetime]:
    """Collect all positive window end times for global overlap checking."""
    ends = []
    for label, _ in positives:
        end = _label_window_end(label)
        if end is not None:
            ends.append(end)
    return ends


class _TimeMatchState:
    """Internal state for time-matched negative selection."""

    def __init__(
        self,
        strategy: NegativeWindowStrategy,
        max_total: int,
        positive_ends: list[datetime],
        lookback_minutes: int,
    ) -> None:
        self.strategy = strategy
        self.max_total = max_total
        self.positive_ends = positive_ends
        self.lookback_minutes = lookback_minutes
        self.tolerance = timedelta(minutes=strategy.time_tolerance_minutes)
        self.selected: list[tuple[BucketLabel, LookbackWindow]] = []
        self.used: set[str] = set()
        self.assigned: defaultdict[str, int] = defaultdict(int)

    def select_from(
        self,
        positives: list[tuple[BucketLabel, LookbackWindow]],
        negatives: list[tuple[BucketLabel, LookbackWindow]],
    ) -> list[tuple[BucketLabel, LookbackWindow]]:
        """Select negatives matched to each positive."""
        for pos_label, _ in positives:
            if len(self.selected) >= self.max_total:
                break
            self._process_positive(pos_label, negatives)
        return self.selected

    def _process_positive(
        self, pos_label: BucketLabel, negatives: list[tuple[BucketLabel, LookbackWindow]]
    ) -> None:
        pos_key = _label_bucket_key(pos_label)
        if self.assigned[pos_key] >= self.strategy.max_negatives_per_positive:
            return
        pos_end = _label_window_end(pos_label)
        if pos_end is None:
            return
        candidates = self._find_candidates(pos_key, pos_end, negatives)
        self._select_from_candidates(pos_key, candidates)

    def _find_candidates(
        self,
        pos_key: str,
        pos_end: datetime,
        negatives: list[tuple[BucketLabel, LookbackWindow]],
    ) -> list[tuple[tuple[int, float], tuple[BucketLabel, LookbackWindow]]]:
        candidates = []
        for neg_pair in negatives:
            score = self._score_candidate(pos_key, pos_end, neg_pair)
            if score is not None:
                candidates.append((score, neg_pair))
        candidates.sort(key=lambda item: item[0])
        return candidates

    def _score_candidate(
        self,
        pos_key: str,
        pos_end: datetime,
        neg_pair: tuple[BucketLabel, LookbackWindow],
    ) -> tuple[int, float] | None:
        neg_label, _ = neg_pair
        neg_key = _label_bucket_key(neg_label)
        if neg_key in self.used or neg_key == pos_key:
            return None
        neg_end = _label_window_end(neg_label)
        if neg_end is None:
            return None
        delta = neg_end - pos_end
        if abs(delta) > self.tolerance:
            return None
        if self.strategy.skip_adjacency and _overlaps_any_positive(
            neg_end, self.positive_ends, self.lookback_minutes
        ):
            return None
        priority = 0 if delta >= timedelta(0) else 1
        return (priority, abs(delta.total_seconds()))

    def _select_from_candidates(
        self,
        pos_key: str,
        candidates: list[tuple[tuple[int, float], tuple[BucketLabel, LookbackWindow]]],
    ) -> None:
        for _, pair in candidates:
            if self.assigned[pos_key] >= self.strategy.max_negatives_per_positive:
                break
            if len(self.selected) >= self.max_total:
                break
            self.selected.append(pair)
            self.used.add(_label_bucket_key(pair[0]))
            self.assigned[pos_key] += 1


def _overlaps_any_positive(
    neg_end: datetime,
    positive_ends: list[datetime],
    lookback_minutes: int,
) -> bool:
    """Check if a negative window overlaps with ANY positive's lookback period."""
    return any(
        _windows_overlap(pos_end, neg_end, lookback_minutes) for pos_end in positive_ends
    )


def _select_template_matched_negatives(
    positives: list[tuple[BucketLabel, LookbackWindow]],
    negatives: list[tuple[BucketLabel, LookbackWindow]],
    strategy: NegativeWindowStrategy,
    lookback_minutes: int,
) -> list[tuple[BucketLabel, LookbackWindow]]:
    if not strategy.symptom_templates:
        return []

    selected: list[tuple[BucketLabel, LookbackWindow]] = []
    limit = strategy.max_total_negatives
    if limit is None:
        limit = strategy.max_negatives_per_positive * len(positives)
    limit = max(0, limit)
    if limit == 0:
        return []

    positive_info = [(label.bucket_id, _label_window_end(label)) for label, _ in positives]

    for neg_pair in negatives:
        if len(selected) >= limit:
            break
        neg_label, neg_window = neg_pair
        if any(neg_label.bucket_id == bucket_id for bucket_id, _ in positive_info):
            continue
        neg_end = _label_window_end(neg_label)
        if neg_end is None:
            continue
        if strategy.skip_adjacency and any(
            end is not None and _windows_overlap(end, neg_end, lookback_minutes)
            for _, end in positive_info
        ):
            continue
        if not _window_contains_templates(neg_window, set(strategy.symptom_templates)):
            continue
        selected.append(neg_pair)

    return selected


def _label_window_end(label: BucketLabel) -> datetime | None:
    return label.window_end or label.start_time


def _windows_overlap(
    end_a: datetime,
    end_b: datetime,
    lookback_minutes: int,
) -> bool:
    span = timedelta(minutes=lookback_minutes)
    start_a = end_a - span
    start_b = end_b - span
    latest_start = max(start_a, start_b)
    earliest_end = min(end_a, end_b)
    return latest_start <= earliest_end


def _window_contains_templates(window: LookbackWindow, template_ids: set[str]) -> bool:
    if not template_ids:
        return False
    for bucket in window.buckets:
        for template_id in (bucket.templates or {}):
            if template_id in template_ids:
                return True
    return False


def _label_bucket_key(label: BucketLabel) -> str:
    return label.bucket_id or f"bucketless::{id(label)}"


def cluster_windows(
    window_vectors: Sequence[dict[str, float]],
    max_clusters: int = 5,
    top_features: int = 5,
) -> list[dict]:
    """Group windows by their dominant feature."""
    clusters: dict[str, list[dict[str, float]]] = {}
    prefix = "shap_mean::"

    for vector in window_vectors:
        if not vector:
            continue
        dominant_feature = max(
            vector.items(),
            key=lambda item: abs(item[1]),
        )[0]
        cluster_id = dominant_feature.removeprefix(prefix)
        clusters.setdefault(cluster_id, []).append(vector)

    ranked = sorted(
        clusters.items(),
        key=lambda item: sum(
            abs(vec.get(f"{prefix}{item[0]}", 0.0)) for vec in item[1]
        ),
        reverse=True,
    )[:max_clusters]

    results: list[dict] = []
    for cluster_id, vectors in ranked:
        mean_values: dict[str, float] = {}
        for vector in vectors:
            for feature, value in vector.items():
                mean_values[feature] = mean_values.get(feature, 0.0) + value
        size = len(vectors)
        for feature in list(mean_values):
            mean_values[feature] /= size

        feature_list = sorted(
            (
                (feat.removeprefix(prefix), val)
                for feat, val in mean_values.items()
            ),
            key=lambda item: abs(item[1]),
            reverse=True,
        )[:top_features]

        results.append(
            {
                "cluster_id": cluster_id,
                "size": size,
                "top_features": feature_list,
            }
        )

    return results


def mine_association_rules(
    itemsets: Sequence[set[str]],
    min_support: float = 0.2,
    min_confidence: float = 0.5,
) -> list[dict]:
    """Mine simple pairwise association rules from binary itemsets."""
    total = len(itemsets)
    if total == 0:
        return []

    item_counts: dict[str, int] = {}
    pair_counts: dict[tuple[str, str], int] = {}

    for itemset in itemsets:
        for item in itemset:
            item_counts[item] = item_counts.get(item, 0) + 1
        for a, b in combinations(sorted(itemset), 2):
            pair_counts[(a, b)] = pair_counts.get((a, b), 0) + 1

    rules: list[dict] = []
    for (a, b), count in pair_counts.items():
        support = count / total
        if support < min_support:
            continue

        confidence_ab = count / item_counts[a]
        if confidence_ab >= min_confidence:
            rules.append(
                {
                    "antecedent": [a],
                    "consequent": [b],
                    "support": support,
                    "confidence": confidence_ab,
                }
            )

        confidence_ba = count / item_counts[b]
        if confidence_ba >= min_confidence:
            rules.append(
                {
                    "antecedent": [b],
                    "consequent": [a],
                    "support": support,
                    "confidence": confidence_ba,
                }
            )

    return rules


def mine_frequent_sequences(
    event_sequences: Sequence[Sequence[tuple[datetime, str]]],
    min_support: float = 0.2,
) -> list[dict]:
    """Mine frequent ordered pairs from event sequences."""
    total = len(event_sequences)
    if total == 0:
        return []

    pair_counts: dict[tuple[str, str], int] = {}

    for events in event_sequences:
        ordered = sorted(events, key=lambda event: event[0])
        seen_pairs: set[tuple[str, str]] = set()
        for i in range(len(ordered)):
            for j in range(i + 1, len(ordered)):
                pair = (ordered[i][1], ordered[j][1])
                seen_pairs.add(pair)
        for pair in seen_pairs:
            pair_counts[pair] = pair_counts.get(pair, 0) + 1

    sequences: list[dict] = []
    for (a, b), count in pair_counts.items():
        support = count / total
        if support < min_support:
            continue
        sequences.append(
            {
                "sequence": [a, b],
                "support": support,
            }
        )

    sequences.sort(key=lambda record: record["support"], reverse=True)
    return sequences

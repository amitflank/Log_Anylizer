"""Evaluation workflow for the lightweight GBDT pipeline CLI."""
# ruff: noqa: I001

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

from log_ingest.bucket_building.time_bucket import TimeBucket
from log_ingest.lookback.window_plan import (
    LookBackWindowPlanner,
    PlannerOptions,
    WindowArtifacts,
)
from gbdt_pipeline.dataset_manager import DatasetManager, DatasetSplits
from gbdt_pipeline.exclusion import TemplateExclusion
from gbdt_pipeline.feature_builder import FeatureBuilder, FeatureConfig
from gbdt_pipeline.episode_gap import EpisodeGapDiagnostics, estimate_episode_gap_minutes
from gbdt_pipeline.incident_catalog import IncidentTarget
from gbdt_pipeline.label_builder import BucketLabel, LabelBuilder, SymptomFilter
from gbdt_pipeline.scorer import GBDTScorer
from gbdt_pipeline.template_inventory import TemplateInventory, TemplateSummary
from gbdt_pipeline.shap_report import ShapReportOptions, write_shap_reports
from gbdt_pipeline.trainer import GBDTTrainer, TrainingConfig
from gbdt_pipeline.window_analysis import (  # isort: split
    NegativeWindowStrategy,
    WindowSnapshotArtifacts,
    WindowSnapshotStore,
    build_binary_itemset,
    cluster_windows,
    compute_window_summaries,
    compute_interaction_summaries,
    extract_top_interactions,
    load_event_sequences,
    mine_association_rules,
    mine_frequent_sequences,
    select_negative_windows,
)
from gbdt_pipeline.window_extractor import LookbackWindow, LookbackWindowExtractor

DEFAULT_METRIC_FIELDS: Sequence[str] = (
    "total_events",
    "error_count",
    "warning_count",
    "info_count",
    "cpu_util",
    "memory_util",
    "disk_util",
    "read_latency_ms",
    "write_latency_ms",
)

MAX_TEMPLATE_FEATURES = 200

@dataclass(frozen=True)
class EvaluationConfig:
    """User-supplied options controlling an evaluation run."""

    incident_template: str
    equivalent_templates: Sequence[str]
    bucket_path: Path
    output_root: Path
    lead_minutes: int
    max_minutes: int
    threshold: float
    filters: dict[str, str] = field(default_factory=dict)
    threshold_policy: str = "negative-quantile"
    negative_quantile_rate: float = 0.001
    reuse_artifacts: Path | None = None
    summary_output_path: Path | None = None
    shap_output_path: Path | None = None
    shap_top_k: int = 5
    shap_bucket_limit: int = 10
    shap_report_top_n: int = 10
    details: str = "compact"
    auto_lookback_enabled: bool = False
    auto_lookback_leads: tuple[int, ...] = ()
    auto_lookback_metric: str = "recall"
    exclude_templates: tuple[str, ...] = ()
    exclude_patterns: tuple[str, ...] = ()
    exclude_metrics: tuple[str, ...] = ()
    balance_classes: bool = True
    symptom_substring: str | None = None
    symptom_var_filters: dict[str, str] | None = None
    cv_strategy: Literal["none", "time-series"] = "none"
    cv_folds: int = 5
    cv_test_size: int | None = None
    cv_min_train_size: int | None = None
    window_analysis_enabled: bool = False
    window_analysis_threshold: float = 1.0
    window_analysis_min_support: float = 0.2
    window_analysis_min_confidence: float = 0.5
    window_negative_strategy: NegativeWindowStrategy = field(
        default_factory=NegativeWindowStrategy
    )
    compute_interactions: bool = False
    interaction_top_k: int = 30
    interaction_batch_size: int = 500
    episode_collapse_enabled: bool = False
    episode_gap_min_intervals: int = 20
    episode_gap_min_minutes: int = 1
    episode_gap_max_minutes: int = 120


@dataclass(frozen=True)
class EvaluationSummary:
    """Outcome of an evaluation run."""

    artifact_dir: Path
    model_path: Path
    positive_windows: int
    negative_windows: int
    feature_columns: list[str]
    train_auc: float
    threshold: float
    confusion: dict[str, int]
    metrics: dict[str, float]
    artifact_summary: dict[str, object]
    shap_highlights: list[dict[str, object]] | None = None
    cv_summary: dict[str, object] | None = None
    data_summary: dict[str, object] | None = None


@dataclass(frozen=True)
class DataSummaryInputs:
    """Inputs required to build the evaluation data summary."""

    train_df: pd.DataFrame
    test_df: pd.DataFrame
    scored: pd.DataFrame
    expected_value: float | None
    data_scope: str
    shap_model_path: Path


@dataclass(frozen=True)
class ShapPreparation:
    """Prepared data needed for SHAP computation."""

    model_path: Path
    train_df: pd.DataFrame
    test_df: pd.DataFrame
    scored: pd.DataFrame
    data_scope: str


@dataclass(frozen=True)
class ShapPrepInputs:
    """Inputs required to prepare SHAP datasets."""

    artifacts: WindowArtifacts
    target: IncidentTarget
    feature_columns: list[str]
    splits: DatasetSplits
    scored: pd.DataFrame
    model_path: Path
    config: EvaluationConfig


@dataclass(frozen=True)
class LookbackCandidateScore:
    """Score summary for an auto-lookback candidate."""

    lead_minutes: int
    max_minutes: int
    metric: str
    score: float
    cv_summary: dict[str, object]


@dataclass(frozen=True)
class WindowExtractionResult:
    """Lightweight description of extracted windows."""

    bucket_labels: tuple[BucketLabel, ...]
    template_summary: TemplateSummary


class EvaluationError(RuntimeError):
    """Raised when the evaluation pipeline cannot complete."""



@dataclass(frozen=True)
class DatasetBuildContext:
    """Inputs required to build datasets and snapshots."""

    target: IncidentTarget
    exclusion: TemplateExclusion
    config: EvaluationConfig
    artifacts: WindowArtifacts
    symptom_filter: SymptomFilter | None


@dataclass(frozen=True)
class IngestContext:
    """Inputs required while ingesting windows."""

    target: IncidentTarget
    feature_builder: FeatureBuilder
    dataset_manager: DatasetManager
    feature_columns: set[str]
    snapshot_store: WindowSnapshotStore | None
    snapshot_positive_bucket_ids: set[str] | None = None


@dataclass(frozen=True)
class ShapOutputContext:
    """Inputs required to write SHAP outputs."""

    feature_columns: list[str]
    scored: pd.DataFrame
    config: EvaluationConfig
    snapshot_artifacts: WindowSnapshotArtifacts | None


@dataclass(frozen=True)
class WindowAnalysisInputs:
    """Inputs required for window-level analysis."""

    snapshot_artifacts: WindowSnapshotArtifacts | None
    shap_path: Path | None
    feature_columns: list[str]
    config: EvaluationConfig
    scorer: GBDTScorer | None
    test_df: pd.DataFrame | None


@dataclass(frozen=True)
class InteractionContext:
    """Inputs required to compute SHAP interactions."""

    snapshot_artifacts: WindowSnapshotArtifacts
    scorer: GBDTScorer
    test_df: pd.DataFrame
    feature_columns: list[str]
    config: EvaluationConfig


def run_evaluation(config: EvaluationConfig) -> EvaluationSummary:
    """Execute the end-to-end evaluation pipeline."""
    selection_summary: dict[str, object] | None = None
    if config.auto_lookback_enabled:
        config, selection_summary = _select_auto_lookback(config)

    artifacts, artifact_summary = _resolve_artifacts(config)
    if selection_summary is not None:
        artifact_summary["lookback_selection"] = selection_summary
    target, buckets, template_exclusion, window_result, symptom_filter = _prepare_windows(
        config, artifact_summary
    )

    build_context = DatasetBuildContext(
        target=target,
        exclusion=template_exclusion,
        config=config,
        artifacts=artifacts,
        symptom_filter=symptom_filter,
    )
    (
        dataset_manager,
        positive_count,
        negative_count,
        feature_columns,
        snapshot_artifacts,
    ) = _build_dataset(
        window_result=window_result,
        buckets=buckets,
        context=build_context,
    )
    if positive_count == 0:
        raise EvaluationError(
            "No positive windows were generated; cannot train the surrogate model."
        )

    feature_columns = sorted(feature_columns)
    splits, cv_summary = _resolve_splits(
        dataset_manager=dataset_manager,
        artifacts=artifacts,
        target=target,
        feature_columns=feature_columns,
        config=config,
    )

    model_path, train_auc, scored, threshold_used, confusion, metrics = _train_and_score(
        artifacts=artifacts,
        target=target,
        feature_columns=feature_columns,
        splits=splits,
        config=config,
    )

    shap_prep = _prepare_shap_data(
        ShapPrepInputs(
            artifacts=artifacts,
            target=target,
            feature_columns=feature_columns,
            splits=splits,
            scored=scored,
            model_path=model_path,
            config=config,
        )
    )

    shap_context = ShapOutputContext(
        feature_columns=feature_columns,
        scored=shap_prep.scored,
        config=config,
        snapshot_artifacts=snapshot_artifacts,
    )
    shap_path, shap_highlights, scorer, expected_value = _maybe_compute_shap(
        model_path=shap_prep.model_path,
        test_df=shap_prep.test_df,
        shap_context=shap_context,
    )

    # Build additional interpretability artifacts (aggregate + per-bucket tables).
    if shap_path is not None and shap_path.exists():
        report_paths = write_shap_reports(
            shap_path=shap_path,
            template_summary=window_result.template_summary,
            highlights=shap_highlights,
            output_dir=artifacts.summary_path.parent,
            options=ShapReportOptions(
                bucket_limit=int(config.shap_bucket_limit),
                top_n_features=int(config.shap_report_top_n),
                top_n_templates=int(config.shap_report_top_n),
            ),
        )
        artifact_summary["shap_report"] = report_paths

    # NOTE: interaction computation depends on window snapshots, so we treat
    # --compute-interactions as an implicit request for window analysis artifacts.
    if config.window_analysis_enabled or config.compute_interactions:
        analysis_inputs = WindowAnalysisInputs(
            snapshot_artifacts=snapshot_artifacts,
            shap_path=shap_path,
            feature_columns=feature_columns,
            config=config,
            scorer=scorer,
            test_df=shap_prep.test_df,
        )
        _run_window_analysis(artifacts=artifacts, inputs=analysis_inputs)

    data_summary = _build_data_summary(
        DataSummaryInputs(
            train_df=shap_prep.train_df,
            test_df=shap_prep.test_df,
            scored=shap_prep.scored,
            expected_value=expected_value,
            data_scope=shap_prep.data_scope,
            shap_model_path=shap_prep.model_path,
        )
    )
    summary = EvaluationSummary(
        artifact_dir=artifacts.summary_path.parent,
        model_path=model_path,
        positive_windows=positive_count,
        negative_windows=negative_count,
        feature_columns=feature_columns,
        train_auc=train_auc,
        threshold=threshold_used,
        confusion=confusion,
        metrics=metrics,
        artifact_summary=artifact_summary,
        shap_highlights=shap_highlights,
        cv_summary=cv_summary,
        data_summary=data_summary,
    )

    if config.summary_output_path:
        _write_summary_json(summary, config.summary_output_path)

    return summary


def _resolve_splits(
    *,
    dataset_manager: DatasetManager,
    artifacts: WindowArtifacts,
    target: IncidentTarget,
    feature_columns: list[str],
    config: EvaluationConfig,
) -> tuple[DatasetSplits, dict[str, object] | None]:
    cv_summary: dict[str, object] | None = None
    if config.cv_strategy == "time-series":
        folds = dataset_manager.time_series_folds(
            n_splits=config.cv_folds,
            test_size=config.cv_test_size,
            min_train_size=config.cv_min_train_size,
        )
        cv_summary = _run_time_series_cv(
            artifacts=artifacts,
            target=target,
            feature_columns=feature_columns,
            folds=folds,
            config=config,
        )
        splits = folds[-1]
    elif config.cv_strategy in ("none", ""):
        splits = dataset_manager.finalise(
            train_ratio=0.8,
            val_ratio=0.0,
            enforce_label_balance=True,
        )
    else:
        raise EvaluationError(f"Unknown cv_strategy '{config.cv_strategy}'")
    return splits, cv_summary


def _prepare_shap_data(inputs: ShapPrepInputs) -> ShapPreparation:
    shap_needed = (
        inputs.config.shap_output_path is not None
        or inputs.config.window_analysis_enabled
        or inputs.config.compute_interactions
    )
    shap_model_path = inputs.model_path
    shap_train_df = inputs.splits.train_df
    shap_test_df = inputs.splits.test_df
    shap_scored = inputs.scored
    shap_data_scope = "last-fold"
    if inputs.config.cv_strategy == "time-series" and shap_needed:
        full_df = (
            pd.concat([inputs.splits.train_df, inputs.splits.test_df], ignore_index=True)
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        shap_data_scope = "full-data"
        shap_train_df = full_df
        shap_test_df = full_df
        shap_model_path = inputs.artifacts.summary_path.parent / "evaluation" / "model_full.json"
        full_splits = DatasetSplits(
            train_df=full_df, val_df=pd.DataFrame(), test_df=full_df
        )
        _train_model_to_path(
            model_path=shap_model_path,
            target=inputs.target,
            feature_columns=inputs.feature_columns,
            splits=full_splits,
            balance_classes=inputs.config.balance_classes,
        )
        shap_scored = _score_dataframe(
            model_path=shap_model_path,
            feature_columns=inputs.feature_columns,
            dataframe=full_df,
        )
    return ShapPreparation(
        model_path=shap_model_path,
        train_df=shap_train_df,
        test_df=shap_test_df,
        scored=shap_scored,
        data_scope=shap_data_scope,
    )


def _select_auto_lookback(
    config: EvaluationConfig,
) -> tuple[EvaluationConfig, dict[str, object]]:
    selection_config = config
    if selection_config.cv_strategy != "time-series":
        selection_config = replace(selection_config, cv_strategy="time-series")

    candidates = _build_lookback_candidates(selection_config)
    if not candidates:
        raise EvaluationError("Auto-lookback candidate grid is empty.")

    scores: list[LookbackCandidateScore] = []
    errors: list[dict[str, object]] = []
    for lead_minutes, max_minutes in candidates:
        candidate_config = replace(
            selection_config,
            lead_minutes=lead_minutes,
            max_minutes=max_minutes,
            reuse_artifacts=None,
            shap_output_path=None,
            window_analysis_enabled=False,
            compute_interactions=False,
            output_root=selection_config.output_root
            / "lookback_search"
            / f"lead{lead_minutes}_max{max_minutes}",
            summary_output_path=None,
        )
        try:
            candidate_score = _score_lookback_candidate(candidate_config)
        except EvaluationError as exc:
            errors.append(
                {
                    "lead_minutes": lead_minutes,
                    "max_minutes": max_minutes,
                    "error": str(exc),
                }
            )
            continue
        scores.append(candidate_score)

    if not scores:
        raise EvaluationError(
            "Auto-lookback evaluation failed for all candidates.",
        )

    selected = _select_best_candidate(scores)
    updated_config = replace(
        selection_config,
        lead_minutes=selected.lead_minutes,
        max_minutes=selected.max_minutes,
    )
    summary = {
        "metric": selected.metric,
        "selected": {
            "lead_minutes": selected.lead_minutes,
            "max_minutes": selected.max_minutes,
            "score": selected.score,
        },
        "candidates": [
            {
                "lead_minutes": score.lead_minutes,
                "max_minutes": score.max_minutes,
                "score": score.score,
            }
            for score in scores
        ],
        "errors": errors,
        "cv_strategy": updated_config.cv_strategy,
    }
    return updated_config, summary


def _build_lookback_candidates(config: EvaluationConfig) -> list[tuple[int, int]]:
    base_leads = list(config.auto_lookback_leads) if config.auto_lookback_leads else []
    default_leads = [5, 10, 15, 30]
    lead_candidates = sorted({*base_leads, *default_leads, config.lead_minutes})
    lead_candidates = [lead for lead in lead_candidates if lead > 0]

    candidates: set[tuple[int, int]] = set()
    max_cap = max(config.max_minutes, config.lead_minutes)
    for lead in lead_candidates:
        if lead > max_cap:
            continue
        max_candidates = {lead, min(lead * 2, max_cap), max_cap}
        for max_minutes in max_candidates:
            if max_minutes >= lead:
                candidates.add((lead, max_minutes))
    return sorted(candidates)


def _score_lookback_candidate(config: EvaluationConfig) -> LookbackCandidateScore:
    artifacts, artifact_summary = _resolve_artifacts(config)
    target, buckets, template_exclusion, window_result, symptom_filter = _prepare_windows(
        config, artifact_summary
    )
    build_context = DatasetBuildContext(
        target=target,
        exclusion=template_exclusion,
        config=config,
        artifacts=artifacts,
        symptom_filter=symptom_filter,
    )
    dataset_manager, positive_count, _, feature_columns, _ = _build_dataset(
        window_result=window_result,
        buckets=buckets,
        context=build_context,
    )
    if positive_count <= 0:
        raise EvaluationError("No positive windows available for lookback scoring.")

    feature_columns = sorted(feature_columns)
    folds = dataset_manager.time_series_folds(
        n_splits=config.cv_folds,
        test_size=config.cv_test_size,
        min_train_size=config.cv_min_train_size,
    )
    cv_summary = _run_time_series_cv(
        artifacts=artifacts,
        target=target,
        feature_columns=feature_columns,
        folds=folds,
        config=config,
    )
    score = _extract_candidate_score(cv_summary, config.auto_lookback_metric)
    return LookbackCandidateScore(
        lead_minutes=config.lead_minutes,
        max_minutes=config.max_minutes,
        metric=config.auto_lookback_metric,
        score=score,
        cv_summary=cv_summary,
    )


def _extract_candidate_score(cv_summary: dict[str, object], metric: str) -> float:
    aggregate = cv_summary.get("aggregate", {})
    metrics = aggregate.get("metrics", {})
    if metric == "train_auc":
        train_auc = aggregate.get("train_auc", {})
        value = train_auc.get("mean")
        if value is None:
            raise EvaluationError("CV summary missing train_auc mean.")
        return float(value)
    if metric not in metrics:
        raise EvaluationError(f"Unsupported lookback metric '{metric}'.")
    metric_summary = metrics[metric]
    value = metric_summary.get("mean")
    if value is None:
        raise EvaluationError(f"CV summary missing '{metric}' mean.")
    return float(value)


def _select_best_candidate(scores: list[LookbackCandidateScore]) -> LookbackCandidateScore:
    metric = scores[0].metric
    minimize = metric == "false_positive_rate"

    def _sort_key(score: LookbackCandidateScore) -> tuple[float, int, int]:
        primary = score.score if not minimize else -score.score
        return (primary, -score.lead_minutes, -score.max_minutes)

    return max(scores, key=_sort_key)


def _prepare_windows(
    config: EvaluationConfig, artifact_summary: dict[str, object]
) -> tuple[
    IncidentTarget,
    list[TimeBucket],
    TemplateExclusion,
    WindowExtractionResult,
    SymptomFilter | None,
]:
    actual_lead = int(artifact_summary.get("lead_time_used_minutes") or config.lead_minutes)
    target = _build_incident_target(config, actual_lead)
    buckets = _load_buckets(config.bucket_path, config.filters)
    if not buckets:
        raise EvaluationError("No buckets matched the provided filters; unable to build dataset.")

    template_exclusion = _build_template_exclusion(config)
    symptom_filter = _build_symptom_filter(config)
    window_result = _extract_windows(
        buckets, target, template_exclusion, symptom_filter
    )
    if not window_result.bucket_labels:
        raise EvaluationError("No look-back windows could be constructed for the requested target.")
    return target, buckets, template_exclusion, window_result, symptom_filter


def _maybe_compute_shap(
    model_path: Path,
    test_df: pd.DataFrame,
    shap_context: ShapOutputContext,
) -> tuple[Path | None, list[dict[str, object]] | None, GBDTScorer | None, float | None]:
    shap_needed = (
        shap_context.config.shap_output_path is not None
        or shap_context.config.window_analysis_enabled
        or shap_context.config.compute_interactions
    )
    if not shap_needed or test_df.empty:
        return None, None, None, None

    scorer = GBDTScorer(model_path, shap_context.feature_columns)
    shap_path, shap_highlights, expected_value = _handle_shap_output(
        scorer=scorer,
        test_df=test_df,
        context=shap_context,
    )
    if not shap_context.config.shap_output_path:
        shap_highlights = None
    return shap_path, shap_highlights, scorer, expected_value


def _train_and_score(
    artifacts: WindowArtifacts,
    target: IncidentTarget,
    feature_columns: list[str],
    splits: DatasetSplits,
    config: EvaluationConfig,
) -> tuple[Path, float, pd.DataFrame, float, dict[str, int], dict[str, float]]:
    model_path, train_auc = _train_model(
        artifacts, target, feature_columns, splits, config.balance_classes
    )
    scored, threshold_used, confusion, metrics = _score_and_evaluate(
        model_path, feature_columns, splits.test_df, config
    )
    return model_path, train_auc, scored, threshold_used, confusion, metrics

def _build_incident_target(config: EvaluationConfig, lead_minutes: int) -> IncidentTarget:
    """Construct the IncidentTarget from config."""
    return IncidentTarget(
        name=f"incident-{config.incident_template}",
        template_hashes=[config.incident_template, *config.equivalent_templates],
        initial_lookback_minutes=lead_minutes,
        max_lookback_minutes=config.max_minutes,
        filters=dict(config.filters),
    )


def _build_template_exclusion(config: EvaluationConfig) -> TemplateExclusion:
    """Build template exclusion from config."""
    auto_excludes = {config.incident_template, *config.equivalent_templates}
    return TemplateExclusion.from_lists(
        templates=tuple(sorted(set(config.exclude_templates) | auto_excludes)),
        pattern_strings=config.exclude_patterns,
    )


def _train_model(
    artifacts: WindowArtifacts,
    target: IncidentTarget,
    feature_columns: list[str],
    splits: DatasetSplits,
    balance_classes: bool,
) -> tuple[Path, float]:
    """Train the LightGBM model and return model path and AUC."""
    evaluation_dir = artifacts.summary_path.parent / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)
    model_path = evaluation_dir / "model.json"
    return _train_model_to_path(
        model_path=model_path,
        target=target,
        feature_columns=feature_columns,
        splits=splits,
        balance_classes=balance_classes,
    )


def _train_model_to_path(
    model_path: Path,
    target: IncidentTarget,
    feature_columns: list[str],
    splits: DatasetSplits,
    balance_classes: bool,
) -> tuple[Path, float]:
    """Train the LightGBM model and return model path and AUC."""
    model_path.parent.mkdir(parents=True, exist_ok=True)
    training_config = TrainingConfig(
        target_name=target.name,
        feature_columns=feature_columns,
        label_column="label",
        model_output_path=model_path,
        balance_classes=balance_classes,
    )

    trainer = GBDTTrainer(training_config)
    report = trainer.train(splits.train_df, splits.val_df, splits.test_df)
    train_auc = float(report.metrics.get("auc", 0.0))
    return model_path, train_auc


def _run_time_series_cv(
    artifacts: WindowArtifacts,
    target: IncidentTarget,
    feature_columns: list[str],
    folds: Sequence[DatasetSplits],
    config: EvaluationConfig,
) -> dict[str, object]:
    """Run walk-forward CV and return per-fold + aggregate metrics."""
    if not folds:
        raise EvaluationError("No folds generated for time-series CV.")

    cv_dir = artifacts.summary_path.parent / "cv_models"
    cv_dir.mkdir(parents=True, exist_ok=True)
    fold_payloads: list[dict[str, object]] = []

    for idx, fold in enumerate(folds, start=1):
        fold_model_path = cv_dir / f"fold_{idx}.json"
        model_path, train_auc = _train_model_to_path(
            model_path=fold_model_path,
            target=target,
            feature_columns=feature_columns,
            splits=fold,
            balance_classes=config.balance_classes,
        )
        scored, threshold_used, confusion, metrics = _score_and_evaluate(
            model_path, feature_columns, fold.test_df, config
        )
        train_pos = int(fold.train_df["label"].sum())
        train_total = int(len(fold.train_df))
        test_pos = int(fold.test_df["label"].sum())
        test_total = int(len(fold.test_df))
        mean_probability = (
            float(scored["probability"].mean()) if not scored.empty else 0.0
        )
        fold_payloads.append(
            {
                "fold": idx,
                "train_size": len(fold.train_df),
                "test_size": len(fold.test_df),
                "train_auc": train_auc,
                "threshold": threshold_used,
                "mean_probability": mean_probability,
                "train_positive_rate": train_pos / train_total if train_total else 0.0,
                "test_positive_rate": test_pos / test_total if test_total else 0.0,
                "confusion": confusion,
                "metrics": metrics,
            }
        )

    train_auc_values = np.array([row["train_auc"] for row in fold_payloads], dtype=float)
    threshold_values = np.array([row["threshold"] for row in fold_payloads], dtype=float)
    metric_keys = list(fold_payloads[0]["metrics"].keys())
    metric_summary: dict[str, dict[str, float]] = {}
    for key in metric_keys:
        values = np.array([row["metrics"][key] for row in fold_payloads], dtype=float)
        metric_summary[key] = {
            "mean": float(values.mean()),
            "std": float(values.std(ddof=0)),
        }
    mean_prob_values = np.array(
        [row["mean_probability"] for row in fold_payloads], dtype=float
    )
    train_rate_values = np.array(
        [row["train_positive_rate"] for row in fold_payloads], dtype=float
    )
    test_rate_values = np.array(
        [row["test_positive_rate"] for row in fold_payloads], dtype=float
    )

    return {
        "strategy": "time-series",
        "folds": len(folds),
        "test_size": len(folds[0].test_df),
        "min_train_size": len(folds[0].train_df),
        "folds_detail": fold_payloads,
        "aggregate": {
            "train_auc": {
                "mean": float(train_auc_values.mean()),
                "std": float(train_auc_values.std(ddof=0)),
            },
            "threshold": {
                "mean": float(threshold_values.mean()),
                "std": float(threshold_values.std(ddof=0)),
            },
            "mean_probability": {
                "mean": float(mean_prob_values.mean()),
                "std": float(mean_prob_values.std(ddof=0)),
            },
            "train_positive_rate": {
                "mean": float(train_rate_values.mean()),
                "std": float(train_rate_values.std(ddof=0)),
            },
            "test_positive_rate": {
                "mean": float(test_rate_values.mean()),
                "std": float(test_rate_values.std(ddof=0)),
            },
            "metrics": metric_summary,
        },
    }


def _score_and_evaluate(
    model_path: Path,
    feature_columns: list[str],
    test_df: pd.DataFrame,
    config: EvaluationConfig,
) -> tuple[pd.DataFrame, float, dict[str, int], dict[str, float]]:
    """Score test data and compute evaluation metrics."""
    scorer = GBDTScorer(model_path, feature_columns)
    test_features = test_df[["bucket_id", *feature_columns]].copy()
    scored = scorer.score(test_features)
    scored = scored.merge(test_df[["bucket_id", "label"]], on="bucket_id", how="left")
    threshold_used = _resolve_threshold(scored, config)
    confusion, metrics = _compute_metrics(
        scored["probability"], scored["label"], threshold_used
    )
    return scored, threshold_used, confusion, metrics


def _score_dataframe(
    *,
    model_path: Path,
    feature_columns: list[str],
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    scorer = GBDTScorer(model_path, feature_columns)
    test_features = dataframe[["bucket_id", *feature_columns]].copy()
    scored = scorer.score(test_features)
    return scored.merge(dataframe[["bucket_id", "label"]], on="bucket_id", how="left")


def _resolve_threshold(scored: pd.DataFrame, config: EvaluationConfig) -> float:
    """Compute the decision threshold to use for metrics."""
    policy = (config.threshold_policy or "fixed").strip().lower()
    if policy == "fixed":
        return float(config.threshold)
    if policy == "negative-quantile":
        return _compute_negative_quantile_threshold(
            probabilities=scored["probability"],
            labels=scored["label"],
            rate=float(config.negative_quantile_rate),
        )
    raise EvaluationError(f"Unknown threshold policy '{config.threshold_policy}'")


def _compute_negative_quantile_threshold(
    probabilities: pd.Series,
    labels: pd.Series,
    rate: float,
) -> float:
    """Select a threshold so that roughly `rate` of negatives are predicted positive.

    Notes:
    - Uses >= threshold for predictions, so ties at the cutoff can yield more than the
      target negative rate.
    - Raises when no negative examples are present.
    """
    if rate <= 0.0 or rate > 1.0:
        raise EvaluationError(f"Negative quantile rate must be in (0, 1], got {rate}")

    probs = probabilities.astype(float)
    actuals = labels.astype(int)
    neg_probs = probs[actuals == 0]
    nneg = int(neg_probs.shape[0])
    if nneg == 0:
        raise EvaluationError("No negative examples available to compute quantile threshold.")

    k = int(math.ceil(rate * nneg))
    k = max(1, k)
    topk = neg_probs.nlargest(k)
    # The kth largest value (min of top-k) is the cutoff.
    return float(topk.min())


def _resolve_artifacts(config: EvaluationConfig) -> tuple[WindowArtifacts, dict[str, object]]:
    """Either reuse existing artifacts or invoke the planner."""
    if config.reuse_artifacts:
        return _load_existing_artifacts(config.reuse_artifacts)

    options = PlannerOptions(
        equivalent_templates=list(config.equivalent_templates),
        filters=config.filters or None,
        anchor_template_id=config.incident_template,
    )
    planner = LookBackWindowPlanner(
        bucket_path=config.bucket_path,
        output_root=config.output_root,
        lead_time_target=config.lead_minutes,
        max_lead_time=config.max_minutes,
        options=options,
    )

    incident_id = f"incident-{config.incident_template}"
    artifacts = planner.build_incident_windows(incident_id=incident_id)
    summary = json.loads(artifacts.summary_path.read_text())
    return artifacts, summary


def _load_existing_artifacts(path: Path) -> tuple[WindowArtifacts, dict[str, object]]:
    """Load look-back artifacts from an existing directory."""
    reuse_path = Path(path)
    summary_path = (
        reuse_path if reuse_path.is_file() else reuse_path / "window_summary.json"
    )
    if not summary_path.exists():
        raise EvaluationError(f"Could not locate summary JSON at {summary_path}")

    base_dir = summary_path.parent
    events_path = base_dir / "window_events.parquet"
    metrics_path = base_dir / "window_metrics.parquet"
    if not events_path.exists():
        raise EvaluationError(f"Missing events parquet at {events_path}")
    if not metrics_path.exists():
        raise EvaluationError(f"Missing metrics parquet at {metrics_path}")

    post_events_path = base_dir / "post_incident_events.parquet"
    artifacts = WindowArtifacts(
        events_path=events_path,
        metrics_path=metrics_path,
        summary_path=summary_path,
        post_incident_events_path=post_events_path if post_events_path.exists() else None,
    )
    summary = json.loads(summary_path.read_text())
    return artifacts, summary


def _load_buckets(parquet_path: Path, filters: dict[str, str]) -> list[TimeBucket]:
    """Convert the bucket parquet into TimeBucket instances."""
    buckets: list[TimeBucket] = []
    bucket_fields = set(TimeBucket.__dataclass_fields__)
    parquet = pq.ParquetFile(parquet_path)
    for batch in parquet.iter_batches():
        for row in batch.to_pylist():
            templates_payload = row.get("templates")
            if isinstance(templates_payload, str):
                try:
                    row["templates"] = json.loads(templates_payload)
                except json.JSONDecodeError:
                    row["templates"] = {}
            elif templates_payload is None:
                row["templates"] = {}

            core_payload = {name: _ensure_utc(row.get(name)) for name in bucket_fields}
            bucket = TimeBucket(**core_payload)
            for key, value in row.items():
                if key not in bucket_fields:
                    setattr(bucket, key, value)
            if _bucket_matches_filters(bucket, filters):
                buckets.append(bucket)

    buckets.sort(key=lambda b: b.start_time)
    return buckets


def _bucket_matches_filters(bucket: TimeBucket, filters: dict[str, str]) -> bool:
    """Return True when a bucket satisfies the provided filters."""
    if not filters:
        return True
    return all(getattr(bucket, key, None) == expected for key, expected in filters.items())


def _build_symptom_filter(config: EvaluationConfig) -> SymptomFilter | None:
    """Construct a SymptomFilter from config if any criteria are specified."""
    has_substring = config.symptom_substring is not None
    has_var_filters = bool(config.symptom_var_filters)
    if not has_substring and not has_var_filters:
        return None
    return SymptomFilter(
        substring=config.symptom_substring,
        var_filters=config.symptom_var_filters or {},
    )


def _extract_windows(
    buckets: Sequence[TimeBucket],
    target: IncidentTarget,
    exclusion: TemplateExclusion,
    symptom_filter: SymptomFilter | None = None,
) -> WindowExtractionResult:
    """Materialise window metadata while keeping individual windows transient."""
    label_builder = LabelBuilder(buckets, symptom_filter=symptom_filter)
    labeled = label_builder.build_labels(target)
    extractor = LookbackWindowExtractor(buckets, exclusion=exclusion)
    inventory = TemplateInventory(exclusion=exclusion)

    window_labels: list[BucketLabel] = []
    seen_bucket_ids: set[str] = set()
    for bucket_label in labeled.all():
        bucket_id = bucket_label.bucket_id
        if bucket_id in seen_bucket_ids:
            continue
        try:
            window = extractor.build_window(bucket_label, target)
        except ValueError:
            continue
        if not window.buckets:
            continue
        inventory.record_window(window)
        window_labels.append(bucket_label)
        if bucket_id is not None:
            seen_bucket_ids.add(bucket_id)

    summary = inventory.summarise()
    return WindowExtractionResult(bucket_labels=tuple(window_labels), template_summary=summary)


def _build_dataset(
    window_result: WindowExtractionResult,
    buckets: Sequence[TimeBucket],
    context: DatasetBuildContext,
) -> tuple[
    DatasetManager,
    int,
    int,
    set[str],
    WindowSnapshotArtifacts | None,
]:
    """Stream windows into the dataset manager and optional snapshot store."""
    limited_summary = _limit_template_summary(window_result.template_summary, MAX_TEMPLATE_FEATURES)
    metric_fields = list(DEFAULT_METRIC_FIELDS)
    if context.config.exclude_metrics:
        excluded = {str(metric) for metric in context.config.exclude_metrics}
        unknown = excluded - set(DEFAULT_METRIC_FIELDS)
        if unknown:
            raise EvaluationError(
                f"Unknown metric fields in --exclude-metric: {sorted(unknown)}. "
                f"Known metrics: {list(DEFAULT_METRIC_FIELDS)}"
            )
        metric_fields = [metric for metric in metric_fields if metric not in excluded]
    feature_builder = FeatureBuilder(
        FeatureConfig(metric_fields=metric_fields),
        limited_summary,
        exclusion=context.exclusion,
    )
    dataset_root = context.artifacts.summary_path.parent / "dataset_cache"
    dataset_root.mkdir(parents=True, exist_ok=True)
    dataset_manager = DatasetManager(dataset_root)

    snapshot_store: WindowSnapshotStore | None = None
    if context.config.window_analysis_enabled or context.config.compute_interactions:
        store_dir = context.artifacts.summary_path.parent / "window_snapshots"
        snapshot_store = WindowSnapshotStore(store_dir)

    episode_gap_minutes: int | None = None
    episode_gap_diag: EpisodeGapDiagnostics | None = None
    snapshot_positive_bucket_ids: set[str] | None = None
    if snapshot_store is not None and context.config.episode_collapse_enabled:
        episode_gap_minutes, episode_gap_diag = _learn_episode_gap_minutes(buckets, context)
        if episode_gap_minutes is not None:
            snapshot_positive_bucket_ids = _select_episode_positive_bucket_ids(
                window_result.bucket_labels, episode_gap_minutes
            )
        _write_episode_gap_artifact(
            snapshot_store._directory,  # internal dir owned by the store
            episode_gap_minutes,
            episode_gap_diag,
        )

    feature_columns: set[str] = set()
    extractor = LookbackWindowExtractor(buckets, exclusion=context.exclusion)

    ingest_context = IngestContext(
        target=context.target,
        feature_builder=feature_builder,
        dataset_manager=dataset_manager,
        feature_columns=feature_columns,
        snapshot_store=snapshot_store,
        snapshot_positive_bucket_ids=snapshot_positive_bucket_ids,
    )

    positive_count, negative_count = _ingest_window_labels(
        labels=window_result.bucket_labels,
        extractor=extractor,
        context=ingest_context,
    )

    if snapshot_store is not None:
        negatives = select_negative_windows(
            buckets=buckets,
            target=context.target,
            exclusion=context.exclusion,
            strategy=context.config.window_negative_strategy,
            episode_gap_minutes=episode_gap_minutes,
        )
        for bucket_label, window in negatives:
            snapshot_store.append_window(bucket_label, window)

    snapshot_artifacts = snapshot_store.close() if snapshot_store else None
    return dataset_manager, positive_count, negative_count, feature_columns, snapshot_artifacts


def _learn_episode_gap_minutes(
    buckets: Sequence[TimeBucket],
    context: DatasetBuildContext,
) -> tuple[int | None, EpisodeGapDiagnostics]:
    """Learn episode gap minutes for this run (per target, per dataset)."""
    label_builder = LabelBuilder(buckets, symptom_filter=context.symptom_filter)
    # Important: learn the gap from *per-bucket* symptom anchors (earliest symptom per bucket),
    # not from all raw instances. Many templates emit multiple instances close together,
    # which would bias inter-arrival times toward seconds and yield an unrealistically small gap.
    labeled = label_builder.build_labels(context.target)
    # Use the bucket-aligned prediction anchor (`start_time`) rather than the raw symptom time.
    # This avoids tiny inter-arrivals caused by boundary effects (symptoms just before/after a
    # bucket boundary) while matching how we ultimately sample windows (by bucket).
    timestamps = sorted(
        {label.start_time for label in labeled.positives if label.start_time is not None}
    )
    return estimate_episode_gap_minutes(
        timestamps,
        min_intervals=max(0, int(context.config.episode_gap_min_intervals)),
        min_gap_minutes=max(1, int(context.config.episode_gap_min_minutes)),
        max_gap_minutes=max(1, int(context.config.episode_gap_max_minutes)),
    )


def _select_episode_positive_bucket_ids(
    labels: Sequence[BucketLabel],
    gap_minutes: int,
) -> set[str]:
    """Pick one positive bucket id per episode using the earliest symptom anchor."""
    positives = [label for label in labels if label.label == 1]
    collapsed = LabelBuilder.collapse_positives_by_gap(positives, gap_minutes=gap_minutes)
    return {label.bucket_id for label in collapsed if label.bucket_id is not None}


def _write_episode_gap_artifact(
    snapshot_dir: Path,
    gap_minutes: int | None,
    diagnostics: EpisodeGapDiagnostics | None,
) -> None:
    """Persist learned episode gap and diagnostics for reproducibility."""
    payload: dict[str, object] = {
        "gap_minutes_used": gap_minutes,
    }
    if diagnostics is not None:
        payload.update(
            {
                "num_events": diagnostics.num_events,
                "num_intervals": diagnostics.num_intervals,
                "interval_minutes_p50": diagnostics.interval_minutes_p50,
                "interval_minutes_p90": diagnostics.interval_minutes_p90,
                "interval_minutes_p99": diagnostics.interval_minutes_p99,
                "method": diagnostics.method,
            }
        )
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "episode_gap.json").write_text(json.dumps(payload, indent=2))

def _limit_template_summary(
    summary: TemplateSummary,
    limit: int,
) -> TemplateSummary:
    """Trim template metadata to the most common entries to control feature explosion."""
    templates = summary.templates
    if len(templates) <= limit:
        return summary

    sorted_templates = sorted(
        templates.items(), key=lambda item: item[1].total_instances, reverse=True
    )[:limit]
    return TemplateSummary(dict(sorted_templates))


def _ingest_window_labels(
    labels: Sequence[BucketLabel],
    extractor: LookbackWindowExtractor,
    context: IngestContext,
) -> tuple[int, int]:
    positive_count = 0
    negative_count = 0
    for bucket_label in labels:
        window = _build_window_safe(extractor, bucket_label, context.target)
        if window is None:
            continue
        try:
            feature_row = context.feature_builder.build_features(window, bucket_label)
        except ValueError:
            continue
        context.dataset_manager.add_example(feature_row, bucket_label)
        context.feature_columns.update(feature_row.values.keys())
        if bucket_label.label == 1:
            positive_count += 1
        else:
            negative_count += 1
        if (
            context.snapshot_store is not None
            and bucket_label.label == 1
            and (
                context.snapshot_positive_bucket_ids is None
                or bucket_label.bucket_id in context.snapshot_positive_bucket_ids
            )
        ):
            context.snapshot_store.append_window(bucket_label, window)
    return positive_count, negative_count


def _build_window_safe(
    extractor: LookbackWindowExtractor,
    bucket_label: BucketLabel,
    target: IncidentTarget,
) -> LookbackWindow | None:
    try:
        window = extractor.build_window(bucket_label, target)
    except ValueError:
        return None
    if not window.buckets:
        return None
    return window


def _handle_shap_output(
    scorer: GBDTScorer,
    test_df: pd.DataFrame,
    context: ShapOutputContext,
) -> tuple[Path, list[dict[str, object]], float | None]:
    """Compute SHAP values in streaming mode, persist parquet, and collect highlights."""
    shap_path = _resolve_shap_path(context.config, context.snapshot_artifacts)
    highlights: list[dict[str, object]] = []
    expected_value: float | None = None
    feature_matrix = test_df[context.feature_columns].copy()
    with _open_shap_writer(shap_path) as writer:
        for meta_slice, score_slice, expected, contrib_df in _stream_shap_batches(
            scorer, feature_matrix, test_df, context.scored
        ):
            if expected_value is None:
                expected_value = float(expected)
            rows = _merge_shap_batch(meta_slice, score_slice, expected, contrib_df)
            _write_shap_rows(writer, rows)
            _update_highlights(highlights, rows, context.config.shap_top_k)
    _maybe_emit_json(context.config.shap_output_path, shap_path)
    final_highlights = _finalise_highlights(
        highlights, shap_path, context.config.shap_top_k
    )
    return shap_path, final_highlights, expected_value


def _compute_metrics(
    probabilities: pd.Series,
    labels: pd.Series,
    threshold: float,
) -> tuple[dict[str, int], dict[str, float]]:
    """Compute confusion matrix and aggregate metrics at the requested threshold."""
    preds = probabilities >= threshold
    actuals = labels.astype(int)

    tp = int(((preds) & (actuals == 1)).sum())
    fp = int(((preds) & (actuals == 0)).sum())
    tn = int(((~preds) & (actuals == 0)).sum())
    fn = int(((~preds) & (actuals == 1)).sum())

    total = tp + tn + fp + fn
    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0

    confusion = {"tp": tp, "fp": fp, "tn": tn, "fn": fn}
    metrics = {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "false_positive_rate": fpr,
    }
    return confusion, metrics


def _build_data_summary(inputs: DataSummaryInputs) -> dict[str, object]:
    train_total = int(len(inputs.train_df))
    test_total = int(len(inputs.test_df))
    train_pos = int(inputs.train_df["label"].sum()) if train_total else 0
    test_pos = int(inputs.test_df["label"].sum()) if test_total else 0
    mean_probability = (
        float(inputs.scored["probability"].mean()) if not inputs.scored.empty else 0.0
    )
    if inputs.scored.empty:
        mean_log_odds = 0.0
        median_log_odds = 0.0
        prob_hist_counts: list[int] = [0] * 20
        prob_hist_edges: list[float] = [i / 20 for i in range(21)]
    else:
        probs = inputs.scored["probability"].astype(float).to_numpy()
        probs = np.clip(probs, 1e-12, 1.0 - 1e-12)
        log_odds = np.log(probs / (1.0 - probs))
        mean_log_odds = float(log_odds.mean())
        median_log_odds = float(np.median(log_odds))
        hist_counts, hist_edges = np.histogram(
            inputs.scored["probability"].astype(float).to_numpy(),
            bins=20,
            range=(0.0, 1.0),
        )
        prob_hist_counts = [int(value) for value in hist_counts.tolist()]
        prob_hist_edges = [float(value) for value in hist_edges.tolist()]
    return {
        "train_total": train_total,
        "test_total": test_total,
        "train_positive_rate": train_pos / train_total if train_total else 0.0,
        "test_positive_rate": test_pos / test_total if test_total else 0.0,
        "mean_probability": mean_probability,
        "mean_log_odds": mean_log_odds,
        "median_log_odds": median_log_odds,
        "probability_histogram": {
            "counts": prob_hist_counts,
            "bin_edges": prob_hist_edges,
        },
        "expected_value": inputs.expected_value,
        "data_scope": inputs.data_scope,
        "shap_model_path": str(inputs.shap_model_path),
    }


def _ensure_utc(value: datetime | None) -> datetime | None:
    """Normalise datetimes to UTC with timezone awareness."""
    if value is None or not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _write_summary_json(summary: EvaluationSummary, path: Path) -> None:
    """Persist a machine-readable summary to disk."""
    payload = {
        "artifact_dir": str(summary.artifact_dir),
        "model_path": str(summary.model_path),
        "positive_windows": summary.positive_windows,
        "negative_windows": summary.negative_windows,
        "feature_columns": summary.feature_columns,
        "train_auc": summary.train_auc,
        "threshold": summary.threshold,
        "confusion": summary.confusion,
        "metrics": summary.metrics,
        "artifact_summary": summary.artifact_summary,
    }
    if summary.shap_highlights is not None:
        payload["shap_highlights"] = summary.shap_highlights
    if summary.cv_summary is not None:
        payload["cv_summary"] = summary.cv_summary
    if summary.data_summary is not None:
        payload["data_summary"] = summary.data_summary
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _run_window_analysis(
    artifacts: WindowArtifacts,
    inputs: WindowAnalysisInputs,
) -> None:
    if (
        inputs.snapshot_artifacts is None
        or inputs.shap_path is None
        or not inputs.shap_path.exists()
    ):
        return

    summaries = compute_window_summaries(
        inputs.snapshot_artifacts, inputs.shap_path, inputs.feature_columns
    )
    clusters = cluster_windows(
        summaries,
        max_clusters=5,
        top_features=5,
    )
    binary_itemsets = [
        build_binary_itemset(summary, inputs.config.window_analysis_threshold)
        for summary in summaries
    ]
    assoc_rules = mine_association_rules(
        binary_itemsets,
        min_support=inputs.config.window_analysis_min_support,
        min_confidence=inputs.config.window_analysis_min_confidence,
    )
    event_sequences = load_event_sequences(inputs.snapshot_artifacts)
    sequences = mine_frequent_sequences(
        event_sequences,
        min_support=inputs.config.window_analysis_min_support,
    )

    output_dir = artifacts.summary_path.parent
    _write_json(output_dir / "window_clusters.json", clusters)
    _write_json(output_dir / "window_association_rules.json", assoc_rules)
    _write_json(output_dir / "window_sequences.json", sequences)

    if (
        inputs.config.compute_interactions
        and inputs.scorer is not None
        and inputs.test_df is not None
    ):
        interaction_context = InteractionContext(
            snapshot_artifacts=inputs.snapshot_artifacts,
            scorer=inputs.scorer,
            test_df=inputs.test_df,
            feature_columns=inputs.feature_columns,
            config=inputs.config,
        )
        interaction_payload = _compute_and_write_interactions(
            window_vectors=summaries,
            context=interaction_context,
        )
        if interaction_payload is not None:
            _write_json(output_dir / "window_interactions.json", interaction_payload)


def _resolve_shap_path(
    config: EvaluationConfig, snapshot_artifacts: WindowSnapshotArtifacts | None
) -> Path:
    if config.shap_output_path and config.shap_output_path.suffix.lower() == ".parquet":
        target = config.shap_output_path
    elif snapshot_artifacts is not None:
        target = snapshot_artifacts.root / "bucket_shap.parquet"
    else:
        target = config.output_root / "bucket_shap.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _shap_schema() -> pa.schema:
    return pa.schema(
        [
            ("bucket_id", pa.string()),
            ("probability", pa.float64()),
            ("label", pa.int32()),
            ("expected_value", pa.float64()),
            ("contributions", pa.string()),
        ]
    )


@contextmanager
def _open_shap_writer(path: Path) -> Iterator[pq.ParquetWriter]:
    writer = pq.ParquetWriter(path, _shap_schema())
    try:
        yield writer
    finally:
        writer.close()


def _stream_shap_batches(
    scorer: GBDTScorer,
    feature_matrix: pd.DataFrame,
    test_df: pd.DataFrame,
    scored: pd.DataFrame,
    batch_size: int = 2000,
) -> Iterator[tuple[pd.DataFrame, pd.DataFrame, float, pd.DataFrame]]:
    start = 0
    for expected_value, contrib_df in scorer.stream_shap_values(
        feature_matrix, batch_size=batch_size
    ):
        end = start + len(contrib_df)
        meta_slice = test_df.iloc[start:end].reset_index(drop=True)
        score_slice = scored.iloc[start:end].reset_index(drop=True)
        yield meta_slice, score_slice, expected_value, contrib_df
        start = end


def _merge_shap_batch(
    meta_slice: pd.DataFrame,
    score_slice: pd.DataFrame,
    expected_value: float,
    contrib_df: pd.DataFrame,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx in range(len(meta_slice)):
        rows.append(
            _build_shap_row(
                bucket_id=str(meta_slice.iloc[idx]["bucket_id"]),
                label=int(meta_slice.iloc[idx]["label"]),
                probability=float(score_slice.iloc[idx]["probability"]),
                expected_value=float(expected_value),
                contributions=contrib_df.iloc[idx].to_dict(),
            )
        )
    return rows


def _build_shap_row(
    bucket_id: str,
    label: int,
    probability: float,
    expected_value: float,
    contributions: dict[str, float],
) -> dict[str, object]:
    return {
        "bucket_id": bucket_id,
        "probability": probability,
        "label": label,
        "expected_value": expected_value,
        "contributions": contributions,
    }


def _write_shap_rows(writer: pq.ParquetWriter, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    serialised = [_serialise_shap_row(row) for row in rows]
    table = pa.Table.from_pylist(serialised, schema=_shap_schema())
    writer.write_table(table)


def _serialise_shap_row(row: dict[str, object]) -> dict[str, object]:
    serialised = dict(row)
    serialised["contributions"] = json.dumps(row["contributions"])
    return serialised


def _maybe_emit_json(shap_output_path: Path | None, shap_path: Path) -> None:
    if shap_output_path is None:
        return
    shap_output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = shap_output_path.suffix.lower()
    if suffix == ".json":
        _write_shap_json(shap_output_path, shap_path)
    elif suffix == ".parquet" and shap_output_path != shap_path:
        shutil.copy2(shap_path, shap_output_path)


def _write_shap_json(json_path: Path, shap_path: Path) -> None:
    with json_path.open("w", encoding="utf-8") as handle:
        handle.write("[")
        first = True
        for row in _iter_shap_rows(shap_path) or []:
            payload = dict(row)
            payload["contributions"] = json.loads(payload["contributions"])
            snippet = json.dumps(payload)
            if not first:
                handle.write(",")
            handle.write(snippet)
            first = False
        handle.write("]")


def _update_highlights(
    highlights: list[dict[str, object]], rows: list[dict[str, object]], top_k: int
) -> None:
    for row in rows:
        if row["label"] != 1:
            continue
        highlights.append(_build_highlight(row, top_k))


def _build_highlight(row: dict[str, object], top_k: int) -> dict[str, object]:
    contributions = row["contributions"]
    top_features = sorted(
        contributions.items(),
        key=lambda item: abs(item[1]),
        reverse=True,
    )[:top_k]
    return {
        "bucket_id": row["bucket_id"],
        "probability": row["probability"],
        "label": row["label"],
        "top_contributions": top_features,
    }


def _finalise_highlights(
    highlights: list[dict[str, object]], shap_path: Path, top_k: int
) -> list[dict[str, object]]:
    if highlights:
        return highlights
    return _ensure_highlights(shap_path, top_k)


def _ensure_highlights(shap_path: Path, top_k: int) -> list[dict[str, object]]:
    fallback: list[dict[str, object]] = []
    for row in _iter_shap_rows(shap_path) or []:
        contributions = json.loads(row["contributions"])
        highlight = {
            "bucket_id": row["bucket_id"],
            "probability": row["probability"],
            "label": row["label"],
            "top_contributions": sorted(
                contributions.items(),
                key=lambda item: abs(item[1]),
                reverse=True,
            )[:top_k],
        }
        fallback.append(highlight)
    fallback.sort(key=lambda item: item["probability"], reverse=True)
    return fallback[:3]


def _iter_shap_rows(path: Path) -> Iterator[dict[str, object]]:
    if not path.exists():
        return
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches():
        yield from batch.to_pylist()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _compute_and_write_interactions(
    window_vectors: list[dict[str, float]],
    context: InteractionContext,
) -> dict | None:
    if context.test_df.empty:
        return None

    feature_matrix = context.test_df[context.feature_columns].copy()
    bucket_ids = list(context.test_df["bucket_id"])
    selected_features: list[str] = []
    start = 0

    def _interaction_batches() -> Iterator[tuple[list[str], np.ndarray, list[str]]]:
        nonlocal start, selected_features
        for _, interactions, feature_names in context.scorer.stream_interaction_values(
            feature_matrix,
            batch_size=context.config.interaction_batch_size,
            top_k_features=context.config.interaction_top_k,
        ):
            end = start + len(interactions)
            slice_ids = bucket_ids[start:end]
            start = end
            if not selected_features:
                selected_features = list(feature_names)
            yield slice_ids, interactions, feature_names

    interaction_summaries = compute_interaction_summaries(
        context.snapshot_artifacts, _interaction_batches()
    )
    global_top = extract_top_interactions(interaction_summaries, top_k=10)
    cluster_memberships = _cluster_memberships(window_vectors)

    per_cluster: list[dict] = []
    for cluster_id, window_indices in cluster_memberships.items():
        subset = [interaction_summaries[idx] for idx in window_indices]
        top = extract_top_interactions(subset, top_k=10)
        per_cluster.append(
            {
                "cluster_id": cluster_id,
                "window_count": len(window_indices),
                "top_interactions": top,
            }
        )

    per_cluster.sort(key=lambda item: item["window_count"], reverse=True)

    return {
        "global_top_interactions": global_top,
        "per_cluster_interactions": per_cluster,
        "features_considered": selected_features,
    }


def _cluster_memberships(window_vectors: list[dict[str, float]]) -> dict[str, list[int]]:
    """Assign windows to clusters based on dominant SHAP mean feature."""
    memberships: dict[str, list[int]] = {}
    prefix = "shap_mean::"
    for idx, vector in enumerate(window_vectors):
        if not vector:
            continue
        dominant_feature = max(
            vector.items(),
            key=lambda item: abs(item[1]),
        )[0]
        cluster_id = dominant_feature.removeprefix(prefix)
        memberships.setdefault(cluster_id, []).append(idx)
    return memberships

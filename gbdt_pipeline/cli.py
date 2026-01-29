"""Command-line entry points for the lightweight GBDT pipeline."""
# ruff: noqa: I001

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import cast
from textwrap import shorten

import click
import pandas as pd
import pyarrow.parquet as pq
import yaml
from gbdt_pipeline.evaluation import (  # isort: split
    EvaluationConfig,
    EvaluationError,
    EvaluationSummary,
    run_evaluation,
)
from gbdt_pipeline.scorer import GBDTScorer
from gbdt_pipeline.trainer import GBDTTrainer, TrainingConfig
from gbdt_pipeline.window_analysis import NegativeWindowStrategy

try:  # Optional dependency: richer console tables when installed.
    from rich.console import Console as _RichConsole
    from rich.table import Table as _RichTable
except Exception:  # pragma: no cover
    Console: type[object] | None = None
    Table: type[object] | None = None
else:  # pragma: no cover
    Console = _RichConsole
    Table = _RichTable


def _shap_optional_str(raw: dict[str, object], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    raise click.ClickException(f"shap config: {key} must be a string")


def _try_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _load_template_strings_from_buckets(bucket_path: Path) -> dict[str, str]:
    """Return template_id -> template_str by scanning the bucket parquet templates column."""
    pf = pq.ParquetFile(bucket_path)
    template_map: dict[str, str] = {}
    for batch in pf.iter_batches(columns=["templates"], batch_size=256):
        for cell in batch.column(0).to_pylist():
            if not cell:
                continue
            try:
                data = json.loads(cell) if isinstance(cell, str) else cell
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            for template_id, payload in data.items():
                tid = str(template_id)
                if tid in template_map:
                    continue
                if isinstance(payload, dict) and isinstance(payload.get("template_str"), str):
                    template_map[tid] = payload["template_str"]
    return template_map


def _parse_template_selection(selection: str, *, max_index: int) -> list[int]:
    """Parse a selection string like '1,3,5' or 'all' into 1-based indices."""
    selection = selection.strip()
    if selection.lower() == "all":
        return list(range(1, max_index + 1))

    tokens = [tok.strip() for tok in selection.split(",") if tok.strip()]
    if not tokens:
        raise click.ClickException("Selection cannot be empty.")

    try:
        indices = [int(tok) for tok in tokens]
    except ValueError as exc:
        raise click.ClickException(
            "Invalid selection. Use comma-separated numbers like '1,3,5' or 'all'."
        ) from exc

    out_of_range = [idx for idx in indices if idx < 1 or idx > max_index]
    if out_of_range:
        raise click.ClickException(f"Selection out of range: {out_of_range}")
    return indices


def _prompt_template_family_selection(
    matches: list[tuple[str, str]],
    *,
    substring: str,
) -> list[str]:
    """Print matches and return selected template ids after confirmation."""
    click.echo(f"Matched {len(matches)} templates for incident_substring={substring!r}:")
    for idx, (template_id, template_str) in enumerate(matches, start=1):
        shown = shorten(template_str, width=120, placeholder="…")
        click.echo(f"  {idx:>2}. {template_id}: {shown}")

    if len(matches) == 1:
        if not click.confirm("Use this template family?", default=False):
            raise click.ClickException("Aborted.")
        return [matches[0][0]]

    selection = click.prompt(
        "Select templates by number (comma-separated) or 'all'",
        default="1",
        show_default=True,
    )
    indices = _parse_template_selection(str(selection), max_index=len(matches))
    selected_ids = [matches[idx - 1][0] for idx in indices]

    if not click.confirm(
        f"Proceed with selected templates: {', '.join(selected_ids)}?",
        default=False,
    ):
        raise click.ClickException("Aborted.")
    return selected_ids


def _resolve_incident_family_from_substring(
    bucket_path: Path,
    substring: str,
) -> tuple[str, tuple[str, ...]]:
    """Interactively resolve incident template family from a substring."""
    needle = substring.strip().lower()
    if not needle:
        raise click.ClickException("incident_substring cannot be empty")

    template_map = _load_template_strings_from_buckets(bucket_path)
    matches = sorted(
        [
            (template_id, template_str)
            for template_id, template_str in template_map.items()
            if needle in template_str.lower()
        ],
        key=lambda item: (_try_int(item[0]) or 10**18, item[0]),
    )
    if not matches:
        raise click.ClickException(
            f"No templates matched incident_substring={substring!r} in {bucket_path}"
        )

    selected_ids = _prompt_template_family_selection(matches, substring=substring)
    selected_unique = sorted(
        set(selected_ids),
        key=lambda tid: (_try_int(tid) or 10**18, tid),
    )
    incident_template = selected_unique[0]
    equivalent_templates = tuple(selected_unique[1:])
    return incident_template, equivalent_templates


def _resolve_shap_incident_templates(
    raw: dict[str, object],
    *,
    bucket_path: Path,
) -> tuple[str, tuple[str, ...]]:
    """Resolve incident_template/equivalent_templates for SHAP config."""
    incident_substring = _shap_optional_str(raw, "incident_substring")
    incident_template = _shap_optional_str(raw, "incident_template")
    if (incident_substring is None) == (incident_template is None):
        raise click.ClickException(
            "shap config: provide exactly one of incident_template or incident_substring"
        )

    equivalent_templates = tuple(
        str(x) for x in _shap_optional_list(raw, "equivalent_templates")
    )

    if incident_substring is None:
        assert incident_template is not None
        return incident_template, equivalent_templates

    resolved_incident, resolved_equivs = _resolve_incident_family_from_substring(
        bucket_path, incident_substring
    )
    union_equivs = set(equivalent_templates) | set(resolved_equivs)
    union_equivs.discard(resolved_incident)
    merged_equivs = tuple(
        sorted(union_equivs, key=lambda tid: (_try_int(tid) or 10**18, tid))
    )
    click.echo(
        f"Resolved incident family: incident_template={resolved_incident}, "
        f"equivalent_templates={list(merged_equivs)}"
    )
    return resolved_incident, merged_equivs


@click.group()
def cli() -> None:
    """Entry-point for the GBDT evaluation CLI."""


@cli.command("train")
@click.option(
    "--data",
    "data_path",
    required=True,
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="CSV containing training rows (must include label column and feature columns).",
)
@click.option("--target-name", "target_name", required=True, type=str)
@click.option("--label-column", "label_column", required=True, type=str)
@click.option(
    "--feature-columns",
    "feature_columns",
    required=True,
    multiple=True,
    help="Feature columns to train on. Repeatable.",
)
@click.option(
    "--model-output",
    "model_output_path",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
    help="Path to write model metadata JSON (booster is written alongside).",
)
def train(
    data_path: Path,
    target_name: str,
    label_column: str,
    feature_columns: tuple[str, ...],
    model_output_path: Path,
) -> None:
    """Train a LightGBM model from a CSV file and write model artifacts."""
    df = pd.read_csv(data_path)
    config = TrainingConfig(
        target_name=target_name,
        feature_columns=list(feature_columns),
        label_column=label_column,
        model_output_path=model_output_path,
    )
    trainer = GBDTTrainer(config)
    trainer.train(df, pd.DataFrame(), df)


@cli.command("score")
@click.option(
    "--data",
    "data_path",
    required=True,
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="CSV containing rows to score (must include feature columns).",
)
@click.option(
    "--feature-columns",
    "feature_columns",
    required=True,
    multiple=True,
    help="Feature columns to score. Repeatable.",
)
@click.option(
    "--model",
    "model_path",
    required=True,
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Model metadata JSON produced by the train command.",
)
@click.option(
    "--predictions",
    "predictions_path",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
    help="CSV output path for predictions.",
)
def score(
    data_path: Path,
    feature_columns: tuple[str, ...],
    model_path: Path,
    predictions_path: Path,
) -> None:
    """Score a CSV file using an existing model and write probabilities."""
    df = pd.read_csv(data_path)
    scorer = GBDTScorer(model_path=model_path, feature_columns=list(feature_columns))
    preds = scorer.score(df)
    preds.to_csv(predictions_path, index=False)


@cli.command("evaluate")
@click.option(
    "--incident-template",
    "incident_template",
    required=True,
    help="Primary incident template identifier (e.g. 970).",
)
@click.option(
    "--bucket-path",
    "bucket_path",
    required=True,
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Path to the bucket parquet file used for feature generation.",
)
@click.option(
    "--lead",
    "lead_minutes",
    type=int,
    default=15,
    show_default=True,
    help="Initial look-back window in minutes.",
)
@click.option(
    "--max",
    "max_minutes",
    type=int,
    default=60,
    show_default=True,
    help="Maximum look-back expansion in minutes.",
)
@click.option(
    "--equivalent-template",
    "equivalent_templates",
    multiple=True,
    help="Additional template identifiers considered equivalent symptoms.",
)
@click.option(
    "--output-root",
    "output_root",
    default=Path("artifacts/lookback"),
    type=click.Path(path_type=Path, file_okay=False),
    show_default=True,
    help="Directory used to store look-back artifacts.",
)
@click.option(
    "--threshold",
    type=float,
    default=0.5,
    show_default=True,
    help="Classification threshold applied to surrogate scores.",
)
@click.option(
    "--threshold-policy",
    "threshold_policy",
    type=click.Choice(["negative-quantile", "fixed"]),
    default="negative-quantile",
    show_default=True,
    help="How to choose the decision threshold used for metrics.",
)
@click.option(
    "--negative-quantile-rate",
    "negative_quantile_rate",
    type=float,
    default=0.001,
    show_default=True,
    help=(
        "Target fraction of negatives allowed above the threshold when using "
        "negative-quantile policy."
    ),
)
@click.option(
    "--reuse-artifacts",
    "reuse_artifacts",
    type=click.Path(path_type=Path),
    help="Reuse an existing artifact directory instead of invoking the window builder.",
)
@click.option(
    "--filter",
    "filters",
    multiple=True,
    help="Optional bucket filters in key=value form passed through to the window builder.",
)
@click.option(
    "--summary-json",
    "summary_json",
    type=click.Path(path_type=Path),
    help="Optional path where the evaluation summary should be written as JSON.",
)
@click.option(
    "--shap-json",
    "shap_json",
    type=click.Path(path_type=Path),
    help="Optional path to write SHAP contributions (LightGBM models only).",
)
@click.option(
    "--shap-top-k",
    "shap_top_k",
    type=int,
    default=5,
    show_default=True,
    help="Number of top SHAP contributors to display in the console.",
)
@click.option(
    "--shap-bucket-limit",
    "shap_bucket_limit",
    type=int,
    default=10,
    show_default=True,
    help=(
        "Maximum number of per-bucket SHAP tables to print to the console. "
        "If there are more, a detailed per-bucket report is written to disk instead."
    ),
)
@click.option(
    "--shap-report-top-n",
    "shap_report_top_n",
    type=int,
    default=10,
    show_default=True,
    help="Top-N features/templates to include in the aggregate SHAP summary tables.",
)
@click.option(
    "--exclude-file",
    "exclude_file",
    type=click.Path(path_type=Path),
    help="YAML/JSON file listing templates or regex patterns to ignore.",
)
@click.option(
    "--balance-classes/--no-balance-classes",
    "balance_classes",
    default=True,
    show_default=True,
    help="Enable LightGBM class weighting (scale_pos_weight) for rare positives.",
)
@click.option(
    "--cv-strategy",
    "cv_strategy",
    type=click.Choice(["none", "time-series"]),
    default="none",
    show_default=True,
    help="Cross-validation strategy for time-aware evaluation.",
)
@click.option(
    "--cv-folds",
    "cv_folds",
    type=int,
    default=5,
    show_default=True,
    help="Number of time-series folds when using --cv-strategy time-series.",
)
@click.option(
    "--cv-test-size",
    "cv_test_size",
    type=int,
    default=None,
    help="Test window size per fold (rows) for time-series CV.",
)
@click.option(
    "--cv-min-train-size",
    "cv_min_train_size",
    type=int,
    default=None,
    help="Minimum training rows for the first time-series fold.",
)
@click.option(
    "--auto-lookback/--no-auto-lookback",
    "auto_lookback_enabled",
    default=False,
    show_default=True,
    help="Select the best lookback window via time-series CV before evaluation.",
)
@click.option(
    "--auto-lookback-lead",
    "auto_lookback_leads",
    type=int,
    multiple=True,
    help="Override the lead-minute candidates for auto-lookback (repeatable).",
)
@click.option(
    "--auto-lookback-metric",
    "auto_lookback_metric",
    type=click.Choice(
        ["accuracy", "precision", "recall", "false_positive_rate", "train_auc"]
    ),
    default="recall",
    show_default=True,
    help="Metric to optimize for auto-lookback selection.",
)
@click.option(
    "--symptom-substring",
    "symptom_substring",
    type=str,
    default=None,
    help="Only label positive when symptom instance text contains this substring.",
)
@click.option(
    "--symptom-var",
    "symptom_vars",
    multiple=True,
    help="Filter symptom instances by variable value (e.g. var4=worker). Repeatable.",
)
@click.option(
    "--window-analysis/--no-window-analysis",
    "window_analysis",
    default=False,
    show_default=True,
    help="Emit window-level SHAP analysis artifacts (clusters, associations, sequences).",
)
@click.option(
    "--window-analysis-threshold",
    "window_analysis_threshold",
    type=float,
    default=1.0,
    show_default=True,
    help="Mean SHAP threshold used when building binary itemsets.",
)
@click.option(
    "--window-analysis-support",
    "window_analysis_support",
    type=float,
    default=0.2,
    show_default=True,
    help="Minimum support applied to association and sequence mining.",
)
@click.option(
    "--window-analysis-confidence",
    "window_analysis_confidence",
    type=float,
    default=0.5,
    show_default=True,
    help="Minimum confidence applied to association rules.",
)
@click.option(
    "--window-negative-mode",
    "window_negative_mode",
    type=click.Choice(["time-matched", "template-matched", "none"]),
    default="time-matched",
    show_default=True,
    help="Strategy used to select contrastive negative windows.",
)
@click.option(
    "--window-negative-time-tolerance",
    "window_negative_time_tolerance",
    type=int,
    default=60,
    show_default=True,
    help="Time tolerance (minutes) when matching negatives by time.",
)
@click.option(
    "--window-negative-max",
    "window_negative_max",
    type=int,
    default=1,
    show_default=True,
    help="Maximum negatives selected per positive window.",
)
@click.option(
    "--window-negative-skip-adjacency/--window-negative-allow-adjacency",
    "window_negative_skip_adjacency",
    default=True,
    show_default=True,
    help="Skip negatives that overlap the positive window.",
)
@click.option(
    "--window-negative-symptom",
    "window_negative_symptoms",
    multiple=True,
    help="Symptom template IDs required for template-matched negatives. Repeatable.",
)
@click.option(
    "--exclude-metric",
    "exclude_metrics",
    multiple=True,
    help="Metric fields to exclude from feature generation (e.g. error_count). Repeatable.",
)
@click.option(
    "--compute-interactions/--no-compute-interactions",
    "compute_interactions",
    default=False,
    show_default=True,
    help="Compute SHAP interaction summaries (expensive).",
)
@click.option(
    "--interaction-top-k",
    "interaction_top_k",
    type=int,
    default=30,
    show_default=True,
    help="Limit interaction computation to the top-K SHAP-variance features.",
)
@click.option(
    "--interaction-batch-size",
    "interaction_batch_size",
    type=int,
    default=500,
    show_default=True,
    help="Batch size used when streaming interaction matrices.",
)
@click.option(
    "--episode-collapse/--no-episode-collapse",
    "episode_collapse_enabled",
    default=False,
    show_default=True,
    help=(
        "Learn an episode gap per run and collapse bursty positives into one "
        "anchor per episode for window snapshots/analysis."
    ),
)
@click.option(
    "--episode-gap-min-intervals",
    "episode_gap_min_intervals",
    type=int,
    default=20,
    show_default=True,
    help="Minimum symptom inter-arrival intervals required before learning an episode gap.",
)
def evaluate(**options: object) -> None:
    """Evaluate the surrogate model against an incident template."""
    config = _build_evaluation_config(options)
    try:
        summary = run_evaluation(config)
    except EvaluationError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # pragma: no cover - safeguard
        raise click.ClickException(f"Evaluation failed: {exc}") from exc

    _render_summary(summary)


@cli.command("shap")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="YAML/JSON config file for a SHAP run (recommended).",
)
@click.option(
    "--init-config",
    "init_config_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Write a default SHAP config template to this path and exit.",
)
def shap_cmd(config_path: Path | None, init_config_path: Path | None) -> None:
    """Run a SHAP-oriented evaluation using a config file (minimal flags)."""
    if (config_path is None) == (init_config_path is None):
        raise click.ClickException("Provide exactly one of --config or --init-config.")

    if init_config_path is not None:
        _write_default_shap_config(init_config_path)
        click.echo(f"Wrote default SHAP config to {init_config_path}")
        return

    assert config_path is not None
    config = _build_shap_evaluation_config(config_path)
    try:
        summary = run_evaluation(config)
    except EvaluationError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # pragma: no cover - safeguard
        raise click.ClickException(f"Evaluation failed: {exc}") from exc
    _render_summary(summary, details=config.details)


@cli.command("window-analysis")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="YAML/JSON config file for a window-analysis run (recommended).",
)
@click.option(
    "--init-config",
    "init_config_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Write a default window-analysis config template to this path and exit.",
)
def window_analysis_cmd(config_path: Path | None, init_config_path: Path | None) -> None:
    """Run a window-analysis-oriented evaluation using a config file (minimal flags)."""
    if (config_path is None) == (init_config_path is None):
        raise click.ClickException("Provide exactly one of --config or --init-config.")

    if init_config_path is not None:
        _write_default_window_analysis_config(init_config_path)
        click.echo(f"Wrote default window-analysis config to {init_config_path}")
        return

    assert config_path is not None
    config = _build_window_analysis_evaluation_config(config_path)
    try:
        summary = run_evaluation(config)
    except EvaluationError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:  # pragma: no cover - safeguard
        raise click.ClickException(f"Evaluation failed: {exc}") from exc
    _render_summary(summary)


def _write_default_window_analysis_config(path: Path) -> None:
    """Write a default config template for `cli window-analysis`."""
    payload = {
        # Required
        "incident_template": "84",
        "equivalent_templates": ["854"],
        "bucket_path": "jan-data/buckets.compacted.twowindows.parquet",
        # Outputs
        "output_root": "artifacts/lookback_window_analysis",
        "summary_json": "artifacts/lookback_window_analysis/summary.json",
        # Window settings
        "lead_minutes": 5,
        "max_minutes": 60,
        # Threshold policy (auto)
        "threshold_policy": "negative-quantile",
        "negative_quantile_rate": 0.001,
        # Window analysis knobs
        "window_analysis": True,
        "window_analysis_threshold": 1.0,
        "window_analysis_support": 0.2,
        "window_analysis_confidence": 0.5,
        # Negative selection
        "window_negative_mode": "time-matched",
        "window_negative_time_tolerance": 60,
        "window_negative_max": 1,
        "window_negative_skip_adjacency": True,
        "window_negative_symptoms": [],
        # Episode collapsing (useful to avoid overcounting positives in analysis)
        "episode_collapse": True,
        "episode_gap_min_intervals": 5,
        # Optional: time-series cross-validation settings
        "cv_strategy": "none",
        "cv_folds": 5,
        "cv_test_size": None,
        "cv_min_train_size": None,
        # Optional: auto-select lookback window via time-series CV.
        "auto_lookback": False,
        "auto_lookback_leads": [],
        "auto_lookback_metric": "recall",
        # Exclusions (optional)
        "exclude_file": None,
        "exclude_metrics": [],
        # Optional symptom filters
        "symptom_substring": None,
        "symptom_vars": {},
        # By default, do NOT write shap_json for window-analysis-only.
        "shap_json": None,
        # Aggregate SHAP overview size (top-N features/templates).
        "shap_report_top_n": 10,
        # Interactions are expensive; keep off by default.
        "compute_interactions": False,
        "interaction_top_k": 30,
        "interaction_batch_size": 500,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".yaml", ".yml"}:
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_window_analysis_evaluation_config(path: Path) -> EvaluationConfig:
    """Build an EvaluationConfig for `cli window-analysis` from a config file + defaults."""
    raw = _load_config_file(path)

    incident_template = _shap_require_str(raw, "incident_template")
    bucket_path = Path(_shap_require_str(raw, "bucket_path")).expanduser()
    if not bucket_path.exists():
        raise click.ClickException(f"window-analysis config: bucket_path not found: {bucket_path}")

    equivalent_templates = tuple(
        str(x) for x in _shap_optional_list(raw, "equivalent_templates")
    )
    output_root = Path(
        str(raw.get("output_root") or "artifacts/lookback_window_analysis")
    ).expanduser()
    lead_minutes = int(raw.get("lead_minutes") or 5)
    max_minutes = int(raw.get("max_minutes") or 60)

    threshold_policy = str(raw.get("threshold_policy") or "negative-quantile")
    negative_quantile_rate = float(raw.get("negative_quantile_rate") or 0.001)
    threshold = float(raw.get("threshold") or 0.5)

    summary_json = _shap_optional_path(raw, "summary_json") or (output_root / "summary.json")
    exclude_file = _shap_optional_path(raw, "exclude_file")
    shap_json = _shap_optional_path(raw, "shap_json")  # default None for window-analysis-only
    shap_report_top_n = int(raw.get("shap_report_top_n") or 10)

    symptom_substring = raw.get("symptom_substring")
    if symptom_substring is not None:
        symptom_substring = str(symptom_substring)
    symptom_vars = _shap_optional_mapping(raw, "symptom_vars")

    exclude_metrics = [str(x) for x in _shap_optional_list(raw, "exclude_metrics")]
    cv_strategy = str(raw.get("cv_strategy") or "none")
    cv_folds = int(raw.get("cv_folds") or 5)
    cv_test_size = raw.get("cv_test_size")
    cv_min_train_size = raw.get("cv_min_train_size")
    auto_lookback_enabled = bool(raw.get("auto_lookback") or False)
    auto_lookback_leads = tuple(int(v) for v in raw.get("auto_lookback_leads", []) or [])
    auto_lookback_metric = str(raw.get("auto_lookback_metric") or "recall")

    analysis_controls = {
        "window_analysis": bool(raw.get("window_analysis") if "window_analysis" in raw else True),
        "window_analysis_threshold": float(raw.get("window_analysis_threshold") or 1.0),
        "window_analysis_support": float(raw.get("window_analysis_support") or 0.2),
        "window_analysis_confidence": float(raw.get("window_analysis_confidence") or 0.5),
        "window_negative_mode": str(raw.get("window_negative_mode") or "time-matched"),
        "window_negative_time_tolerance": int(raw.get("window_negative_time_tolerance") or 60),
        "window_negative_max": int(raw.get("window_negative_max") or 1),
        "window_negative_skip_adjacency": bool(
            raw.get("window_negative_skip_adjacency")
            if "window_negative_skip_adjacency" in raw
            else True
        ),
        "window_negative_symptoms": tuple(
            str(x) for x in _shap_optional_list(raw, "window_negative_symptoms")
        ),
        "compute_interactions": bool(
            raw.get("compute_interactions") if "compute_interactions" in raw else False
        ),
        "interaction_top_k": int(raw.get("interaction_top_k") or 30),
        "interaction_batch_size": int(raw.get("interaction_batch_size") or 500),
    }

    episode_collapse = bool(
        raw.get("episode_collapse") if "episode_collapse" in raw else True
    )
    episode_gap_min_intervals = int(raw.get("episode_gap_min_intervals") or 5)

    analysis_opts: dict[str, object] = {
        **analysis_controls,
        "episode_collapse_enabled": episode_collapse,
        "episode_gap_min_intervals": episode_gap_min_intervals,
    }

    options: dict[str, object] = {
        "incident_template": incident_template,
        "equivalent_templates": equivalent_templates,
        "bucket_path": bucket_path,
        "output_root": output_root,
        "lead_minutes": lead_minutes,
        "max_minutes": max_minutes,
        "threshold": threshold,
        "threshold_policy": threshold_policy,
        "negative_quantile_rate": negative_quantile_rate,
        "reuse_artifacts": None,
        "filters": (),
        "summary_json": summary_json,
        "shap_json": shap_json,
        "shap_top_k": 5,
        "shap_bucket_limit": 10,
        "shap_report_top_n": shap_report_top_n,
        "exclude_file": exclude_file,
        "balance_classes": True,
        "cv_strategy": cv_strategy,
        "cv_folds": cv_folds,
        "cv_test_size": cv_test_size,
        "cv_min_train_size": cv_min_train_size,
        "auto_lookback_enabled": auto_lookback_enabled,
        "auto_lookback_leads": auto_lookback_leads,
        "auto_lookback_metric": auto_lookback_metric,
        "symptom_substring": symptom_substring,
        "symptom_vars": tuple(f"{k}={v}" for k, v in symptom_vars.items()),
        "exclude_metrics": tuple(exclude_metrics),
    }
    options.update(analysis_opts)
    return _build_evaluation_config(options)

def _write_default_shap_config(path: Path) -> None:
    """Write a default config template for `cli shap`."""
    payload = {
        # Required
        "incident_template": "84",
        # Alternative to incident_template: interactive search by substring.
        # "incident_substring": "are stuck",
        "equivalent_templates": ["854"],
        "bucket_path": "jan-data/buckets.compacted.twowindows.parquet",
        # Outputs
        "output_root": "artifacts/lookback_shap",
        "summary_json": "artifacts/lookback_shap/summary.json",
        # Prefer a readable SHAP format by default.
        "shap_json": "artifacts/lookback_shap/shap.json",
        # Window settings
        "lead_minutes": 5,
        "max_minutes": 60,
        # Threshold policy (auto)
        "threshold_policy": "negative-quantile",
        "negative_quantile_rate": 0.001,
        # SHAP analysis knobs
        "shap_top_k": 5,
        "shap_bucket_limit": 10,
        "shap_report_top_n": 10,
        "details": "compact",
        "window_analysis": True,
        # Episode collapsing
        "episode_collapse": True,
        "episode_gap_min_intervals": 5,
        # Exclusions (optional)
        "exclude_file": None,
        "exclude_metrics": [],
        # Optional: time-series cross-validation settings
        "cv_strategy": "none",
        "cv_folds": 5,
        "cv_test_size": None,
        "cv_min_train_size": None,
        # Optional: auto-select lookback window via time-series CV.
        "auto_lookback": False,
        "auto_lookback_leads": [],
        "auto_lookback_metric": "recall",
        # Optional: if you want to filter symptoms to a substring or variable constraint.
        "symptom_substring": None,
        "symptom_vars": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".yaml", ".yml"}:
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_config_file(path: Path) -> dict[str, object]:
    """Load a YAML/JSON config file."""
    suffix = path.suffix.lower()
    raw_text = path.read_text(encoding="utf-8")
    if suffix in {".yaml", ".yml"}:
        loaded = yaml.safe_load(raw_text) or {}
    elif suffix == ".json":
        loaded = json.loads(raw_text) or {}
    else:
        # Default to YAML for convenience.
        loaded = yaml.safe_load(raw_text) or {}
    if not isinstance(loaded, dict):
        raise click.ClickException(f"Config file {path} must contain a mapping/object.")
    return cast(dict[str, object], loaded)


def _shap_require_str(raw: dict[str, object], key: str) -> str:
    value = str(raw.get(key) or "").strip()
    if not value:
        raise click.ClickException(f"shap config: {key} is required")
    return value


def _shap_optional_list(raw: dict[str, object], key: str) -> list[object]:
    value = raw.get(key)
    if value is None:
        return []
    if isinstance(value, (str, int, float)):
        return [value]
    if isinstance(value, list):
        return value
    raise click.ClickException(f"shap config: {key} must be a list")


def _shap_optional_mapping(raw: dict[str, object], key: str) -> dict[str, object]:
    value = raw.get(key)
    if value is None:
        return {}
    if isinstance(value, dict):
        return cast(dict[str, object], value)
    raise click.ClickException(f"shap config: {key} must be a mapping")


def _shap_optional_path(raw: dict[str, object], key: str) -> Path | None:
    value = raw.get(key)
    if value in (None, ""):
        return None
    return Path(str(value)).expanduser()


def _build_shap_evaluation_config(path: Path) -> EvaluationConfig:
    """Build an EvaluationConfig for `cli shap` from a config file + defaults."""
    raw = _load_config_file(path)
    bucket_path = Path(_shap_require_str(raw, "bucket_path")).expanduser()
    if not bucket_path.exists():
        raise click.ClickException(f"shap config: bucket_path not found: {bucket_path}")

    incident_template, equivalent_templates = _resolve_shap_incident_templates(
        raw,
        bucket_path=bucket_path,
    )

    output_root = Path(str(raw.get("output_root") or "artifacts/lookback_shap")).expanduser()
    lead_minutes = int(raw.get("lead_minutes") or 5)
    max_minutes = int(raw.get("max_minutes") or 60)

    threshold_policy = str(raw.get("threshold_policy") or "negative-quantile")
    negative_quantile_rate = float(raw.get("negative_quantile_rate") or 0.001)
    threshold = float(raw.get("threshold") or 0.5)

    shap_top_k = int(raw.get("shap_top_k") or 5)
    shap_bucket_limit = int(raw.get("shap_bucket_limit") or 10)
    shap_report_top_n = int(raw.get("shap_report_top_n") or 10)
    details = str(raw.get("details") or "compact").strip().lower()
    if details not in {"full", "compact"}:
        raise click.ClickException(
            "shap config: details must be one of ['full', 'compact']"
        )
    window_analysis = bool(raw.get("window_analysis") if "window_analysis" in raw else True)

    episode_collapse = bool(raw.get("episode_collapse") if "episode_collapse" in raw else True)
    episode_gap_min_intervals = int(raw.get("episode_gap_min_intervals") or 5)

    # Paths (optional).
    summary_json = _shap_optional_path(raw, "summary_json") or (output_root / "summary.json")
    shap_json = _shap_optional_path(raw, "shap_json") or (output_root / "shap.json")
    exclude_file = _shap_optional_path(raw, "exclude_file")

    # Optional symptom filters.
    symptom_substring = raw.get("symptom_substring")
    if symptom_substring is not None:
        symptom_substring = str(symptom_substring)
    symptom_vars = _shap_optional_mapping(raw, "symptom_vars")

    # Optional metric exclusions.
    exclude_metrics = [str(x) for x in _shap_optional_list(raw, "exclude_metrics")]
    cv_strategy = str(raw.get("cv_strategy") or "none")
    cv_folds = int(raw.get("cv_folds") or 5)
    cv_test_size = raw.get("cv_test_size")
    cv_min_train_size = raw.get("cv_min_train_size")
    auto_lookback_enabled = bool(raw.get("auto_lookback") or False)
    auto_lookback_leads = tuple(int(v) for v in raw.get("auto_lookback_leads", []) or [])
    auto_lookback_metric = str(raw.get("auto_lookback_metric") or "recall")

    # Feed the existing evaluation config builder to keep behavior consistent.
    options: dict[str, object] = {
        "incident_template": incident_template,
        "equivalent_templates": equivalent_templates,
        "bucket_path": bucket_path,
        "output_root": output_root,
        "lead_minutes": lead_minutes,
        "max_minutes": max_minutes,
        "threshold": threshold,
        "threshold_policy": threshold_policy,
        "negative_quantile_rate": negative_quantile_rate,
        "reuse_artifacts": None,
        "filters": (),
        "summary_json": summary_json,
        "shap_json": shap_json,
        "shap_top_k": shap_top_k,
        "shap_bucket_limit": shap_bucket_limit,
        "shap_report_top_n": shap_report_top_n,
        "details": details,
        "exclude_file": exclude_file,
        "balance_classes": True,
        "cv_strategy": cv_strategy,
        "cv_folds": cv_folds,
        "cv_test_size": cv_test_size,
        "cv_min_train_size": cv_min_train_size,
        "auto_lookback_enabled": auto_lookback_enabled,
        "auto_lookback_leads": auto_lookback_leads,
        "auto_lookback_metric": auto_lookback_metric,
        "symptom_substring": symptom_substring,
        "symptom_vars": tuple(f"{k}={v}" for k, v in symptom_vars.items()),
        "window_analysis": window_analysis,
        "window_analysis_threshold": 1.0,
        "window_analysis_support": 0.2,
        "window_analysis_confidence": 0.5,
        "window_negative_mode": "time-matched",
        "window_negative_time_tolerance": 60,
        "window_negative_max": 1,
        "window_negative_skip_adjacency": True,
        "window_negative_symptoms": (),
        "exclude_metrics": tuple(exclude_metrics),
        "compute_interactions": False,
        "interaction_top_k": 30,
        "interaction_batch_size": 500,
        "episode_collapse_enabled": episode_collapse,
        "episode_gap_min_intervals": episode_gap_min_intervals,
    }
    return _build_evaluation_config(options)

def _build_evaluation_config(options: object) -> EvaluationConfig:
    params = cast(dict[str, object], options)
    incident_template = cast(str, params["incident_template"])
    equivalent_templates = tuple(cast(Iterable[str], params["equivalent_templates"]))

    filter_dict = _parse_key_value_pairs(cast(Iterable[str], params["filters"]))
    symptom_vars = cast(Iterable[str], params["symptom_vars"])
    symptom_var_filters = _parse_key_value_pairs(symptom_vars) if symptom_vars else None

    exclude_file = cast(Path | None, params["exclude_file"])
    file_excludes, pattern_excludes = _load_exclusions(exclude_file)
    auto_excludes = {incident_template, *equivalent_templates}
    combined_excludes = tuple(sorted(set(file_excludes) | auto_excludes))

    window_negative_strategy = NegativeWindowStrategy(
        mode=cast(str, params["window_negative_mode"]),
        time_tolerance_minutes=cast(int, params["window_negative_time_tolerance"]),
        max_negatives_per_positive=max(0, cast(int, params["window_negative_max"])),
        skip_adjacency=cast(bool, params["window_negative_skip_adjacency"]),
        symptom_templates=tuple(cast(Iterable[str], params["window_negative_symptoms"])),
    )

    return EvaluationConfig(
        incident_template=incident_template,
        equivalent_templates=equivalent_templates,
        bucket_path=cast(Path, params["bucket_path"]),
        output_root=cast(Path, params["output_root"]),
        lead_minutes=cast(int, params["lead_minutes"]),
        max_minutes=cast(int, params["max_minutes"]),
        threshold=cast(float, params["threshold"]),
        threshold_policy=cast(str, params["threshold_policy"]),
        negative_quantile_rate=float(cast(float, params["negative_quantile_rate"])),
        filters=filter_dict,
        reuse_artifacts=cast(Path | None, params["reuse_artifacts"]),
        summary_output_path=cast(Path | None, params["summary_json"]),
        shap_output_path=cast(Path | None, params["shap_json"]),
        shap_top_k=cast(int, params["shap_top_k"]),
        shap_bucket_limit=max(0, cast(int, params.get("shap_bucket_limit", 10))),
        shap_report_top_n=max(1, cast(int, params.get("shap_report_top_n", 10))),
        details=str(params.get("details") or "compact"),
        exclude_templates=combined_excludes,
        exclude_patterns=tuple(pattern_excludes),
        exclude_metrics=tuple(
            str(metric) for metric in cast(Iterable[str], params.get("exclude_metrics", ()))
        ),
        balance_classes=cast(bool, params["balance_classes"]),
        cv_strategy=str(params.get("cv_strategy") or "none"),
        cv_folds=max(1, int(cast(int, params.get("cv_folds", 5)))),
        cv_test_size=(
            int(params["cv_test_size"]) if params.get("cv_test_size") is not None else None
        ),
        cv_min_train_size=(
            int(params["cv_min_train_size"])
            if params.get("cv_min_train_size") is not None
            else None
        ),
        auto_lookback_enabled=bool(params.get("auto_lookback_enabled", False)),
        auto_lookback_leads=tuple(
            int(v) for v in cast(Iterable[int], params.get("auto_lookback_leads", ()))
        ),
        auto_lookback_metric=str(params.get("auto_lookback_metric") or "recall"),
        symptom_substring=cast(str | None, params["symptom_substring"]),
        symptom_var_filters=symptom_var_filters,
        window_analysis_enabled=cast(bool, params["window_analysis"]),
        window_analysis_threshold=cast(float, params["window_analysis_threshold"]),
        window_analysis_min_support=cast(float, params["window_analysis_support"]),
        window_analysis_min_confidence=cast(float, params["window_analysis_confidence"]),
        window_negative_strategy=window_negative_strategy,
        compute_interactions=cast(bool, params["compute_interactions"]),
        interaction_top_k=cast(int, params["interaction_top_k"]),
        interaction_batch_size=cast(int, params["interaction_batch_size"]),
        episode_collapse_enabled=cast(bool, params["episode_collapse_enabled"]),
        episode_gap_min_intervals=max(0, cast(int, params["episode_gap_min_intervals"])),
    )


def _parse_key_value_pairs(pairs: Iterable[str]) -> dict[str, str]:
    """Convert CLI key=value arguments into a dictionary."""
    parsed: dict[str, str] = {}
    for item in pairs:
        if "=" not in item:
            raise click.BadParameter(f"Filters must be expressed as key=value, received '{item}'")
        key, value = item.split("=", 1)
        parsed[key] = value
    return parsed


def _load_exclusions(path: Path | None) -> tuple[set[str], list[str]]:
    """Load exclusion rules from a YAML/JSON file."""
    templates: set[str] = set()
    patterns: list[str] = []
    if path is None:
        return templates, patterns

    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError) as exc:  # pragma: no cover - filesystem / parse errors
        raise click.ClickException(f"Failed to parse exclude file {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise click.ClickException(f"Exclude file {path} must contain a mapping.")

    file_templates = raw.get("templates") or []
    file_patterns = raw.get("patterns") or raw.get("regex") or []

    for template_id in file_templates:
        templates.add(str(template_id))
    for pattern in file_patterns:
        patterns.append(str(pattern))

    return templates, patterns


def _render_summary(summary: EvaluationSummary, details: str = "compact") -> None:
    """Pretty-print an evaluation summary to the console."""
    if details == "compact":
        _render_shap_overview(summary, compact=True)
        return
    click.echo("GBDT Evaluation ----------------------------")
    click.echo(
        "Windows processed: "
        f"{summary.positive_windows} positives, "
        f"{summary.negative_windows} negatives",
    )
    click.echo("Model backend: LightGBM (linear tree)")
    click.echo(f"Feature count: {len(summary.feature_columns)}")
    click.echo(f"Train AUC: {summary.train_auc:.3f}")
    confusion = summary.confusion
    click.echo(
        "Confusion matrix @ threshold "
        f"{summary.threshold:.2f}: TP={confusion['tp']}, FP={confusion['fp']}, "
        f"TN={confusion['tn']}, FN={confusion['fn']}",
    )
    metrics = summary.metrics
    click.echo(
        "Accuracy: "
        f"{metrics['accuracy']:.3f}  Precision: {metrics['precision']:.3f}  "
        f"Recall: {metrics['recall']:.3f}  FPR: {metrics['false_positive_rate']:.3f}",
    )
    selection = None
    if summary.artifact_summary:
        selection = summary.artifact_summary.get("lookback_selection")
    if isinstance(selection, dict):
        selected = selection.get("selected", {})
        metric = selection.get("metric", "recall")
        click.echo(
            "Auto-lookback selection: "
            f"lead={selected.get('lead_minutes')} max={selected.get('max_minutes')} "
            f"metric={metric} score={selected.get('score')}"
        )
    click.echo(
        "Notes: prob=surrogate model predicted probability of the positive class; "
        "FPR=false positive rate=FP/(FP+TN)."
    )
    click.echo(
        "SHAP note: contributions are in log-odds space; positive values push prob up."
    )
    report_info = _render_shap_overview(summary, compact=False)
    _render_shap_highlights(summary, report_info)


def _render_shap_overview(
    summary: EvaluationSummary, *, compact: bool
) -> dict[str, object]:
    """Render aggregated SHAP overview (if present) and return report metadata."""
    report = summary.artifact_summary.get("shap_report") if summary.artifact_summary else None
    if not isinstance(report, dict):
        return {}

    aggregate_path = report.get("aggregate_json")
    per_bucket_path = report.get("per_bucket_text")
    per_bucket_json = report.get("per_bucket_json")
    if not (isinstance(aggregate_path, str) and Path(aggregate_path).exists()):
        return {
            "per_bucket_path": per_bucket_path,
            "per_bucket_json": per_bucket_json,
        }

    try:
        payload = json.loads(Path(aggregate_path).read_text(encoding="utf-8"))
    except Exception:
        return {
            "per_bucket_path": per_bucket_path,
            "per_bucket_json": per_bucket_json,
        }

    meta = payload.get("meta") or {}
    bucket_limit = int(meta.get("bucket_limit")) if "bucket_limit" in meta else None
    click.echo("")
    click.echo("SHAP overview (aggregated across buckets):")

    if Console is not None and Table is not None:
        console = Console()
        _print_shap_overview_tables(console, payload)
    else:
        for row in (payload.get("top_templates") or []):
            tid = row.get("template_id")
            mean_abs = float(row.get("mean_abs_shap") or 0.0)
            tstr = row.get("template_str") or ""
            click.echo(f"  {mean_abs:6.3f} | template {tid}: {str(tstr)[:120]}")

    if compact:
        return {}

    click.echo(f"Full aggregate report: {aggregate_path}")
    if isinstance(per_bucket_path, str):
        click.echo(f"Per-bucket report: {per_bucket_path}")

    return {
        "bucket_limit": bucket_limit,
        "per_bucket_path": per_bucket_path,
        "per_bucket_json": per_bucket_json,
    }


def _print_shap_overview_tables(console: object, payload: dict[str, object]) -> None:
    """Print Rich tables for aggregated SHAP overview."""
    assert Table is not None
    _print_shap_overview_template_table(console, payload)
    _print_shap_overview_feature_table(console, payload)


def _print_shap_overview_template_table(console: object, payload: dict[str, object]) -> None:
    assert Table is not None
    t_templates = Table(title="Top templates by abs(mean SHAP)")
    t_templates.add_column("mean(SHAP)", justify="right")
    t_templates.add_column("mean(|SHAP|)", justify="right")
    t_templates.add_column("template_id")
    t_templates.add_column("template_str")
    for row in (payload.get("top_templates") or []):
        tid = str(row.get("template_id") or "")
        mean_signed = float(row.get("mean_shap") or 0.0)
        mean_abs = float(row.get("mean_abs_shap") or 0.0)
        tstr = str(row.get("template_str") or "")
        t_templates.add_row(
            f"{mean_signed:+.3f}",
            f"{mean_abs:.3f}",
            tid,
            tstr[:120],
        )
    console.print(t_templates)


def _print_shap_overview_feature_table(console: object, payload: dict[str, object]) -> None:
    assert Table is not None
    t_features = Table(title="Top features by abs(mean SHAP)", expand=True)
    t_features.add_column("mean(SHAP)", justify="right")
    t_features.add_column("mean(|SHAP|)", justify="right")
    t_features.add_column("frac+", justify="right")
    t_features.add_column("feature", overflow="fold", ratio=2)
    t_features.add_column("template_id")
    t_features.add_column("var_type")
    t_features.add_column("template_str", overflow="fold", ratio=4)
    for row in (payload.get("top_features") or []):
        mean_signed = float(row.get("mean_shap") or 0.0)
        mean_abs = float(row.get("mean_abs_shap") or 0.0)
        frac_pos = float(row.get("frac_positive") or 0.0)
        feature = str(row.get("feature") or "")
        tid = str(row.get("template_id") or "")
        vtype = str(row.get("var_type_coarse") or row.get("var_type") or "")
        tstr = str(row.get("template_str") or "")
        t_features.add_row(
            f"{mean_signed:+.3f}",
            f"{mean_abs:.3f}",
            f"{frac_pos:.2f}",
            feature,
            tid,
            vtype,
            tstr,
        )
    console.print(t_features)


def _render_shap_highlights(summary: EvaluationSummary, report_info: dict[str, object]) -> None:
    """Render per-bucket SHAP highlights (tables or fallback text)."""
    if not summary.shap_highlights:
        return

    bucket_limit = report_info.get("bucket_limit")
    per_bucket_path = report_info.get("per_bucket_path")
    per_bucket_json = report_info.get("per_bucket_json")

    if (
        isinstance(bucket_limit, int)
        and isinstance(per_bucket_json, str)
        and Path(per_bucket_json).exists()
        and len(summary.shap_highlights) > bucket_limit
        and Console is not None
        and Table is not None
    ):
        ok = _try_render_per_bucket_tables(per_bucket_json, limit=bucket_limit)
        if ok:
            if isinstance(per_bucket_path, str):
                total = len(summary.shap_highlights)
                click.echo(
                    "Per-bucket SHAP tables written to "
                    f"{per_bucket_path} ({total} buckets > limit={bucket_limit})."
                )
            return

    if (
        isinstance(bucket_limit, int)
        and isinstance(per_bucket_json, str)
        and Path(per_bucket_json).exists()
        and len(summary.shap_highlights) <= bucket_limit
        and Console is not None
        and Table is not None
    ):
        ok = _try_render_per_bucket_tables(per_bucket_json)
        if ok:
            return

    _render_legacy_highlights(summary)


def _try_render_per_bucket_tables(per_bucket_json: str, limit: int | None = None) -> bool:
    """Return True if per-bucket Rich tables were rendered successfully."""
    if Console is None or Table is None:
        return False
    try:
        buckets_payload = json.loads(Path(per_bucket_json).read_text(encoding="utf-8"))
        buckets = buckets_payload.get("buckets") or []
    except Exception:
        return False

    console = Console()
    assert Table is not None
    for idx, bucket in enumerate(buckets):
        if limit is not None and idx >= limit:
            break
        bucket_id = bucket.get("bucket_id")
        label = bucket.get("label")
        prob = float(bucket.get("probability") or 0.0)
        table = Table(title=f"Bucket {bucket_id} (label={label}, prob={prob:.3f})", expand=True)
        table.add_column("SHAP", justify="right")
        # Avoid truncating long feature names; fold/wrap instead of ellipsizing.
        table.add_column("feature", overflow="fold", ratio=4)
        table.add_column("template_id")
        table.add_column("template_str", overflow="fold", ratio=3)
        table.add_column("var")
        table.add_column("var_type")
        for row in bucket.get("rows") or []:
            tstr = str(row.get("template_str") or "")
            table.add_row(
                f"{float(row.get('shap_value') or 0.0):+.3f}",
                str(row.get("feature") or ""),
                str(row.get("template_id") or ""),
                tstr[:200],
                str(row.get("var_name") or ""),
                str(row.get("var_type_coarse") or row.get("var_type") or ""),
            )
        console.print(table)
    return True


def _render_legacy_highlights(summary: EvaluationSummary) -> None:
    click.echo("Top SHAP contributors:")
    for highlight in summary.shap_highlights or []:
        parts = ", ".join(
            f"{feature}={value:+.3f}" for feature, value in highlight["top_contributions"]
        )
        click.echo(
            f"  bucket {highlight['bucket_id']} "
            f"(label={highlight['label']}, prob={highlight['probability']:.3f}): {parts}"
        )


def main(argv: list[str] | None = None) -> int:
    """Programmatic entrypoint for tests and notebooks.

    Click's default behavior is to raise SystemExit; we disable standalone mode so
    callers can invoke commands without needing to catch exceptions.
    """
    rv = cli.main(args=argv, standalone_mode=False)
    return 0 if rv is None else int(rv)


if __name__ == "__main__":
    cli()

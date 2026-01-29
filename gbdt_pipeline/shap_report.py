"""Helpers for rendering more interpretable SHAP outputs.

This module turns raw SHAP "feature_name -> contribution" pairs into richer,
human-friendly tables by:

- parsing feature names into structured meaning (metric vs template vs variable)
- attaching template strings + inferred variable types (from TemplateSummary)
- aggregating SHAP scores across all scored buckets

The reporting logic is intentionally lightweight: it reads `bucket_shap.parquet`
emitted by the evaluation pipeline and writes small JSON/text summaries.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from textwrap import shorten
from typing import Any

import pyarrow.parquet as pq

from gbdt_pipeline.template_inventory import TemplateSummary

try:  # Rich is optional for gbdt_pipeline; we fall back to JSON-only outputs.
    from rich.console import Console as _RichConsole
    from rich.table import Table as _RichTable
except Exception:  # pragma: no cover
    Console: type[object] | None = None
    Table: type[object] | None = None
else:  # pragma: no cover
    Console = _RichConsole
    Table = _RichTable


@dataclass(frozen=True)
class FeatureInfo:
    """Parsed metadata for a model feature name."""

    feature: str
    kind: str  # metric|template|template_var|other
    template_id: str | None = None
    metric_name: str | None = None
    stat: str | None = None
    var_name: str | None = None
    var_value: str | None = None
    var_agg: str | None = None  # last|mean (numeric variables)


def parse_feature_name(feature: str) -> FeatureInfo:
    """Parse a feature name into structured metadata.

    This is best-effort: unknown patterns return kind="other".
    """
    metric = _parse_metric_feature(feature)
    if metric is not None:
        return metric

    template = _parse_template_feature(feature)
    if template is not None:
        return template

    template_var = _parse_template_var_feature(feature)
    if template_var is not None:
        return template_var

    return FeatureInfo(feature=feature, kind="other")


def _parse_metric_feature(feature: str) -> FeatureInfo | None:
    if not feature.startswith("metric_"):
        return None
    # metric_current_error_count, metric_mean_cpu_util, metric_delta_total_events
    expected_parts = 3
    parts = feature.split("_", 2)
    if len(parts) == expected_parts:
        _, stat, metric_name = parts
        return FeatureInfo(
            feature=feature,
            kind="metric",
            metric_name=metric_name,
            stat=stat,
        )
    return FeatureInfo(feature=feature, kind="metric")


def _parse_template_feature(feature: str) -> FeatureInfo | None:
    for prefix, stat in (
        ("template_present_", "present"),
        ("template_count_", "count"),
        ("template_lead_minutes_", "lead_minutes"),
    ):
        if feature.startswith(prefix):
            return FeatureInfo(
                feature=feature,
                kind="template",
                template_id=feature[len(prefix) :],
                stat=stat,
            )
    return None


def _parse_template_var_feature(feature: str) -> FeatureInfo | None:
    if not feature.startswith("template_"):
        return None
    rest = feature[len("template_") :]
    # Numeric: template_{template_id}_{var}_last|mean
    if rest.endswith("_last") or rest.endswith("_mean"):
        base, agg = rest.rsplit("_", 1)
        template_id, _, var_name = base.partition("_")
        if template_id and var_name:
            return FeatureInfo(
                feature=feature,
                kind="template_var",
                template_id=template_id,
                var_name=var_name,
                var_agg=agg,
            )
    # Categorical indicator: template_{template_id}_{var}={value}
    if "=" in rest:
        left, value = rest.split("=", 1)
        template_id, _, var_name = left.partition("_")
        if template_id and var_name:
            return FeatureInfo(
                feature=feature,
                kind="template_var",
                template_id=template_id,
                var_name=var_name,
                var_value=value,
            )
    return FeatureInfo(feature=feature, kind="template_var")


def _template_str(summary: TemplateSummary, template_id: str | None) -> str | None:
    if not template_id:
        return None
    meta = summary.templates.get(template_id)
    if meta is None:
        return None
    return meta.template_str


def _var_type(
    summary: TemplateSummary,
    template_id: str | None,
    var_name: str | None,
) -> str | None:
    if not template_id or not var_name:
        return None
    meta = summary.templates.get(template_id)
    if meta is None:
        return None
    return meta.variable_types.get(var_name)


def _var_type_coarse(var_type: str | None) -> str | None:
    if var_type is None:
        return None
    return "numeric" if var_type == "numeric" else "other"


@dataclass
class FeatureAggregate:
    """Running sums for aggregating SHAP values per feature."""

    sum_abs: float = 0.0
    sum: float = 0.0
    n: int = 0
    pos_sign: int = 0
    neg_sign: int = 0
    sum_abs_pos: float = 0.0
    sum_pos: float = 0.0
    n_pos: int = 0
    sum_abs_neg: float = 0.0
    sum_neg: float = 0.0
    n_neg: int = 0


def aggregate_shap_parquet(shap_path: Path) -> tuple[dict[str, FeatureAggregate], dict[str, int]]:
    """Aggregate SHAP contributions across all rows in bucket_shap.parquet."""
    parquet = pq.ParquetFile(shap_path)
    aggregates: dict[str, FeatureAggregate] = {}
    counts = {"rows": 0, "pos": 0, "neg": 0}

    for batch in parquet.iter_batches(columns=["label", "contributions"]):
        label_col = batch.column(0).to_pylist()
        contrib_col = batch.column(1).to_pylist()
        for label, contrib_json in zip(label_col, contrib_col, strict=False):
            counts["rows"] += 1
            is_pos = int(label) == 1
            counts["pos" if is_pos else "neg"] += 1
            try:
                contributions = json.loads(contrib_json) if isinstance(contrib_json, str) else {}
            except Exception:
                contributions = {}
            if not isinstance(contributions, dict):
                continue
            for feature, value in contributions.items():
                try:
                    f = str(feature)
                    v = float(value)
                except Exception:
                    continue
                agg = aggregates.setdefault(f, FeatureAggregate())
                _update_feature_aggregate(agg, v, is_pos)
    return aggregates, counts


def _update_feature_aggregate(agg: FeatureAggregate, value: float, is_pos: bool) -> None:
    agg.n += 1
    agg.sum += value
    agg.sum_abs += abs(value)
    if value > 0:
        agg.pos_sign += 1
    elif value < 0:
        agg.neg_sign += 1
    if is_pos:
        agg.n_pos += 1
        agg.sum_pos += value
        agg.sum_abs_pos += abs(value)
    else:
        agg.n_neg += 1
        agg.sum_neg += value
        agg.sum_abs_neg += abs(value)


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def build_aggregate_payload(
    *,
    shap_path: Path,
    template_summary: TemplateSummary,
    bucket_limit: int,
    top_n_features: int = 30,
    top_n_templates: int = 30,
) -> dict[str, Any]:
    """Build the JSON payload for the aggregate SHAP report."""
    aggregates, counts = aggregate_shap_parquet(shap_path)

    # Feature-level rows.
    rows: list[dict[str, Any]] = []
    denom = max(1, counts["rows"])
    for feature_raw, agg in aggregates.items():
        info = parse_feature_name(feature_raw)
        var_type = _var_type(template_summary, info.template_id, info.var_name)
        feature_display = normalise_feature_display_name(feature_raw)
        mean_abs = agg.sum_abs / denom
        mean_signed = agg.sum / denom
        abs_mean_signed = abs(mean_signed)
        sign_total = agg.pos_sign + agg.neg_sign
        frac_positive = (agg.pos_sign / sign_total) if sign_total else 0.0
        rows.append(
            {
                "feature": feature_display,
                "feature_raw": feature_raw,
                "mean_shap": mean_signed,
                "abs_mean_shap": abs_mean_signed,
                "mean_abs_shap": mean_abs,
                "frac_positive": frac_positive,
                "mean_abs_shap_pos": (
                    (agg.sum_abs_pos / max(1, counts["pos"])) if counts["pos"] else 0.0
                ),
                "mean_shap_pos": (
                    (agg.sum_pos / max(1, counts["pos"])) if counts["pos"] else 0.0
                ),
                "mean_abs_shap_neg": (
                    (agg.sum_abs_neg / max(1, counts["neg"])) if counts["neg"] else 0.0
                ),
                "mean_shap_neg": (
                    (agg.sum_neg / max(1, counts["neg"])) if counts["neg"] else 0.0
                ),
                "kind": info.kind,
                "stat": info.stat,
                "metric_name": info.metric_name,
                "template_id": info.template_id,
                "template_str": _template_str(template_summary, info.template_id),
                "var_name": info.var_name,
                "var_type": var_type,
                "var_type_coarse": _var_type_coarse(var_type),
                "var_agg": info.var_agg,
                "var_value": info.var_value,
            }
        )

    rows.sort(key=lambda r: float(r["mean_abs_shap"]), reverse=True)

    # Template-level rows (group by template_id).
    template_rollup: dict[str, dict[str, float]] = {}
    for row in rows:
        tid = row.get("template_id")
        if not tid:
            continue
        entry = template_rollup.setdefault(
            str(tid),
            {"sum_abs": 0.0, "sum": 0.0},
        )
        entry["sum_abs"] += float(row["mean_abs_shap"]) * denom
        entry["sum"] += float(row["mean_shap"]) * denom

    template_rows: list[dict[str, Any]] = []
    for tid, sums in template_rollup.items():
        mean_abs = sums["sum_abs"] / denom
        mean_signed = sums["sum"] / denom
        abs_mean_signed = abs(mean_signed)
        template_rows.append(
            {
                "template_id": tid,
                "template_str": _template_str(template_summary, tid),
                "mean_shap": mean_signed,
                "abs_mean_shap": abs_mean_signed,
                "mean_abs_shap": mean_abs,
            }
        )
    template_rows.sort(key=lambda r: float(r["mean_abs_shap"]), reverse=True)

    return {
        "meta": {
            "generated_at": _utc_now_iso(),
            "shap_path": str(shap_path),
            "row_count": counts["rows"],
            "positive_rows": counts["pos"],
            "negative_rows": counts["neg"],
            "bucket_limit": int(bucket_limit),
            "top_n_features": int(top_n_features),
            "top_n_templates": int(top_n_templates),
            "shap_value_space": "log-odds",
            "ranking": "mean_abs_shap",
            "explanation": (
                "Tree SHAP values are contributions in log-odds space. "
                "For each row: logit(probability) ≈ expected_value + sum(contributions). "
                "Positive SHAP pushes probability up; negative pushes it down."
            ),
        },
        "top_features": rows[: max(1, int(top_n_features))],
        "top_templates": template_rows[: max(1, int(top_n_templates))],
    }


def _enrich_highlights(
    highlights: list[dict[str, object]],
    *,
    template_summary: TemplateSummary,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for highlight in highlights:
        top = highlight.get("top_contributions") or []
        rows: list[dict[str, Any]] = []
        for feature_raw, value in list(top):
            feature_raw_str = str(feature_raw)
            info = parse_feature_name(feature_raw_str)
            var_type = _var_type(template_summary, info.template_id, info.var_name)
            rows.append(
                {
                    "feature": normalise_feature_display_name(feature_raw_str),
                    "feature_raw": feature_raw_str,
                    "shap_value": float(value),
                    "kind": info.kind,
                    "stat": info.stat,
                    "metric_name": info.metric_name,
                    "template_id": info.template_id,
                    "template_str": _template_str(template_summary, info.template_id),
                    "var_name": info.var_name,
                    "var_type": var_type,
                    "var_type_coarse": _var_type_coarse(var_type),
                    "var_agg": info.var_agg,
                    "var_value": info.var_value,
                }
            )
        enriched.append(
            {
                "bucket_id": highlight.get("bucket_id"),
                "label": highlight.get("label"),
                "probability": highlight.get("probability"),
                "rows": rows,
            }
        )
    return enriched


def write_per_bucket_report(
    *,
    highlights: list[dict[str, object]],
    template_summary: TemplateSummary,
    output_path: Path,
) -> None:
    """Write a per-bucket Rich table report to a text file."""
    enriched = _enrich_highlights(highlights, template_summary=template_summary)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Always write a JSON companion (easy for downstream tooling).
    json_path = output_path.with_suffix(".json")
    json_path.write_text(json.dumps({"buckets": enriched}, indent=2), encoding="utf-8")

    if Console is None or Table is None:
        # No rich available; nothing else to do.
        return

    with output_path.open("w", encoding="utf-8") as handle:
        console = Console(file=handle, force_terminal=False, color_system=None, width=180)
        console.print("SHAP per-bucket highlights (top contributors)")
        console.print(
            "Notes: SHAP values are in log-odds space; positive values push probability up."
        )
        console.print("")

        for bucket in enriched:
            bucket_id = bucket.get("bucket_id")
            label = bucket.get("label")
            prob = bucket.get("probability")
            console.print(f"Bucket {bucket_id} (label={label}, prob={prob:.3f})")

            table = Table(show_header=True, header_style="bold", expand=True)
            table.add_column("SHAP", justify="right")
            table.add_column("Feature", overflow="fold", ratio=4)
            table.add_column("Kind")
            table.add_column("Template ID")
            table.add_column("Template String", overflow="fold", ratio=3)
            table.add_column("Var")
            table.add_column("Var type")

            for row in bucket.get("rows", []):
                template_str = row.get("template_str")
                shown_template = (
                    shorten(str(template_str), width=80, placeholder="…")
                    if template_str
                    else ""
                )
                var_type = row.get("var_type_coarse") or row.get("var_type") or ""
                table.add_row(
                    f"{float(row.get('shap_value', 0.0)):+.3f}",
                    str(row.get("feature") or ""),
                    str(row.get("kind") or ""),
                    str(row.get("template_id") or ""),
                    shown_template,
                    str(row.get("var_name") or ""),
                    str(var_type),
                )
            console.print(table)
            console.print("")


@dataclass(frozen=True)
class ShapReportOptions:
    """Options controlling how SHAP reports are rendered."""

    bucket_limit: int = 10
    top_n_features: int = 10
    top_n_templates: int = 10


def normalise_feature_display_name(feature: str) -> str:
    """Normalise confusing internal feature prefixes for display.

    We keep model features stable for training/scoring, but present a slightly
    more readable form in reports.
    """
    return feature


def write_shap_reports(
    *,
    shap_path: Path,
    template_summary: TemplateSummary,
    highlights: list[dict[str, object]] | None,
    output_dir: Path,
    options: ShapReportOptions,
) -> dict[str, str]:
    """Write aggregate + per-bucket SHAP reports; return paths."""
    output_dir.mkdir(parents=True, exist_ok=True)

    aggregate_path = output_dir / "shap_report_aggregate.json"
    payload = build_aggregate_payload(
        shap_path=shap_path,
        template_summary=template_summary,
        bucket_limit=options.bucket_limit,
        top_n_features=options.top_n_features,
        top_n_templates=options.top_n_templates,
    )

    per_bucket_path = output_dir / "shap_report_per_bucket.txt"
    payload["meta"]["per_bucket_report_path"] = str(per_bucket_path)

    aggregate_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if highlights:
        write_per_bucket_report(
            highlights=highlights,
            template_summary=template_summary,
            output_path=per_bucket_path,
        )

    return {
        "aggregate_json": str(aggregate_path),
        "per_bucket_text": str(per_bucket_path),
        "per_bucket_json": str(per_bucket_path.with_suffix(".json")),
    }

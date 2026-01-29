"""CLI entrypoint for look-back window planning."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import click

from log_ingest.lookback.window_plan import LookBackWindowPlanner


def _parse_filters(filters: tuple[str, ...]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in filters:
        if "=" not in item:
            raise click.BadParameter(f"Filter values must be key=value, got '{item}'")
        key, value = item.split("=", 1)
        parsed[key] = value
    return parsed


@click.command("window-builder")
@click.option(
    "--incident-template",
    "incident_template",
    required=True,
    help="Primary incident template ID (e.g. 970).",
)
@click.option(
    "--lead",
    "lead_minutes",
    default=15,
    show_default=True,
    type=int,
    help="Initial look-back window size in minutes.",
)
@click.option(
    "--max",
    "max_lead_minutes",
    default=60,
    show_default=True,
    type=int,
    help="Maximum look-back window size in minutes.",
)
@click.option(
    "--equivalent",
    "equivalent_templates",
    multiple=True,
    help="One or more template IDs treated as equivalent symptoms.",
)
@click.option(
    "--bucket-path",
    "bucket_path",
    required=True,
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Path to the bucket parquet file.",
)
@click.option(
    "--filters",
    "filter_args",
    multiple=True,
    help="Optional bucket filters expressed as key=value pairs.",
)
@click.option(
    "--output-root",
    "output_root",
    default=Path("artifacts/lookback"),
    type=click.Path(path_type=Path, file_okay=False),
    show_default=True,
    help="Directory to place look-back artifacts in.",
)
@click.option(
    "--manual-expansion",
    "manual_expansion",
    type=int,
    help="Manually override the computed lead window (minutes).",
)
def window_builder(
    incident_template: str,
    lead_minutes: int,
    max_lead_minutes: int,
    equivalent_templates: tuple[str, ...],
    bucket_path: Path,
    filter_args: tuple[str, ...],
    output_root: Path,
    manual_expansion: int | None,
) -> None:
    """Construct look-back windows for an incident template."""
    filters = _parse_filters(filter_args)
    incident_id = f"incident-{incident_template}"

    planner = LookBackWindowPlanner(
        bucket_path=bucket_path,
        output_root=output_root,
        lead_time_target=lead_minutes,
        max_lead_time=max_lead_minutes,
        equivalent_templates=list(equivalent_templates),
        filters=filters,
        anchor_template_id=incident_template,
    )

    artifacts = planner.build_incident_windows(
        incident_id=incident_id,
        manual_expansion_minutes=manual_expansion,
    )

    click.echo(f"Created window artifacts at {artifacts.summary_path}")

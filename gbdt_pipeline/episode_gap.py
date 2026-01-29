"""Episode gap estimation helpers.

This module provides a small, dependency-light mechanism for learning an "episode gap"
(`g`) from symptom event timestamps. The goal is to collapse bursts of repeated symptom
events into single "episodes" without requiring domain-specific reset markers.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

import numpy as np

MIN_EVENTS_FOR_INTERVALS = 2


@dataclass(frozen=True)
class EpisodeGapDiagnostics:
    """Diagnostics emitted alongside an episode-gap estimate."""

    num_events: int
    num_intervals: int
    interval_minutes_p50: float | None
    interval_minutes_p90: float | None
    interval_minutes_p99: float | None
    method: str


def estimate_episode_gap_minutes(
    timestamps: list[datetime],
    *,
    min_intervals: int = 20,
    min_gap_minutes: int = 1,
    max_gap_minutes: int = 120,
    histogram_bins: int = 64,
) -> tuple[int | None, EpisodeGapDiagnostics]:
    """Estimate an episode gap (minutes) from symptom timestamps.

    Implementation details:
    - Computes inter-arrival times (Δt) in minutes.
    - Works in log-space (log1p(Δt)) to reduce heavy-tail effects.
    - Uses Otsu thresholding on a histogram to split "within-episode" vs "between-episode".
    - Converts the threshold back to minutes and clamps to [min_gap_minutes, max_gap_minutes].

    Returns:
    - gap_minutes: the learned gap in minutes, or None when not enough evidence exists.
    - diagnostics: basic summary statistics useful for debugging/reproducibility.
    """
    cleaned = sorted({ts for ts in timestamps if ts is not None})
    if len(cleaned) < MIN_EVENTS_FOR_INTERVALS:
        return None, EpisodeGapDiagnostics(
            num_events=len(cleaned),
            num_intervals=max(0, len(cleaned) - 1),
            interval_minutes_p50=None,
            interval_minutes_p90=None,
            interval_minutes_p99=None,
            method="insufficient-events",
        )

    deltas = np.diff(np.array([ts.timestamp() for ts in cleaned], dtype=float)) / 60.0
    deltas = deltas[np.isfinite(deltas)]
    deltas = deltas[deltas > 0.0]
    num_intervals = int(deltas.size)
    if num_intervals < min_intervals:
        return None, EpisodeGapDiagnostics(
            num_events=len(cleaned),
            num_intervals=num_intervals,
            interval_minutes_p50=float(np.quantile(deltas, 0.50)) if num_intervals else None,
            interval_minutes_p90=float(np.quantile(deltas, 0.90)) if num_intervals else None,
            interval_minutes_p99=float(np.quantile(deltas, 0.99)) if num_intervals else None,
            method="insufficient-intervals",
        )

    log_deltas = np.log1p(deltas)
    if float(np.min(log_deltas)) == float(np.max(log_deltas)):
        raw = int(math.ceil(float(np.median(deltas))))
        gap = int(min(max(raw, min_gap_minutes), max_gap_minutes))
        return gap, EpisodeGapDiagnostics(
            num_events=len(cleaned),
            num_intervals=num_intervals,
            interval_minutes_p50=float(np.quantile(deltas, 0.50)),
            interval_minutes_p90=float(np.quantile(deltas, 0.90)),
            interval_minutes_p99=float(np.quantile(deltas, 0.99)),
            method="degenerate-median",
        )

    bins = max(8, int(histogram_bins))
    hist, edges = np.histogram(log_deltas, bins=bins)
    hist = hist.astype(float)
    total = float(hist.sum())
    if total <= 0.0:
        return None, EpisodeGapDiagnostics(
            num_events=len(cleaned),
            num_intervals=num_intervals,
            interval_minutes_p50=float(np.quantile(deltas, 0.50)),
            interval_minutes_p90=float(np.quantile(deltas, 0.90)),
            interval_minutes_p99=float(np.quantile(deltas, 0.99)),
            method="empty-histogram",
        )

    # Otsu thresholding on histogram bins (bin centers used as representative values).
    centers = (edges[:-1] + edges[1:]) / 2.0
    weight_bg = np.cumsum(hist) / total
    weight_fg = 1.0 - weight_bg
    mean_bg = np.cumsum(hist * centers) / np.maximum(np.cumsum(hist), 1e-12)
    mean_total = float(np.sum(hist * centers) / total)
    mean_fg = (mean_total - weight_bg * mean_bg) / np.maximum(weight_fg, 1e-12)

    between = weight_bg * weight_fg * (mean_bg - mean_fg) ** 2
    idx = int(np.nanargmax(between))
    threshold_log = float(centers[idx])
    threshold_minutes = float(np.expm1(threshold_log))
    if not math.isfinite(threshold_minutes) or threshold_minutes <= 0.0:
        return None, EpisodeGapDiagnostics(
            num_events=len(cleaned),
            num_intervals=num_intervals,
            interval_minutes_p50=float(np.quantile(deltas, 0.50)),
            interval_minutes_p90=float(np.quantile(deltas, 0.90)),
            interval_minutes_p99=float(np.quantile(deltas, 0.99)),
            method="invalid-threshold",
        )

    # Prefer a gap that represents "between-burst cadence" for episode collapsing.
    #
    # Many targets have a bimodal gap distribution:
    # - short gaps within a burst (e.g., 5 minutes)
    # - longer gaps between bursts (e.g., ~30 minutes)
    #
    # Pure Otsu can choose a threshold close to the short-gap mode. To avoid under-collapsing
    # (treating a burst as multiple episodes), we floor the learned gap at the median gap.
    median_minutes = float(np.quantile(deltas, 0.50))
    gap_minutes = int(math.ceil(max(threshold_minutes, median_minutes)))
    gap_minutes = int(min(max(gap_minutes, min_gap_minutes), max_gap_minutes))
    return gap_minutes, EpisodeGapDiagnostics(
        num_events=len(cleaned),
        num_intervals=num_intervals,
        interval_minutes_p50=median_minutes,
        interval_minutes_p90=float(np.quantile(deltas, 0.90)),
        interval_minutes_p99=float(np.quantile(deltas, 0.99)),
        method="otsu-log1p+median-floor",
    )

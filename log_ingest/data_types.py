"""Project-specific type definitions for log_ingest.

This module contains custom types, type aliases, and data structures
used throughout the log ingestion pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class LogFileCategories:
    """Categorized log files for processing.

    Groups log files into three categories based on their content type:
    - event_logs: General event/activity logs (NAS, PlatformClient)
    - latency_metrics: Performance and latency measurement files
    - system_info: System information and configuration logs

    This is an immutable dataclass to prevent accidental modification
    after file discovery phase.
    """

    event_logs: List[Path]
    latency_metrics: List[Path]
    system_info: List[Path]

    def total_files(self) -> int:
        """Total number of files across all categories."""
        return len(self.event_logs) + len(self.latency_metrics) + len(self.system_info)

    def is_empty(self) -> bool:
        """Check if no files found in any category."""
        return self.total_files() == 0

    def summary(self) -> str:
        """Return a summary string of file counts."""
        return (
            f"Events: {len(self.event_logs)}, "
            f"Latency: {len(self.latency_metrics)}, "
            f"System: {len(self.system_info)}"
        )

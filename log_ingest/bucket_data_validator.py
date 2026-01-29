"""bucket_data_validator.py

Pre-flight validation for bucket creation data completeness.
Validates that discovered files provide sufficient data categories for meaningful analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import click

from log_ingest.bucket_file_finder import DiscoveryResult

# Processing time estimates (seconds per file)
EVENT_FILE_PROCESSING_TIME_SEC = 2.4
LATENCY_FILE_PROCESSING_TIME_SEC = 1.5
SYSTEM_FILE_PROCESSING_TIME_SEC = 1.0


@dataclass(frozen=True)
class ValidationResult:
    """Results from data category validation.

    Attributes:
        is_valid: Whether validation passed (at least one category present)
        missing_categories: List of category names that have no files
        warnings: List of warning messages for missing categories
        estimated_processing_time_sec: Estimated total processing time in seconds
    """

    is_valid: bool
    missing_categories: List[str]
    warnings: List[str]
    estimated_processing_time_sec: float


class DataCategoryValidator:
    """Pre-flight validator for bucket creation data completeness.

    Validates that discovered files provide sufficient data categories for
    meaningful analysis. Warns users about missing categories and allows them
    to proceed with incomplete data if desired.
    """

    def __init__(self, interactive: bool = True) -> None:
        """Initialize validator with interaction mode.

        Args:
            interactive: If True, prompt user for confirmation on warnings.
                        If False, proceed automatically (useful for batch mode).
        """
        self.interactive = interactive

    def validate(self, discovery: DiscoveryResult) -> ValidationResult:
        """Validate discovered files have sufficient data categories.

        Checks if each data category (events, latency, system) has files.
        Warns users about missing categories and prompts for confirmation if
        categories are missing.

        Args:
            discovery: DiscoveryResult from BucketFileFinder with categorized files

        Returns:
            ValidationResult with validation status and warnings

        Raises:
            click.Abort: If user declines to continue with incomplete data
            ValueError: If no data categories are present (cannot proceed)

        Example:
            >>> validator = DataCategoryValidator(interactive=True)
            >>> result = validator.validate(discovery_result)
            >>> if result.is_valid:
            ...     print(f"Estimated time: {result.estimated_processing_time_sec}s")
        """
        self._validate_has_files(discovery)

        missing = self._identify_missing_categories(discovery)
        warnings = self._build_warning_messages(missing)
        estimated_time = self._estimate_processing_time(discovery)

        if missing:
            self._handle_missing_categories(missing, warnings)

        return ValidationResult(
            is_valid=True,
            missing_categories=missing,
            warnings=warnings,
            estimated_processing_time_sec=estimated_time,
        )

    def _validate_has_files(self, discovery: DiscoveryResult) -> None:
        """Validate discovery result has at least one file.

        Args:
            discovery: DiscoveryResult to validate

        Raises:
            ValueError: If no files discovered in any category
        """
        if discovery.total_files == 0:
            raise ValueError(
                "No files discovered in any category. " "Cannot proceed with bucket creation."
            )

    def _identify_missing_categories(self, discovery: DiscoveryResult) -> List[str]:
        """Identify which data categories have no files.

        Args:
            discovery: DiscoveryResult with categorized files

        Returns:
            List of missing category names (e.g., ['events', 'latency'])
        """
        missing = []

        if not discovery.event_files:
            missing.append("events")

        if not discovery.latency_files:
            missing.append("latency")

        if not discovery.system_files:
            missing.append("system")

        return missing

    def _build_warning_messages(self, missing_categories: List[str]) -> List[str]:
        """Build warning messages explaining implications of missing categories.

        Args:
            missing_categories: List of missing category names

        Returns:
            List of warning messages with specific implications
        """
        category_implications = {
            "events": "No event files found - buckets will have no log events for analysis.",
            "latency": "No latency files found - buckets will have no performance metrics.",
            "system": "No system files found - buckets will have no resource utilization data.",
        }

        return [
            category_implications[cat] for cat in missing_categories if cat in category_implications
        ]

    def _estimate_processing_time(self, discovery: DiscoveryResult) -> float:
        """Estimate total processing time based on file counts.

        Uses empirical processing time estimates per file type from spec.

        Args:
            discovery: DiscoveryResult with file counts

        Returns:
            Estimated processing time in seconds

        Note:
            Estimates are based on spec performance data: ~2.4 sec/file for events.
            Actual time may vary based on file size and system load.
        """
        event_time = len(discovery.event_files) * EVENT_FILE_PROCESSING_TIME_SEC
        latency_time = len(discovery.latency_files) * LATENCY_FILE_PROCESSING_TIME_SEC
        system_time = len(discovery.system_files) * SYSTEM_FILE_PROCESSING_TIME_SEC

        return event_time + latency_time + system_time

    def _handle_missing_categories(self, missing: List[str], warnings: List[str]) -> None:
        """Handle missing categories by warning and prompting user.

        Displays warnings about missing categories and their implications.
        In interactive mode, prompts user to confirm continuation.

        Args:
            missing: List of missing category names
            warnings: List of warning messages to display

        Raises:
            click.Abort: If user declines to continue in interactive mode
        """
        if self.interactive:
            self._display_warnings(warnings)
            self._prompt_user_confirmation(missing)

    def _display_warnings(self, warnings: List[str]) -> None:
        """Display warning messages to user.

        Args:
            warnings: List of warning messages to display
        """
        click.echo()
        click.secho("Warning: Incomplete data categories detected", fg="yellow", bold=True)
        click.echo()

        for warning in warnings:
            click.secho(f"  • {warning}", fg="yellow")

        click.echo()

    def _prompt_user_confirmation(self, missing: List[str]) -> None:
        """Prompt user to confirm continuation with incomplete data.

        Args:
            missing: List of missing category names

        Raises:
            click.Abort: If user declines to continue
        """
        missing_str = ", ".join(missing)
        message = f"Continue with missing categories ({missing_str})?"

        if not click.confirm(message, default=False):
            raise click.Abort()


# ============================================================
#                    PUBLIC API
# ============================================================


def validate_discovery_data(
    discovery: DiscoveryResult, interactive: bool = True
) -> ValidationResult:
    """Validate discovered files for bucket creation.

    Convenience function that creates a validator and validates in one call.

    Args:
        discovery: DiscoveryResult from BucketFileFinder
        interactive: Whether to prompt user for confirmation on warnings

    Returns:
        ValidationResult with validation status and warnings

    Raises:
        click.Abort: If user declines to continue with incomplete data
        ValueError: If no data categories are present
    """
    validator = DataCategoryValidator(interactive=interactive)
    return validator.validate(discovery)


def format_validation_summary(result: ValidationResult) -> str:
    """Format validation result as human-readable summary.

    Args:
        result: ValidationResult to format

    Returns:
        Formatted summary string

    Example:
        >>> summary = format_validation_summary(result)
        >>> print(summary)
        Validation Status: PASSED
        Missing Categories: latency
        Estimated Processing Time: 12.5 seconds
    """
    lines = []

    status = "PASSED" if result.is_valid else "FAILED"
    lines.append(f"Validation Status: {status}")

    if result.missing_categories:
        missing_str = ", ".join(result.missing_categories)
        lines.append(f"Missing Categories: {missing_str}")
    else:
        lines.append("Missing Categories: None")

    time_str = _format_time_estimate(result.estimated_processing_time_sec)
    lines.append(f"Estimated Processing Time: {time_str}")

    return "\n".join(lines)


def _format_time_estimate(seconds: float) -> str:
    """Format time estimate as human-readable string.

    Args:
        seconds: Time in seconds

    Returns:
        Formatted string (e.g., "2.5 seconds", "3.2 minutes", "1.5 hours")
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    if seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    hours = seconds / 3600
    return f"{hours:.1f} hours"

"""Rich-based display for TimeBucket objects.

Provides formatted display using Rich library without duplication.
RichBucketDisplay methods accept TimeBucket directly.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterable
from datetime import datetime
from typing import Literal

from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.json import JSON
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from log_ingest.bucket_building.bucket_builder import TimeBucket
from log_ingest.utils import normalize_datetime, parse_ts

# ============================================================================
# Module-level constants
# ============================================================================

logger = logging.getLogger(__name__)

# Severity thresholds
SEVERITY_HIGH_ERROR_THRESHOLD = 10
SEVERITY_HIGH_WARNING_THRESHOLD = 20

# System resource color thresholds
RESOURCE_CRITICAL_THRESHOLD = 90.0  # Red above this
RESOURCE_WARNING_THRESHOLD = 70.0  # Yellow above this

# Latency color thresholds (ms)
LATENCY_CRITICAL_MS = 100.0  # Red
LATENCY_WARNING_MS = 50.0  # Yellow

# Display limits
MAX_COMPONENTS_DISPLAYED = 10
MAX_LIVE_BUCKETS_WINDOW = 10

# Error highlighting thresholds
ERROR_COUNT_CRITICAL = 5
ERROR_COUNT_WARNING = 0


# ============================================================================
# Helper Functions
# ============================================================================


def sanitize_bucket_for_display(bucket: TimeBucket) -> TimeBucket:
    """Sanitize bucket data for display by converting NaN to None.

    Replaces Pydantic's field validators with simple defensive sanitization.
    Modifies bucket in-place.

    Args:
        bucket: TimeBucket to sanitize

    Returns:
        The same bucket (modified in-place)
    """
    # Numeric fields that might have NaN
    numeric_fields = [
        "read_latency_ms",
        "write_latency_ms",
        "read_iops",
        "write_iops",
        "read_throughput_bps",
        "write_throughput_bps",
        "cpu_util",
        "memory_util",
        "disk_util",
        "total_pools",
        "total_volumes",
        "hdd_count",
        "ssd_count",
        "degraded_volumes",
    ]

    for field_name in numeric_fields:
        value = getattr(bucket, field_name, None)
        if value is not None:
            try:
                if math.isnan(value):
                    setattr(bucket, field_name, None)
            except (TypeError, ValueError):
                pass  # Not a float, skip

    # Boolean fields - convert None to False
    bool_fields = ["is_leader", "rebuild_active", "peer_available"]
    for field_name in bool_fields:
        value = getattr(bucket, field_name, None)
        if value is None:
            setattr(bucket, field_name, False)

    return bucket


# ============================================================================
# Helper functions for bucket display
# ============================================================================


def get_duration_minutes(bucket: TimeBucket) -> float:
    """Calculate bucket duration in minutes."""
    return (bucket.end_time - bucket.start_time).total_seconds() / 60


def get_error_rate(bucket: TimeBucket) -> float:
    """Calculate error rate percentage."""
    if bucket.total_events == 0:
        return 0.0
    return (bucket.error_count / bucket.total_events) * 100


def get_severity_emoji(bucket: TimeBucket) -> str:
    """Get severity indicator based on metrics."""
    if bucket.has_spike or bucket.error_count > SEVERITY_HIGH_ERROR_THRESHOLD:
        return "🔴"
    if (
        bucket.warning_count > SEVERITY_HIGH_WARNING_THRESHOLD
        or bucket.error_count > ERROR_COUNT_WARNING
    ):
        return "🟡"
    return "🟢"


def format_time_range(bucket: TimeBucket) -> str:
    """Format bucket time range for display."""
    if bucket.start_time is None or bucket.end_time is None:
        return "N/A"
    start = bucket.start_time
    end = bucket.end_time
    if start.date() == end.date():
        return f"{start:%Y-%m-%d %H:%M}-{end:%H:%M}"
    return f"{start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}"


def _coerce_instance_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return normalize_datetime(value)
    if isinstance(value, str):
        try:
            return parse_ts(value)
        except Exception:
            return None
    return None


def _count_template_instances(bucket: TimeBucket, template_info: dict[str, object]) -> int:
    instances = template_info.get("instances") or []
    if not instances:
        return 0
    if bucket.start_time is None or bucket.end_time is None:
        return len(instances)

    start = normalize_datetime(bucket.start_time)
    end = normalize_datetime(bucket.end_time)
    count = 0
    for instance in instances:
        if not isinstance(instance, dict):
            count += 1
            continue
        ts = _coerce_instance_timestamp(instance.get("timestamp"))
        if ts is None:
            count += 1
            continue
        if start <= ts < end:
            count += 1
    return count


def create_performance_table(bucket: TimeBucket) -> Table:
    """Create a Rich table for performance metrics."""
    table = Table(title="Performance Metrics", box=box.ROUNDED)
    table.add_column("Metric", style="cyan")
    table.add_column("Read", justify="right", style="green")
    table.add_column("Write", justify="right", style="blue")

    # Latency
    read_lat = f"{bucket.read_latency_ms:.1f} ms" if bucket.read_latency_ms is not None else "N/A"
    write_lat = (
        f"{bucket.write_latency_ms:.1f} ms" if bucket.write_latency_ms is not None else "N/A"
    )
    table.add_row("Latency", read_lat, write_lat)

    # IOPS
    read_iops = f"{bucket.read_iops:.0f}" if bucket.read_iops is not None else "N/A"
    write_iops = f"{bucket.write_iops:.0f}" if bucket.write_iops is not None else "N/A"
    table.add_row("IOPS", read_iops, write_iops)

    # Throughput
    if bucket.read_throughput_bps is not None:
        read_tp = f"{bucket.read_throughput_bps / (1024*1024):.1f} MB/s"
    else:
        read_tp = "N/A"
    if bucket.write_throughput_bps is not None:
        write_tp = f"{bucket.write_throughput_bps / (1024*1024):.1f} MB/s"
    else:
        write_tp = "N/A"
    table.add_row("Throughput", read_tp, write_tp)

    return table


def create_progress_bar(value: float, max_val: float, label: str) -> Text:
    """Create a visual progress bar with color coding.

    Args:
        value: Current value
        max_val: Maximum value for percentage calculation
        label: Label text for the bar

    Returns:
        Rich Text object with colored progress bar
    """
    percentage = (value / max_val) * 100 if max_val > 0 else 0
    bar_width = 20
    filled = int((percentage / 100) * bar_width)

    # Choose color based on percentage thresholds
    if percentage > RESOURCE_CRITICAL_THRESHOLD:
        color = "red"
    elif percentage > RESOURCE_WARNING_THRESHOLD:
        color = "yellow"
    else:
        color = "green"

    bar = "█" * filled + "░" * (bar_width - filled)
    return Text(f"{label:8} [{bar}] {percentage:5.1f}%", style=color)


def create_system_panel(bucket: TimeBucket) -> Panel:
    """Create a panel for system resources."""
    items: list[Text] = []
    values = (bucket.cpu_util, bucket.memory_util, bucket.disk_util)
    has_metrics = any(value is not None for value in values)

    if bucket.cpu_util is not None:
        items.append(create_progress_bar(bucket.cpu_util, 100, "CPU"))
    if bucket.memory_util is not None:
        items.append(create_progress_bar(bucket.memory_util, 100, "Memory"))
    if bucket.disk_util is not None:
        items.append(create_progress_bar(bucket.disk_util, 100, "Disk"))

    if not has_metrics:
        items.append(Text("No system metrics available", style="dim"))

    return Panel(
        Text("\n".join(str(item) for item in items)),
        title="🖥️ System Resources",
        border_style="blue",
    )


def create_components_tree(bucket: TimeBucket) -> Tree | None:
    """Create a tree view of component counts.

    Parses component_counts_json and displays top components
    as a Rich Tree. Expects JSON dict format: {component: count}.

    Returns:
        Rich Tree with top components, or None if no data or parse error
    """
    if not bucket.component_counts_json:
        return None

    try:
        components = json.loads(bucket.component_counts_json)
    except json.JSONDecodeError as e:
        logger.warning(
            "Failed to parse component_counts_json for bucket %s: %s. " "JSON content: %r",
            bucket.bucket_id,
            e,
            bucket.component_counts_json[:100],
        )
        return None
    else:
        tree = Tree("📦 Component Activity")

        # Sort by count descending
        sorted_components = sorted(components.items(), key=lambda x: x[1], reverse=True)

        for component, count in sorted_components[:MAX_COMPONENTS_DISPLAYED]:
            style = "red" if "error" in component.lower() else "green"
            tree.add(f"[{style}]{component}[/{style}]: {count} events")

        return tree


def create_templates_table(bucket: TimeBucket) -> Table | None:
    """Create a table view of templates with counts.

    Shows template_id, count, and template_str pattern.
    Does not show variable instances - those are accessed via drill-down commands.

    Returns:
        Rich Table with template summary, or None if no templates
    """
    if not bucket.templates or len(bucket.templates) == 0:
        return None

    # Create table
    table = Table(title="📋 Templates", box=box.ROUNDED, expand=True)
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Count", justify="right", style="yellow")
    table.add_column("Pattern", style="white", overflow="fold", ratio=4)

    # Calculate counts and sort by descending count
    template_summary = []
    for template_id, template_data in bucket.templates.items():
        count = _count_template_instances(bucket, template_data)
        if count == 0:
            continue
        template_str = template_data.get("template_str", "<unknown>")
        template_summary.append((template_id, count, template_str))

    # Sort by count descending
    template_summary.sort(key=lambda x: x[1], reverse=True)

    # Add rows
    total_instances = 0
    for template_id, count, template_str in template_summary:
        table.add_row(str(template_id), str(count), template_str)
        total_instances += count

    # Add summary footer
    table.caption = f"{len(template_summary)} unique patterns, {total_instances} total instances"

    return table


class RichBucketDisplay:
    """Display TimeBucket data using Rich library."""

    def __init__(self, theme: Literal["default", "monokai", "dracula"] = "default"):
        """Initialize with optional theme."""
        self.console = Console()
        self.theme = theme

    def show_compact(self, bucket: TimeBucket) -> None:
        """Show compact one-line view."""
        if bucket is None:
            self.console.print("[red]Error: No bucket provided[/red]")
            return

        # Sanitize NaN values
        bucket = sanitize_bucket_for_display(bucket)

        # Build compact display manually
        parts = [
            f"[cyan]{bucket.bucket_id}[/cyan]",
            f"time={format_time_range(bucket)}",
            f"events={bucket.total_events}",
        ]

        # Error count with red styling if > 0
        if bucket.error_count > 0:
            parts.append(f"[red]errors={bucket.error_count}[/red]")
        else:
            parts.append(f"errors={bucket.error_count}")

        # Warning count with yellow styling if > 0
        if bucket.warning_count > 0:
            parts.append(f"[yellow]warnings={bucket.warning_count}[/yellow]")
        else:
            parts.append(f"warnings={bucket.warning_count}")

        # Spike status
        if bucket.has_spike:
            parts.append("[bold red]spike=⚡ DETECTED[/bold red]")
        else:
            parts.append("spike=No")

        # Write latency
        if bucket.write_latency_ms is not None:
            parts.append(f"w_latency={bucket.write_latency_ms:.1f}ms")
        else:
            parts.append("[dim]w_latency=N/A[/dim]")

        self.console.print(" ".join(parts))

    def show_normal(self, bucket: TimeBucket) -> None:
        """Show normal view with key sections."""
        if bucket is None:
            self.console.print("[red]Error: No bucket provided[/red]")
            return

        # Sanitize NaN values
        bucket = sanitize_bucket_for_display(bucket)

        # Header with severity
        severity = get_severity_emoji(bucket)
        header = Text(
            f"{severity} Bucket {bucket.bucket_id} ({format_time_range(bucket)})",
            style="bold",
        )
        self.console.print(Panel(header, expand=False))

        # Events and performance columns
        columns = self._create_normal_view_columns(bucket)
        self.console.print(Columns(columns, padding=(1, 2)))

        # System resources (if available)
        if any([bucket.cpu_util, bucket.memory_util, bucket.disk_util]):
            self.console.print(create_system_panel(bucket))

        # Status flags
        self._print_status_flags(bucket)

    def _create_normal_view_columns(self, bucket: TimeBucket) -> list[object]:
        """Create columns for normal view display.

        Args:
            bucket: Bucket containing data to display

        Returns:
            List of Rich renderables for column layout
        """
        columns = []

        # Events summary table
        events_table = Table(title="📊 Events", box=box.SIMPLE)
        events_table.add_column("Type")
        events_table.add_column("Count", justify="right")
        events_table.add_row("Total", str(bucket.total_events))
        if bucket.error_count > 0:
            events_table.add_row("[red]Errors[/red]", f"[red]{bucket.error_count}[/red]")
        if bucket.warning_count > 0:
            events_table.add_row(
                "[yellow]Warnings[/yellow]", f"[yellow]{bucket.warning_count}[/yellow]"
            )
        columns.append(events_table)

        # Performance metrics (if available)
        if any([bucket.read_latency_ms, bucket.write_latency_ms]):
            columns.append(create_performance_table(bucket))

        return columns

    def _print_status_flags(self, bucket: TimeBucket) -> None:
        """Print status flags for bucket.

        Args:
            bucket: Bucket containing status flags
        """
        if bucket.has_spike:
            self.console.print("[bold red]⚡ Latency Spike Detected![/bold red]")
        if bucket.rebuild_active:
            self.console.print("[yellow]🔄 Rebuild Active[/yellow]")

    def show_detailed(self, bucket: TimeBucket) -> None:
        """Show detailed view with all data."""
        if bucket is None:
            self.console.print("[red]Error: No bucket provided[/red]")
            return

        # Sanitize NaN values
        bucket = sanitize_bucket_for_display(bucket)

        layout = self._create_detailed_layout(bucket)
        self._populate_detailed_left(layout["left"], bucket)
        self._populate_detailed_right(layout["right"], bucket)
        self._populate_detailed_footer(layout["footer"], bucket)
        self.console.print(layout)

    def _create_detailed_layout(self, bucket: TimeBucket) -> Layout:
        """Create 3-section layout structure for detailed view.

        Args:
            bucket: Bucket to display (used for header text)

        Returns:
            Layout with header, body (left/right), and footer sections
        """
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", ratio=3),
            Layout(name="footer", ratio=2, minimum_size=8),
        )

        # Header
        severity = get_severity_emoji(bucket)
        header_text = Text(
            f"{severity} DETAILED BUCKET ANALYSIS ({format_time_range(bucket)})",
            style="bold",
            justify="center",
        )
        layout["header"].update(Panel(header_text))

        # Split body into left and right
        layout["body"].split_row(Layout(name="left"), Layout(name="right"))

        return layout

    def _populate_detailed_left(self, layout: Layout, bucket: TimeBucket) -> None:
        """Populate left section with performance and system metrics.

        Args:
            layout: Layout section to populate
            bucket: Bucket containing metrics to display
        """
        left_items = []

        # Add performance table or placeholder
        has_performance = any([bucket.read_latency_ms, bucket.write_latency_ms])
        if has_performance:
            left_items.append(create_performance_table(bucket))
        else:
            left_items.append("No performance data")

        # Add system resources
        left_items.append(create_system_panel(bucket))

        layout.update(Panel(Columns(left_items), title="Metrics"))

    def _populate_detailed_right(self, layout: Layout, bucket: TimeBucket) -> None:
        """Populate right section with events and components.

        Args:
            layout: Layout section to populate
            bucket: Bucket containing event data to display
        """
        right_items = []

        # Add event breakdown table
        events_table = self._create_event_breakdown_table(bucket)
        right_items.append(events_table)

        # Add components tree if available
        tree = create_components_tree(bucket)
        if tree:
            right_items.append(tree)

        layout.update(Panel(Columns(right_items), title="Events & Components"))

    def _create_event_breakdown_table(self, bucket: TimeBucket) -> Table:
        """Create table showing event counts by level.

        Args:
            bucket: Bucket containing event counts

        Returns:
            Rich Table with event breakdown by level
        """
        table = Table(title="Event Breakdown", box=box.MINIMAL)
        table.add_column("Level")
        table.add_column("Count", justify="right")
        table.add_column("Rate", justify="right")

        for level, count in [
            ("Error", bucket.error_count),
            ("Warning", bucket.warning_count),
            ("Info", bucket.info_count),
            ("Verbose", bucket.verbose_count),
            ("Debug", bucket.debug_count),
        ]:
            if count and count > 0:
                rate = (
                    f"{(count/bucket.total_events*100):.1f}%" if bucket.total_events > 0 else "0%"
                )
                style = "red" if level == "Error" else "yellow" if level == "Warning" else "dim"
                table.add_row(f"[{style}]{level}[/{style}]", str(count), rate)

        return table

    def _populate_detailed_footer(self, layout: Layout, bucket: TimeBucket) -> None:
        """Populate footer with templates table.

        Args:
            layout: Layout section to populate
            bucket: Bucket containing template data
        """
        templates_table = create_templates_table(bucket)
        if templates_table:
            layout.update(templates_table)
        else:
            layout.update(Panel(Text("No templates available", style="dim"), title="Templates"))

    def show_json(self, bucket: TimeBucket) -> None:
        """Show as formatted JSON."""
        if bucket is None:
            self.console.print("[red]Error: No bucket provided[/red]")
            return

        # Use TimeBucket's built-in to_json() method
        json_str = bucket.to_json()

        # Rich's JSON widget provides syntax highlighting
        self.console.print(JSON(json_str))

    def show_comparison(self, buckets: list[TimeBucket]) -> None:
        """Show comparison table of multiple buckets side-by-side.

        Displays a table comparing key metrics across buckets:
        - Bucket ID and time range
        - Event counts and errors
        - Write latency with color coding (red >100ms, yellow >50ms, green ≤50ms)
        - CPU utilization
        - Status emoji indicator

        Args:
            buckets: List of TimeBucket objects to compare

        Note:
            Latency values are color-coded based on thresholds:
            - Red: >100ms (critical)
            - Yellow: >50ms (warning)
            - Green: ≤50ms (healthy)
        """
        # Validate and filter input
        valid_buckets = self._validate_buckets_for_comparison(buckets)
        if not valid_buckets:
            return

        # Create and populate comparison table
        table = self._create_comparison_table(valid_buckets)
        self.console.print(table)

    def _validate_buckets_for_comparison(self, buckets: list[TimeBucket]) -> list[TimeBucket]:
        """Validate and filter buckets for comparison.

        Args:
            buckets: List of buckets to validate

        Returns:
            List of valid buckets, empty if invalid input
        """
        if not buckets:
            self.console.print("[yellow]No buckets to compare[/yellow]")
            return []

        if not isinstance(buckets, list):
            self.console.print("[red]Error: buckets must be a list[/red]")
            return []

        # Filter out None values defensively
        valid_buckets = [b for b in buckets if b is not None]
        if not valid_buckets:
            self.console.print("[yellow]No valid buckets to compare[/yellow]")
            return []

        return valid_buckets

    def _create_comparison_table(self, buckets: list[TimeBucket]) -> Table:
        """Create comparison table with bucket metrics.

        Args:
            buckets: List of valid buckets to include

        Returns:
            Rich Table with comparison data
        """
        table = Table(title="Bucket Comparison", box=box.DOUBLE_EDGE)

        # Add column headers
        table.add_column("Bucket ID", style="cyan")
        table.add_column("Time", style="dim")
        table.add_column("Events", justify="right")
        table.add_column("Errors", justify="right", style="red")
        table.add_column("W-Latency", justify="right")
        table.add_column("CPU %", justify="right")
        table.add_column("Status", justify="center")

        # Add rows for each bucket
        for bucket in buckets:
            time_str = f"{bucket.start_time.strftime('%H:%M')}-{bucket.end_time.strftime('%H:%M')}"
            latency_str = self._format_latency_with_color(bucket.write_latency_ms)
            cpu_str = f"{bucket.cpu_util:.1f}" if bucket.cpu_util is not None else "N/A"
            severity = get_severity_emoji(bucket)

            table.add_row(
                bucket.bucket_id,
                time_str,
                str(bucket.total_events),
                str(bucket.error_count) if bucket.error_count > 0 else "0",
                latency_str,
                cpu_str,
                severity,
            )

        return table

    def _format_latency_with_color(self, latency_ms: float | None) -> str:
        """Format latency value with color coding based on thresholds.

        Args:
            latency_ms: Latency value in milliseconds or None

        Returns:
            Rich-formatted string with color: red (>100ms), yellow (>50ms), green (≤50ms)
        """
        if latency_ms is None:
            return "N/A"

        if latency_ms > LATENCY_CRITICAL_MS:
            return f"[red]{latency_ms:.1f}[/red]"
        if latency_ms > LATENCY_WARNING_MS:
            return f"[yellow]{latency_ms:.1f}[/yellow]"
        return f"[green]{latency_ms:.1f}[/green]"

    def live_monitor(
        self, bucket_generator: Iterable[TimeBucket], refresh_rate: int = 1
    ) -> None:
        """Live monitoring display that updates as new buckets arrive.

        Args:
            bucket_generator: Iterator yielding TimeBucket objects
            refresh_rate: Refresh rate in updates per second (default: 1)
        """
        with Live(console=self.console, refresh_per_second=refresh_rate) as live:
            buckets = []
            for bucket in bucket_generator:
                buckets.append(bucket)
                # Keep sliding window of last N buckets
                buckets = buckets[-MAX_LIVE_BUCKETS_WINDOW:]

                # Create live display
                layout = Layout()
                layout.split_column(
                    Layout(Panel("📊 Live Bucket Monitor", style="bold cyan"), size=3),
                    Layout(self._create_live_table(buckets)),
                )

                live.update(layout)

    def _create_live_table(self, buckets: list[TimeBucket]) -> Table:
        """Create table for live monitoring view.

        Args:
            buckets: List of recent buckets to display

        Returns:
            Rich Table with live metrics and sparkline
        """
        table = Table(box=box.SIMPLE_HEAD)
        table.add_column("Time", style="dim")
        table.add_column("Events/min", justify="right")
        table.add_column("Errors", justify="right")
        table.add_column("W-Latency", justify="right")
        table.add_column("Sparkline", width=20)

        for bucket in buckets:
            # Events per minute
            duration = get_duration_minutes(bucket)
            events_per_min = bucket.total_events / duration if duration > 0 else 0

            # Create sparkline for latency
            sparkline = self._create_sparkline(
                [b.write_latency_ms or 0 for b in buckets], current_idx=buckets.index(bucket)
            )

            # Color based on error thresholds
            row_style = (
                "red"
                if bucket.error_count > ERROR_COUNT_CRITICAL
                else "yellow"
                if bucket.error_count > ERROR_COUNT_WARNING
                else None
            )

            table.add_row(
                bucket.start_time.strftime("%H:%M:%S"),
                f"{events_per_min:.0f}",
                str(bucket.error_count),
                f"{bucket.write_latency_ms:.1f}" if bucket.write_latency_ms is not None else "N/A",
                sparkline,
                style=row_style,
            )

        return table

    def _create_sparkline(self, values: list[float], current_idx: int = -1) -> str:
        """Create a sparkline visualization for numeric values.

        Args:
            values: List of numeric values to visualize
            current_idx: Index of current value to highlight (default: -1 for no highlight)

        Returns:
            String with Unicode block characters representing the data
        """
        if not values:
            return ""

        chars = "▁▂▃▄▅▆▇█"
        max_val = max(values) if values else 1
        min_val = min(values) if values else 0
        range_val = max_val - min_val if max_val != min_val else 1

        sparkline = ""
        for i, v in enumerate(values):
            normalized = (v - min_val) / range_val
            char_idx = int(normalized * (len(chars) - 1))
            if i == current_idx:
                sparkline += f"[bold red]{chars[char_idx]}[/bold red]"
            else:
                sparkline += chars[char_idx]

        return sparkline


# ============================================================================
# DEMO / EXAMPLES (for development/documentation only)
# ============================================================================


def demo() -> None:
    """Demonstrate the Rich display capabilities.

    WARNING: This is example code for development/documentation.
    Not intended for production use.
    """
    console = Console()

    # Create sample bucket data
    sample_data = {
        "bucket_id": "2024-01-15_14:00",
        "start_time": datetime(2024, 1, 15, 14, 0),
        "end_time": datetime(2024, 1, 15, 14, 5),
        "total_events": 1523,
        "error_count": 5,
        "warning_count": 23,
        "info_count": 1200,
        "verbose_count": 295,
        "write_latency_ms": 45.6,
        "read_latency_ms": 12.3,
        "cpu_util": 65.5,
        "memory_util": 78.2,
        "disk_util": 45.0,
        "has_spike": False,
        "component_counts_json": json.dumps(
            {
                "StorageManager": 450,
                "NetworkHandler": 380,
                "CacheController": 220,
                "ErrorHandler": 5,
            }
        ),
    }

    # Create bucket model
    bucket = TimeBucket(**sample_data)
    display = RichBucketDisplay()

    console.print("\n[bold cyan]═══ COMPACT VIEW ═══[/bold cyan]")
    display.show_compact(bucket)

    console.print("\n[bold cyan]═══ NORMAL VIEW ═══[/bold cyan]")
    display.show_normal(bucket)

    console.print("\n[bold cyan]═══ DETAILED VIEW ═══[/bold cyan]")
    display.show_detailed(bucket)

    console.print("\n[bold cyan]═══ JSON VIEW ═══[/bold cyan]")
    display.show_json(bucket)

    # Show comparison
    console.print("\n[bold cyan]═══ COMPARISON VIEW ═══[/bold cyan]")
    buckets = []
    demo_spike_index = 2
    for i in range(3):
        data = sample_data.copy()
        data["bucket_id"] = f"2024-01-15_14:{i*5:02d}"
        data["error_count"] = i * 3
        data["write_latency_ms"] = 45.6 + i * 20
        data["has_spike"] = i == demo_spike_index
        buckets.append(TimeBucket(**data))

    display.show_comparison(buckets)


if __name__ == "__main__":
    demo()

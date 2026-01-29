#!/usr/bin/env python3
"""CLI for selecting and filtering bucket data from parquet files.

Provides POSIX-style commands for querying TimeBucket data with rich output formatting.
All commands use RichBucketDisplay for consistent, beautiful output.
"""

import csv
import json
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Optional, Union

import click
import duckdb
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.status import Status
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from log_ingest.cli_commands.bucket_display_rich import RichBucketDisplay
from log_ingest.cli_commands.bucket_selector import BucketSelector

# Global Rich console
# Global Rich console
console = Console()

_MAX_DISPLAY_ROWS = 100

def parse_flag_arg(ctx: click.Context, param: click.Parameter, value: tuple) -> dict[str, bool]:
    """Parse flag arguments like --flag key=value."""
    flags = {}
    truth_val = ("true", "1", "yes", "on")
    false_val = ("false", "0", "no", "off")

    for flag_str in value:
        if "=" not in flag_str:
            raise click.BadParameter(f"Flag must be in format key=value, got: {flag_str}")
        key, val = flag_str.split("=", 1)
        if val.lower() in truth_val:
            flags[key] = True
        elif val.lower() in false_val:
            flags[key] = False
        else:
            raise click.BadParameter(f"Flag value must be true/false, got: {val}")
    return flags


def show_error(message: str, details: Optional[str] = None) -> None:
    """Display error message with Rich formatting."""
    error_text = Text("❌ Error", style="bold red")
    if details:
        error_panel = Panel(
            Text(f"{message}\n\n[dim]{details}[/dim]"),
            title=error_text,
            border_style="red",
            padding=(1, 2),
        )
    else:
        error_panel = Panel(Text(message), title=error_text, border_style="red", padding=(1, 2))
    console.print(error_panel)


def show_success(message: str) -> None:
    """Display success message with Rich formatting."""
    console.print(f"[green]✅ {message}[/green]")


def show_info(message: str) -> None:
    """Display info message with Rich formatting."""
    console.print(f"[blue]i  {message}[/blue]")


def show_warning(message: str) -> None:
    """Display warning message with Rich formatting."""
    console.print(f"[yellow]⚠️  {message}[/yellow]")


# ============================================================
#                    HELPER FUNCTIONS
# ============================================================


def _write_count_to_file(total: int, output_file: str, format_type: str) -> None:
    """Write bucket count to file in specified format."""
    with Path(output_file).open("w") as f:
        if format_type == "json":
            json.dump({"total_buckets": total}, f)
        else:
            f.write(f"Total buckets: {total}\n")
    show_success(f"Count saved to {output_file}")


def _display_count_to_console(total: int) -> None:
    """Display bucket count to console with Rich formatting."""
    count_table = Table(show_header=False, box=box.ROUNDED, padding=(0, 2))
    count_table.add_column(style="cyan", justify="right")
    count_table.add_column(style="bold green")
    count_table.add_row("📊 Total buckets:", str(total))
    console.print(Panel(count_table, title="[bold blue]Bucket Count[/bold blue]", expand=False))


def _get_event_count(file: str) -> int:
    """Get total event count across all buckets from parquet file."""
    conn = duckdb.connect()
    result = conn.execute(
        f"SELECT SUM(total_events) as total FROM read_parquet('{file}')"
    ).fetchone()
    return int(result[0]) if result and result[0] else 0


def _write_event_count_to_file(total: int, output_file: str, format_type: str) -> None:
    """Write event count to file in specified format."""
    with Path(output_file).open("w") as f:
        if format_type == "json":
            json.dump({"total_events": total}, f)
        else:
            f.write(f"Total events: {total:,}\n")
    show_success(f"Event count saved to {output_file}")


def _display_event_count_to_console(total: int, format_type: str) -> None:
    """Display event count to console with Rich formatting."""
    if format_type == "json":
        console.print(json.dumps({"total_events": total}))
    else:
        count_table = Table(show_header=False, box=box.ROUNDED, padding=(0, 2))
        count_table.add_column(style="cyan", justify="right")
        count_table.add_column(style="bold green")
        count_table.add_row("📈 Total events:", f"{total:,}")
        console.print(Panel(count_table, title="[bold blue]Event Count[/bold blue]", expand=False))


def _convert_buckets_to_json_data(buckets: list) -> list[dict]:
    """Convert bucket objects to JSON-serializable data."""
    bucket_data = []
    for bucket in buckets:
        bucket_data.append(bucket.to_dict())
    return bucket_data


def _write_json_to_file(data: list[dict], output_file: str) -> None:
    """Write JSON data to file."""
    with Path(output_file).open("w") as f:
        json.dump(data, f, indent=2, default=str)


def _save_buckets_to_file(buckets: list, output_file: str, format_type: str) -> None:
    """Save buckets to file with progress indication."""
    if format_type != "json":
        show_error(
            f"File output for format '{format_type}' not yet implemented",
            "Use 'json' format for file output",
        )
        return

    with Status(f"[bold blue]Writing results to {output_file}...", console=console):
        try:
            bucket_data = _convert_buckets_to_json_data(buckets)
            _write_json_to_file(bucket_data, output_file)
            show_success(f"Results saved to {output_file}")
        except Exception as e:
            show_error(f"Failed to write to {output_file}", str(e))


def _show_query_summary(buckets: list, format_type: str) -> None:
    """Show query results summary table."""
    if len(buckets) <= 1:
        return

    summary_table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
    summary_table.add_column(style="cyan")
    summary_table.add_column(style="green")
    summary_table.add_row("📊 Found", f"{len(buckets)} buckets")
    summary_table.add_row("📋 Format", format_type)
    console.print(Panel(summary_table, title="[bold blue]Query Results[/bold blue]", expand=False))
    console.print()


def _display_single_bucket(bucket: object, format_type: str, display: RichBucketDisplay) -> None:
    """Display a single bucket in the specified format."""
    if format_type == "compact":
        display.show_compact(bucket)
    elif format_type == "detailed":
        display.show_detailed(bucket)
    elif format_type == "json":
        display.show_json(bucket)
    else:  # normal
        display.show_normal(bucket)


def _iterate_and_display_buckets(
    buckets: list,
    format_type: str,
    display: RichBucketDisplay,
    progress_task: Optional[object] = None,
) -> None:
    """Display all buckets with separators, optionally updating progress."""
    for i, bucket in enumerate(buckets):
        _display_single_bucket(bucket, format_type, display)
        if i < len(buckets) - 1:
            console.print()  # Separator between buckets
        if progress_task:
            progress_task.update(advance=1)


def _create_progress_bar() -> Progress:
    """Create and return a configured progress bar."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    )


def _display_buckets_console(buckets: list, format_type: str) -> None:
    """Display buckets to console with appropriate progress handling."""
    display = RichBucketDisplay()
    _show_query_summary(buckets, format_type)
    _iterate_and_display_buckets(buckets, format_type, display)


# ============================================================
#                    MAIN DISPLAY FUNCTION
# ============================================================


def display_buckets(
    buckets: list, format_type: str = "normal", output_file: Optional[str] = None
) -> None:
    """Display buckets using RichBucketDisplay with enhanced formatting."""
    # Guard clause: Handle empty buckets early
    if not buckets:
        show_warning("No buckets found matching criteria.")
        return

    # Happy path: Console display (most common case)
    if not output_file:
        _display_buckets_console(buckets, format_type)
        return

    # Special case: File output
    _save_buckets_to_file(buckets, output_file, format_type)


# Common options for all commands
def common_options(f: Callable) -> Callable:
    """Decorator to add common options to all commands."""
    f = click.option(
        "--file",
        "-f",
        default="buckets.parquet",
        type=click.Path(exists=True),
        help="Parquet file path [default: buckets.parquet]",
    )(f)
    f = click.option(
        "--format",
        type=click.Choice(["compact", "normal", "detailed", "json", "csv"]),
        default="normal",
        help="Output format [default: normal]",
    )(f)
    f = click.option(
        "--output-file", "-o", type=click.Path(), help="Save output to file instead of stdout"
    )(f)
    return click.option(
        "--limit", type=int, help="Limit number of results displayed (for large result sets)"
    )(f)


@click.group()
def cli() -> None:
    """Select and filter bucket data from parquet files.

    USAGE
        log_ingest select-buckets COMMAND [OPTIONS]

    COMMANDS
        count           Show total number of buckets
        event-count     Show total number of events across all buckets
        random          Get random sample of buckets
        top             Get top N buckets by field value
        bottom          Get bottom N buckets by field value
        by-flags        Filter buckets by boolean flags
        by-thresholds   Filter buckets by numeric thresholds
        by-time         Filter buckets by time range
        info            Show file overview (count + time range)

    Examples:
        log_ingest select-buckets random
        log_ingest select-buckets top --field write_latency_ms -n 5
        log_ingest select-buckets by-flags --flag has_spike=true
        log_ingest select-buckets by-thresholds --write-latency-ms-gt 100
        log_ingest select-buckets by-time --start "2024-01-01 10:00" --end "2024-01-01 11:00"
    """
    pass


@cli.command()
@common_options
def count(file: str, format: str, output_file: Optional[str], limit: Optional[int]) -> None:
    """Show total number of buckets in parquet file.

    USAGE
        log_ingest select-buckets count [OPTIONS]

    OPTIONS
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout

    Examples:
        log_ingest select-buckets count
        log_ingest select-buckets count --file my-buckets.parquet
    """
    try:
        total = _get_bucket_count(file)

        if output_file:
            _write_count_to_file(total, output_file, format)
            return

        _display_count_to_console(total)

    except Exception as e:
        show_error("Failed to count buckets", str(e))
        sys.exit(1)


@cli.command("event-count")
@common_options
def event_count(file: str, format: str, output_file: Optional[str], limit: Optional[int]) -> None:
    """Show total number of events across all buckets.

    USAGE
        log_ingest select-buckets event-count [OPTIONS]

    OPTIONS
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout

    Examples:
        log_ingest select-buckets event-count
        log_ingest select-buckets event-count --file my-buckets.parquet
        log_ingest select-buckets event-count --format json
    """
    try:
        total = _get_event_count(file)

        if output_file:
            _write_event_count_to_file(total, output_file, format)
            return

        _display_event_count_to_console(total, format)

    except Exception as e:
        show_error("Failed to count events", str(e))
        sys.exit(1)


def _get_bucket_count(file: str) -> int:
    """Get total bucket count from parquet file."""
    selector = BucketSelector(Path(file))
    return selector.count()


def _get_random_buckets(file: str, count: int) -> list:
    """Get random buckets from parquet file."""
    selector = BucketSelector(Path(file))
    return selector.get_random(count)


def _get_top_buckets(file: str, field: str, count: int) -> list:
    """Get top N buckets by field value from parquet file."""
    selector = BucketSelector(Path(file))
    return selector.top_n(field, count)


def _get_bottom_buckets(file: str, field: str, count: int) -> list:
    """Get bottom N buckets by field value from parquet file."""
    selector = BucketSelector(Path(file))
    return selector.bottom_n(field, count)


def _apply_limit_to_buckets(buckets: list, limit: Optional[int]) -> list:
    """Apply limit to bucket list if specified."""
    if limit is not None:
        return buckets[:limit]
    return buckets


def _get_file_info(file: str) -> dict:
    """Get file information including count and time range."""
    selector = BucketSelector(Path(file))
    total = selector.count()
    time_range = selector.get_time_range()
    return {"file": str(file), "total_buckets": total, "time_range": _format_time_range(time_range)}


def _format_time_range(time_range: Optional[tuple]) -> Optional[dict]:
    """Format time range tuple into dictionary with ISO strings."""
    if not time_range:
        return None

    return {"start": time_range[0].isoformat(), "end": time_range[1].isoformat()}


def _format_info_as_text(info_data: dict) -> str:
    """Format info data as text string."""
    lines = [f"File: {info_data['file']}", f"Total buckets: {info_data['total_buckets']}"]

    if info_data["time_range"]:
        time_line = (
            f"Time range: {info_data['time_range']['start']} to {info_data['time_range']['end']}"
        )
    else:
        time_line = "Time range: No data"

    lines.append(time_line)
    return "\n".join(lines) + "\n"


def _write_info_to_file(info_data: dict, output_file: str, format_type: str) -> None:
    """Write file info to file in specified format."""
    with Path(output_file).open("w") as f:
        if format_type == "json":
            json.dump(info_data, f, indent=2)
        else:
            f.write(_format_info_as_text(info_data))
    show_success(f"Info saved to {output_file}")


def _display_info_to_console(info_data: dict, format_type: str) -> None:
    """Display file info to console with Rich formatting."""
    if format_type == "json":
        console.print(
            Syntax(json.dumps(info_data, indent=2), "json", theme="monokai", line_numbers=True)
        )
    else:
        info_table = Table(show_header=False, box=box.ROUNDED, padding=(0, 2))
        info_table.add_column(style="cyan", justify="right")
        info_table.add_column(style="bold green")
        info_table.add_row("📁 File:", str(info_data["file"]))
        info_table.add_row("📊 Total buckets:", str(info_data["total_buckets"]))
        if info_data["time_range"]:
            start_time = info_data["time_range"]["start"]
            end_time = info_data["time_range"]["end"]
            info_table.add_row("⏰ Time range:", f"{start_time} to {end_time}")
        else:
            info_table.add_row("⏰ Time range:", "[red]No data[/red]")

        console.print(
            Panel(info_table, title="[bold blue]Bucket File Information[/bold blue]", expand=False)
        )


@cli.command()
@click.option("-n", "--count", default=3, type=int, help="Number of random buckets [default: 3]")
@common_options
def random(
    file: str, format: str, output_file: Optional[str], limit: Optional[int], count: int
) -> None:
    """Get random sample of buckets for sanity checking.

    USAGE
        log_ingest select-buckets random [OPTIONS]

    OPTIONS
        -n, --count INT     Number of random buckets [default: 3]
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout
        --limit INT         Limit number of results displayed

    Examples:
        log_ingest select-buckets random
        log_ingest select-buckets random -n 5
        log_ingest select-buckets random --format compact
    """
    try:
        buckets = _get_random_buckets(file, count)
        buckets = _apply_limit_to_buckets(buckets, limit)
        display_buckets(buckets, format, output_file)

    except Exception as e:
        show_error("Failed to get random buckets", str(e))
        sys.exit(1)


@cli.command()
@click.option("--field", required=True, help="Field name to sort by (e.g., write_latency_ms)")
@click.option("-n", "--count", default=3, type=int, help="Number of top buckets [default: 3]")
@common_options
def top(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    field: str,
    count: int,
) -> None:
    """Get top N buckets by field value (descending order).

    USAGE
        log_ingest select-buckets top --field FIELD [OPTIONS]

    REQUIRED OPTIONS
        --field FIELD       Field name to sort by (e.g., write_latency_ms, total_events)

    OPTIONS
        -n, --count INT     Number of top buckets [default: 3]
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout
        --limit INT         Limit number of results displayed

    Examples:
        log_ingest select-buckets top --field write_latency_ms
        log_ingest select-buckets top --field total_events -n 10
        log_ingest select-buckets top --field error_count -n 5 --format detailed
    """
    try:
        buckets = _get_top_buckets(file, field, count)
        buckets = _apply_limit_to_buckets(buckets, limit)
        display_buckets(buckets, format, output_file)

    except Exception as e:
        show_error(f"Failed to get top buckets by {field}", str(e))
        sys.exit(1)


@cli.command()
@click.option("--field", required=True, help="Field name to sort by (e.g., write_latency_ms)")
@click.option("-n", "--count", default=3, type=int, help="Number of bottom buckets [default: 3]")
@common_options
def bottom(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    field: str,
    count: int,
) -> None:
    """Get bottom N buckets by field value (ascending order).

    USAGE
        log_ingest select-buckets bottom --field FIELD [OPTIONS]

    REQUIRED OPTIONS
        --field FIELD       Field name to sort by (e.g., write_latency_ms, total_events)

    OPTIONS
        -n, --count INT     Number of bottom buckets [default: 3]
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout
        --limit INT         Limit number of results displayed

    Examples:
        log_ingest select-buckets bottom --field write_latency_ms
        log_ingest select-buckets bottom --field total_events -n 10
        log_ingest select-buckets bottom --field error_count -n 5 --format compact
    """
    try:
        buckets = _get_bottom_buckets(file, field, count)
        buckets = _apply_limit_to_buckets(buckets, limit)
        display_buckets(buckets, format, output_file)

    except Exception as e:
        show_error(f"Failed to get bottom buckets by {field}", str(e))
        sys.exit(1)


@cli.command("by-flags")
@click.option(
    "--flag",
    multiple=True,
    callback=parse_flag_arg,
    help="Boolean flag filter: key=value (e.g., --flag has_spike=true)",
)
@common_options
def by_flags(
    file: str, format: str, output_file: Optional[str], limit: Optional[int], flag: dict
) -> None:
    """Filter buckets by boolean flag values.

    USAGE
        log_ingest select-buckets by-flags --flag KEY=VALUE [OPTIONS]

    REQUIRED OPTIONS
        --flag KEY=VALUE    Boolean flag filter, can be used multiple times
                           Values: true|false|1|0|yes|no|on|off

    OPTIONS
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout
        --limit INT         Limit number of results displayed

    Examples:
        log_ingest select-buckets by-flags --flag has_spike=true
        log_ingest select-buckets by-flags --flag has_spike=true --flag rebuild_active=false
        log_ingest select-buckets by-flags --flag is_leader=true --format detailed
    """
    if not flag:
        show_error("Must provide at least one --flag option", "Example: --flag has_spike=true")
        sys.exit(1)

    try:
        selector = BucketSelector(Path(file))
        buckets = selector.by_flags(limit=limit, **flag)

        display_buckets(buckets, format, output_file)

    except Exception as e:
        show_error("Failed to filter by flags", str(e))
        sys.exit(1)


@cli.command("by-thresholds")
@click.option("--write-latency-ms-gt", type=float, help="Write latency > VALUE (ms)")
@click.option("--write-latency-ms-gte", type=float, help="Write latency >= VALUE (ms)")
@click.option("--write-latency-ms-lt", type=float, help="Write latency < VALUE (ms)")
@click.option("--write-latency-ms-lte", type=float, help="Write latency <= VALUE (ms)")
@click.option("--read-latency-ms-gt", type=float, help="Read latency > VALUE (ms)")
@click.option("--read-latency-ms-gte", type=float, help="Read latency >= VALUE (ms)")
@click.option("--read-latency-ms-lt", type=float, help="Read latency < VALUE (ms)")
@click.option("--read-latency-ms-lte", type=float, help="Read latency <= VALUE (ms)")
@click.option("--error-count-gt", type=int, help="Error count > VALUE")
@click.option("--error-count-gte", type=int, help="Error count >= VALUE")
@click.option("--error-count-lt", type=int, help="Error count < VALUE")
@click.option("--error-count-lte", type=int, help="Error count <= VALUE")
@click.option("--total-events-gt", type=int, help="Total events > VALUE")
@click.option("--total-events-gte", type=int, help="Total events >= VALUE")
@click.option("--total-events-lt", type=int, help="Total events < VALUE")
@click.option("--total-events-lte", type=int, help="Total events <= VALUE")
@click.option("--cpu-util-gt", type=float, help="CPU utilization > VALUE (%)")
@click.option("--cpu-util-gte", type=float, help="CPU utilization >= VALUE (%)")
@click.option("--cpu-util-lt", type=float, help="CPU utilization < VALUE (%)")
@click.option("--cpu-util-lte", type=float, help="CPU utilization <= VALUE (%)")
@common_options
def by_thresholds(
    file: str, format: str, output_file: Optional[str], limit: Optional[int], **kwargs
) -> None:
    """Filter buckets by numeric threshold comparisons.

    USAGE
        log_ingest select-buckets by-thresholds [THRESHOLD_OPTIONS] [OPTIONS]

    THRESHOLD OPTIONS
        --write-latency-ms-{gt|gte|lt|lte} VALUE   Write latency comparisons (ms)
        --read-latency-ms-{gt|gte|lt|lte} VALUE    Read latency comparisons (ms)
        --error-count-{gt|gte|lt|lte} VALUE        Error count comparisons
        --total-events-{gt|gte|lt|lte} VALUE       Total events comparisons
        --cpu-util-{gt|gte|lt|lte} VALUE           CPU utilization comparisons (%)

        Operators: gt=greater_than, gte=greater_than_equal, lt=less_than, lte=less_than_equal

    OPTIONS
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout
        --limit INT         Limit number of results displayed

    Examples:
        log_ingest select-buckets by-thresholds --write-latency-ms-gt 100
        log_ingest select-buckets by-thresholds --error-count-gte 5 --cpu-util-lt 80
        log_ingest select-buckets by-thresholds --total-events-gt 1000 --format detailed
    """
    # Filter out common options and None values
    # Click already converts --total-events-gt to total_events_gt
    threshold_args = {}
    for key, value in kwargs.items():
        if key not in ["file", "format", "output_file", "limit"] and value is not None:
            threshold_args[key] = value

    if not threshold_args:
        show_error(
            "Must provide at least one threshold option", "Example: --write-latency-ms-gt 100"
        )
        sys.exit(1)

    try:
        selector = BucketSelector(Path(file))
        buckets = selector.by_thresholds(limit=limit, **threshold_args)

        display_buckets(buckets, format, output_file)

    except Exception as e:
        show_error("Failed to filter by thresholds", str(e))
        sys.exit(1)


@cli.command("by-time")
@click.option(
    "--start",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"]),
    help='Start time (ISO format or "YYYY-MM-DD HH:MM")',
)
@click.option(
    "--end",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"]),
    help='End time (ISO format or "YYYY-MM-DD HH:MM")',
)
@common_options
def by_time(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    start: str,
    end: str,
) -> None:
    r"""Filter buckets by time range.

    USAGE
        log_ingest select-buckets by-time --start TIME --end TIME [OPTIONS]

    REQUIRED OPTIONS
        --start TIME        Start time (inclusive)
        --end TIME          End time (exclusive)

        Time formats:
        - ISO format: "2024-01-01T10:00:00Z" or "2024-01-01T10:00:00+00:00"
        - Simple format: "2024-01-01 10:00" (assumes UTC)

    OPTIONS
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout
        --limit INT         Limit number of results displayed

    Examples:
        log_ingest select-buckets by-time --start "2024-01-01 10:00" --end "2024-01-01 11:00"
        log_ingest select-buckets by-time --start "2024-08-17T12:00:00Z" \\
            --end "2024-08-17T13:00:00Z"
        log_ingest select-buckets by-time --start "2024-01-01 09:00" \\
            --end "2024-01-01 10:00" --format detailed
    """
    try:
        selector = BucketSelector(Path(file))
        buckets = selector.by_time_range(start, end)

        if limit:
            buckets = buckets[:limit]

        display_buckets(buckets, format, output_file)

    except Exception as e:
        show_error("Failed to filter by time range", str(e))
        sys.exit(1)


@cli.command()
@common_options
def info(file: str, format: str, output_file: Optional[str], limit: Optional[int]) -> None:
    """Show file overview including count and time range.

    USAGE
        log_ingest select-buckets info [OPTIONS]

    OPTIONS
        -f, --file PATH     Parquet file path [default: buckets.parquet]
        --format FORMAT     Output format: compact|normal|detailed|json [default: normal]
        -o, --output-file   Save output to file instead of stdout

    Examples:
        log_ingest select-buckets info
        log_ingest select-buckets info --file my-buckets.parquet
        log_ingest select-buckets info --format json
    """
    try:
        info_data = _get_file_info(file)

        # Guard clause: File output path
        if output_file:
            _write_info_to_file(info_data, output_file, format)
            return

        # Happy path: Console display
        _display_info_to_console(info_data, format)

    except Exception as e:
        show_error("Failed to get file information", str(e))
        sys.exit(1)


# ============================================================
#                    TEMPLATE ANALYTICS COMMANDS
# ============================================================


def template_common_options(f: Callable) -> Callable:
    """Decorator adding shared filtering options for template commands."""
    decorators = (
        click.option("--template-id", "template_id", multiple=True, help="Filter by template ID"),
        click.option("--substring", help="Filter templates whose string contains substring"),
        click.option(
            "--start",
            type=click.DateTime(formats=["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"]),
            help='Start time (ISO format or "YYYY-MM-DD HH:MM")',
        ),
        click.option(
            "--end",
            type=click.DateTime(formats=["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"]),
            help='End time (ISO format or "YYYY-MM-DD HH:MM")',
        ),
        click.option(
            "--bucket-flag",
            multiple=True,
            callback=parse_flag_arg,
            help="Filter buckets by flag (format: key=value, can be specified multiple times)",
        ),
    )
    decorated = f
    for decorator in decorators:
        decorated = decorator(decorated)
    return decorated


def template_var_options(f: Callable) -> Callable:
    """Decorator to add variable filtering options."""
    decorators = (
        click.option("--var1", help="Filter by var1 value"),
        click.option("--var2", help="Filter by var2 value"),
        click.option("--var3", help="Filter by var3 value"),
        click.option("--var4", help="Filter by var4 value"),
        click.option("--var5", help="Filter by var5 value"),
    )
    decorated = f
    for decorator in decorators:
        decorated = decorator(decorated)
    return decorated


def _build_var_filters(
    var1: Optional[str],
    var2: Optional[str],
    var3: Optional[str],
    var4: Optional[str],
    var5: Optional[str],
) -> dict[str, Any]:
    """Build var_filters dict from CLI options."""
    var_filters: dict[str, Any] = {}
    if var1 is not None:
        var_filters["var1"] = _parse_var_value(var1)
    if var2 is not None:
        var_filters["var2"] = _parse_var_value(var2)
    if var3 is not None:
        var_filters["var3"] = _parse_var_value(var3)
    if var4 is not None:
        var_filters["var4"] = _parse_var_value(var4)
    if var5 is not None:
        var_filters["var5"] = _parse_var_value(var5)
    return var_filters


def _parse_var_value(value: str) -> Union[str, int, float, None]:
    """Parse variable value, handling None/null strings."""
    if value.lower() in ("none", "null", ""):
        return None
    # Try to parse as int
    try:
        return int(value)
    except ValueError:
        pass
    # Try to parse as float
    try:
        return float(value)
    except ValueError:
        pass
    # Return as string
    return value


def _display_template_instances_json(
    instances: list[dict[str, Any]], output_file: Optional[str]
) -> None:
    """Display template instances as JSON."""
    if output_file:
        with Path(output_file).open("w") as f:
            json.dump(instances, f, indent=2, default=str)
        show_success(f"Template instances saved to {output_file}")
    else:
        console.print(json.dumps(instances, indent=2, default=str))


def _display_template_instances_csv(
    instances: list[dict[str, Any]], output_file: Optional[str]
) -> None:
    """Display template instances as CSV."""
    if not instances:
        if output_file:
            Path(output_file).touch()
        return

    fieldnames = list(instances[0].keys())
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(instances)

    if output_file:
        with Path(output_file).open("w", newline="") as f:
            f.write(output.getvalue())
        show_success(f"Template instances saved to {output_file}")
    else:
        console.print(output.getvalue())


def _display_template_instances_table(instances: list[dict[str, Any]], format_type: str) -> None:
    """Display template instances as Rich table."""
    if not instances:
        console.print("[dim]No template instances found[/dim]")
        return

    table = Table(show_header=True, box=box.ROUNDED)
    fieldnames = list(instances[0].keys())

    # Limit columns for compact format
    if format_type == "compact":
        key_fields = ["template_id", "bucket_id", "start_time"]
        display_fields = [f for f in key_fields if f in fieldnames] + [
            f for f in fieldnames if f not in key_fields
        ][:5]
    else:
        display_fields = fieldnames[:15] if format_type == "normal" else fieldnames

    for field in display_fields:
        table.add_column(field, overflow="fold")

    for instance in instances[:_MAX_DISPLAY_ROWS]:  # Limit rows for display
        row_values = [str(instance.get(field, "")) for field in display_fields]
        table.add_row(*row_values)

    console.print(table)
    if len(instances) > _MAX_DISPLAY_ROWS:
        remaining = len(instances) - _MAX_DISPLAY_ROWS
        console.print(f"[dim]... and {remaining} more rows[/dim]")


def _display_template_frequency_json(
    frequency: dict[str, int] | dict[str, dict[str, int]],
    output_file: Optional[str],
) -> None:
    """Display template frequency as JSON."""
    if output_file:
        with Path(output_file).open("w") as f:
            json.dump(frequency, f, indent=2)
        show_success(f"Template frequency saved to {output_file}")
    else:
        console.print(json.dumps(frequency, indent=2))


def _display_template_frequency_table(
    frequency: dict[str, int] | dict[str, dict[str, int]], format_type: str
) -> None:
    """Display template frequency as Rich table."""
    is_cohort_format = (
        isinstance(frequency, dict)
        and frequency
        and isinstance(next(iter(frequency.values())), dict)
    )
    if is_cohort_format:
        # Cohort comparison format
        table = Table(show_header=True, box=box.ROUNDED, title="Template Frequency by Cohort")
        table.add_column("Template ID", style="cyan")
        cohorts = list(frequency.keys())
        for cohort in cohorts:
            table.add_column(cohort, justify="right")

        all_template_ids = set()
        for cohort_data in frequency.values():
            all_template_ids.update(cohort_data.keys())

        for template_id in sorted(all_template_ids):
            row = [template_id]
            for cohort in cohorts:
                count = frequency[cohort].get(template_id, 0)
                row.append(str(count))
            table.add_row(*row)

        console.print(table)
    else:
        # Simple frequency format
        table = Table(show_header=True, box=box.ROUNDED, title="Template Frequency")
        table.add_column("Template ID", style="cyan")
        table.add_column("Count", justify="right", style="green")

        sorted_freq = sorted(frequency.items(), key=lambda x: x[1], reverse=True)
        for template_id, count in sorted_freq:
            table.add_row(template_id, str(count))

        console.print(table)


def _display_precursors_json(precursors: list[dict[str, Any]], output_file: Optional[str]) -> None:
    """Display precursors as JSON."""
    if output_file:
        with Path(output_file).open("w") as f:
            json.dump(precursors, f, indent=2)
        show_success(f"Precursors saved to {output_file}")
    else:
        console.print(json.dumps(precursors, indent=2))


def _display_precursors_table(precursors: list[dict[str, Any]], format_type: str) -> None:
    """Display precursors as Rich table."""
    if not precursors:
        console.print("[dim]No precursors found[/dim]")
        return

    table = Table(show_header=True, box=box.ROUNDED, title="Precursor Templates")
    table.add_column("Template ID", style="cyan")
    table.add_column("Frequency", justify="right", style="green")

    for precursor in precursors:
        table.add_row(precursor["template_id"], str(precursor["frequency"]))

    console.print(table)


@cli.group("templates")
def templates_group() -> None:
    """Template analytics commands for analyzing template instances.

    USAGE
        log_ingest select-buckets templates COMMAND [OPTIONS]

    COMMANDS
        instances      Get template instances with filtering
        frequency      Count template frequency
        timeline       Export timeline data (metrics + events)
        precursors     Find templates preceding an anchor template

    Examples:
        log_ingest select-buckets templates instances --template-id 200
        log_ingest select-buckets templates frequency --top-n 5
        log_ingest select-buckets templates timeline --template-id 970 --format csv
        log_ingest select-buckets templates precursors --anchor 970 --lookback 10
    """
    pass


@templates_group.command("instances")
@common_options
@template_common_options
@template_var_options
@click.option("--include-vars", is_flag=True, help="Include flattened variable columns")
def templates_instances(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    template_id: tuple[str, ...],
    substring: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
    bucket_flag: dict[str, bool],
    var1: Optional[str],
    var2: Optional[str],
    var3: Optional[str],
    var4: Optional[str],
    var5: Optional[str],
    include_vars: bool,
) -> None:
    """Get template instances with filtering options.

    Returns flattened template instances matching the provided filters.
    Each row represents one template instance occurrence.
    """
    try:
        selector = BucketSelector(Path(file))
        template_ids_list = list(template_id) if template_id else None
        var_filters = _build_var_filters(var1, var2, var3, var4, var5)

        instances = selector.template_instances(
            template_ids=template_ids_list,
            template_substring=substring,
            start=start.replace(tzinfo=timezone.utc) if start and start.tzinfo is None else start,
            end=end.replace(tzinfo=timezone.utc) if end and end.tzinfo is None else end,
            bucket_filters=bucket_flag if bucket_flag else None,
            var_filters=var_filters if var_filters else None,
            include_vars=include_vars,
        )

        if limit:
            instances = instances[:limit]

        if format == "json":
            _display_template_instances_json(instances, output_file)
        elif format == "csv":
            _display_template_instances_csv(instances, output_file)
        elif output_file:
            # For table formats with output file, use JSON
            _display_template_instances_json(instances, output_file)
        else:
            _display_template_instances_table(instances, format)

    except Exception as e:
        show_error("Failed to get template instances", str(e))
        sys.exit(1)


@templates_group.command("frequency")
@common_options
@template_common_options
@click.option("--top-n", "top_n", type=int, help="Limit to top N most frequent templates")
@click.option(
    "--compare-cohort",
    multiple=True,
    help="Cohort name for comparison (can specify multiple)",
)
@click.option(
    "--cohort-filter",
    multiple=True,
    help="Cohort filter (format: cohort_name:key=value)",
)
def templates_frequency(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    template_id: tuple[str, ...],
    substring: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
    bucket_flag: dict[str, bool],
    top_n: Optional[int],
    compare_cohort: tuple[str, ...],
    cohort_filter: tuple[str, ...],
) -> None:
    """Count template frequency with optional filtering and cohort comparison.

    Returns a dictionary mapping template IDs to their occurrence counts.
    """
    try:
        selector = BucketSelector(Path(file))
        template_ids_list = list(template_id) if template_id else None

        compare_cohorts_dict: Optional[dict[str, dict[str, Any]]] = None
        if compare_cohort:
            compare_cohorts_dict = {}
            for cohort_name in compare_cohort:
                compare_cohorts_dict[cohort_name] = {}
            # Parse cohort filters
            for filter_str in cohort_filter:
                if ":" not in filter_str:
                    continue
                cohort_name, filter_part = filter_str.split(":", 1)
                if cohort_name in compare_cohorts_dict and "=" in filter_part:
                    # Parse key=value
                    key, value = filter_part.split("=", 1)
                    truth_val = ("true", "1", "yes", "on")
                    false_val = ("false", "0", "no", "off")
                    if value.lower() in truth_val:
                        compare_cohorts_dict[cohort_name][key] = True
                    elif value.lower() in false_val:
                        compare_cohorts_dict[cohort_name][key] = False
                    else:
                        compare_cohorts_dict[cohort_name][key] = value

        frequency = selector.template_frequency(
            template_ids=template_ids_list,
            template_substring=substring,
            start=start.replace(tzinfo=timezone.utc) if start and start.tzinfo is None else start,
            end=end.replace(tzinfo=timezone.utc) if end and end.tzinfo is None else end,
            bucket_filters=bucket_flag if bucket_flag else None,
            top_n=top_n,
            compare_cohorts=compare_cohorts_dict,
        )

        if output_file or format == "json":
            _display_template_frequency_json(frequency, output_file)
        else:
            _display_template_frequency_table(frequency, format)

    except Exception as e:
        show_error("Failed to get template frequency", str(e))
        sys.exit(1)


@templates_group.command("timeline")
@common_options
@template_common_options
def templates_timeline(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    template_id: tuple[str, ...],
    substring: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
    bucket_flag: dict[str, bool],
) -> None:
    """Export timeline data combining bucket metrics with template instances.

    Returns time-series data suitable for plotting and cohort comparisons.
    """
    try:
        selector = BucketSelector(Path(file))
        template_ids_list = list(template_id) if template_id else None

        timeline = selector.timeline_export(
            template_ids=template_ids_list,
            template_substring=substring,
            start=start.replace(tzinfo=timezone.utc) if start and start.tzinfo is None else start,
            end=end.replace(tzinfo=timezone.utc) if end and end.tzinfo is None else end,
            bucket_filters=bucket_flag if bucket_flag else None,
        )

        if limit:
            timeline = timeline[:limit]

        if format == "json":
            _display_template_instances_json(timeline, output_file)
        elif format == "csv":
            _display_template_instances_csv(timeline, output_file)
        elif output_file:
            # For table formats with output file, use JSON
            _display_template_instances_json(timeline, output_file)
        else:
            _display_template_instances_table(timeline, format)

    except Exception as e:
        show_error("Failed to export timeline", str(e))
        sys.exit(1)


@templates_group.command("precursors")
@common_options
@click.option("--anchor", required=True, help="Anchor template ID to find precursors for")
@click.option("--lookback", type=int, required=True, help="Lookback window in minutes")
@click.option(
    "--bucket-flag",
    multiple=True,
    callback=parse_flag_arg,
    help="Filter buckets by flag (format: key=value)",
)
def templates_precursors(
    file: str,
    format: str,
    output_file: Optional[str],
    limit: Optional[int],
    anchor: str,
    lookback: int,
    bucket_flag: dict[str, bool],
) -> None:
    """Find templates that appear before an anchor template.

    Returns a ranked list of precursor templates with their frequencies.
    """
    try:
        selector = BucketSelector(Path(file))

        precursors = selector.precursor_sweep(
            anchor_template_id=anchor,
            lookback_minutes=lookback,
            bucket_filters=bucket_flag if bucket_flag else None,
        )

        if limit:
            precursors = precursors[:limit]

        if format == "json":
            _display_precursors_json(precursors, output_file)
        elif output_file:
            # For table formats with output file, use JSON
            _display_precursors_json(precursors, output_file)
        else:
            _display_precursors_table(precursors, format)

    except Exception as e:
        show_error("Failed to find precursors", str(e))
        sys.exit(1)


if __name__ == "__main__":
    cli()

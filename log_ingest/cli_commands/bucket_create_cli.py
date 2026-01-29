#!/usr/bin/env python3
"""CLI for creating time-aligned buckets from log files.

Orchestrates the entire bucket creation workflow:
1. File discovery and categorization
2. Pre-flight data validation
3. Bucket building
4. Verification and output

Provides rich progress indication and user feedback throughout the process.
"""

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import click
import yaml
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.status import Status
from rich.table import Table
from rich.text import Text

from log_ingest.bucket_building.bucket_builder import BucketBuilder
from log_ingest.bucket_data_validator import DataCategoryValidator, ValidationResult
from log_ingest.bucket_file_finder import BucketFileFinder, DiscoveryResult
from log_ingest.cli_commands.bucket_display_rich import RichBucketDisplay
from log_ingest.cli_commands.bucket_selector import BucketSelector

# Setup
console = Console()
logger = logging.getLogger(__name__)

# Constants
DEFAULT_DATA_DIR = "./data"
DEFAULT_OUTPUT_ROOT = "out"
DEFAULT_DRAIN3_DIRNAME = "drain3"
DEFAULT_BUCKETS_DIRNAME = "buckets"
DEFAULT_VERIFICATION_DIRNAME = "verification"
DEFAULT_BUCKET_DURATION_MIN = 5
DEFAULT_SPIKE_THRESHOLD_MS = 100.0
DEFAULT_FILE_LIMIT = 50

VERIFICATION_SAMPLE_COUNT = 3  # Number of random bucket samples to show during verification

# Size and time formatting constants
BYTES_PER_KB = 1024.0
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
# Default file discovery patterns
DEFAULT_EVENT_PATTERNS = ["*NAS*", "*PlatformClient*", "*PlatformManager*"]
DEFAULT_LATENCY_PATTERN = "*latency*"
DEFAULT_SYSTEM_PATTERN = "*Information*"


def _load_config_file(path: Path) -> dict:
    """Load a YAML/JSON config file for bucket creation."""
    raw_text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(raw_text) or {}
    elif suffix == ".json":
        data = json.loads(raw_text) or {}
    else:
        data = yaml.safe_load(raw_text) or {}
    if not isinstance(data, dict):
        raise click.ClickException(f"Config file {path} must contain a mapping/object.")
    return data


def _write_default_bucket_create_config(path: Path) -> None:
    """Write a default config template for `log_ingest bucket create`."""
    payload = {
        "data_dir": "./data",
        "event_patterns": ["*NAS*", "*PlatformClient*", "*PlatformManager*", "*console.log*"],
        "latency_pattern": ["*latency*"],
        "system_pattern": ["*Information*"],
        "extensions": [],
        "file_limit": 50,
        "force": False,
        "bucket_duration": 5,
        "spike_threshold": 100.0,
        "output_root": "out",
        "output": None,
        "no_verify": False,
        "save_summary": False,
        "skip_validation": False,
        "strict_mode": False,
        "yes": False,
        "quiet": False,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".yaml", ".yml"}:
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _config_list(config: dict, *keys: str) -> list:
    for key in keys:
        if key in config and config[key] is not None:
            value = config[key]
            if isinstance(value, list):
                return value
            return [value]
    return []


def _apply_bucket_create_config(
    config_path: Path,
    *,
    data_dir: Path,
    output_root: Path,
    output: Path | None,
    event_patterns: tuple[str, ...],
    latency_pattern: tuple[str, ...],
    system_pattern: tuple[str, ...],
    extensions: tuple[str, ...],
    file_limit: int,
    force: bool,
    bucket_duration: int,
    spike_threshold: float,
    no_verify: bool,
    save_summary: bool,
    skip_validation: bool,
    strict_mode: bool,
    yes: bool,
    quiet: bool,
) -> tuple[
    Path,
    Path,
    Path | None,
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    int,
    bool,
    int,
    float,
    bool,
    bool,
    bool,
    bool,
    bool,
    bool,
]:
    """Apply config file values only for parameters not explicitly set via CLI."""
    cfg = _load_config_file(config_path)
    ctx = click.get_current_context()

    def maybe_override(name: str, current: object, *cfg_keys: str) -> object:
        if ctx.get_parameter_source(name) != click.core.ParameterSource.DEFAULT:
            return current
        for k in cfg_keys:
            if k in cfg and cfg[k] is not None:
                return cfg[k]
        return current

    data_dir = Path(maybe_override("data_dir", data_dir, "data_dir")).expanduser()
    output_root = Path(maybe_override("output_root", output_root, "output_root")).expanduser()
    out_val = maybe_override("output", output, "output")
    output = Path(str(out_val)).expanduser() if out_val else None

    ep = maybe_override(
        "event_patterns",
        list(event_patterns),
        "event_patterns",
        "pattern",
        "event_pattern",
    )
    lp = maybe_override(
        "latency_pattern",
        list(latency_pattern),
        "latency_pattern",
        "latency_patterns",
    )
    sp = maybe_override(
        "system_pattern",
        list(system_pattern),
        "system_pattern",
        "system_patterns",
    )
    exts = maybe_override("extensions", list(extensions), "extensions", "extension", "exts")

    event_patterns = tuple(str(x) for x in (ep or [])) or event_patterns
    latency_pattern = tuple(str(x) for x in (lp or [])) or latency_pattern
    system_pattern = tuple(str(x) for x in (sp or [])) or system_pattern
    extensions = tuple(str(x) for x in (exts or [])) or extensions

    file_limit = int(maybe_override("file_limit", file_limit, "file_limit"))
    force = bool(maybe_override("force", force, "force"))
    bucket_duration = int(maybe_override("bucket_duration", bucket_duration, "bucket_duration"))
    spike_threshold = float(maybe_override("spike_threshold", spike_threshold, "spike_threshold"))
    no_verify = bool(maybe_override("no_verify", no_verify, "no_verify"))
    save_summary = bool(maybe_override("save_summary", save_summary, "save_summary"))
    skip_validation = bool(maybe_override("skip_validation", skip_validation, "skip_validation"))
    strict_mode = bool(maybe_override("strict_mode", strict_mode, "strict_mode"))
    yes = bool(maybe_override("yes", yes, "yes"))
    quiet = bool(maybe_override("quiet", quiet, "quiet"))

    return (
        data_dir,
        output_root,
        output,
        event_patterns,
        latency_pattern,
        system_pattern,
        extensions,
        file_limit,
        force,
        bucket_duration,
        spike_threshold,
        no_verify,
        save_summary,
        skip_validation,
        strict_mode,
        yes,
        quiet,
    )


# ============================================================
#                    DISPLAY UTILITIES
# ============================================================


def show_error(message: str, details: Optional[str] = None) -> None:
    """Display error message with Rich formatting."""
    error_text = Text("ERROR", style="bold red")
    content = f"{message}"
    if details:
        content += f"\n\n[dim]{details}[/dim]"

    console.print(Panel(Text(content), title=error_text, border_style="red", padding=(1, 2)))


def show_success(message: str) -> None:
    """Display success message with Rich formatting.

    Args:
        message: Success message to display
    """
    console.print(f"[green]✓ {message}[/green]")


def show_info(message: str) -> None:
    """Display info message with Rich formatting.

    Args:
        message: Info message to display
    """
    console.print(f"[blue]i {message}[/blue]")


def show_warning(message: str) -> None:
    """Display warning message with Rich formatting.

    Args:
        message: Warning message to display
    """
    console.print(f"[yellow]⚠ {message}[/yellow]")


def format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    # Guard against negative input
    if size_bytes < 0:
        return "0.0 B"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < BYTES_PER_KB:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= BYTES_PER_KB
    return f"{size_bytes:.1f} PB"


def format_time(seconds: float) -> str:
    """Format time in seconds as human-readable string."""
    # Guard against negative input
    if seconds < 0:
        return "0.0s"

    if seconds < SECONDS_PER_MINUTE:
        return f"{seconds:.1f}s"
    if seconds < SECONDS_PER_HOUR:
        minutes = seconds / SECONDS_PER_MINUTE
        return f"{minutes:.1f}m"
    hours = seconds / SECONDS_PER_HOUR
    return f"{hours:.1f}h"


# ============================================================
#                    WORKFLOW STAGES
# ============================================================


def stage_file_discovery(
    data_dir: Path,
    event_patterns: list[str],
    latency_patterns: list[str],
    system_patterns: list[str],
    extensions: Optional[list[str]],
    file_limit: int,
    force: bool,
) -> DiscoveryResult:
    """Stage 1: Discover and categorize log files.

    Args:
        data_dir: Root directory to search
        event_patterns: Patterns for event files
        latency_patterns: Patterns for latency files
        system_patterns: Patterns for system info files
        extensions: File extensions to include
        file_limit: Maximum files allowed
        force: Skip file limit confirmation

    Returns:
        DiscoveryResult with categorized files

    Raises:
        click.Abort: If user cancels discovery
        ValueError: If discovery fails validation
    """
    with Status("[bold blue]Discovering files...", console=console):
        finder = BucketFileFinder(data_dir)

        patterns = {
            "events": event_patterns,
            "latency": latency_patterns,
            "system": system_patterns,
        }

        try:
            discovery = finder.discover(
                patterns=patterns,
                extensions=extensions,
                file_limit=file_limit if not force else None,
            )
        except ValueError as e:
            show_error("File discovery failed", str(e))
            raise click.Abort from e

    _display_discovery_summary(discovery)
    return discovery


def _resolve_output_root(output_path: Path) -> Path:
    """Infer the output root directory from the bucket parquet path."""
    if output_path.parent.name == DEFAULT_BUCKETS_DIRNAME:
        return output_path.parent.parent
    return output_path.parent


def stage_data_validation(
    discovery: DiscoveryResult, skip_validation: bool, auto_confirm: bool
) -> ValidationResult:
    """Stage 3: Validate discovered data completeness.

    Args:
        discovery: DiscoveryResult to validate
        skip_validation: Skip validation checks
        auto_confirm: Auto-confirm prompts

    Returns:
        ValidationResult with warnings and estimates

    Raises:
        click.Abort: If validation fails or user cancels
    """
    if skip_validation:
        show_warning("Skipping data validation (--skip-validation)")
        # Return dummy result with no validation
        return ValidationResult(
            is_valid=True, missing_categories=[], warnings=[], estimated_processing_time_sec=0.0
        )

    try:
        validator = DataCategoryValidator(interactive=not auto_confirm)
        result = validator.validate(discovery)

        if result.warnings:
            show_warning(f"Validation passed with {len(result.warnings)} warnings")
        else:
            show_success("Data validation passed")

    except ValueError as e:
        show_error("Validation failed", str(e))
        raise click.Abort from e
    else:
        return result


def stage_bucket_building(
    discovery: DiscoveryResult,
    output_path: Path,
    bucket_duration_min: int,
    spike_threshold_ms: float,
    strict_mode: bool,
    quiet: bool,
    persistence_path: Path | None = None,
) -> BucketBuilder:
    """Stage 4: Build time buckets from discovered files.

    Args:
        discovery: DiscoveryResult with files to process
        output_path: Path to output parquet file
        bucket_duration_min: Bucket duration in minutes
        spike_threshold_ms: Latency spike threshold
        strict_mode: Abort on any parsing errors
        quiet: Suppress progress bars
        persistence_path: Optional Drain3 state path for template persistence

    Raises:
        click.Abort: If building fails
    """
    builder = BucketBuilder(
        bucket_duration=timedelta(minutes=bucket_duration_min),
        spike_threshold_ms=spike_threshold_ms,
        persistence_path=persistence_path,
    )

    try:
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Process incrementally with streaming API
        with Status("[bold blue]Building buckets...", console=console):
            builder.process_incremental(
                event_files=discovery.event_files,
                latency_files=discovery.latency_files,
                system_files=discovery.system_files,
                output_path=output_path,
            )

    except Exception as e:
        logger.exception("Bucket building failed: %s", type(e).__name__)
        show_error("Bucket building failed", str(e))
        if strict_mode:
            raise click.Abort from e
        show_warning("Continuing with partial data (use --strict-mode to abort)")
    return builder


def stage_verification(output_path: Path, no_verify: bool, save_summary: bool) -> None:
    """Stage 5: Verify bucket output with random sampling.

    Args:
        output_path: Path to bucket output file
        no_verify: Skip verification
        save_summary: Save verification summary
    """
    if no_verify:
        show_info("Skipping verification (--no-verify)")
        return

    console.print()
    console.print("[bold blue]Verifying output...[/bold blue]")
    console.print()

    try:
        selector = BucketSelector(output_path)
        display = RichBucketDisplay()

        # Get file info
        total_count = selector.count()
        time_range = selector.get_time_range()

        # Display summary
        _display_verification_summary(total_count, time_range, output_path)

        # Show 3 random samples
        console.print("[bold blue]Random samples:[/bold blue]")
        console.print()

        samples = selector.get_random(VERIFICATION_SAMPLE_COUNT)
        for i, bucket in enumerate(samples):
            # Pass TimeBucket directly (no conversion needed)
            display.show_compact(bucket)
            if i < len(samples) - 1:
                console.print()

        if save_summary:
            _save_verification_summary(total_count, time_range, output_path)

    except Exception as e:
        logger.warning(f"Verification failed: {type(e).__name__}: {e}")
        show_warning(f"Verification failed: {e}")


# ============================================================
#                    HELPER FUNCTIONS
# ============================================================


def _display_discovery_summary(discovery: DiscoveryResult) -> None:
    """Display file discovery summary table."""
    table = Table(show_header=True, header_style="bold cyan", box=box.ROUNDED, padding=(0, 2))

    table.add_column("Category", style="cyan")
    table.add_column("Files", justify="right", style="green")
    table.add_column("Size", justify="right", style="yellow")

    # Calculate per-category sizes
    event_size = sum(f.stat().st_size for f in discovery.event_files if f.exists())
    latency_size = sum(f.stat().st_size for f in discovery.latency_files if f.exists())
    system_size = sum(f.stat().st_size for f in discovery.system_files if f.exists())

    table.add_row("Events", str(len(discovery.event_files)), format_size(event_size))
    table.add_row("Latency", str(len(discovery.latency_files)), format_size(latency_size))
    table.add_row("System", str(len(discovery.system_files)), format_size(system_size))
    table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{discovery.total_files}[/bold]",
        f"[bold]{format_size(discovery.total_size_bytes)}[/bold]",
    )

    console.print(Panel(table, title="[bold blue]Discovery Summary[/bold blue]", expand=False))


def _display_verification_summary(
    total_count: int, time_range: Optional[tuple[datetime, datetime]], output_path: Path
) -> None:
    """Display verification summary table."""
    table = Table(show_header=False, box=box.ROUNDED, padding=(0, 2))
    table.add_column(style="cyan", justify="right")
    table.add_column(style="green")

    table.add_row("Total buckets", str(total_count))
    table.add_row("Output file", str(output_path))

    if time_range:
        start_str = time_range[0].strftime("%Y-%m-%d %H:%M")
        end_str = time_range[1].strftime("%Y-%m-%d %H:%M")
        table.add_row("Time range", f"{start_str} to {end_str}")

    console.print(Panel(table, title="[bold blue]Verification Summary[/bold blue]", expand=False))


def _save_verification_summary(
    total_count: int,
    time_range: Optional[tuple],
    output_path: Path,
) -> None:
    """Save verification summary to file."""
    verification_dir = _resolve_output_root(output_path) / DEFAULT_VERIFICATION_DIRNAME
    summary_path = verification_dir / "verification_summary.txt"
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with summary_path.open("w") as f:
        f.write("Bucket Verification Summary\n")
        f.write("==========================\n\n")
        f.write(f"Total buckets: {total_count}\n")
        f.write(f"Output file: {output_path}\n")

        if time_range:
            start_str = time_range[0].strftime("%Y-%m-%d %H:%M")
            end_str = time_range[1].strftime("%Y-%m-%d %H:%M")
            f.write(f"Time range: {start_str} to {end_str}\n")

    show_success(f"Verification summary saved to {summary_path}")


# ============================================================
#                    CLI COMMAND
# ============================================================


@click.command(name="create")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="YAML/JSON config file for bucket creation (recommended). CLI flags override config.",
)
@click.option(
    "--init-config",
    "init_config_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Write a default bucket-create config template to this path and exit.",
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=DEFAULT_DATA_DIR,
    help=f"Root directory to search for log files [default: {DEFAULT_DATA_DIR}]",
)
@click.option(
    "--pattern",
    "event_patterns",
    multiple=True,
    default=DEFAULT_EVENT_PATTERNS,
    help=(
        "Pattern for event files (can be used multiple times) "
        "[default: *NAS*, *PlatformClient*, *PlatformManager*]"
    ),
)
@click.option(
    "--latency-pattern",
    multiple=True,
    default=[DEFAULT_LATENCY_PATTERN],
    help=f"Pattern for latency files [default: {DEFAULT_LATENCY_PATTERN}]",
)
@click.option(
    "--system-pattern",
    multiple=True,
    default=[DEFAULT_SYSTEM_PATTERN],
    help=f"Pattern for system info files [default: {DEFAULT_SYSTEM_PATTERN}]",
)
@click.option(
    "--extension",
    "extensions",
    multiple=True,
    help="File extensions to include (e.g., .txt, .log). If not specified, all extensions allowed.",
)
@click.option(
    "--file-limit",
    type=int,
    default=DEFAULT_FILE_LIMIT,
    help=f"Maximum number of files to process [default: {DEFAULT_FILE_LIMIT}]",
)
@click.option("--force", is_flag=True, help="Override file limit without confirmation")
@click.option(
    "--bucket-duration",
    type=int,
    default=DEFAULT_BUCKET_DURATION_MIN,
    help=f"Bucket duration in minutes [default: {DEFAULT_BUCKET_DURATION_MIN}]",
)
@click.option(
    "--spike-threshold",
    type=float,
    default=DEFAULT_SPIKE_THRESHOLD_MS,
    help=f"Latency spike threshold in milliseconds [default: {DEFAULT_SPIKE_THRESHOLD_MS}]",
)
@click.option(
    "--output-root",
    type=click.Path(path_type=Path),
    default=DEFAULT_OUTPUT_ROOT,
    show_default=True,
    help=(
        "Base directory for outputs (buckets/, drain3/, verification/) "
        f"[default: {DEFAULT_OUTPUT_ROOT}]"
    ),
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=None,
    help=(
        "Output parquet file path. If omitted, uses "
        f"'{DEFAULT_OUTPUT_ROOT}/{DEFAULT_BUCKETS_DIRNAME}/buckets.parquet'."
    ),
)
@click.option("--no-verify", is_flag=True, help="Skip automatic verification after bucket creation")
@click.option("--save-summary", is_flag=True, help="Save verification summary to file")
@click.option("--skip-validation", is_flag=True, help="Skip pre-flight data validation checks")
@click.option(
    "--strict-mode",
    is_flag=True,
    help="Abort if any file fails to parse (default: skip corrupt files)",
)
@click.option("--yes", is_flag=True, help="Auto-confirm all prompts")
@click.option("--quiet", is_flag=True, help="Suppress progress bars (use logging instead)")
def create_buckets(
    config_path: Path | None,
    init_config_path: Path | None,
    data_dir: Path,
    event_patterns: tuple[str],
    latency_pattern: tuple[str],
    system_pattern: tuple[str],
    extensions: tuple[str],
    file_limit: int,
    force: bool,
    bucket_duration: int,
    spike_threshold: float,
    output_root: Path,
    output: Path | None,
    no_verify: bool,
    save_summary: bool,
    skip_validation: bool,
    strict_mode: bool,
    yes: bool,
    quiet: bool,
) -> None:
    r"""Create time-aligned buckets from log files.

    This command orchestrates the entire bucket creation workflow:

    \b
    1. File Discovery - Find and categorize log files
    2. Data Validation - Verify data completeness
    3. Bucket Building - Process files into time buckets
    4. Output - Save to parquet file
    5. Verification - Random sampling and validation

    \b
    EXAMPLES
        # Basic usage with defaults
        log_ingest bucket create

        # Custom data directory and output
        log_ingest bucket create --data-dir /path/to/logs --output my_buckets.parquet

        # Process more files with custom threshold
        log_ingest bucket create --file-limit 100 --spike-threshold 50.0

        # Batch mode (no prompts, quiet)
        log_ingest bucket create --yes --quiet --no-verify
    """
    if config_path is not None and init_config_path is not None:
        raise click.ClickException("Provide only one of --config or --init-config.")

    if init_config_path is not None:
        _write_default_bucket_create_config(init_config_path)
        click.echo(f"Wrote default bucket-create config to {init_config_path}")
        return

    if config_path is not None:
        (
            data_dir,
            output_root,
            output,
            event_patterns,
            latency_pattern,
            system_pattern,
            extensions,
            file_limit,
            force,
            bucket_duration,
            spike_threshold,
            no_verify,
            save_summary,
            skip_validation,
            strict_mode,
            yes,
            quiet,
        ) = _apply_bucket_create_config(
            config_path,
            data_dir=data_dir,
            output_root=output_root,
            output=output,
            event_patterns=event_patterns,
            latency_pattern=latency_pattern,
            system_pattern=system_pattern,
            extensions=extensions,
            file_limit=file_limit,
            force=force,
            bucket_duration=bucket_duration,
            spike_threshold=spike_threshold,
            no_verify=no_verify,
            save_summary=save_summary,
            skip_validation=skip_validation,
            strict_mode=strict_mode,
            yes=yes,
            quiet=quiet,
        )

    console.print()
    console.print(
        Panel.fit(
            "[bold blue]Bucket Creation Pipeline[/bold blue]\n"
            "Creating time-aligned buckets from log files",
            border_style="blue",
        )
    )
    console.print()

    try:
        output_root = output_root.expanduser()
        if output is None:
            output = output_root / DEFAULT_BUCKETS_DIRNAME / "buckets.parquet"
        else:
            output = output.expanduser()
            # If the user supplied an explicit output file, anchor other artifacts next to it.
            output_root = output.parent

        # Ensure output directories exist early.
        (output_root / DEFAULT_BUCKETS_DIRNAME).mkdir(parents=True, exist_ok=True)
        (output_root / DEFAULT_DRAIN3_DIRNAME).mkdir(parents=True, exist_ok=True)
        (output_root / DEFAULT_VERIFICATION_DIRNAME).mkdir(parents=True, exist_ok=True)

        # Stage 1: File Discovery
        console.print("[bold cyan]Stage 1: File Discovery[/bold cyan]")
        discovery = stage_file_discovery(
            data_dir=data_dir,
            event_patterns=list(event_patterns),
            latency_patterns=list(latency_pattern),
            system_patterns=list(system_pattern),
            extensions=list(extensions) if extensions else None,
            file_limit=file_limit,
            force=force,
        )
        console.print()

        # Stage 2: Data Validation
        console.print("[bold cyan]Stage 2: Data Validation[/bold cyan]")
        validation = stage_data_validation(
            discovery=discovery, skip_validation=skip_validation, auto_confirm=yes
        )

        if validation.estimated_processing_time_sec > 0:
            estimated_time = format_time(validation.estimated_processing_time_sec)
            show_info(f"Estimated processing time: {estimated_time}")
        console.print()

        # Stage 3: Bucket Building (now includes output save)
        console.print("[bold cyan]Stage 3: Bucket Building[/bold cyan]")
        persistence_path = output_root / DEFAULT_DRAIN3_DIRNAME / "drain3_state.bin"
        builder = stage_bucket_building(
            discovery=discovery,
            output_path=output,
            bucket_duration_min=bucket_duration,
            spike_threshold_ms=spike_threshold,
            strict_mode=strict_mode,
            quiet=quiet,
            persistence_path=persistence_path,
        )

        # Verify output was created
        if not output.exists():
            show_error("No buckets created", "Check input files and patterns")
            sys.exit(1)

        show_success(f"Buckets saved to {output}")

        console.print()

        # Stage 4: Verification
        console.print("[bold cyan]Stage 4: Verification[/bold cyan]")
        stage_verification(output_path=output, no_verify=no_verify, save_summary=save_summary)
        console.print()

        snapshot = builder.save_drain3_snapshot(output_root / DEFAULT_DRAIN3_DIRNAME)
        if snapshot:
            show_success(f"Drain3 snapshot saved to {snapshot}")

        # Final success message
        console.print(
            Panel.fit(
                f"[bold green]✓ Bucket creation completed successfully![/bold green]\n"
                f"Output: {output}",
                border_style="green",
            )
        )

    except click.Abort:
        console.print()
        show_warning("Operation cancelled by user")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print()
        show_warning("Operation interrupted by user")
        sys.exit(130)
    except Exception as e:
        console.print()
        logger.exception("Unexpected error: %s", type(e).__name__)
        show_error("Unexpected error", str(e))
        if not quiet:
            console.print_exception()
        sys.exit(1)


# ============================================================
#                    CLI GROUP
# ============================================================


@click.group(name="bucket")
def cli() -> None:
    """Bucket creation and management commands.

    Create time-aligned buckets from log files for time-series analysis.
    """
    pass


# Add create command to group
cli.add_command(create_buckets)


if __name__ == "__main__":
    cli()

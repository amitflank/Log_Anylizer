# log_ingest

## Project Overview

Log ingestion and analysis system for StorONE storage logs. Parses structured log files, extracts performance metrics, system information, and events, then organizes them into fixed time-aligned buckets for time-series analysis.

## Quick Start

```bash
# Install
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create buckets from logs (CLI)
python -m log_ingest.cli /path/to/console.log /path/to/log_*.txt latency_info.txt --outdir out --parquet

# Or use the API directly (recommended)
from pathlib import Path
from datetime import timedelta
from log_ingest.bucket_building.bucket_builder import BucketBuilder

builder = BucketBuilder(bucket_duration=timedelta(minutes=5), spike_threshold_ms=100.0)
builder.process_incremental(
    event_files=[Path("console.log")],
    latency_files=[Path("latency_info.txt")],
    system_files=[Path("system_info.txt")],
    output_path=Path("out/buckets/buckets.parquet"),
    processing_duration=timedelta(hours=4)  # Optional: process 4-hour windows
)
```

Outputs `out/logs.parquet`, `out/metrics.parquet`, `out/periodic.parquet` (or CSV if not using `--parquet`).

---

## Migration Guide: Legacy API to process_incremental()

The old multi-method workflow (`load_events()` → `load_latency()` → `load_system_info()` → `create_buckets()` → `save_to_parquet()`) has been deprecated in favor of the new streaming API `process_incremental()`.

### Why Migrate?

- **Bounded Memory**: Processes data in fixed time windows instead of loading everything into memory
- **Streaming Output**: Writes directly to parquet incrementally, no need for `get_buckets()` or `save_to_parquet()`
- **Simpler API**: Single method call replaces 5-method sequence
- **Better Performance**: Processes large datasets without memory exhaustion

### Migration Examples

**Old API (Deprecated):**
```python
# OLD - Do not use
builder = BucketBuilder(bucket_duration=timedelta(minutes=5), spike_threshold_ms=100.0)
builder.load_events(event_files)
builder.load_latency(latency_files)
builder.load_system_info(system_files)
builder.create_buckets()
builder.save_to_parquet(output_path)
```

**New API (Recommended):**
```python
# NEW - Use this
builder = BucketBuilder(bucket_duration=timedelta(minutes=5), spike_threshold_ms=100.0)
builder.process_incremental(
    event_files=event_files,
    latency_files=latency_files,
    system_files=system_files,
    output_path=output_path,
    processing_duration=timedelta(hours=4)  # Optional: defaults to 4 hours
)
# Output is written directly to parquet - no need for save_to_parquet()
```

### Key Differences

| Aspect | Old API | New API |
|--------|---------|---------|
| **Data loading** | 3 separate calls | Single call with all files |
| **Memory usage** | Load all to memory | Streaming (bounded memory) |
| **Output** | In-memory buckets via `get_buckets()` | Direct parquet write |
| **Bucket access** | `get_buckets()` returns list | Read parquet with `pq.read_table()` |
| **Processing window** | All data | Configurable window (default 4 hours) |

### Reading Buckets from Parquet

After using `process_incremental()`, read buckets from the parquet file:

```python
import pyarrow.parquet as pq

# Read all buckets
table = pq.read_table(output_path)
buckets_df = table.to_pandas()

# Or use BucketSelector for filtering
from log_ingest.cli_commands.bucket_selector import BucketSelector

selector = BucketSelector(output_path)
buckets = selector.select_all()  # Returns list of TimeBucket objects
```

---

## Project Structure

```yaml
architecture:
  log_ingest/:
    purpose: "Log ingestion and bucket creation system"

    # Top-level modules
    files:
      __init__.py:
        description: "Package initialization (empty)"

      __main__.py:
        description: "Main entry point for running as python -m log_ingest"

      message_parser.py:
        description: "Log line parsing with regex and Drain3 template extraction"
        classes:
          - CleanResult: "Result from log line cleaning"
          - DrainParser: "Drain3-based template mining log parser"

      parse_latency.py:
        description: "Parsers for structured latency tables in multiple formats"
        classes:
          - LatencyTableParser: "Parser for box-drawn latency tables"
          - JsonLatencyParser: "Parser for JSON-formatted latency data"

      system_info_parser.py:
        description: "System metrics, pool info, HA status, and network extraction"
        classes:
          - SystemInfoRecord: "Dataclass for system information records"
          - PeriodicSectionParser: "Parser for periodic system information sections"

      bucket_file_finder.py:
        description: "File discovery engine for categorizing logs into event/latency/system"
        classes:
          - DiscoveryResult: "Result from file discovery with categorized groups"
          - BucketFileFinder: "Smart file discovery with pattern-based categorization"

      bucket_file_tracker.py:
        description: "Manifest-based file tracking for cache-aware incremental processing"
        classes:
          - FileMetadata: "Metadata for tracking file state"
          - FileComparisonResult: "Result of comparing file states"
          - ProcessedManifest: "Manifest of processed files"
          - ProcessedFileTracker: "Tracks processed files for incremental updates"

      bucket_data_validator.py:
        description: "Pre-flight validation for data completeness with user prompts"
        classes:
          - ValidationResult: "Result from data validation"
          - DataCategoryValidator: "Validates bucket creation data completeness"

      file_util.py:
        description: "File search utilities with keyword matching and extension filtering"
        classes:
          - FindRecord: "Record of a file search result"
          - FileFinder: "Utility for finding files with patterns"

      utils.py:
        description: "Utility functions for timestamp parsing and data normalization"
        classes: []

      data_types.py:
        description: "Custom type definitions and dataclasses for categorized file groups"
        classes:
          - LogFileCategories: "Categorized groups of log files"

      schemas.py:
        description: "Data schemas for LogRecord, MetricRecord, and PeriodicSection"
        classes:
          - LogRecord: "Schema for generic log line records"
          - MetricRecord: "Schema for latency and performance metrics"
          - PeriodicSection: "Schema for periodic system information"

    # Subdirectories
    subdirectories:
      bucket_building/:
        purpose: "Core bucket processing engine with incremental writes and time windows"
        description: "Processes log events into fixed time-aligned buckets for time-series analysis. Handles both batch and incremental processing modes with support for out-of-order events, timestamp extraction, and efficient parquet streaming."

        files:
          __init__.py:
            description: "Bucket building module exports and documentation"

          bucket_builder.py:
            description: "Main orchestration logic for bucket creation from multiple data sources"
            classes:
              - IncrementalComponents: "NamedTuple holding incremental processing components"
              - ProcessingCounters: "NamedTuple for tracking processing statistics"
              - WindowContext: "NamedTuple for time window processing context"
              - BucketBuilder: "Orchestrates bucket creation from events, latency metrics, and system info"

          bucket_container.py:
            description: "Base classes for bucket management and time window containers"
            classes:
              - BucketContainer: "Base class for bucket management with shared functionality"
              - BucketWindow: "Window-based bucket container with time boundaries for incremental processing"
              - PendingBucketJail: "Holds incomplete buckets until time window advances"

          bucket_update_helpers.py:
            description: "Helper functions for updating bucket fields with events"
            classes: []

          constants.py:
            description: "Shared constants for bucket operations and field mappings"
            classes: []

          file_helpers.py:
            description: "File I/O utilities for efficient line reading"
            classes: []

          file_queue.py:
            description: "File queue management for incremental processing"
            classes:
              - FileQueue: "Manages queue of log files with position bookmarks"

          incremental_parquet_writer.py:
            description: "Parquet streaming and incremental file writes"
            classes:
              - IncrementalParquetWriter: "Writes TimeBucket data to parquet incrementally"

          scrambled_event_buffer.py:
            description: "Buffer for handling out-of-order events"
            classes:
              - ScrambledEventBuffer: "Buffer for out-of-order events with parquet flush support"

          time_bucket.py:
            description: "TimeBucket dataclass and parquet schema definition"
            classes:
              - TimeBucket: "Dataclass representing a fixed-duration time bucket"

          time_range_scanner.py:
            description: "File timestamp range extraction for processing windows"
            classes:
              - FileTimeRange: "Time range for a single file"
              - FileTimeRanges: "Collection of file time ranges"
              - TimeRangeScanner: "Scans files to extract timestamp ranges"

          timestamp_extraction.py:
            description: "Functions for extracting timestamps from log lines"
            classes: []

          timestamp_helpers.py:
            description: "Timestamp alignment and bucket boundary calculations"
            classes: []

      cli_commands/:
        purpose: "Command-line interface for bucket creation, viewing, and querying"
        description: "Command-line interface for log ingestion and bucket analysis operations. Provides commands for creating time-aligned buckets from log files, viewing bucket data, and selecting buckets based on various criteria."

        files:
          __init__.py:
            description: "Available CLI commands documentation and package exports"

          cli.py:
            description: "Main CLI entry point that assembles all command groups"
            classes: []

          bucket_create_cli.py:
            description: "Bucket creation workflow with staged pipeline and rich output"
            classes: []

          bucket_viewer.py:
            description: "View and validate bucket parquet files with multiple display formats"
            classes:
              - BucketAnalyzer: "Analyzes bucket parquet files for validation and display"

          bucket_selector_cli.py:
            description: "POSIX-style commands for querying and filtering bucket data"
            classes: []

          bucket_selector.py:
            description: "Selection logic for querying parquet files using DuckDB"
            classes:
              - BucketSelector: "Queries and filters bucket data using DuckDB"

          bucket_display_rich.py:
            description: "Rich-based display formatting for TimeBucket objects"
            classes:
              - RichBucketDisplay: "Formats TimeBucket objects for rich terminal display"
```

---

## Key Classes Reference

Quick lookup for major classes and their locations:

| Class | File | Purpose |
|-------|------|---------|
| `BucketBuilder` | `bucket_building/bucket_builder.py` | Orchestrates bucket creation from events, latency metrics, and system info |
| `BucketFileFinder` | `bucket_file_finder.py` | Smart file discovery with pattern-based categorization |
| `DataCategoryValidator` | `bucket_data_validator.py` | Pre-flight validator for bucket creation data completeness |
| `ProcessedFileTracker` | `bucket_file_tracker.py` | Manifest-based file tracking for incremental processing |
| `DrainParser` | `message_parser.py` | Log line parser with Drain3 template mining support |
| `LatencyTableParser` | `parse_latency.py` | Parser for box-drawn latency tables and performance metrics |
| `PeriodicSectionParser` | `system_info_parser.py` | Parser for periodic system information sections |
| `BucketWindow` | `bucket_building/bucket_container.py` | Window-based bucket container with time boundaries |
| `PendingBucketJail` | `bucket_building/bucket_container.py` | Holds incomplete buckets until time window advances |
| `FileQueue` | `bucket_building/file_queue.py` | Manages queue of log files for incremental processing |
| `IncrementalParquetWriter` | `bucket_building/incremental_parquet_writer.py` | Writes TimeBucket data to parquet files incrementally |
| `ScrambledEventBuffer` | `bucket_building/scrambled_event_buffer.py` | Buffer for out-of-order events with parquet flush support |
| `TimeRangeScanner` | `bucket_building/time_range_scanner.py` | Scans files to extract timestamp ranges for processing windows |
| `TimeBucket` | `bucket_building/time_bucket.py` | Dataclass representing a fixed-duration time bucket |
| `BucketAnalyzer` | `cli_commands/bucket_viewer.py` | Analyzes bucket parquet files for validation and display |
| `BucketSelector` | `cli_commands/bucket_selector.py` | Queries and filters bucket data using DuckDB |
| `RichBucketDisplay` | `cli_commands/bucket_display_rich.py` | Formats TimeBucket objects for rich terminal display |

---

## Data Schemas

Core data structures used throughout the system:

- **`LogRecord`** (`schemas.py`): Generic log line records with timestamp, level, component, message, key-value pairs
- **`MetricRecord`** (`schemas.py`): Latency and performance metrics with normalized units
- **`PeriodicSection`** (`schemas.py`): Periodic system information sections with nested dictionaries
- **`TimeBucket`** (`bucket_building/time_bucket.py`): Fixed-duration time bucket containing aggregated events and metrics
- **`LogFileCategories`** (`data_types.py`): Categorized groups of log files (event logs, latency files, system info files)

---

## Navigation Guide

**For detailed API documentation** (classes, methods, function signatures):
- See `log_ingest/README.md` for top-level module details
- See `log_ingest/bucket_building/README.md` for bucket processing internals
- See `log_ingest/cli_commands/README.md` for CLI command implementations

**Common entry points:**
- CLI entry: `cli_commands/cli.py`
- Bucket creation: `bucket_building/bucket_builder.py::BucketBuilder`
- Log parsing: `message_parser.py::DrainParser`
- File discovery: `bucket_file_finder.py::BucketFileFinder`

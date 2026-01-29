# cli_commands

## Purpose

Command-line interface for log ingestion and bucket analysis operations. Provides commands for creating time-aligned buckets from log files, viewing bucket data, and selecting buckets based on various criteria. Orchestrates the entire bucket creation workflow from file discovery through validation to parquet output.

This directory contains the user-facing CLI that makes the log_ingest system accessible through intuitive commands.

## Files

- `__init__.py`: Available CLI commands documentation and package exports
- `cli.py`: Main CLI entry point that assembles all command groups
- `bucket_create_cli.py`: Bucket creation workflow with staged pipeline and rich output
- `bucket_viewer.py`: View and validate bucket parquet files with multiple display formats
  - **Classes**:
    - `BucketAnalyzer`: Analyzes buckets for anomalies and patterns
      - `__init__()`: Initialize bucket analyzer with defensive validation
      - `get_anomaly_indicators()`: Get anomaly indicators for a bucket
      - `get_historical_context()`: Get historical context for a bucket (last hour if 5-min buckets)
      - `get_time_patterns()`: Identify time-based patterns in the bucket
- `bucket_selector_cli.py`: POSIX-style commands for querying and filtering bucket data
- `bucket_selector.py`: Selection logic for querying parquet files using DuckDB
  - **Classes**:
    - `BucketSelector`: Handles selection of buckets from a parquet file for various display/analysis purposes
      - `__init__()`: Initialize with parquet file path
      - `is_empty()`: Check if selector has any buckets
      - `get_random()`: Get n random buckets for sanity checking
      - `by_flags()`: Filter buckets by boolean flag values
      - `by_thresholds()`: Filter buckets by numeric threshold comparisons
      - `by_time_range()`: Filter buckets within a time range
      - `top_n()`: Get the top N buckets by a field value (descending order)
      - `bottom_n()`: Get the bottom N buckets by a field value (ascending order)
      - `count()`: Get the total number of buckets in the parquet file
      - `exists()`: Check if any buckets exist in the file
      - `get_time_range()`: Get the time range covered by all buckets
- `bucket_display_rich.py`: Rich-based display formatting for TimeBucket objects
  - **Classes**:
    - `RichBucketDisplay`: Display TimeBucket data using Rich library
      - `__init__()`: Initialize with optional theme
      - `show_compact()`: Show compact one-line view
      - `show_normal()`: Show normal view with key sections
      - `show_detailed()`: Show detailed view with all data
      - `show_json()`: Show as formatted JSON
      - `show_comparison()`: Show comparison table of multiple buckets side-by-side
      - `live_monitor()`: Live monitoring display that updates as new buckets arrive

## Subdirectories

None

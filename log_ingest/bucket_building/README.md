# bucket_building

## Purpose

Processes log events into fixed time-aligned buckets for time-series analysis. Handles both batch and incremental processing modes with support for out-of-order events, timestamp extraction, and efficient parquet streaming. The core engine for transforming raw parsed events into analyzable bucket structures.

This module implements the bucket building pipeline, managing time windows, file queues, incremental writes, and out-of-order event buffering.

## Recommended API

**Use `BucketBuilder.process_incremental()`** for all bucket creation tasks. This method provides:
- Streaming processing with bounded memory usage
- Direct parquet output without intermediate in-memory storage
- Configurable time windows for incremental processing
- Better performance and scalability for large datasets

**Deprecated:** The old multi-method workflow (`load_events()` → `load_latency()` → `load_system_info()` → `create_buckets()` → `save_to_parquet()`) is deprecated. See the Migration Guide in the root README.md for details.

## Classes

- `bucket_builder.py::BucketBuilder`: Orchestrates bucket creation from events, latency metrics, and system info
- `bucket_container.py::BucketContainer`: Base class for bucket management with shared functionality
- `bucket_container.py::BucketWindow`: Window-based bucket container with time boundaries for incremental processing
- `bucket_container.py::PendingBucketJail`: Holds incomplete buckets until time window advances
- `file_queue.py::FileQueue`: Manages queue of log files for incremental processing with position bookmarks
- `incremental_parquet_writer.py::IncrementalParquetWriter`: Writes TimeBucket data to parquet files incrementally
- `scrambled_event_buffer.py::ScrambledEventBuffer`: Buffer for out-of-order events with parquet flush support
- `time_range_scanner.py::TimeRangeScanner`: Scans files to extract timestamp ranges for processing windows

## Files

- `__init__.py`: Bucket building module exports and documentation
- `bucket_builder.py`: Main orchestration logic for bucket creation from multiple data sources
  - **Classes**:
    - `IncrementalComponents`: Components needed for incremental processing
    - `ProcessingCounters`: Counters for tracking incremental processing progress
    - `WindowContext`: Context for a single processing window
    - `BucketBuilder`: Builds time buckets from parsed data sources with streaming support
      - `__init__()`: Initialize builder with bucket size and spike threshold
      - `generate_bucket_id()`: Generate a bucket ID from timestamp
      - `get_bucket_for_timestamp()`: Get bucket ID and boundaries for a timestamp
      - `process_incremental()`: **[RECOMMENDED]** Process event files incrementally with streaming components
      - `save_drain3_snapshot()`: Copy Drain3 persistence file to immutable snapshot
      - `load_events()`: **[DEPRECATED]** Load and parse event log files into pre-created buckets
      - `load_latency()`: **[DEPRECATED]** Load and parse latency table files
      - `load_system_info()`: **[DEPRECATED]** Load and parse system info files
      - `create_buckets()`: **[DEPRECATED]** Create time buckets and assign data to them
      - `save_to_parquet()`: **[DEPRECATED]** Save buckets directly to Parquet using PyArrow
- `bucket_container.py`: Base classes for bucket management and time window containers
  - **Classes**:
    - `BucketContainer`: Base class for managing time buckets with shared functionality
      - `__init__()`: Initialize bucket container with specified duration
      - `generate_bucket_id()`: Generate a bucket ID from timestamp
      - `get_bucket_for_timestamp()`: Get bucket ID and boundaries for a timestamp
      - `add_event()`: Add event to appropriate bucket, creating bucket if needed
    - `BucketWindow`: Window-based bucket container with time boundaries
      - `__init__()`: Initialize bucket window with time boundaries
      - `add_event()`: Add event to appropriate bucket with window boundary validation
      - `assign_latency()`: Assign latency metrics to buckets from provided records
      - `assign_system_info()`: Assign system info to buckets using forward-fill strategy
      - `get_non_empty_buckets()`: Filter and return buckets containing events
      - `clear()`: Clear all buckets to prepare window for reuse
    - `PendingBucketJail`: Holds future events with promotion support
      - `__init__()`: Initialize jail with specified bucket duration
      - `add_event()`: Add future event to jail, creating bucket if needed
      - `promote_eligible()`: Promote buckets that fall within specified window
      - `check_size_threshold()`: Check jail size and log warnings at logarithmic thresholds
      - `get_size()`: Return total bucket count in jail
      - `is_empty()`: Check if jail has any buckets
      - `get_oldest_bucket_time()`: Get earliest bucket start time in jail
      - `get_newest_bucket_time()`: Get latest bucket start time in jail
- `bucket_update_helpers.py`: Helper functions for updating bucket fields with events
- `constants.py`: Shared constants for bucket operations and field mappings
- `file_helpers.py`: File I/O utilities for efficient line reading
- `file_queue.py`: File queue management for incremental processing
  - **Classes**:
    - `FileQueue`: Manages queue of log files for incremental processing
      - `__init__()`: Initialize FileQueue with file time ranges from scanner
      - `queue_files_for_window()`: Queue files for the current time window
      - `open_next_file()`: Open next file from queue for reading
      - `read_next_line()`: Read one line from currently open file
      - `bookmark_current_file()`: Save current position and requeue file for resume
      - `mark_current_file_complete()`: Mark current file as completely processed
      - `has_files_in_queue()`: Check if there are files in the queue to process
      - `get_queue_size()`: Get number of files currently in queue
      - `get_current_file()`: Get path of currently open file, or None if no file open
- `incremental_parquet_writer.py`: Parquet streaming and incremental file writes
  - **Classes**:
    - `IncrementalParquetWriter`: Write TimeBucket data to parquet files incrementally
      - `__init__()`: Initialize writer with output path and schema
      - `append_buckets()`: Write buckets to parquet file, creating new or appending to existing
- `scrambled_event_buffer.py`: Buffer for handling out-of-order events
  - **Classes**:
    - `ScrambledEventBuffer`: Buffer for out-of-order events with parquet flush support
      - `__init__()`: Initialize buffer with bucket duration and parquet writer
      - `add_event()`: Add scrambled event to buffer, tracking count
      - `flush()`: Flush all buckets to parquet and clear buffer
      - `check_scrambled_percentage()`: Calculate and validate scrambled event percentage
      - `get_scrambled_count()`: Return total scrambled events tracked
      - `has_pending_buckets()`: Check if buffer has unflushed buckets
      - `get_bucket_count()`: Return number of buckets in buffer
- `time_bucket.py`: TimeBucket dataclass and parquet schema definition
  - **Classes**:
    - `TimeBucket`: A fixed time window containing aggregated metrics and events
      - `to_dict()`: Convert bucket to dictionary with serializable types
      - `to_json()`: Convert bucket to JSON string
- `time_range_scanner.py`: File timestamp range extraction for processing windows
  - **Classes**:
    - `FileTimeRange`: Represents time range and bookmark position for a single log file
    - `FileTimeRanges`: Collection of FileTimeRange objects, sorted by start_time
      - `start_time`: Property returning earliest file start time
      - `end_time`: Property returning latest file end time
      - `get_files_starting_before()`: Return files with start_time strictly before cutoff
    - `TimeRangeScanner`: Scans log files to extract timestamp ranges
      - `scan_file_timestamp_range()`: Scan single file for min/max timestamps
      - `read_file_lines()`: Read only first and last lines efficiently for timestamp scanning
      - `extract_first_timestamp()`: Extract timestamp from first valid line
      - `extract_last_timestamp()`: Extract timestamp from last valid line
      - `parse_line_timestamp()`: Parse timestamp from single line
      - `scan_files()`: Scan multiple files for timestamp ranges
- `timestamp_extraction.py`: Functions for extracting timestamps from log lines
- `timestamp_helpers.py`: Timestamp alignment and bucket boundary calculations

## Subdirectories

None

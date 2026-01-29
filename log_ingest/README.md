# log_ingest

## Purpose

Log ingestion and analysis system for StorONE storage logs. Parses structured log files, extracts performance metrics, system information, and events, then organizes them into fixed time-aligned buckets for time-series analysis. Provides CLI tools for bucket creation, querying, and visualization with support for incremental processing (cache support in the CLI is currently disabled).

The system transforms raw log data into analyzable bucket structures, enabling performance analysis, anomaly detection, and template mining across storage system operations.

## Classes

- `bucket_file_finder.py::BucketFileFinder`: Smart file discovery for bucket creation with pattern-based categorization
- `bucket_data_validator.py::DataCategoryValidator`: Pre-flight validator for bucket creation data completeness
- `bucket_file_tracker.py::ProcessedFileTracker`: Manifest-based file tracking for incremental processing
- `message_parser.py::DrainParser`: Log line parser with Drain3 template mining support
- `parse_latency.py::LatencyTableParser`: Parser for box-drawn latency tables and performance metrics
- `system_info_parser.py::PeriodicSectionParser`: Parser for periodic system information sections

## Files

- `__init__.py`: Package initialization (empty)
- `__main__.py`: Main entry point for running as python -m log_ingest
- `message_parser.py`: Log line parsing with regex and Drain3 template extraction
  - **Classes**:
    - `CleanResult`: Result of step-1 message sanitation
    - `DrainParser`: Optimized log parser with Drain3 template mining
      - `__init__()`: Initialize parser with optional persistence path
      - `parse_message()`: Parse log message using Drain3 template mining with parameter extraction
      - `parse_line()`: Parse single log line into structured record with optimized datetime parsing
      - `parse_file()`: Parse entire file into list (accumulates in memory)
      - `parse_file_streaming()`: Stream parse file line-by-line without accumulating in memory
      - `parse_files()`: Parse many files eagerly into list
      - `iter_files()`: Parse many files with streaming iterator
      - `ingest_by_filename_keywords()`: Find files by filename keywords, then ingest via streaming
- `parse_latency.py`: Parsers for structured latency tables in multiple formats
  - **Classes**:
    - `LatencyTableParser`: Parser for box-drawn and space-separated latency tables
      - `__init__()`: Initialize the parser
      - `parse()`: Parse latency table lines into MetricRecords
    - `JsonLatencyParser`: Parser for JSON format performance statistics
      - `__init__()`: Initialize the parser
      - `parse()`: Parse JSON latency data into MetricRecords
- `system_info_parser.py`: System metrics, pool info, HA status, and network extraction
  - **Classes**:
    - `SystemInfoRecord`: Structured record for periodic system information
    - `PeriodicSectionParser`: Parser for periodic information sections
      - `parse_section()`: Parse a single periodic information section into SystemInfoRecord
- `bucket_file_finder.py`: File discovery engine for categorizing logs into event/latency/system
  - **Classes**:
    - `DiscoveryResult`: Results from file discovery with categorized file lists
    - `BucketFileFinder`: Smart file discovery for bucket creation with categorization
      - `__init__()`: Initialize file finder with root directory
      - `discover()`: Discover and categorize files based on patterns
- `bucket_file_tracker.py`: Manifest-based file tracking for cache-aware incremental processing
  - **Classes**:
    - `FileMetadata`: Metadata for a single processed file
    - `FileComparisonResult`: Results from comparing discovered files against manifest
      - `needs_processing()`: Check if any files need processing
      - `report_summary()`: Generate human-readable summary
    - `ProcessedManifest`: Manifest of processed files with metadata and statistics
    - `ProcessedFileTracker`: Tracks processed files using JSON manifest for incremental processing
      - `__init__()`: Initialize tracker with output directory
      - `load_manifest()`: Load existing manifest or return None if not found/corrupt
      - `check_files()`: Compare discovered files against manifest
      - `update_manifest()`: Atomically update manifest after processing files
- `bucket_data_validator.py`: Pre-flight validation for data completeness with user prompts
  - **Classes**:
    - `ValidationResult`: Results from data category validation
    - `DataCategoryValidator`: Pre-flight validator for bucket creation data completeness
      - `__init__()`: Initialize validator with interaction mode
      - `validate()`: Validate discovered files have sufficient data categories
- `file_util.py`: File search utilities with keyword matching and extension filtering
- `utils.py`: Utility functions for timestamp parsing and data normalization
- `data_types.py`: Custom type definitions and dataclasses for categorized file groups
- `schemas.py`: Data schemas for LogRecord, MetricRecord, and PeriodicSection

## Subdirectories

- `bucket_building/`: Core bucket processing engine with incremental writes and time windows
- `cli_commands/`: Command-line interface for bucket creation, viewing, and querying

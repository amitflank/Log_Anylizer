"""ABOUTME: Bucket building module - processes log events into time buckets.

ABOUTME: Contains incremental and batch processing for event aggregation.
"""

from log_ingest.bucket_building.bucket_builder import BucketBuilder
from log_ingest.bucket_building.bucket_container import (
    BucketContainer,
    BucketWindow,
    PendingBucketJail,
)
from log_ingest.bucket_building.file_queue import FileQueue
from log_ingest.bucket_building.incremental_parquet_writer import IncrementalParquetWriter
from log_ingest.bucket_building.scrambled_event_buffer import ScrambledEventBuffer
from log_ingest.bucket_building.time_range_scanner import (
    FileTimeRange,
    FileTimeRanges,
    TimeRangeScanner,
)

__all__ = [
    "BucketBuilder",
    "BucketContainer",
    "BucketWindow",
    "FileQueue",
    "FileTimeRange",
    "FileTimeRanges",
    "IncrementalParquetWriter",
    "PendingBucketJail",
    "ScrambledEventBuffer",
    "TimeRangeScanner",
]

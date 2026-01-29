"""bucket_builder.py.

Builds time-aligned buckets from events, latency metrics, and system info.
Organizes all data into fixed time windows for time-series analysis.
"""

from __future__ import annotations

import json
import logging
import shutil
import warnings
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, NamedTuple

from log_ingest.bucket_building.bucket_container import BucketWindow, PendingBucketJail
from log_ingest.bucket_building.bucket_update_helpers import (
    is_duplicate_event,
    store_template_instance,
    update_bucket_component_counts,
    update_bucket_with_event,
)
from log_ingest.bucket_building.constants import BUCKET_ID_FORMAT, LATENCY_FIELD_NAMES
from log_ingest.bucket_building.file_helpers import read_file_lines_efficiently
from log_ingest.bucket_building.file_queue import FileQueue
from log_ingest.bucket_building.incremental_parquet_writer import (
    IncrementalParquetWriter,
)
from log_ingest.bucket_building.scrambled_event_buffer import ScrambledEventBuffer
from log_ingest.bucket_building.time_bucket import (
    TimeBucket,
    define_bucket_parquet_schema,
)
from log_ingest.bucket_building.time_range_scanner import TimeRangeScanner
from log_ingest.bucket_building.timestamp_extraction import (
    extract_first_timestamp,
    extract_last_timestamp,
)
from log_ingest.bucket_building.timestamp_helpers import (
    align_timestamp_to_bucket_start,
    calculate_bucket_boundaries,
    calculate_window_count,
    generate_bucket_id,
)
from log_ingest.message_parser import DrainParser
from log_ingest.parse_latency import LatencyFormat, LatencyFormatDetector, create_parser
from log_ingest.system_info_parser import SystemInfoRecord, parse_file
from log_ingest.utils import make_json_serializable, parse_ts

logger = logging.getLogger(__name__)


class IncrementalComponents(NamedTuple):
    """Components needed for incremental processing."""

    parser: DrainParser
    file_queue: FileQueue
    parquet_writer: IncrementalParquetWriter
    jail: PendingBucketJail
    buffer: ScrambledEventBuffer
    window_start: datetime
    window_count: int  # For progress logging only


class ProcessingCounters(NamedTuple):
    """Counters for tracking incremental processing progress."""

    total_events: int = 0
    total_buckets_written: int = 0
    window_index: int = 0


class WindowContext(NamedTuple):
    """Context for a single processing window."""

    start: datetime
    end: datetime
    window: BucketWindow


class BucketBuilder:
    """Builds time buckets from parsed data sources.

    Streaming-only mode: Single-pass processing with minimal memory usage.
    """

    def __init__(
        self,
        bucket_duration: timedelta = timedelta(minutes=5),
        spike_threshold_ms: float = 100.0,
        persistence_path: str | Path | None = None,
    ):
        """Initialize BucketBuilder with bucket size and spike threshold.

        Args:
            bucket_duration: Fixed duration for each time bucket
            spike_threshold_ms: Latency threshold in milliseconds for spike detection
            persistence_path: Path to Drain3 persistence file for template mining
        """
        if bucket_duration.total_seconds() <= 0:
            raise ValueError(f"bucket_duration must be positive, got {bucket_duration}")

        if spike_threshold_ms < 0:
            raise ValueError(f"spike_threshold_ms must be non-negative, got {spike_threshold_ms}")

        self.bucket_duration = bucket_duration
        self.spike_threshold_ms = spike_threshold_ms
        self.persistence_path = Path(persistence_path) if persistence_path else None
        self.buckets: dict[str, TimeBucket] = {}

        self.latency_records = []
        self.system_records = []

    def generate_bucket_id(self, timestamp: datetime) -> str:
        """Generate a bucket ID from timestamp."""
        return generate_bucket_id(timestamp, BUCKET_ID_FORMAT)

    def get_bucket_for_timestamp(self, ts: datetime) -> tuple[str, datetime, datetime]:
        """Get bucket ID and boundaries for a timestamp."""
        start, end = calculate_bucket_boundaries(ts, self.bucket_duration)
        bucket_id = self.generate_bucket_id(start)
        return bucket_id, start, end

    def _scan_timestamps_from_files(
        self, event_files: list[Path]
    ) -> tuple[datetime | None, datetime | None]:
        """Quick scan of files to determine time range without full parsing.

        Reads first and last valid log lines to extract timestamps.
        Returns (min_time, max_time) or (None, None) if no valid timestamps found.
        """
        min_time = None
        max_time = None
        parser = DrainParser()

        for file_path in event_files:
            file_min, file_max = self._scan_file_timestamp_range(file_path, parser)
            min_time = min(filter(None, [min_time, file_min]), default=None)
            max_time = max(filter(None, [max_time, file_max]), default=None)

        return min_time, max_time

    def _scan_file_timestamp_range(
        self, file_path: Path, parser: DrainParser
    ) -> tuple[datetime | None, datetime | None]:
        """Scan single file for min/max timestamps."""
        try:
            lines = read_file_lines_efficiently(file_path)
            if not lines:
                return None, None

            first_ts = extract_first_timestamp(lines[:10], parser)
            last_ts = extract_last_timestamp(lines[-10:], parser)
        except Exception as e:
            logger.warning(f"Error scanning timestamps from {file_path}: {e}")
            return None, None
        else:
            return first_ts, last_ts

    def load_events(self, event_files: list[Path]) -> None:
        """Load and parse event log files.

        .. deprecated::
            This method is deprecated. Use :meth:`process_incremental` instead,
            which provides streaming, bounded memory usage, and direct parquet output.

        Streams events directly into pre-created buckets.
        """
        warnings.warn(
            "load_events() is deprecated. Use process_incremental() instead for "
            "streaming, bounded memory usage, and direct parquet output.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._load_events_streaming(event_files)

    def _load_events_streaming(self, event_files: list[Path]) -> None:
        """Stream events directly into buckets without storing in memory.

        Streaming mode optimization: Events assigned directly to
        pre-created buckets without intermediate storage.
        """
        min_time, max_time = self._scan_timestamp_range(event_files)
        if not min_time or not max_time:
            logger.warning("No valid timestamps found in event files")
            return

        self._create_empty_buckets(min_time, max_time)
        logger.info(f"Pre-created {len(self.buckets)} buckets from {min_time} to {max_time}")

        total_events = self._stream_and_assign_events(event_files)
        logger.info(f"Streamed {total_events} events into buckets")

    def _scan_timestamp_range(
        self, event_files: list[Path]
    ) -> tuple[datetime | None, datetime | None]:
        """Scan files to determine time range for bucket creation."""
        logger.info("Scanning timestamps to determine time range...")
        return self._scan_timestamps_from_files(event_files)

    def _stream_and_assign_events(self, event_files: list[Path]) -> int:
        """Stream parse files and assign events to buckets."""
        parser = DrainParser(persistence_path=self.persistence_path, defer_persistence=True)
        total_events = 0

        for file_path in event_files:
            logger.info(f"Streaming events from {file_path}")
            total_events += self._process_file_stream(file_path, parser)

        # Save Drain3 state at the end
        parser.save_state()
        return total_events

    def _process_file_stream(self, file_path: Path, parser: DrainParser) -> int:
        """Process single file stream and assign events to buckets.

        Uses true streaming - events are parsed and assigned one at a time
        without accumulating entire file in memory.
        """
        event_count = 0

        # Use streaming parser instead of parse_file() which returns a list
        for i, event in enumerate(parser.parse_file_streaming(file_path)):
            event["event_id"] = f"{file_path.stem}_{i}"
            event["source_file"] = str(file_path)

            self._assign_event_to_bucket(event)
            event_count += 1

        return event_count

    def _assign_event_to_bucket(self, event: dict[str, Any]) -> None:
        """Assign single event to appropriate time bucket.

        Streaming mode optimization: Events assigned directly to
        pre-created buckets without intermediate storage.
        """
        ts = self._parse_ts_from_event(event)
        bucket_id, _, _ = self.get_bucket_for_timestamp(ts)

        if bucket_id in self.buckets:
            bucket = self.buckets[bucket_id]

            # Check for duplicate before incrementing counts
            if is_duplicate_event(bucket, event.get("template_id"), ts, event.get("params", {})):
                return  # Skip duplicate event

            update_bucket_with_event(bucket, event)

            component = event.get("component", "unknown")
            update_bucket_component_counts(bucket, component)

            if "template_id" in event:
                store_template_instance(
                    bucket,
                    event.get("template_id"),
                    event.get("template_str"),
                    event.get("params", {}),
                    timestamp=ts,
                )

    def _create_empty_buckets(self, min_time: datetime, max_time: datetime) -> None:
        """Pre-create empty buckets for the entire time range."""
        current_time = align_timestamp_to_bucket_start(min_time, self.bucket_duration)

        while current_time <= max_time:
            bucket_id = self.generate_bucket_id(current_time)
            self.buckets[bucket_id] = TimeBucket(
                bucket_id=bucket_id,
                start_time=current_time,
                end_time=current_time + self.bucket_duration,
            )
            current_time += self.bucket_duration

    def load_latency(self, latency_files: list[Path]) -> None:
        """Load and parse latency files with automatic format detection.

        Supports both table format and JSON format latency files.
        Format is detected by examining file content, not file extension.
        """
        for file_path in latency_files:
            logger.info(f"Loading latency from {file_path}")

            detector = LatencyFormatDetector()
            format = detector.detect(file_path)
            parser = create_parser(format)

            if format == LatencyFormat.JSON:
                records = list(parser.parse(file_path, str(file_path)))
            else:
                with file_path.open() as f:
                    records = list(parser.parse(f, str(file_path)))

            self.latency_records.extend(records)

        logger.info(f"Loaded {len(self.latency_records)} latency records")

    def load_system_info(self, system_files: list[Path]) -> None:
        """Load and parse system info files."""
        for file_path in system_files:
            logger.info(f"Loading system info from {file_path}")
            records = parse_file(file_path)
            self.system_records.extend(records)

        logger.info(f"Loaded {len(self.system_records)} system info records")

    def create_buckets(self, time_period: int = 5) -> None:
        """Create time buckets and assign data to them.

        .. deprecated::
            This method is deprecated. Use :meth:`process_incremental` instead,
            which provides streaming, bounded memory usage, and direct parquet output.

        Buckets already created and events assigned by load_events().
        Only need to assign latency and system info.
        """
        warnings.warn(
            "create_buckets() is deprecated. Use process_incremental() instead for "
            "streaming, bounded memory usage, and direct parquet output.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._create_buckets_streaming()

    def _create_buckets_streaming(self) -> None:
        """Finalize buckets in streaming mode.

        Events already assigned by load_events_streaming().
        Only need to assign latency and system info.
        """
        if not self.buckets:
            logger.warning("No buckets created - load_events() may not have been called")
            return

        logger.info(f"Finalizing {len(self.buckets)} pre-created buckets")

        # Assign latency and system info
        self._assign_latency_to_buckets()
        self._assign_system_info_to_buckets()

    def _parse_ts_from_event(self, event: dict[str, Any]) -> datetime:
        """Parse event timestamp to datetime object.

        Events from message_parser already have UTC-aware timestamps,
        but we still handle edge cases for robustness.
        """
        if not isinstance(event, dict):
            raise TypeError(f"Expected dict, got {type(event).__name__}")

        ts = event.get("ts")
        if ts is None:
            raise ValueError(f"Event missing timestamp: {event}")

        if isinstance(ts, datetime):
            # Should already be UTC-aware from message_parser
            return ts

        if isinstance(ts, str):
            # Shouldn't happen, but handle it anyway
            return parse_ts(ts)

        raise ValueError(f"Unexpected timestamp type: {type(ts)}")

    def _assign_latency_to_buckets(self) -> None:
        """Assign latency metrics to buckets (one measurement per bucket)."""
        for record in self.latency_records:
            bucket_id, _, _ = self.get_bucket_for_timestamp(record.ts)

            if bucket_id in self.buckets:
                bucket = self.buckets[bucket_id]

                for field in LATENCY_FIELD_NAMES:
                    if field in record.fields:
                        setattr(bucket, field, record.fields[field])

                # Simple spike detection based on threshold
                if bucket.write_latency_ms:
                    bucket.has_spike = bucket.write_latency_ms > self.spike_threshold_ms

    def _assign_system_info_to_buckets(self) -> None:
        """Forward-fill system info into buckets."""
        if not self.system_records:
            return

        sorted_records = sorted(self.system_records, key=lambda x: x.timestamp)

        for bucket_id in sorted(self.buckets.keys()):
            bucket = self.buckets[bucket_id]
            latest_record = self._find_latest_system_record(bucket, sorted_records)

            if latest_record:
                self._apply_system_record_to_bucket(bucket, latest_record)

    def _find_latest_system_record(
        self, bucket: TimeBucket, sorted_records: list
    ) -> SystemInfoRecord | None:
        """Find most recent system record before or during bucket."""
        latest_record = None
        for record in sorted_records:
            if record.timestamp <= bucket.end_time:
                latest_record = record
            else:
                break
        return latest_record

    def _apply_system_record_to_bucket(self, bucket: TimeBucket, record: SystemInfoRecord) -> None:
        """Apply system record fields to bucket."""
        bucket.cpu_util = record.cpu_util
        bucket.memory_util = record.memory_util
        bucket.disk_util = record.disk_util
        bucket.total_pools = record.total_pools
        bucket.pools_json = record.pools_json
        bucket.hdd_count = record.hdd_count
        bucket.ssd_count = record.ssd_count
        bucket.total_volumes = record.total_volumes
        bucket.volumes_json = record.volumes_json
        bucket.ha_state = record.ha_state
        bucket.is_leader = record.is_leader
        bucket.peer_available = record.peer_available
        bucket.degraded_volumes = record.degraded_volumes
        bucket.rebuild_active = record.rebuild_active

        if record.ip_addresses:
            bucket.ip_addresses_json = json.dumps(record.ip_addresses)

    def save_to_parquet(self, output_path: Path) -> int:
        """Save buckets directly to Parquet using PyArrow (no DataFrame intermediate).

        Args:
            output_path: Path to write parquet file

        Returns:
            Number of buckets saved

        Raises:
            ValueError: If output_path is None
            IOError: If write fails
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        if output_path is None:
            raise ValueError("output_path cannot be None")

        # Convert buckets to Parquet-compatible records
        bucket_records = [
            self._serialize_bucket_for_parquet(bucket)
            for bucket in sorted(self.buckets.values(), key=lambda b: b.start_time)
        ]

        if not bucket_records:
            logger.warning("No buckets to save")
            return 0

        # Define schema with proper types
        schema = define_bucket_parquet_schema()

        # Create PyArrow table and write
        table = pa.Table.from_pylist(bucket_records, schema=schema)
        pq.write_table(table, output_path)

        logger.info(f"Saved {len(bucket_records)} buckets to {output_path}")
        return len(bucket_records)

    def _serialize_bucket_for_parquet(self, bucket: TimeBucket) -> dict:
        """Convert bucket to Parquet-compatible dict with serialized templates.

        Args:
            bucket: TimeBucket to serialize

        Returns:
            Dictionary with all fields, templates as JSON string
        """
        record = asdict(bucket)
        record["templates"] = self._serialize_templates(
            record.get("templates"), record["bucket_id"]
        )
        return record

    def _serialize_templates(self, templates: dict | None, bucket_id: str) -> str | None:
        """Serialize templates dict to JSON for Parquet storage.

        Args:
            templates: Templates dictionary or None
            bucket_id: Bucket ID for logging

        Returns:
            JSON string if templates valid and non-empty, None otherwise
        """
        if not templates:
            return None

        if not isinstance(templates, dict):
            logger.warning(f"Bucket {bucket_id}: templates is {type(templates)}, expected dict")
            return None

        templates_serializable = make_json_serializable(templates)
        return json.dumps(templates_serializable)

    def save_drain3_snapshot(self, output_dir: str | Path = "out") -> Path | None:
        """Copy Drain3 persistence file to immutable snapshot.

        Creates a timestamped copy of the Drain3 state file in drain3_snapshots/
        subdirectory. This preserves the template mining state at bucket creation time.

        Args:
            output_dir: Base output directory (default: "out")

        Returns:
            Path to the snapshot file if created, None if no persistence configured
        """
        if output_dir is None:
            raise ValueError("output_dir cannot be None")

        if not self.persistence_path or not self.persistence_path.exists():
            logger.warning("No Drain3 persistence file found - skipping snapshot")
            return None

        snapshot_path = self._create_snapshot_path(output_dir)
        shutil.copy(self.persistence_path, snapshot_path)
        logger.info(f"Created immutable Drain3 snapshot: {snapshot_path}")

        return snapshot_path

    def _create_snapshot_path(self, output_dir: str | Path) -> Path:
        """Create snapshot directory and generate timestamped path."""
        snapshot_dir = Path(output_dir) / "drain3_snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_name = f"drain3_snapshot_{timestamp}.bin"

        return snapshot_dir / snapshot_name

    def _log_memory_usage(self) -> None:
        """Log estimated memory usage of loaded records."""
        import sys

        latency_size = sum(sys.getsizeof(r) for r in self.latency_records)
        system_size = sum(sys.getsizeof(r) for r in self.system_records)
        total_size = latency_size + system_size

        total_mb = total_size / (1024 * 1024)
        logger.info(f"Estimated memory usage: {total_mb:.2f} MB")

    def _initialize_incremental_components(
        self,
        event_files: list[Path],
        output_path: Path,
        processing_duration: timedelta,
    ) -> IncrementalComponents | None:
        """Initialize all components for incremental processing.

        Args:
            event_files: Event log files to process
            output_path: Output path for parquet file
            processing_duration: How much data to process in this run

        Returns:
            IncrementalComponents if successful, None if files are empty
        """
        # Use deferred persistence for performance - saves only at end
        parser = DrainParser(persistence_path=self.persistence_path, defer_persistence=True)
        self._main_parser = parser  # Keep reference for final save
        scanner = TimeRangeScanner()

        file_ranges = scanner.scan_files(event_files, parser)

        # Align window boundaries to bucket boundaries
        window_start = align_timestamp_to_bucket_start(file_ranges.start_time, self.bucket_duration)

        # Calculate bucket count for progress logging only (not used in processing logic)
        window_count = calculate_window_count(
            window_start, file_ranges.end_time, self.bucket_duration
        )

        # Initialize components
        file_queue = FileQueue(file_ranges)
        parquet_writer = IncrementalParquetWriter(output_path)
        jail = PendingBucketJail(self.bucket_duration)
        buffer = ScrambledEventBuffer(self.bucket_duration, parquet_writer)

        logger.info(
            f"Data range: {file_ranges.start_time} to {file_ranges.end_time}, "
            f"window size: {processing_duration}, "
            f"{window_count} buckets total"
        )

        return IncrementalComponents(
            parser=parser,
            file_queue=file_queue,
            parquet_writer=parquet_writer,
            jail=jail,
            buffer=buffer,
            window_start=window_start,
            window_count=window_count,
        )

    def _process_single_window_incremental(
        self,
        window_start: datetime,
        window_end: datetime,
        components: IncrementalComponents,
        counters: ProcessingCounters,
    ) -> ProcessingCounters:
        """Process a single window in incremental mode.

        Args:
            window_start: Start of current window
            window_end: End of current window
            components: Incremental processing components
            counters: Current processing counters

        Returns:
            Updated counters with events and buckets processed
        """
        # Create BucketWindow for current window
        window = BucketWindow(window_start, window_end, self.bucket_duration)

        # Promote eligible buckets from jail
        promoted = components.jail.promote_eligible(window_start, window_end)
        window.buckets.update(promoted)

        # Queue files for window
        components.file_queue.queue_files_for_window(window_end)

        # Process files and route events
        total_events, _ = self._process_files_for_window(
            window_start, window_end, window, components, counters.total_events
        )

        # Assign latency and system info to buckets
        window.assign_latency(self.latency_records, self.spike_threshold_ms)
        window.assign_system_info(self.system_records)

        # Get non-empty buckets and write to parquet
        buckets = window.get_non_empty_buckets()
        components.parquet_writer.append_buckets(buckets)
        total_buckets_written = counters.total_buckets_written + len(buckets)

        window_index = counters.window_index + 1
        logger.info(
            f"Processed window {window_index}/{components.window_count}: {len(buckets)} buckets"
        )

        return ProcessingCounters(
            total_events=total_events,
            total_buckets_written=total_buckets_written,
            window_index=window_index,
        )

    def _process_files_for_window(
        self,
        window_start: datetime,
        window_end: datetime,
        window: BucketWindow,
        components: IncrementalComponents,
        total_events: int,
    ) -> tuple[int, bool]:
        """Process files for current window, routing events appropriately.

        Args:
            window_start: Start of current window
            window_end: End of current window
            window: BucketWindow to add events to
            components: Incremental processing components
            total_events: Current total event count

        Returns:
            Tuple of (updated_total_events, window_bookmarked)
        """
        window_bookmarked = False
        context = WindowContext(start=window_start, end=window_end, window=window)

        while components.file_queue.has_files_in_queue() and not window_bookmarked:
            components.file_queue.open_next_file()

            while True:
                line = components.file_queue.read_next_line()

                # EOF reached
                if line is None:
                    components.file_queue.mark_current_file_complete()
                    components.buffer.flush()
                    break

                # Parse line to event
                event = components.parser.parse_line(line.strip())
                if not event or "ts" not in event:
                    continue

                # Parse timestamp and route event
                ts = self._parse_ts_from_event(event)
                total_events += 1

                window_bookmarked = self._route_event_by_timestamp(event, ts, context, components)

                if window_bookmarked:
                    break

        return total_events, window_bookmarked

    def _route_event_by_timestamp(
        self,
        event: dict[str, Any],
        ts: datetime,
        context: WindowContext,
        components: IncrementalComponents,
    ) -> bool:
        """Route event to window, jail, or buffer based on timestamp.

        Args:
            event: Event to route
            ts: Event timestamp
            context: Window context (start, end, window)
            components: Incremental processing components

        Returns:
            True if file should be bookmarked (event beyond window), False otherwise
        """
        if context.start <= ts < context.end:
            # Event in current window
            context.window.add_event(event)
            return False

        if ts >= context.end:
            # Event beyond window - jail it
            components.jail.add_event(event)

            # Monitor jail size at thresholds
            jail_size = components.jail.get_size()
            if jail_size in (1000, 10000, 100000, 1000000):
                logger.warning(f"Jail has {jail_size} buckets pending")

            components.file_queue.bookmark_current_file("event beyond window")
            return True

        # ts < context.start: Scrambled event (past) - buffer it
        components.buffer.add_event(event)
        return False

    def _log_incremental_results(
        self, components: IncrementalComponents, counters: ProcessingCounters
    ) -> None:
        """Log final statistics for incremental processing.

        Args:
            components: Incremental processing components
            counters: Final processing counters
        """
        jail_size = components.jail.get_size()
        scrambled_count = components.buffer.get_scrambled_count()

        logger.info(
            f"Pipeline complete: {counters.window_index} windows, "
            f"{counters.total_buckets_written} buckets written"
        )
        logger.info(f"Jail size at end: {jail_size} buckets")
        logger.info(f"Scrambled events: {scrambled_count}")

    def process_incremental(
        self,
        event_files: list[Path],
        latency_files: list[Path],
        system_files: list[Path],
        output_path: Path,
        processing_duration: timedelta = timedelta(hours=4),
    ) -> None:
        """Process event files incrementally with streaming components.

        Initializes all components needed for incremental processing:
        parser, scanner, file queue, parquet writer, jail, and buffer.

        Args:
            event_files: Event log files to process
            latency_files: Latency data files to load
            system_files: System info files to load
            output_path: Output path for parquet file
            processing_duration: How much data to process in this run (default 4 hours)
        """
        # Load supplemental data
        self.load_latency(latency_files)
        self.load_system_info(system_files)
        self._log_memory_usage()

        # Initialize incremental processing components
        components = self._initialize_incremental_components(
            event_files, output_path, processing_duration
        )
        if not components:
            return  # Empty files

        # Process all windows
        counters = ProcessingCounters()
        current_window_start = components.window_start

        try:
            while current_window_start <= components.file_queue.file_time_ranges.end_time:
                current_window_end = current_window_start + processing_duration
                counters = self._process_single_window_incremental(
                    current_window_start, current_window_end, components, counters
                )

                current_window_start = current_window_end

            # Check scrambled percentage at end. This is primarily a monitoring signal.
            # Some datasets can be heavily out-of-order but we still want to complete
            # bucket creation rather than abort late.
            try:
                components.buffer.check_scrambled_percentage(counters.total_events)
            except ValueError as exc:
                logger.warning(str(exc))

        except Exception:
            logger.exception(f"Error processing window {counters.window_index}")
            raise
        finally:
            # Log final statistics
            self._log_incremental_results(components, counters)

            # Save Drain3 state at the end (deferred persistence)
            if hasattr(self, "_main_parser") and self._main_parser:
                self._main_parser.save_state()
                logger.info(
                    f"Drain3 discovered {self._main_parser.get_template_count()} templates"
                )

    def _all_files_empty(self, file_paths: list[Path]) -> bool:
        """Check if all files in list are empty (size 0).

        Args:
            file_paths: List of file paths to check

        Returns:
            True if all files exist and are empty, False otherwise
        """
        return all(path.exists() and path.stat().st_size == 0 for path in file_paths)


# Example usage
if __name__ == "__main__":
    # Example of how to use the bucket builder
    builder = BucketBuilder(spike_threshold_ms=50.0)

    # Load data
    event_files = list(Path("data").glob("*NAS*"))
    latency_files = list(Path("data").glob("*latency*"))
    system_files = list(Path("data").glob("*Information*"))

    print("Loading data files:")
    print(f"  Event files: {len(event_files)}")
    print(f"  Latency files: {len(latency_files)}")
    print(f"  System files: {len(system_files)}")

    builder.load_events(event_files)
    builder.load_latency(latency_files)
    builder.load_system_info(system_files)

    # Create buckets
    builder.create_buckets()

    # Save to Parquet
    output_path = Path("out/buckets/buckets.parquet")
    bucket_count = builder.save_to_parquet(output_path)

    print(f"\nCreated {bucket_count} buckets")
    print(f"Saved to {output_path}")

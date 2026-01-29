"""ABOUTME: Base class for bucket management with shared functionality.

ABOUTME: Provides bucket identification and timestamp calculation methods.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from log_ingest.bucket_building.bucket_update_helpers import (
    is_duplicate_event,
    store_template_instance,
    update_bucket_component_counts,
    update_bucket_with_event,
)
from log_ingest.bucket_building.constants import BUCKET_ID_FORMAT, LATENCY_FIELD_NAMES
from log_ingest.bucket_building.time_bucket import TimeBucket
from log_ingest.bucket_building.timestamp_helpers import (
    align_timestamp_to_bucket_start,
    calculate_bucket_boundaries,
    generate_bucket_id,
)


class BucketContainer:
    """Base class for managing time buckets with shared functionality."""

    def __init__(self, bucket_duration: timedelta):
        """Initialize bucket container with specified duration."""
        self.bucket_duration = bucket_duration
        self.buckets: dict[str, TimeBucket] = {}

    def generate_bucket_id(self, timestamp: datetime) -> str:
        """Generate a bucket ID from timestamp."""
        return generate_bucket_id(timestamp, BUCKET_ID_FORMAT)

    def get_bucket_for_timestamp(self, ts: datetime) -> tuple[str, datetime, datetime]:
        """Get bucket ID and boundaries for a timestamp."""
        start, end = calculate_bucket_boundaries(ts, self.bucket_duration)
        bucket_id = self.generate_bucket_id(start)
        return bucket_id, start, end

    def add_event(self, event: dict[str, Any]) -> None:
        """Add event to appropriate bucket, creating bucket if needed."""
        ts = event["ts"]
        bucket_id, start, end = self.get_bucket_for_timestamp(ts)

        if bucket_id not in self.buckets:
            self.buckets[bucket_id] = TimeBucket(
                bucket_id=bucket_id, start_time=start, end_time=end
            )

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


class BucketWindow(BucketContainer):
    """Window-based bucket container with time boundaries.

    Pre-creates buckets for a specific time window and enforces boundaries.
    """

    def __init__(
        self,
        window_start: datetime,
        window_end: datetime,
        bucket_duration: timedelta,
    ):
        """Initialize bucket window with time boundaries.

        Args:
            window_start: Start of time window
            window_end: End of time window
            bucket_duration: Duration of each bucket
        """
        super().__init__(bucket_duration)
        self.window_start = window_start
        self.window_end = window_end

        # Pre-create empty buckets for window
        self._create_empty_buckets(window_start, window_end)

    def _create_empty_buckets(self, min_time: datetime, max_time: datetime) -> None:
        """Pre-create empty buckets for the entire time range.

        Args:
            min_time: Start of time range
            max_time: End of time range
        """
        current_time = align_timestamp_to_bucket_start(min_time, self.bucket_duration)

        while current_time < max_time:
            bucket_id = self.generate_bucket_id(current_time)
            self.buckets[bucket_id] = TimeBucket(
                bucket_id=bucket_id,
                start_time=current_time,
                end_time=current_time + self.bucket_duration,
            )
            current_time += self.bucket_duration

    def add_event(self, event: dict[str, Any]) -> None:
        """Add event to appropriate bucket with window boundary validation.

        Args:
            event: Event dictionary with 'ts' field

        Raises:
            AssertionError: If event timestamp is outside window boundaries
        """
        ts = event["ts"]

        # Validate timestamp is within window boundaries [window_start, window_end)
        assert (
            ts >= self.window_start
        ), f"Event timestamp {ts} is before window start {self.window_start}"
        assert (
            ts < self.window_end
        ), f"Event timestamp {ts} is at or after window end {self.window_end}"

        # Delegate to parent class for actual event processing
        super().add_event(event)

    def assign_latency(self, latency_records: list, spike_threshold_ms: float) -> None:
        """Assign latency metrics to buckets from provided records.

        Args:
            latency_records: List of MetricRecord objects with latency data
            spike_threshold_ms: Threshold for spike detection
        """
        for record in latency_records:
            bucket_id, _, _ = self.get_bucket_for_timestamp(record.ts)

            if bucket_id in self.buckets:
                bucket = self.buckets[bucket_id]

                for field in LATENCY_FIELD_NAMES:
                    if field in record.fields:
                        setattr(bucket, field, record.fields[field])

                # Simple spike detection based on threshold
                if bucket.write_latency_ms:
                    bucket.has_spike = bucket.write_latency_ms > spike_threshold_ms

    def assign_system_info(self, system_records: list) -> None:
        """Assign system info to buckets using forward-fill strategy.

        Args:
            system_records: List of SystemInfoRecord objects
        """
        if not system_records:
            return

        sorted_records = sorted(system_records, key=lambda x: x.timestamp)

        for bucket_id in sorted(self.buckets.keys()):
            bucket = self.buckets[bucket_id]
            latest_record = self._find_latest_system_record(bucket, sorted_records)

            if latest_record:
                self._apply_system_record_to_bucket(bucket, latest_record)

    def _find_latest_system_record(self, bucket: TimeBucket, sorted_records: list) -> object | None:
        """Find most recent system record before or during bucket.

        Args:
            bucket: Target bucket
            sorted_records: System records sorted by timestamp

        Returns:
            Latest applicable SystemInfoRecord or None
        """
        latest_record = None
        for record in sorted_records:
            if record.timestamp <= bucket.end_time:
                latest_record = record
            else:
                break
        return latest_record

    def _apply_system_record_to_bucket(self, bucket: TimeBucket, record: object) -> None:
        """Apply system record fields to bucket.

        Args:
            bucket: Target bucket
            record: SystemInfoRecord to apply
        """
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

    def get_non_empty_buckets(self) -> dict[str, TimeBucket]:
        """Filter and return buckets containing events.

        Returns:
            Dictionary of bucket_id -> TimeBucket for buckets with total_events > 0
        """
        logger = logging.getLogger(__name__)

        non_empty = {
            bucket_id: bucket
            for bucket_id, bucket in self.buckets.items()
            if bucket.total_events > 0
        }

        logger.info(f"Total buckets: {len(self.buckets)}, Non-empty buckets: {len(non_empty)}")

        return non_empty

    def clear(self) -> None:
        """Clear all buckets to prepare window for reuse."""
        self.buckets.clear()


class PendingBucketJail(BucketContainer):
    """Holds future events (ts >= window_end) with promotion support.

    Stores events that arrive ahead of the current processing window.
    Promotes eligible buckets when the window advances.
    """

    def __init__(self, bucket_duration: timedelta):
        """Initialize jail with specified bucket duration.

        Args:
            bucket_duration: Duration of each bucket
        """
        super().__init__(bucket_duration)
        self.warning_threshold = 1000

    def add_event(self, event: dict[str, Any]) -> None:
        """Add future event to jail, creating bucket if needed.

        Args:
            event: Event dictionary with 'ts' field
        """
        super().add_event(event)
        self.check_size_threshold()

    def promote_eligible(
        self, window_start: datetime, window_end: datetime
    ) -> dict[str, TimeBucket]:
        """Promote buckets that fall within specified window.

        Args:
            window_start: Start of window (inclusive)
            window_end: End of window (exclusive)

        Returns:
            Dict of promoted bucket_id -> TimeBucket
        """
        logger = logging.getLogger(__name__)

        promoted = {}
        for bucket_id, bucket in list(self.buckets.items()):
            if bucket.start_time >= window_start and bucket.start_time < window_end:
                promoted[bucket_id] = bucket
                del self.buckets[bucket_id]

        if promoted:
            logger.info(f"Promoted {len(promoted)} buckets from jail")

        return promoted

    def check_size_threshold(self) -> None:
        """Check jail size and log warnings at logarithmic thresholds."""
        logger = logging.getLogger(__name__)

        size = len(self.buckets)
        if size >= self.warning_threshold:
            logger.warning(f"WARNING: Jail has {size} buckets (indicates timestamp issues)")
            self.warning_threshold *= 10

    def get_size(self) -> int:
        """Return total bucket count in jail.

        Returns:
            Number of buckets currently in jail
        """
        return len(self.buckets)

    def is_empty(self) -> bool:
        """Check if jail has any buckets.

        Returns:
            True if jail has no buckets, False otherwise
        """
        return len(self.buckets) == 0

    def get_oldest_bucket_time(self) -> datetime | None:
        """Get earliest bucket start time in jail.

        Returns:
            Earliest bucket start_time or None if jail is empty
        """
        if not self.buckets:
            return None
        return min(bucket.start_time for bucket in self.buckets.values())

    def get_newest_bucket_time(self) -> datetime | None:
        """Get latest bucket start time in jail.

        Returns:
            Latest bucket start_time or None if jail is empty
        """
        if not self.buckets:
            return None
        return max(bucket.start_time for bucket in self.buckets.values())

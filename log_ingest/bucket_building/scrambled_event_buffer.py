"""ABOUTME: Buffer for out-of-order (scrambled) events with parquet flush support.

ABOUTME: Inherits from BucketContainer and tracks events arriving out of order.
"""

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from log_ingest.bucket_building.bucket_container import BucketContainer

if TYPE_CHECKING:
    from log_ingest.bucket_building.incremental_parquet_writer import IncrementalParquetWriter

logger = logging.getLogger(__name__)

# Threshold constants for scrambled event monitoring
SCRAMBLED_WARNING_THRESHOLD = 0.5  # Warn if > 0.5% scrambled
SCRAMBLED_ERROR_THRESHOLD = 10.0  # Error if > 10% scrambled


class ScrambledEventBuffer(BucketContainer):
    """Buffer for out-of-order events with parquet flush support.

    Accepts events where event.ts < window_start, batches them by bucket,
    and writes to parquet when flushed. Monitors scrambled event percentage.
    """

    def __init__(self, bucket_duration: timedelta, parquet_writer: "IncrementalParquetWriter"):
        """Initialize buffer with bucket duration and parquet writer.

        Args:
            bucket_duration: Duration of each time bucket
            parquet_writer: IncrementalParquetWriter instance for parquet operations
        """
        super().__init__(bucket_duration)
        self.parquet_writer = parquet_writer
        self.total_scrambled_events = 0

    def add_event(self, event: dict[str, Any]) -> None:
        """Add scrambled event to buffer, tracking count.

        Args:
            event: Event dictionary with 'ts' field
        """
        self.total_scrambled_events += 1
        super().add_event(event)

    def flush(self) -> None:
        """Flush all buckets to parquet and clear buffer."""
        if not self.buckets:
            return

        bucket_count = len(self.buckets)
        self.parquet_writer.append_buckets(self.buckets)
        self.buckets.clear()

        logger.info(f"Flushed {bucket_count} scrambled buckets to parquet")

    def check_scrambled_percentage(self, total_events: int) -> None:
        """Calculate and validate scrambled event percentage.

        Args:
            total_events: Total number of events processed

        Raises:
            ValueError: If scrambled percentage exceeds threshold
        """
        if total_events == 0:
            return

        percentage = (self.total_scrambled_events / total_events) * 100

        if percentage > SCRAMBLED_ERROR_THRESHOLD:
            msg = (
                f"Scrambled event percentage {percentage:.2f}% exceeds "
                f"{SCRAMBLED_ERROR_THRESHOLD}% threshold "
                f"({self.total_scrambled_events}/{total_events} events)"
            )
            raise ValueError(msg)

        if percentage > SCRAMBLED_WARNING_THRESHOLD:
            logger.warning(
                f"Scrambled event percentage {percentage:.2f}% exceeds "
                f"{SCRAMBLED_WARNING_THRESHOLD}% threshold "
                f"({self.total_scrambled_events}/{total_events} events)"
            )

    def get_scrambled_count(self) -> int:
        """Return total scrambled events tracked.

        Returns:
            Total number of scrambled events added to buffer
        """
        return self.total_scrambled_events

    def has_pending_buckets(self) -> bool:
        """Check if buffer has unflushed buckets.

        Returns:
            True if buckets exist in buffer, False otherwise
        """
        return len(self.buckets) > 0

    def get_bucket_count(self) -> int:
        """Return number of buckets in buffer.

        Returns:
            Number of buckets currently in buffer
        """
        return len(self.buckets)

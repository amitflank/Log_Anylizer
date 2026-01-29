"""ABOUTME: Pure functions for timestamp alignment and bucket calculations.

ABOUTME: Extracted from BucketBuilder and BucketContainer to eliminate duplication.
"""

from datetime import datetime, timedelta


def align_timestamp_to_bucket_start(timestamp: datetime, bucket_duration: timedelta) -> datetime:
    """Align timestamp to start of its bucket.

    Args:
        timestamp: Timestamp to align
        bucket_duration: Duration of each bucket

    Returns:
        Timestamp aligned to bucket start boundary
    """
    total_minutes = bucket_duration.seconds // 60
    minutes_offset = timestamp.minute % total_minutes
    bucket_start_minute = timestamp.minute - minutes_offset
    return timestamp.replace(minute=bucket_start_minute, second=0, microsecond=0)


def align_timestamp_to_bucket_end(timestamp: datetime, bucket_duration: timedelta) -> datetime:
    """Align timestamp to end of its bucket.

    Args:
        timestamp: Timestamp to align
        bucket_duration: Duration of each bucket

    Returns:
        Timestamp aligned to bucket end boundary
    """
    bucket_start = align_timestamp_to_bucket_start(timestamp, bucket_duration)
    return bucket_start + bucket_duration


def calculate_bucket_boundaries(
    timestamp: datetime, bucket_duration: timedelta
) -> tuple[datetime, datetime]:
    """Calculate bucket start and end times for a timestamp.

    Args:
        timestamp: Timestamp to find bucket for
        bucket_duration: Duration of each bucket

    Returns:
        Tuple of (bucket_start, bucket_end)
    """
    start = align_timestamp_to_bucket_start(timestamp, bucket_duration)
    end = start + bucket_duration
    return start, end


def generate_bucket_id(timestamp: datetime, id_format: str = "%Y%m%d_%H%M") -> str:
    """Generate a bucket ID from timestamp.

    Args:
        timestamp: Timestamp to generate ID for
        id_format: strftime format string for ID

    Returns:
        Bucket ID string
    """
    return f"b_{timestamp.strftime(id_format)}"


def calculate_window_count(
    start_time: datetime, end_time: datetime, bucket_duration: timedelta
) -> int:
    """Calculate number of bucket windows in time range.

    Args:
        start_time: Start of time range
        end_time: End of time range
        bucket_duration: Duration of each bucket

    Returns:
        Number of bucket windows needed
    """
    if start_time is None or end_time is None:
        return 0

    duration = end_time - start_time
    window_seconds = bucket_duration.total_seconds()
    return int(duration.total_seconds() / window_seconds) + 1

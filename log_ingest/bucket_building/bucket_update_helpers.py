"""ABOUTME: Helper functions for updating bucket metrics and counts.

ABOUTME: Extracted from BucketBuilder and BucketContainer to eliminate duplication.
"""

import json
from datetime import datetime
from typing import Any

from log_ingest.bucket_building.constants import (
    LOG_LEVEL_TO_ATTRIBUTE_MAP,
)
from log_ingest.bucket_building.time_bucket import TimeBucket


def update_bucket_with_event(bucket: TimeBucket, event: dict[str, Any]) -> None:
    """Update bucket metrics with a single event.

    Args:
        bucket: TimeBucket to update
        event: Event dictionary with 'level' field
    """
    bucket.total_events += 1

    level = event.get("level", "").lower()
    attr_name = LOG_LEVEL_TO_ATTRIBUTE_MAP.get(level)

    if attr_name:
        current_value = getattr(bucket, attr_name, 0)
        setattr(bucket, attr_name, current_value + 1)


def update_bucket_component_counts(bucket: TimeBucket, component: str) -> None:
    """Incrementally update component counts in bucket.

    Updates the JSON field directly without needing full event lookup.

    Args:
        bucket: TimeBucket to update
        component: Component name to increment
    """
    counts = json.loads(bucket.component_counts_json) if bucket.component_counts_json else {}
    counts[component] = counts.get(component, 0) + 1
    bucket.component_counts_json = json.dumps(counts)


def is_duplicate_event(
    bucket: TimeBucket, template_id: str | None, timestamp: datetime, params: dict[str, Any]
) -> bool:
    """Check if an event with same template_id, timestamp, and params already exists in bucket.

    Returns:
        True if duplicate found, False otherwise
    """
    if template_id is None:
        return False

    template_id_str = str(template_id)
    template_entry = bucket.templates.get(template_id_str)
    if template_entry is None:
        return False

    existing_instances = template_entry.get("instances", [])
    return _is_duplicate_instance(existing_instances, timestamp, params)


def _is_duplicate_instance(
    existing_instances: list[dict[str, Any]], timestamp: datetime, params: dict[str, Any]
) -> bool:
    """Check if a template instance with same timestamp + params already exists.

    Returns:
        True if duplicate found, False otherwise
    """
    for instance in existing_instances:
        instance_timestamp = instance.get("timestamp")
        # Compare timestamp and params (excluding timestamp from instance)
        instance_params = {k: v for k, v in instance.items() if k != "timestamp"}
        if instance_timestamp == timestamp and instance_params == params:
            return True
    return False


def store_template_instance(
    bucket: TimeBucket,
    template_id: str | None,
    template_str: str | None,
    params: dict[str, Any],
    timestamp: datetime,
) -> bool:
    """Store template instance with full parameter details and deduplication.

    Returns:
        True if instance was stored, False if duplicate was skipped
    """
    if not template_id:
        return False  # Skip if no template_id

    template_id_str = str(template_id)

    # Store full template details if available
    if template_str:
        # Initialize template entry if not exists
        if template_id_str not in bucket.templates:
            bucket.templates[template_id_str] = {"template_str": template_str, "instances": []}

        existing_instances = bucket.templates[template_id_str]["instances"]

        # Check for duplicate
        if _is_duplicate_instance(existing_instances, timestamp, params):
            return False  # Duplicate found, skip

        # Add timestamp to params before storing
        params_with_timestamp = params.copy()
        params_with_timestamp["timestamp"] = timestamp

        # Store parameter instance
        bucket.templates[template_id_str]["instances"].append(params_with_timestamp)
        return True

    return False

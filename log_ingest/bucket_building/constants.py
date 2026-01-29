"""constants.py.

ABOUTME: Shared constants for bucket building operations.
"""

# Bucket ID format for timestamp-based bucket naming
BUCKET_ID_FORMAT = "%Y%m%d_%H%M"

# Maximum number of top templates to track per bucket
MAX_TOP_TEMPLATES = 10

# Latency field names for metric assignment
LATENCY_FIELD_NAMES = [
    "read_latency_ms",
    "write_latency_ms",
    "read_iops",
    "write_iops",
    "read_throughput_bps",
    "write_throughput_bps",
]

# Log level to bucket attribute mapping
LOG_LEVEL_TO_ATTRIBUTE_MAP = {
    "error": "error_count",
    "warning": "warning_count",
    "information": "info_count",
    "info": "info_count",
    "verbose": "verbose_count",
    "debug": "debug_count",
}

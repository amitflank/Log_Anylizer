"""ABOUTME: Functions for extracting timestamps from log lines.

ABOUTME: Extracted from TimeRangeScanner and BucketBuilder to eliminate duplication.
"""

from datetime import datetime

from log_ingest.message_parser import DrainParser


def parse_line_timestamp(line: str, parser: DrainParser) -> datetime | None:
    """Parse timestamp from single line."""
    parsed = parser.parse_line(line.strip())
    if parsed and "ts" in parsed:
        return parsed["ts"]
    return None


def extract_first_timestamp(lines: list[str], parser: DrainParser) -> datetime | None:
    """Extract timestamp from first valid line"""
    for line in lines:
        return parse_line_timestamp(line, parser)


def extract_last_timestamp(lines: list[str], parser: DrainParser) -> datetime | None:
    """Extract timestamp from last valid line."""
    for line in reversed(lines):
        return parse_line_timestamp(line, parser)

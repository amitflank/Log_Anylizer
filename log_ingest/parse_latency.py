"""ABOUTME: Parsers for structured data latency tables.

ABOUTME: Supports box-drawn tables and JSON format performance statistics.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Iterable, Iterator
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from log_ingest.schemas import MetricRecord
from log_ingest.utils import parse_ms, parse_ts, to_bytes


class LatencyFormat(Enum):
    """Latency data format types."""

    JSON = "json"
    TABLE = "table"


class LatencyFormatDetector:
    """Detects latency file format by examining file content."""

    def detect(self, file_path: Path) -> LatencyFormat:
        """Detect format of latency file.

        Args:
            file_path: Path to latency file

        Returns:
            LatencyFormat.JSON if file starts with '{' or '['
            LatencyFormat.TABLE if file contains date line pattern
            LatencyFormat.TABLE as default for empty/unknown formats

        Raises:
            FileNotFoundError: If file does not exist
            PermissionError: If file cannot be read
            OSError: For other file access errors
        """
        with file_path.open("r", encoding="utf-8-sig") as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue

                if stripped.startswith("{") or stripped.startswith("["):
                    return LatencyFormat.JSON

                if LatencyTableParser.DATE_LINE_RE.match(stripped):
                    return LatencyFormat.TABLE

        return LatencyFormat.TABLE


def create_parser(format: LatencyFormat) -> LatencyTableParser | JsonLatencyParser:
    """Create parser instance for specified format.

    Args:
        format: LatencyFormat enum value specifying parser type

    Returns:
        New parser instance matching format type

    Raises:
        TypeError: If format is not a LatencyFormat enum value
    """
    if not isinstance(format, LatencyFormat):
        raise TypeError(f"format must be LatencyFormat enum, got {type(format).__name__}")

    if format == LatencyFormat.JSON:
        return JsonLatencyParser()
    return LatencyTableParser()


class LatencyTableParser:
    """Parser for box-drawn latency tables.

    Parses tables like:
    Time                  ║ Read  ║ Write  ║ Read     ║ Write    ║ Read Io  ║ Write    ║ Read       ║ Write
                          ║ Iops  ║ Iops   ║ Latency  ║ Latency  ║ Size     ║ Io Size  ║ Throughput ║ Throughput
    ══════════════════════╬═══════╬════════╬══════════╬══════════╬══════════╬══════════╬════════════╬════════════
    8/18/2025 10:46:49 AM ║ 0     ║ 49.2   ║ 0 ms     ║ 0.96 ms  ║ 0 MB     ║ 12.7 KB  ║ 0 MB       ║ 625.4 KB

    Also supports simplified space-separated format:
    Timestamp (UTC)          Read(ms)  Write(ms)  Read(IOPS)  Write(IOPS)  Read(MB/s)  Write(MB/s)
    2024-01-01 10:00:00      10.5      25.3       1000        500          100         50
    """

    # Regex to identify data lines (US format date or ISO-like date)
    DATE_LINE_RE = re.compile(
        r"^(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(AM|PM)|\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
    )
    MIN_PARTS = 7  # Minimum parts expected in data line (simplified format has 7 columns)
    BOX_FORMAT_COLUMNS = 8  # Box-drawn format has 8 columns
    SIMPLE_FORMAT_COLUMNS = 6  # Simplified format has 6 columns

    def __init__(self) -> None:
        """Initialize the parser."""
        self.errors = []

    def parse(self, lines: Iterable[str], source_file: str | None = None) -> Iterator[MetricRecord]:
        """Parse latency table lines into MetricRecords.

        Args:
            lines: Iterable of text lines
            source_file: Optional source file path for tracking

        Yields:
            MetricRecord objects with parsed metrics
        """
        for line_no, raw_line in enumerate(lines, 1):
            line = raw_line.strip("\n")

            # Skip empty lines
            if not line.strip():
                continue

            # Check if this is a data line (starts with date/time)
            if not self.DATE_LINE_RE.match(line):
                continue

            try:
                record = self._parse_data_line(line, source_file)
                if record:
                    yield record
            except Exception as e:
                self.errors.append({"line_no": line_no, "line": line[:100], "error": str(e)})

    def _parse_data_line(self, line: str, source_file: str | None) -> MetricRecord | None:
        """Parse a single data line into a MetricRecord."""
        # Split on box character or multiple spaces
        parts = [p.strip() for p in re.split(r"\s*║\s*|\s{2,}", line) if p.strip()]

        if len(parts) < self.MIN_PARTS:
            return None

        # Parse timestamp (first element)
        try:
            ts = parse_ts(parts[0])
        except Exception:
            return None

        # Build fields dictionary
        fields = self._extract_fields(parts[1:])

        if not fields:
            return None

        return MetricRecord(
            ts=ts, metric="latency_table_row", fields=fields, source_file=source_file
        )

    def _extract_fields(self, parts: list) -> dict[str, Any]:
        """Extract field values from parsed parts.

        Supports two formats:
        - Box format (8 columns): Read IOPS, Write IOPS, Read Latency, Write Latency, Read Size, Write Size, Read Throughput, Write Throughput
        - Simple format (6 columns): Read Latency, Write Latency, Read IOPS, Write IOPS, Read Throughput, Write Throughput
        """
        fields = {}

        if len(parts) >= self.BOX_FORMAT_COLUMNS:
            # Box-drawn format
            field_mappings = [
                ("read_iops", self._parse_float),
                ("write_iops", self._parse_float),
                ("read_latency_ms", self._parse_latency),
                ("write_latency_ms", self._parse_latency),
                ("read_size_bytes", self._parse_size),
                ("write_io_size_bytes", self._parse_size),
                ("read_throughput_Bps", self._parse_throughput),
                ("write_throughput_Bps", self._parse_throughput),
            ]
        elif len(parts) >= self.SIMPLE_FORMAT_COLUMNS:
            # Simplified space-separated format (column order from test fixture)
            field_mappings = [
                ("read_latency_ms", self._parse_float),
                ("write_latency_ms", self._parse_float),
                ("read_iops", self._parse_float),
                ("write_iops", self._parse_float),
                ("read_throughput_bps", self._parse_throughput_mbps),
                ("write_throughput_bps", self._parse_throughput_mbps),
            ]
        else:
            return {}

        for i, (field_name, parser_func) in enumerate(field_mappings):
            if i < len(parts):
                value = parser_func(parts[i])
                if value is not None:
                    fields[field_name] = value

        return fields

    def _parse_float(self, s: str) -> float | None:
        """Parse a float value, handling '0' as 0.0."""
        if not s or s == "0":
            return 0.0

        # Remove any trailing units if present
        s = re.sub(r"[A-Za-z/]+$", "", s).strip()

        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    def _parse_latency(self, s: str) -> float | None:
        """Parse latency value (expecting 'X ms' format)."""
        if not s:
            return None

        # Handle '0 ms' or '0' as 0.0
        if s in {"0", "0 ms"}:
            return 0.0

        return parse_ms(s)

    def _parse_size(self, s: str) -> int | None:
        """Parse size value with unit (e.g., '12.7 KB', '0 MB')."""
        if not s or s == "0":
            return 0

        # Handle '0 MB' or similar
        if s.startswith("0 "):
            return 0

        value, unit = self._split_value_unit(s)
        if value is not None and unit:
            return to_bytes(value, unit)

        return None

    def _parse_throughput(self, s: str) -> int | None:
        """Parse throughput value (e.g., '625.4 KB/s')."""
        if not s or s == "0":
            return 0

        # Handle '0 MB' or similar
        if s.startswith("0 "):
            return 0

        # Remove /s or /S suffix if present
        s = re.sub(r"/[sS]$", "", s).strip()

        value, unit = self._split_value_unit(s)
        if value is not None and unit:
            return to_bytes(value, unit)

        return None

    def _parse_throughput_mbps(self, s: str) -> int | None:
        """Parse throughput value in MB/s (simplified format).

        Converts MB/s to bytes/s for consistency with other parsers.
        """
        if not s or s == "0":
            return 0

        try:
            mb_per_sec = float(s)
            return int(mb_per_sec * 1024 * 1024)
        except (ValueError, TypeError):
            return None

    def _split_value_unit(self, s: str) -> tuple[float | None, str | None]:
        """Split a string into numeric value and unit."""
        match = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?\s*$", s)
        if not match:
            return None, None

        value = float(match.group(1))
        unit = match.group(2) or ""
        return value, unit


class JsonLatencyParser:
    """Parser for JSON format performance statistics.

    Parses JSON with PerformanceStatstics array containing latency metrics.
    Handles dict, JSON string, or Path input types.
    """

    def __init__(self) -> None:
        """Initialize the parser."""
        self.errors = []

    def parse(
        self, json_input: dict | str | Path, source_file: str | None = None
    ) -> Iterator[MetricRecord]:
        """Parse JSON latency data into MetricRecords.

        Args:
            json_input: Dict, JSON string, or Path to JSON file
            source_file: Optional source file path for tracking

        Yields:
            MetricRecord objects with parsed metrics
        """
        self.errors = []

        data = self._load_json(json_input)
        if data is None:
            return

        records = self._extract_records(data)
        if records is None:
            return

        for record in records:
            metric_record = self._parse_record(record, source_file)
            if metric_record:
                yield metric_record

    def _load_json(self, json_input: dict | str | Path) -> dict | None:
        """Load JSON input into dict."""
        if isinstance(json_input, dict):
            return copy.deepcopy(json_input)

        if isinstance(json_input, Path):
            return self._load_json_file(json_input)

        return self._load_json_string(json_input)

    def _load_json_file(self, path: Path) -> dict | None:
        """Load JSON from file path."""
        try:
            content = path.read_text()
            return json.loads(content)
        except Exception as e:
            self.errors.append({"error": f"Failed to load file: {e}"})
            return None

    def _load_json_string(self, json_str: str) -> dict | None:
        """Load JSON from string."""
        try:
            return json.loads(json_str)
        except Exception as e:
            self.errors.append({"error": f"Invalid JSON: {e}"})
            return None

    def _extract_records(self, data: dict) -> list | None:
        """Extract PerformanceStatstics array from data."""
        if "PerformanceStatstics" not in data:
            self.errors.append({"error": "Missing PerformanceStatstics key"})
            return None

        records = data["PerformanceStatstics"]
        if records is None:
            return []

        return records

    def _parse_record(self, record: dict, source_file: str | None) -> MetricRecord | None:
        """Parse single record into MetricRecord."""
        ts = self._parse_timestamp(record)
        if ts is None:
            return None

        fields = self._extract_fields(record)
        return MetricRecord(
            ts=ts,
            metric="latency_json_row",
            fields=fields,
            source_file=source_file,
        )

    def _parse_timestamp(self, record: dict) -> datetime | None:
        """Parse ISO 8601 timestamp from record."""
        if "Time" not in record:
            return None

        try:
            return parse_ts(record["Time"])
        except Exception as e:
            self.errors.append({"error": f"Invalid timestamp: {e}"})
            return None

    def _extract_fields(self, record: dict) -> dict[str, float]:
        """Extract and map numeric fields from record."""
        field_map = {
            "ReadIops": "read_iops",
            "WriteIops": "write_iops",
            "ReadLatencyInMilliseconds": "read_latency_ms",
            "WriteLatencyInMilliseconds": "write_latency_ms",
            "ReadIoSizeInBytes": "read_size_bytes",
            "WriteIoSizeInBytes": "write_io_size_bytes",
            "ReadThroughputInBytes": "read_throughput_Bps",
            "WriteThroughputInBytes": "write_throughput_Bps",
        }

        fields = {}
        for json_key, metric_key in field_map.items():
            value = record.get(json_key)
            fields[metric_key] = 0.0 if value is None else float(value)

        return fields

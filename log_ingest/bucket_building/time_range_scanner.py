"""ABOUTME: TimeRangeScanner - scans files to extract timestamp ranges.

ABOUTME: Extracted from BucketBuilder to support incremental processing.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TextIO

from log_ingest.message_parser import DrainParser

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileTimeRange:
    """Represents time range and bookmark position for a single log file.

    Immutable dataclass used for tracking file metadata during incremental processing.

    Attributes:
        path: Path to the log file
        start_time: Timestamp of first event in file
        end_time: Timestamp of last event in file
        position: Current read position in file (bookmark for resuming), default 0
    """

    path: Path
    start_time: datetime
    end_time: datetime
    position: int = 0

    def __post_init__(self) -> None:
        """Validate field values after initialization."""
        if self.end_time < self.start_time:
            msg = f"end_time must be >= start_time, got {self.end_time} < {self.start_time}"
            raise ValueError(msg)
        if self.position < 0:
            msg = f"position must be >= 0, got {self.position}"
            raise ValueError(msg)

    def __str__(self) -> str:
        """Readable string representation for logging."""
        return (
            f"FileTimeRange({self.path.name}, "
            f"{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
            f" → {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"pos={self.position})"
        )


@dataclass(frozen=True)
class FileTimeRanges:
    """Collection of FileTimeRange objects, sorted by start_time.

    Immutable dataclass for managing multiple file time ranges.
    Provides aggregate time range queries and filtering.

    Attributes:
        file_ranges: List of FileTimeRange objects, sorted by start_time ascending
    """

    file_ranges: list[FileTimeRange] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Sort file_ranges by start_time after initialization."""
        sorted_ranges = sorted(self.file_ranges, key=lambda fr: fr.start_time)
        object.__setattr__(self, "file_ranges", sorted_ranges)

    @property
    def start_time(self) -> datetime | None:
        """Return earliest file start time, or None if empty."""
        if not self.file_ranges:
            return None
        return self.file_ranges[0].start_time

    @property
    def end_time(self) -> datetime | None:
        """Return latest file end time, or None if empty."""
        if not self.file_ranges:
            return None
        return max(fr.end_time for fr in self.file_ranges)

    def get_files_starting_before(self, cutoff: datetime) -> list[FileTimeRange]:
        """Return files with start_time strictly before cutoff.

        Args:
            cutoff: Timestamp threshold

        Returns:
            New list of FileTimeRange objects starting before cutoff
        """
        return [fr for fr in self.file_ranges if fr.start_time < cutoff]


class TimeRangeScanner:
    """Scans log files to extract timestamp ranges."""

    def scan_file_timestamp_range(
        self, file_path: Path, parser: DrainParser
    ) -> tuple[datetime | None, datetime | None]:
        """Scan single file for min/max timestamps."""
        try:
            lines = self.read_file_lines(file_path)
            if not lines:
                return None, None

            first_ts = self.extract_first_timestamp(lines[:10], parser)
            last_ts = self.extract_last_timestamp(lines[-10:], parser)
        except Exception as e:
            logger.warning(f"Error scanning timestamps from {file_path}: {e}")
            return None, None
        else:
            return first_ts, last_ts

    def read_file_lines(self, file_path: Path) -> list[str]:
        """Read only first and last lines efficiently for timestamp scanning.

        Only reads ~20 lines from start and ~20 from end instead of entire file.
        Reduces memory usage from GB to KB for large files.
        """
        with file_path.open(encoding="utf-8", errors="ignore") as f:
            first_lines = self._read_first_n_lines(f, 20)
            last_lines = self._read_last_lines_from_end(f, 4096)
        return first_lines + last_lines

    def _read_first_n_lines(self, file: TextIO, n: int) -> list[str]:
        """Read first n lines from file."""
        lines = []
        for _ in range(n):
            line = file.readline()
            if not line:
                break
            lines.append(line)
        return lines

    def _read_last_lines_from_end(self, file: TextIO, buffer_size: int) -> list[str]:
        """Read last ~buffer_size bytes from file and return as lines."""
        try:
            file.seek(0, 2)  # Go to end of file
            file_size = file.tell()
            seek_pos = max(0, file_size - buffer_size)
            file.seek(seek_pos)

            if seek_pos > 0:
                file.readline()  # Skip partial first line

            return file.readlines()
        except Exception as e:
            logger.debug(f"Could not read end of file: {e}")
            return []

    def extract_first_timestamp(self, lines: list[str], parser: DrainParser) -> datetime | None:
        """Extract timestamp from first valid line."""
        for line in lines:
            ts = self.parse_line_timestamp(line, parser)
            if ts:
                return ts
        return None

    def extract_last_timestamp(self, lines: list[str], parser: DrainParser) -> datetime | None:
        """Extract timestamp from last valid line."""
        for line in reversed(lines):
            ts = self.parse_line_timestamp(line, parser)
            if ts:
                return ts
        return None

    def parse_line_timestamp(self, line: str, parser: DrainParser) -> datetime | None:
        """Parse timestamp from single line."""
        parsed = parser.parse_line(line.strip())
        if parsed and "ts" in parsed:
            return parsed["ts"]
        return None

    def scan_files(self, file_paths: list[Path], parser: DrainParser) -> FileTimeRanges:
        """Scan multiple files for timestamp ranges.

        Args:
            file_paths: List of paths to scan
            parser: DrainParser for timestamp extraction

        Returns:
            FileTimeRanges with all successfully scanned files

        Raises:
            ValueError: If no files have valid timestamps
        """
        file_ranges = []

        for file_path in file_paths:
            file_range = self._build_file_range(file_path, parser)
            if file_range:
                file_ranges.append(file_range)

        if not file_ranges:
            msg = "No files have valid timestamps"
            raise ValueError(msg)

        result = FileTimeRanges(file_ranges=file_ranges)
        self._log_scan_results(result)
        return result

    def _build_file_range(self, file_path: Path, parser: DrainParser) -> FileTimeRange | None:
        """Build FileTimeRange for single file, handling clock skew."""
        start_ts, end_ts = self.scan_file_timestamp_range(file_path, parser)

        if start_ts is None or end_ts is None:
            logger.warning(f"Skipping {file_path}: no valid timestamps found")
            return None

        if end_ts < start_ts:
            logger.warning(
                f"Clock skew detected in {file_path}: "
                f"end_time ({end_ts}) < start_time ({start_ts})"
            )
            start_ts, end_ts = end_ts, start_ts

        return FileTimeRange(path=file_path, start_time=start_ts, end_time=end_ts, position=0)

    def _log_scan_results(self, result: FileTimeRanges) -> None:
        """Log summary of file scan results."""
        logger.info(
            f"Scanned {len(result.file_ranges)} files, time range: "
            f"{result.start_time} to {result.end_time}"
        )
        file_names = ", ".join(fr.path.name for fr in result.file_ranges)
        logger.info(f"File processing ordering: {file_names}")

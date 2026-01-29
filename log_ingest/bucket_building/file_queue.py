"""ABOUTME: FileQueue - manages queue of log files for incremental processing.

ABOUTME: Tracks file processing state across time windows with position bookmarks.
"""

import logging
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Optional

from log_ingest.bucket_building.time_range_scanner import FileTimeRange, FileTimeRanges

logger = logging.getLogger(__name__)


class FileQueue:
    """Manages queue of log files for incremental processing.

    Tracks which files need to be read for current time window and maintains
    read positions for resumable processing.

    Attributes:
        file_time_ranges: All available files with time ranges and positions
        current_queue: Files queued for current window
        processed_files: Files that have reached EOF
        current_file_handle: Open file being read (only one at a time)
        current_file_range: FileTimeRange metadata for current file
    """

    def __init__(self, file_time_ranges: FileTimeRanges) -> None:
        """Initialize FileQueue with file time ranges from scanner.

        Args:
            file_time_ranges: Collection of file ranges sorted by start_time
        """
        self.file_time_ranges = file_time_ranges
        self.current_queue: list[FileTimeRange] = []
        self.processed_files: set[Path] = set()
        self.current_file_handle: Optional[object] = None
        self.current_file_range: Optional[FileTimeRange] = None

    def queue_files_for_window(self, window_end: datetime) -> None:
        """Queue files for the current time window.

        Keeps all files already in queue and adds new files that should be
        processed within the window. Files are sorted with resumed files
        (position > 0) first, then by start_time.

        Args:
            window_end: End timestamp of the current window
        """
        existing_files = self.current_queue.copy()
        new_files = self._get_new_files_for_window(window_end)

        resumed_files = [fr for fr in existing_files if fr.position > 0]

        self.current_queue = self._sort_queue_files(existing_files, new_files)

        self._log_queue_operation(resumed_files, new_files)

    def _get_new_files_for_window(self, window_end: datetime) -> list[FileTimeRange]:
        """Find files that should be queued for this window."""
        queued_paths = {fr.path for fr in self.current_queue}

        return [
            fr
            for fr in self.file_time_ranges.file_ranges
            if fr.start_time < window_end
            and fr.path not in self.processed_files
            and fr.path not in queued_paths
        ]

    def _sort_queue_files(
        self, existing_files: list[FileTimeRange], new_files: list[FileTimeRange]
    ) -> list[FileTimeRange]:
        """Sort queue: resumed files first, then other files by start_time."""
        all_files = existing_files + new_files
        resumed = sorted([fr for fr in all_files if fr.position > 0], key=lambda fr: fr.start_time)
        fresh = sorted([fr for fr in all_files if fr.position == 0], key=lambda fr: fr.start_time)
        return resumed + fresh

    def _log_queue_operation(
        self, resumed_files: list[FileTimeRange], new_files: list[FileTimeRange]
    ) -> None:
        """Log information about queued and resumed files."""
        if new_files:
            logger.info(f"Queued {len(new_files)} new files for processing")

        if resumed_files:
            positions_info = ", ".join(
                f"{fr.path.name} at position {fr.position}" for fr in resumed_files
            )
            logger.info(f"Resuming {len(resumed_files)} files: {positions_info}")

    def open_next_file(self) -> bool:
        """Open next file from queue for reading.

        Returns False if queue is empty. Closes any currently open file
        before opening the next one.

        Returns:
            True if file was opened, False if queue is empty
        """
        if not self.current_queue:
            return False

        self._close_current_file()

        file_range = self.current_queue.pop(0)
        file_handle = open(file_range.path, encoding="utf-8", errors="ignore")
        file_handle.seek(file_range.position)

        self.current_file_handle = file_handle
        self.current_file_range = file_range

        return True

    def read_next_line(self) -> Optional[str]:
        """Read one line from currently open file.

        Returns:
            Line from file (including newline), or None at EOF

        Raises:
            RuntimeError: If no file is currently open
        """
        if self.current_file_handle is None:
            raise RuntimeError("No file is currently open")

        line = self.current_file_handle.readline()
        return line if line else None

    def _close_current_file(self) -> None:
        """Close currently open file if one exists."""
        if self.current_file_handle is not None:
            self.current_file_handle.close()

    def bookmark_current_file(self, reason: str) -> None:
        """Save current position and requeue file for resume.

        Creates new FileTimeRange with updated position, closes file handle,
        and adds file back to queue for processing in next window.

        Args:
            reason: Description of why file is being bookmarked

        Raises:
            RuntimeError: If no file is currently open
        """
        if self.current_file_handle is None:
            raise RuntimeError("No file is currently open")

        current_position = self.current_file_handle.tell()

        bookmarked_range = replace(self.current_file_range, position=current_position)

        logger.info(
            f"Bookmarking {bookmarked_range.path.name} at position " f"{current_position}: {reason}"
        )

        self._close_current_file()
        self.current_queue.append(bookmarked_range)

        self.current_file_handle = None
        self.current_file_range = None

    def mark_current_file_complete(self) -> None:
        """Mark current file as completely processed.

        Closes file handle and adds file to processed set.
        File will not be requeued in future windows.

        Raises:
            RuntimeError: If no file is currently open
        """
        if self.current_file_handle is None:
            raise RuntimeError("No file is currently open")

        bytes_processed = self.current_file_handle.tell()
        file_path = self.current_file_range.path

        logger.info(f"Completed {file_path.name}, processed {bytes_processed} bytes")

        self._close_current_file()
        self.processed_files.add(file_path)

        self.current_file_handle = None
        self.current_file_range = None

    def has_files_in_queue(self) -> bool:
        """Check if there are files in the queue to process."""
        return len(self.current_queue) > 0

    def get_queue_size(self) -> int:
        """Get number of files currently in queue."""
        return len(self.current_queue)

    def get_current_file(self) -> Optional[Path]:
        """Get path of currently open file, or None if no file open."""
        if self.current_file_range is None:
            return None
        return self.current_file_range.path

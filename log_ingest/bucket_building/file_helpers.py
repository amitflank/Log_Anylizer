"""ABOUTME: Pure functions for efficient file reading operations.

ABOUTME: Extracted from TimeRangeScanner and BucketBuilder to eliminate duplication.
"""

from pathlib import Path
from typing import TextIO


def read_file_lines_efficiently(
    file_path: Path, head_lines: int = 20, tail_buffer_size: int = 4096
) -> list[str]:
    """Read only first and last lines efficiently for timestamp scanning.

    Only reads ~20 lines from start and ~20 from end instead of entire file.
    Reduces memory usage from GB to KB for large files.

    Args:
        file_path: Path to file to read
        head_lines: Number of lines to read from start
        tail_buffer_size: Size of buffer to read from end (bytes)

    Returns:
        List of lines from start and end of file
    """
    with file_path.open(encoding="utf-8", errors="ignore") as f:
        first_lines = read_first_n_lines(f, head_lines)
        last_lines = read_last_lines_from_end(f, tail_buffer_size)
    return first_lines + last_lines


def read_first_n_lines(file: TextIO, n: int) -> list[str]:
    """Read first n lines from file.

    Args:
        file: Open file handle
        n: Number of lines to read

    Returns:
        List of up to n lines from start of file
    """
    lines = []
    for _ in range(n):
        line = file.readline()
        if not line:
            break
        lines.append(line)
    return lines


def read_last_lines_from_end(file: TextIO, buffer_size: int) -> list[str]:
    """Read last ~buffer_size bytes from file and return as lines.

    Args:
        file: Open file handle
        buffer_size: Size of buffer to read from end (bytes)

    Returns:
        List of lines from end of file
    """
    try:
        file.seek(0, 2)  # Go to end of file
        file_size = file.tell()
        seek_pos = max(0, file_size - buffer_size)
        file.seek(seek_pos)

        if seek_pos > 0:
            file.readline()  # Skip partial first line

        return file.readlines()
    except Exception:
        return []

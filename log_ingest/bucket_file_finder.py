"""bucket_file_finder.py

File Discovery Engine for bucket creation CLI.
Discovers and categorizes log files into event/latency/system buckets.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from log_ingest.file_util import FileFinder

# Default patterns for file categorization
DEFAULT_EVENT_PATTERNS = ["*NAS*", "*PlatformClient*", "*PlatformManager*"]
DEFAULT_LATENCY_PATTERNS = ["*latency*"]
DEFAULT_SYSTEM_PATTERNS = ["*Information*"]


@dataclass
class DiscoveryResult:
    """Results from file discovery with categorized file lists.

    Attributes:
        event_files: Files matching event patterns
        latency_files: Files matching latency patterns
        system_files: Files matching system information patterns
        total_files: Total number of discovered files
        total_size_bytes: Total size of all discovered files in bytes
    """

    event_files: List[Path]
    latency_files: List[Path]
    system_files: List[Path]
    total_files: int
    total_size_bytes: int


class BucketFileFinder:
    """Smart file discovery for bucket creation with categorization.

    Extends FileFinder to support multiple pattern types and file categorization.
    Discovers files based on patterns and categorizes them into event/latency/system
    buckets for structured bucket creation workflows.
    """

    def __init__(self, root: Path) -> None:
        """Initialize file finder with root directory.

        Args:
            root: Root directory to search for files
        """
        self.root = Path(root)
        self.finder = FileFinder(root)

    def discover(
        self,
        patterns: Optional[Dict[str, List[str]]] = None,
        extensions: Optional[List[str]] = None,
        file_limit: Optional[int] = None,
    ) -> DiscoveryResult:
        """Discover and categorize files based on patterns.

        Searches for files matching patterns and categorizes them into
        event/latency/system buckets. Enforces file limits and validates results.

        Args:
            patterns: Dictionary mapping category names to pattern lists.
                     Keys: 'events', 'latency', 'system'
                     Values: List of glob-style patterns (e.g., ['*NAS*', '*SCSI*'])
                     If None, uses default patterns.
            extensions: List of allowed file extensions (e.g., ['.txt', '.log']).
                       If None, all extensions are allowed.
            file_limit: Maximum total files to discover. If exceeded, raises ValueError.
                       If None, no limit is enforced.

        Returns:
            DiscoveryResult with categorized file lists and summary statistics

        Raises:
            ValueError: If root directory doesn't exist
            ValueError: If no patterns provided and no defaults available
            ValueError: If total discovered files exceed file_limit
            ValueError: If no files found matching any patterns
        """
        self._validate_root_directory()

        patterns_dict = patterns or self._get_default_patterns()

        # Discover files for each category
        event_files = self._discover_category(patterns_dict.get("events", []), extensions)
        latency_files = self._discover_category(patterns_dict.get("latency", []), extensions)
        system_files = self._discover_category(patterns_dict.get("system", []), extensions)

        # Remove duplicates across categories
        all_files = self._deduplicate_files(event_files, latency_files, system_files)

        # Validate discovery results
        self._validate_discovery_results(all_files, patterns_dict, extensions, file_limit)

        # Build and return result
        return self._build_discovery_result(event_files, latency_files, system_files, all_files)

    def _validate_root_directory(self) -> None:
        """Validate root directory exists and is accessible.

        Raises:
            ValueError: If root doesn't exist or is not a directory
        """
        if not self.root.exists():
            raise ValueError(f"Root directory does not exist: {self.root}")

        if not self.root.is_dir():
            raise ValueError(f"Root path is not a directory: {self.root}")

    def _validate_discovery_results(
        self,
        all_files: List[Path],
        patterns_dict: Dict[str, List[str]],
        extensions: Optional[List[str]],
        file_limit: Optional[int],
    ) -> None:
        """Validate discovery results meet requirements.

        Args:
            all_files: List of all discovered files
            patterns_dict: Patterns used for discovery
            extensions: Extension filters used
            file_limit: Maximum allowed files

        Raises:
            ValueError: If no files found or file limit exceeded
        """
        if not all_files:
            raise ValueError(
                f"No files found matching patterns in {self.root}\n"
                f"Event patterns: {patterns_dict.get('events', [])}\n"
                f"Latency patterns: {patterns_dict.get('latency', [])}\n"
                f"System patterns: {patterns_dict.get('system', [])}\n"
                f"Extensions: {extensions or 'all'}"
            )

        if file_limit is not None and len(all_files) > file_limit:
            raise ValueError(
                f"Discovered {len(all_files)} files, which exceeds limit of {file_limit}\n"
                f"To process all files, increase --file-limit to {len(all_files)}\n"
                f"Or use --force to override confirmation prompts"
            )

    def _build_discovery_result(
        self,
        event_files: List[Path],
        latency_files: List[Path],
        system_files: List[Path],
        all_files: List[Path],
    ) -> DiscoveryResult:
        """Build DiscoveryResult from discovered files.

        Args:
            event_files: Files matching event patterns
            latency_files: Files matching latency patterns
            system_files: Files matching system patterns
            all_files: All unique discovered files

        Returns:
            DiscoveryResult with categorized files and statistics
        """
        total_size = sum(self._get_file_size(f) for f in all_files)

        return DiscoveryResult(
            event_files=event_files,
            latency_files=latency_files,
            system_files=system_files,
            total_files=len(all_files),
            total_size_bytes=total_size,
        )

    def _get_default_patterns(self) -> Dict[str, List[str]]:
        """Get default patterns for file categorization."""
        return {
            "events": DEFAULT_EVENT_PATTERNS,
            "latency": DEFAULT_LATENCY_PATTERNS,
            "system": DEFAULT_SYSTEM_PATTERNS,
        }

    def _discover_category(
        self, patterns: List[str], extensions: Optional[List[str]]
    ) -> List[Path]:
        """Discover files matching any pattern in category.

        Args:
            patterns: List of patterns to search for (case-insensitive)
                     Glob-style patterns like "*NAS*" are converted to substring "NAS"
            extensions: Optional list of allowed extensions

        Returns:
            List of matching file paths (may contain duplicates across patterns)
        """
        if not patterns:
            return []

        # Convert glob-style patterns to keywords for FileFinder
        # "*NAS*" -> "NAS", "*latency*" -> "latency"
        keywords = self._convert_patterns_to_keywords(patterns)

        # Find files matching any pattern in the list
        matches = self.finder.find(keywords=keywords, exts=extensions, recursive=True, mode="any")

        return matches

    def _convert_patterns_to_keywords(self, patterns: List[str]) -> List[str]:
        """Convert glob-style patterns to keywords for substring matching.

        FileFinder uses substring matching, so we need to strip wildcard characters
        from glob-style patterns like "*NAS*" to get "NAS".

        Args:
            patterns: List of glob-style patterns (e.g., ["*NAS*", "*latency*"])

        Returns:
            List of keywords for substring matching (e.g., ["NAS", "latency"])
        """
        keywords = []
        for pattern in patterns:
            # Strip leading and trailing wildcards
            keyword = pattern.strip("*")
            if keyword:  # Only add non-empty keywords
                keywords.append(keyword)
        return keywords

    def _deduplicate_files(
        self, event_files: List[Path], latency_files: List[Path], system_files: List[Path]
    ) -> List[Path]:
        """Remove duplicates and return unique set of files.

        A file may match multiple category patterns. This function
        categorizes each file into the first matching category only,
        but returns the complete unique set across all categories.

        Args:
            event_files: Files matching event patterns
            latency_files: Files matching latency patterns
            system_files: Files matching system patterns

        Returns:
            Unique list of all discovered files
        """
        # Use set to track unique files
        seen = set()
        unique_files = []

        # Process in priority order: events, latency, system
        for file_list in [event_files, latency_files, system_files]:
            for path in file_list:
                if path not in seen:
                    seen.add(path)
                    unique_files.append(path)

        return unique_files

    def _get_file_size(self, path: Path) -> int:
        """Get file size in bytes, returning 0 if file doesn't exist.

        Args:
            path: Path to file

        Returns:
            File size in bytes, or 0 if file doesn't exist or is not accessible
        """
        try:
            return path.stat().st_size
        except (OSError, FileNotFoundError):
            return 0

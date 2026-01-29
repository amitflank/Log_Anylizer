"""bucket_file_tracker.py

Manifest-based file tracking for incremental bucket processing.
Tracks processed files to enable cache-aware re-runs and change detection.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

# ============================================================================
# Constants
# ============================================================================

MANIFEST_VERSION = "1.0"
MANIFEST_FILENAME = "processed_files.json"
MANIFEST_TMP_SUFFIX = ".tmp"


# ============================================================================
# Data Structures
# ============================================================================


@dataclass(frozen=True)
class FileMetadata:
    """Metadata for a single processed file.

    Attributes:
        category: File category (event/latency/system)
        size_bytes: File size in bytes
        mtime: Modification timestamp (ISO format with timezone)
        processed: Processing timestamp (ISO format with timezone)
    """

    category: Literal["event", "latency", "system"]
    size_bytes: int
    mtime: str
    processed: str


@dataclass(frozen=True)
class FileComparisonResult:
    """Results from comparing discovered files against manifest.

    Attributes:
        new_files: Files not in manifest (need processing)
        modified_files: Files with changed mtime (need reprocessing)
        cached_files: Files unchanged since last processing
        total_files: Total discovered files
    """

    new_files: List[Path]
    modified_files: List[Path]
    cached_files: List[Path]
    total_files: int

    def needs_processing(self) -> bool:
        """Check if any files need processing."""
        return len(self.new_files) > 0 or len(self.modified_files) > 0

    def report_summary(self) -> str:
        """Generate human-readable summary."""
        return (
            f"Found {len(self.new_files)} new files, "
            f"{len(self.cached_files)} already processed, "
            f"{len(self.modified_files)} modified"
        )


@dataclass
class ProcessedManifest:
    """Manifest of processed files with metadata and statistics.

    Attributes:
        version: Manifest format version
        created: Manifest creation timestamp
        last_updated: Last update timestamp
        bucket_file: Path to bucket output file
        files: Mapping of file paths to metadata
        total_files: Total number of tracked files
        total_size_bytes: Total size of tracked files
    """

    version: str
    created: str
    last_updated: str
    bucket_file: str
    files: Dict[str, FileMetadata]
    total_files: int
    total_size_bytes: int


# ============================================================================
# Public API
# ============================================================================


class ProcessedFileTracker:
    """Tracks processed files using JSON manifest for incremental processing.

    Maintains a manifest file listing all files used in bucket creation,
    enabling cache-aware re-runs that skip unchanged files and detect
    modifications via mtime comparison.
    """

    def __init__(self, output_dir: Path) -> None:
        """Initialize tracker with output directory.

        Args:
            output_dir: Directory containing bucket outputs and manifest
        """
        self.output_dir = Path(output_dir)
        self.manifest_path = self.output_dir / MANIFEST_FILENAME

    def load_manifest(self) -> Optional[ProcessedManifest]:
        """Load existing manifest or return None if not found/corrupt.

        Gracefully handles missing or malformed manifests by returning None,
        which causes all files to be treated as new.

        Returns:
            ProcessedManifest if valid manifest exists, None otherwise
        """
        if not self.manifest_path.exists():
            return None

        try:
            return self._parse_manifest_file(self.manifest_path)
        except (json.JSONDecodeError, KeyError, ValueError):
            # Corrupt manifest - treat as missing
            return None

    def check_files(self, discovered_files: Dict[str, List[Path]]) -> FileComparisonResult:
        """Compare discovered files against manifest.

        Determines which files are new, modified, or cached by comparing
        paths and modification times against the manifest.

        Args:
            discovered_files: Dict mapping categories to file lists
                             Keys: 'event', 'latency', 'system'
                             Values: List of Path objects

        Returns:
            FileComparisonResult with categorized files
        """
        manifest = self.load_manifest()

        # If no manifest, all files are new
        if manifest is None:
            all_files = self._flatten_file_dict(discovered_files)
            return FileComparisonResult(
                new_files=all_files, modified_files=[], cached_files=[], total_files=len(all_files)
            )

        return self._compare_against_manifest(discovered_files, manifest)

    def update_manifest(self, processed_files: Dict[str, List[Path]], bucket_file: Path) -> None:
        """Atomically update manifest after processing files.

        Writes to temporary file first, then renames to avoid corruption
        if process is interrupted.

        Args:
            processed_files: Dict mapping categories to processed file lists
            bucket_file: Path to generated bucket output file
        """
        self._ensure_output_dir_exists()

        manifest = self._build_manifest(processed_files, bucket_file)

        # Atomic update: write to temp file, then rename
        temp_path = self.manifest_path.with_suffix(self.manifest_path.suffix + MANIFEST_TMP_SUFFIX)

        self._write_manifest_file(manifest, temp_path)
        temp_path.replace(self.manifest_path)

    # ============================================================================
    # Internal Helpers
    # ============================================================================

    def _parse_manifest_file(self, path: Path) -> ProcessedManifest:
        """Parse manifest JSON file into ProcessedManifest object.

        Args:
            path: Path to manifest file

        Returns:
            Parsed ProcessedManifest

        Raises:
            json.JSONDecodeError: If file contains invalid JSON
            KeyError: If required fields missing
            ValueError: If field values invalid
        """
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        files = self._parse_file_metadata_dict(data["files"])

        return ProcessedManifest(
            version=data["version"],
            created=data["created"],
            last_updated=data["last_updated"],
            bucket_file=data["bucket_file"],
            files=files,
            total_files=data["total_files"],
            total_size_bytes=data["total_size_bytes"],
        )

    def _parse_file_metadata_dict(self, files_dict: Dict[str, Dict]) -> Dict[str, FileMetadata]:
        """Parse dictionary of file metadata into FileMetadata objects.

        Args:
            files_dict: Dict mapping file paths to metadata dicts

        Returns:
            Dict mapping file paths to FileMetadata objects
        """
        files = {}
        for path_str, metadata in files_dict.items():
            files[path_str] = FileMetadata(
                category=metadata["category"],
                size_bytes=metadata["size_bytes"],
                mtime=metadata["mtime"],
                processed=metadata["processed"],
            )
        return files

    def _write_manifest_file(self, manifest: ProcessedManifest, path: Path) -> None:
        """Write manifest to JSON file with formatting.

        Args:
            manifest: Manifest object to serialize
            path: Destination file path
        """
        # Convert to dictionary for JSON serialization
        manifest_dict = {
            "version": manifest.version,
            "created": manifest.created,
            "last_updated": manifest.last_updated,
            "bucket_file": manifest.bucket_file,
            "files": {path: asdict(metadata) for path, metadata in manifest.files.items()},
            "total_files": manifest.total_files,
            "total_size_bytes": manifest.total_size_bytes,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest_dict, f, indent=2, ensure_ascii=False)

    def _compare_against_manifest(
        self, discovered_files: Dict[str, List[Path]], manifest: ProcessedManifest
    ) -> FileComparisonResult:
        """Compare discovered files against existing manifest.

        Args:
            discovered_files: Dict of category -> file lists
            manifest: Existing manifest with processed files

        Returns:
            FileComparisonResult categorizing files
        """
        new_files: List[Path] = []
        modified_files: List[Path] = []
        cached_files: List[Path] = []

        for category, files in discovered_files.items():
            for file_path in files:
                self._categorize_file(file_path, manifest, new_files, modified_files, cached_files)

        all_files = new_files + modified_files + cached_files

        return FileComparisonResult(
            new_files=new_files,
            modified_files=modified_files,
            cached_files=cached_files,
            total_files=len(all_files),
        )

    def _categorize_file(
        self,
        file_path: Path,
        manifest: ProcessedManifest,
        new_files: List[Path],
        modified_files: List[Path],
        cached_files: List[Path],
    ) -> None:
        """Categorize single file as new, modified, or cached.

        Args:
            file_path: File to categorize
            manifest: Existing manifest
            new_files: List to append new files to (modified in-place)
            modified_files: List to append modified files to (modified in-place)
            cached_files: List to append cached files to (modified in-place)
        """
        path_str = str(file_path)

        if path_str not in manifest.files:
            new_files.append(file_path)
        elif self._is_file_modified(file_path, manifest.files[path_str]):
            modified_files.append(file_path)
        else:
            cached_files.append(file_path)

    def _is_file_modified(self, path: Path, metadata: FileMetadata) -> bool:
        """Check if file has been modified since last processing.

        Args:
            path: File path to check
            metadata: Stored metadata from manifest

        Returns:
            True if file mtime differs from stored mtime
        """
        current_mtime = self._get_file_mtime_iso(path)
        return current_mtime != metadata.mtime

    def _build_manifest(
        self, processed_files: Dict[str, List[Path]], bucket_file: Path
    ) -> ProcessedManifest:
        """Build manifest from processed files.

        Args:
            processed_files: Dict of category -> processed file lists
            bucket_file: Path to bucket output file

        Returns:
            New ProcessedManifest object
        """
        now_iso = self._get_current_time_iso()

        # Preserve created timestamp if manifest exists
        existing = self.load_manifest()
        created_iso = existing.created if existing else now_iso

        # Collect file metadata
        files, total_size = self._collect_file_metadata(processed_files, now_iso)

        return ProcessedManifest(
            version=MANIFEST_VERSION,
            created=created_iso,
            last_updated=now_iso,
            bucket_file=str(bucket_file),
            files=files,
            total_files=len(files),
            total_size_bytes=total_size,
        )

    def _collect_file_metadata(
        self, processed_files: Dict[str, List[Path]], processed_time_iso: str
    ) -> Tuple[Dict[str, FileMetadata], int]:
        """Collect metadata for all processed files.

        Args:
            processed_files: Dict of category -> processed file lists
            processed_time_iso: ISO timestamp when files were processed

        Returns:
            Tuple of (files dict, total size in bytes)
        """
        files = {}
        total_size = 0

        for category, file_list in processed_files.items():
            for file_path in file_list:
                path_str = str(file_path)
                size_bytes = self._get_file_size(file_path)
                mtime_iso = self._get_file_mtime_iso(file_path)

                files[path_str] = FileMetadata(
                    category=category,
                    size_bytes=size_bytes,
                    mtime=mtime_iso,
                    processed=processed_time_iso,
                )
                total_size += size_bytes

        return files, total_size

    def _flatten_file_dict(self, file_dict: Dict[str, List[Path]]) -> List[Path]:
        """Flatten category -> files dict into single list.

        Args:
            file_dict: Dict mapping categories to file lists

        Returns:
            Flat list of all files
        """
        all_files: List[Path] = []
        for file_list in file_dict.values():
            all_files.extend(file_list)
        return all_files

    def _ensure_output_dir_exists(self) -> None:
        """Create output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_current_time_iso(self) -> str:
        """Get current time as ISO format string with UTC timezone."""
        return datetime.now(timezone.utc).isoformat()

    def _get_file_mtime_iso(self, path: Path) -> str:
        """Get file modification time as ISO format string.

        Args:
            path: File path

        Returns:
            ISO format timestamp string
        """
        mtime_timestamp = path.stat().st_mtime
        mtime_dt = datetime.fromtimestamp(mtime_timestamp, tz=timezone.utc)
        return mtime_dt.isoformat()

    def _get_file_size(self, path: Path) -> int:
        """Get file size in bytes.

        Args:
            path: File path

        Returns:
            File size in bytes
        """
        return path.stat().st_size

"""system_info_parser.py.

Parser for periodic system information sections from StorONE logs.
Extracts system metrics, pool information, HA status, network info, etc.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from log_ingest.utils import normalize_datetime, parse_ts

logger = logging.getLogger(__name__)


@dataclass
class SystemInfoRecord:
    """Structured record for periodic system information."""

    timestamp: datetime

    # CPU/Memory/Disk metrics
    cpu_util: float | None = None
    memory_util: float | None = None
    disk_util: float | None = None

    # Drive counts
    hdd_count: int | None = None
    ssd_count: int | None = None

    # Volume info
    total_volumes: int | None = None

    # HA status
    ha_state: str | None = None
    is_leader: bool | None = None
    peer_available: bool | None = None

    # Network
    ip_addresses: list[str] = field(default_factory=list)

    # Pool information (stored as JSON for flexibility)
    pools_json: str | None = None
    total_pools: int | None = None

    # Volume details (stored as JSON)
    volumes_json: str | None = None

    # Rebuild status
    degraded_volumes: int | None = None
    rebuild_active: bool | None = None

    # Source tracking
    source_file: str | None = None
    raw_text: str | None = None  # Keep raw for debugging


class PeriodicSectionParser:
    """Parser for periodic information sections."""

    # Constants
    MIN_VOLUME_PARTS = 2  # Minimum parts for volume detail line

    # Regex patterns
    TIMESTAMP_RE = re.compile(r"\[(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))\]")
    CPU_RE = re.compile(r"CPU\s+Utilization:\s+([\d.]+)%")
    MEMORY_RE = re.compile(r"Memory\s+Utilization:\s+([\d.]+)%")
    DISK_RE = re.compile(r"Disk\s+Utilization:\s+([\d.]+)%")
    DRIVES_RE = re.compile(r"(\d+)\s+(HDD|SSD)\s+drives")
    VOLUMES_RE = re.compile(r"Volumes\s*:\s*(\d+)")
    HA_STATE_RE = re.compile(r"HA\s+State\s*:\s*(\w+)")
    IS_LEADER_RE = re.compile(r"Is\s+node\s+leader\s*:\s*(True|False)", re.IGNORECASE)
    PEER_STATUS_RE = re.compile(
        r"Peer\s+node\s+status\s*:.*available\s*-\s*(True|False)", re.IGNORECASE
    )
    IP_ADDRESS_RE = re.compile(r"^\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*$")
    DEGRADED_RE = re.compile(r"(\d+)\s+degraded\s+volumes?")
    REBUILD_RE = re.compile(r"rebuild\s+occurring", re.IGNORECASE)

    def parse_section(self, text: str, source_file: str | None = None) -> SystemInfoRecord | None:
        """Parse a single periodic information section.

        Args:
            text: Raw text of the periodic section
            source_file: Optional source file path for tracking

        Returns:
            SystemInfoRecord or None if parsing fails
        """
        if "Periodic Information" not in text:
            return None

        record = SystemInfoRecord(
            timestamp=self._extract_timestamp(text),
            source_file=source_file,
            raw_text=text if logger.isEnabledFor(logging.DEBUG) else None,
        )

        # Extract metrics
        self._extract_utilization_metrics(text, record)
        self._extract_drive_info(text, record)
        self._extract_volume_info(text, record)
        self._extract_ha_info(text, record)
        self._extract_network_info(text, record)
        self._extract_pool_info(text, record)
        self._extract_rebuild_info(text, record)

        return record

    def _extract_timestamp(self, text: str) -> datetime:
        """Extract timestamp from section."""
        match = self.TIMESTAMP_RE.search(text)
        if match:
            from dateutil import parser

            dt = parser.parse(match.group(1))
            return normalize_datetime(dt)

        # Fallback to current time if no timestamp found
        logger.warning("No timestamp found in periodic section, using current time")
        return datetime.now(timezone.utc)

    def _extract_utilization_metrics(self, text: str, record: SystemInfoRecord) -> None:
        """Extract CPU, memory, disk utilization."""
        if match := self.CPU_RE.search(text):
            record.cpu_util = float(match.group(1))

        if match := self.MEMORY_RE.search(text):
            record.memory_util = float(match.group(1))

        if match := self.DISK_RE.search(text):
            record.disk_util = float(match.group(1))

    def _extract_drive_info(self, text: str, record: SystemInfoRecord) -> None:
        """Extract drive counts."""
        for match in self.DRIVES_RE.finditer(text):
            count = int(match.group(1))
            drive_type = match.group(2).upper()

            if drive_type == "HDD":
                record.hdd_count = count
            elif drive_type == "SSD":
                record.ssd_count = count

    def _extract_volume_info(self, text: str, record: SystemInfoRecord) -> None:
        """Extract volume information."""
        if match := self.VOLUMES_RE.search(text):
            record.total_volumes = int(match.group(1))

        # Extract detailed volume info if present
        volume_details = []
        volume_section = False

        for line in text.split("\n"):
            if (
                "Volumes" in line
                and "----" in text[max(0, text.index(line) - 50) : text.index(line) + 50]
            ):
                volume_section = True
                continue
            if volume_section and "----" in line:
                volume_section = False
                break
            if volume_section and "Volume" in line and ":" in line:
                # Parse volume detail lines
                parts = line.split(":")
                if len(parts) >= self.MIN_VOLUME_PARTS:
                    vol_info = {
                        "id": parts[0].strip().replace("Volume", "").strip(),
                        "details": parts[1].strip(),
                    }
                    volume_details.append(vol_info)

        if volume_details:
            record.volumes_json = json.dumps(volume_details)

    def _extract_ha_info(self, text: str, record: SystemInfoRecord) -> None:
        """Extract HA (High Availability) information."""
        if match := self.HA_STATE_RE.search(text):
            record.ha_state = match.group(1)

        if match := self.IS_LEADER_RE.search(text):
            record.is_leader = match.group(1).lower() == "true"

        if match := self.PEER_STATUS_RE.search(text):
            record.peer_available = match.group(1).lower() == "true"

    def _extract_network_info(self, text: str, record: SystemInfoRecord) -> None:
        """Extract network information (IP addresses)."""
        in_network_section = False

        for line in text.split("\n"):
            if (
                "Network" in line
                and "----" in text[max(0, text.index(line) - 50) : text.index(line) + 50]
            ):
                in_network_section = True
                continue
            if in_network_section and "----" in line:
                break
            if in_network_section and (match := self.IP_ADDRESS_RE.match(line)):
                record.ip_addresses.append(match.group(1))

    def _extract_pool_info(self, text: str, record: SystemInfoRecord) -> None:
        """Extract storage pool information."""
        pools = []

        # Look for "N Manual Pools:" pattern
        pool_count_match = re.search(r"(\d+)\s+(?:Manual\s+)?Pools?:", text)

        if pool_count_match:
            record.total_pools = int(pool_count_match.group(1))

        # Parse pool lines - handles both formats:
        # "SSD : 11 drives. 10 volumes. 0TB / 0TB"
        # "datapool : 9 drives. 6 volumes. 0.062277025792TB / 161.907650592768TB"
        for line in text.split("\n"):
            # Try to match pool line
            match = re.match(
                r"^\s*(\w+)\s*:\s*(\d+)\s+drives?\.\s*(\d+)\s+volumes?\.\s*"
                r"([\d.]+)\s*([A-Z]*)\s*/\s*([\d.]+)\s*([A-Z]*)",
                line,
            )

            if match:
                # Handle cases where units might be missing (shown as "0TB")
                used_value = float(match.group(4))
                used_unit = match.group(5) if match.group(5) else "TB"
                capacity_value = float(match.group(6))
                capacity_unit = match.group(7) if match.group(7) else "TB"

                pool = {
                    "name": match.group(1),
                    "drives": int(match.group(2)),
                    "volumes": int(match.group(3)),
                    "used_tb": self._convert_to_tb(used_value, used_unit),
                    "capacity_tb": self._convert_to_tb(capacity_value, capacity_unit),
                }
                # Calculate utilization percentage
                if pool["capacity_tb"] > 0:
                    pool["util_pct"] = (pool["used_tb"] / pool["capacity_tb"]) * 100
                else:
                    pool["util_pct"] = 0.0

                pools.append(pool)

        if pools:
            record.pools_json = json.dumps(pools)
            if not record.total_pools:
                record.total_pools = len(pools)

    def _extract_rebuild_info(self, text: str, record: SystemInfoRecord) -> None:
        """Extract rebuild/degraded volume information."""
        if match := self.DEGRADED_RE.search(text):
            record.degraded_volumes = int(match.group(1))
        # Check for "No degraded volumes"
        elif "No degraded volumes" in text:
            record.degraded_volumes = 0

        record.rebuild_active = bool(self.REBUILD_RE.search(text))

    def _convert_to_tb(self, value: float, unit: str) -> float:
        """Convert storage value to TB."""
        unit = unit.upper()
        conversions = {"B": 1e-12, "KB": 1e-9, "MB": 1e-6, "GB": 1e-3, "TB": 1.0, "PB": 1e3}
        return value * conversions.get(unit, 1.0)


def _extract_complete_sections(
    content: str, parser: PeriodicSectionParser, file_path: str
) -> list[SystemInfoRecord]:
    """Extract and parse complete periodic sections from content buffer.

    Returns parsed records and removes them from buffer.
    """
    records = []
    periodic_pattern = re.compile(
        r"={40,}\s*\n\s*\[([^\]]+)\]\s*Periodic Information\s*\n={40,}(.*?)(?=={40,}|\Z)",
        re.DOTALL | re.MULTILINE,
    )

    for match in periodic_pattern.finditer(content):
        timestamp_str = match.group(1)
        section_content = match.group(2)

        # Combine timestamp and content for parsing
        full_section = f"[{timestamp_str}] Periodic Information\n{section_content}"

        if record := parser.parse_section(full_section, file_path):
            records.append(record)

    return records


def parse_file(file_path: Path) -> list[SystemInfoRecord]:
    """Parse all periodic sections from a file.

    Args:
        file_path: Path to the log file

    Returns:
        List of SystemInfoRecord objects
    """
    parser = PeriodicSectionParser()
    records = []

    # Stream file in chunks to avoid loading entire file into memory
    chunk_size = 1024 * 1024  # 1 MB chunks
    content = ""

    with file_path.open(encoding="utf-8", errors="ignore") as f:
        while chunk := f.read(chunk_size):
            content += chunk

            # Keep only last 100KB when buffer gets too large (for overlapping patterns)
            if len(content) > chunk_size * 5:
                # Process completed sections before trimming
                records.extend(_extract_complete_sections(content, parser, str(file_path)))
                # Keep last 100KB for potential overlapping section
                content = content[-102400:]

    # Find all periodic information sections in final content
    periodic_pattern = re.compile(
        r"={40,}\s*\n\s*\[([^\]]+)\]\s*Periodic Information\s*\n={40,}(.*?)(?=={40,}|\Z)",
        re.DOTALL | re.MULTILINE,
    )

    for match in periodic_pattern.finditer(content):
        timestamp_str = match.group(1)
        section_content = match.group(2)

        # Combine timestamp and content for parsing
        full_section = f"[{timestamp_str}] Periodic Information\n{section_content}"

        if record := parser.parse_section(full_section, str(file_path)):
            records.append(record)

    # If no sections found with that pattern, try alternative parsing
    if not records:
        msg = f"No periodic sections found with standard pattern in {file_path}"
        logger.warning(f"{msg}, trying alternative parsing")

        # Try splitting just on "Periodic Information"
        if "Periodic Information" in content:
            sections = re.split(r"(?=\[.*?\]\s*Periodic Information)", content)

            for section in sections:
                if "Periodic Information" in section and (
                    record := parser.parse_section(section, str(file_path))
                ):
                    records.append(record)

        # Try parsing as JSON lines format (one JSON object per line)
        if not records and file_path.suffix == ".json":
            records = _parse_json_lines(file_path)

    return records


def _parse_json_lines(file_path: Path) -> list[SystemInfoRecord]:
    """Parse system info from JSON lines format.

    Each line contains a JSON object with SystemInfoRecord fields.
    """
    records = []

    with file_path.open(encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, 1):
            line_text = raw_line.strip()
            if not line_text:
                continue

            try:
                data = json.loads(line_text)
                record = _json_to_system_info_record(data, str(file_path))
                if record:
                    records.append(record)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON on line {line_no} in {file_path}: {e}")
            except Exception as e:
                logger.warning(f"Error processing line {line_no} in {file_path}: {e}")

    return records


def _json_to_system_info_record(data: dict, source_file: str) -> SystemInfoRecord | None:
    """Convert JSON dict to SystemInfoRecord."""
    try:
        # Parse timestamp
        timestamp_str = data.get("timestamp")
        if not timestamp_str:
            return None

        timestamp = parse_ts(timestamp_str) if isinstance(timestamp_str, str) else timestamp_str

        # Create record with all fields
        return SystemInfoRecord(
            timestamp=timestamp,
            cpu_util=data.get("cpu_util", 0.0),
            memory_util=data.get("memory_util", 0.0),
            disk_util=data.get("disk_util", 0.0),
            total_pools=data.get("total_pools", 0),
            pools_json=data.get("pools_json", "{}"),
            hdd_count=data.get("hdd_count", 0),
            ssd_count=data.get("ssd_count", 0),
            total_volumes=data.get("total_volumes", 0),
            volumes_json=data.get("volumes_json", "{}"),
            ha_state=data.get("ha_state", "Unknown"),
            is_leader=data.get("is_leader", False),
            peer_available=data.get("peer_available", False),
            ip_addresses=data.get("ip_addresses", []),
            degraded_volumes=data.get("degraded_volumes", 0),
            rebuild_active=data.get("rebuild_active", False),
            source_file=data.get("source_file", source_file),
        )
    except Exception as e:
        logger.warning(f"Failed to convert JSON to SystemInfoRecord: {e}")
        return None


def parse_directory(dir_path: Path, pattern: str = "*") -> list[SystemInfoRecord]:
    """Parse all matching files in a directory.

    Args:
        dir_path: Directory path
        pattern: Glob pattern for files (default: "*")

    Returns:
        Combined list of SystemInfoRecord objects
    """
    all_records = []

    for file_path in dir_path.glob(pattern):
        if file_path.is_file():
            try:
                records = parse_file(file_path)
                all_records.extend(records)
                logger.info(f"Parsed {len(records)} sections from {file_path.name}")
            except Exception:
                logger.exception(f"Failed to parse {file_path}")

    return all_records

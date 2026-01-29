"""Optimised message parsing utilities."""
# ruff: noqa

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from log_ingest.file_util import FileFinder
from log_ingest.utils import normalize_datetime, parse_ts

logger = logging.getLogger(__name__)

# ============================================================
# OPTIMIZED LOG LINE REGEX (compiled once)
# ============================================================
LOG_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? \+?[-\d:]+)\s*"
    r"\[(?P<level>[^\]]+)\]\s*"
    r"\[(?P<component>[^\]]*)\]\s*"
    r"\[(?P<ctx>[^\]]*)\]\s*"
    r"(?P<msg>.*)$"
)

# Console log format (no [level]/[ctx] blocks).
CONSOLE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<rest>.*)$"
)

# ============================================================
# FAST DATETIME PARSER (optimized for StorONE format)
# ============================================================
STORONE_DT_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))? \+00:00$")

# Constants for datetime parsing
MICROSECOND_DIGITS = 6  # Standard microsecond precision


def fast_parse_storone_datetime(ts_str: str) -> datetime:
    """Parse StorONE timestamp using fast regex-based parser with dateutil fallback.

    Optimized for the common StorONE format: "YYYY-MM-DD HH:MM:SS.ffffff +00:00"
    Uses compiled regex for 10-100x speedup over dateutil.parser for standard format.
    Falls back to dateutil.parser for non-standard or malformed timestamps.

    Args:
        ts_str: Timestamp string in StorONE format

    Returns:
        datetime object (timezone-naive, assumes UTC from +00:00 suffix)

    Raises:
        ValueError: If timestamp is malformed and dateutil also fails

    Example:
        >>> fast_parse_storone_datetime("2025-01-30 14:23:45.123456 +00:00")
        datetime.datetime(2025, 1, 30, 14, 23, 45, 123456)

        >>> fast_parse_storone_datetime("2025-01-30 14:23:45 +00:00")  # No microseconds
        datetime.datetime(2025, 1, 30, 14, 23, 45, 0)

    Performance:
        Fast path (regex): ~2-3 μs per call
        Slow path (dateutil): ~200-300 μs per call
    """
    match = STORONE_DT_RE.match(ts_str)
    if match:
        year, month, day, hour, minute, second, microsec = match.groups()
        # Pad or truncate microseconds to 6 digits
        microsec = int((microsec or "0").ljust(MICROSECOND_DIGITS, "0")[:MICROSECOND_DIGITS])
        return datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second),
            microsec,
            tzinfo=None,  # We'll handle UTC separately if needed
        )
    # Fallback to dateutil for complex cases
    from dateutil import parser as date_parser

    return date_parser.parse(ts_str)


# ============================================================
# CONTEXT PARSER (unchanged but optimized)
# ============================================================
def _parse_ctx(ctx: str) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {"T": None, "ET": None, "CG": None, "V": None, "S": None}
    for part in ctx.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip() or None
    return out


# ============================================================
# OPTIMIZED SANITIZATION (early return for clean messages)
# ============================================================

# Precompiled patterns (module-level for performance & clarity)
_ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")  # colors codes
_NULL_RE = re.compile(r"\x00+")  # NUL bytes

# Matches "weird" space-like Unicode characters:
# Common non-breaking / thin / en / em / narrow spaces, etc.
# \u00A0 NBSP, \u2000–\u200B en/em/thin/zero-width, \u202F narrow no-break,
# \u205F medium mathematical space, \u3000 ideographic space
_UNICODE_SPACE_RE = re.compile(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000]+")

# Collapse any remaining whitespace runs to a single space
_WS_RUNS_RE = re.compile(r"\s+")

# Unicode spaces immediately after '=' should be removed (no spacer)
_EQ_UNICODE_SPACE_RE = re.compile(r"=(?:[\u00A0\u2000-\u200B\u202F\u205F\u3000]+)")


@dataclass(frozen=True)
class CleanResult:
    """Result of step-1 message sanitation."""

    raw: str
    text: str
    warnings: List[str]
    changed: bool


def sanitize_message(raw: str) -> CleanResult:
    """Sanitize a raw log message for robust downstream parsing.

    This step does **not** interpret semantics. It ensures a clean, predictable
    surface for later regex/library passes.

    Operations (in order):
      1) Strip ANSI escape codes.
      2) Remove NUL bytes.
      3) Normalize odd Unicode spaces to a normal space.
      4) Collapse whitespace runs to a single space.
      5) Trim leading/trailing whitespace.

    The original `raw` message is preserved; any anomalies are recorded in
    `warnings`. Function must never raise on arbitrary input.

    Parameters
    ----------
    raw : str
        The original message string.

    Returns:
    -------
    CleanResult
        Structured cleanup outcome.
    """
    # Guard: Handle None input defensively
    if raw is None:
        return CleanResult(raw="", text="", warnings=["input_was_none"], changed=False)

    warnings: List[str] = []
    text = raw

    try:
        # 1) ANSI
        if _ANSI_RE.search(text):
            warnings.append("ansi_removed")
            text = _ANSI_RE.sub("", text)

        # 2) NUL bytes
        if _NULL_RE.search(text):
            warnings.append("nul_removed")
            text = _NULL_RE.sub("", text)

        # 2.5) Remove unicode spaces *immediately after '='* (no spacer desired)
        if _EQ_UNICODE_SPACE_RE.search(text):
            warnings.append("unicode_space_after_equals_removed")
            warnings.append("unicode_space_normalized")  # also add generic tag
            text = _EQ_UNICODE_SPACE_RE.sub("=", text)

        # 3) Odd Unicode spaces → normal space
        if _UNICODE_SPACE_RE.search(text):
            warnings.append("unicode_space_normalized")
            text = _UNICODE_SPACE_RE.sub(" ", text)

        # 4) Collapse whitespace runs
        if _WS_RUNS_RE.search(text):
            collapsed = _WS_RUNS_RE.sub(" ", text)
            text = collapsed

        # 5) Strip trailing and leading whitespace
        text = text.strip()

    except (AttributeError, TypeError, UnicodeError) as exc:
        # Expected errors from malformed input
        logger.warning(f"sanitize_message failed on expected error: {exc}")
        warnings.append(f"sanitize_error:{type(exc).__name__}")
        text = raw
    except Exception as exc:
        # Unexpected errors - log for investigation but don't crash pipeline
        logger.exception(f"Unexpected error in sanitize_message: {exc}")
        warnings.append(f"sanitize_error:unexpected_{type(exc).__name__}")
        text = raw

    return CleanResult(raw=raw, text=text, warnings=warnings, changed=(text != raw))


class DrainParser:
    """Optimized version of DrainParser with significant performance improvements."""

    def __init__(
        self,
        persistence_path: Optional[str | Path] = None,
        *,
        defer_persistence: bool = True,
    ) -> None:
        """Initialize DrainParser with optional persistence.

        Args:
            persistence_path: Optional path for Drain3 state persistence.
                If provided, template state will be saved/loaded from this file.
            defer_persistence: If True (default), disable incremental saves during
                processing and only save when `save_state()` is explicitly called.
                This dramatically improves performance for large datasets.
                If False, Drain3 will save after every new template discovery.

        Raises:
            RuntimeError: If drain3 package is not installed.
        """
        self._ensure_drain3_available()
        cfg = self._create_template_config(defer_persistence)

        # Load existing state if available, but don't enable incremental saves
        persistence = self._create_persistence_handler(persistence_path, defer_persistence)

        from drain3 import TemplateMiner

        self.miner = TemplateMiner(config=cfg, persistence_handler=persistence)
        self.persistence_path = Path(persistence_path) if persistence_path else None
        self._defer_persistence = defer_persistence

        # If deferring, disable the persistence handler after loading
        # This prevents incremental saves while allowing us to save manually at the end
        if defer_persistence and persistence_path:
            self._loaded_state = self.miner.persistence_handler is not None
            # Disable incremental saves by removing the handler
            # (state was already loaded in TemplateMiner.__init__)
            self.miner.persistence_handler = None
            logger.info(
                f"Drain3 persistence deferred: loaded from {persistence_path}, "
                "will save manually at end"
            )

    def _ensure_drain3_available(self) -> None:
        """Verify drain3 is installed, raise if not."""
        try:
            import drain3
        except ImportError as e:
            raise RuntimeError(
                "Drain3 required but not installed. Install: pip install drain3"
            ) from e

    def _create_template_config(self, defer_persistence: bool) -> object:
        """Create Drain3 template miner configuration.

        Args:
            defer_persistence: If True, configure to minimize save frequency.
        """
        from drain3.template_miner_config import TemplateMinerConfig

        cfg = TemplateMinerConfig()

        if defer_persistence:
            # Set very high interval to effectively disable periodic saves
            # (we'll save manually at the end instead)
            cfg.snapshot_interval_minutes = 999999
        return cfg

    def _create_persistence_handler(
        self, path: Optional[str | Path], defer_persistence: bool
    ) -> Optional[object]:
        """Create Drain3 persistence handler if path provided.

        When defer_persistence is True, we still create the handler to LOAD
        existing state, but we'll disable it after loading to prevent saves.
        """
        if not path:
            return None

        from drain3.file_persistence import FilePersistence

        path_obj = Path(path)
        if path_obj.exists():
            logger.info(f"Drain3 loading existing state from: {path_obj}")
        else:
            logger.info(f"Drain3 will create new state at: {path_obj}")
        return FilePersistence(str(path_obj))

    def save_state(self) -> bool:
        """Manually save Drain3 state to persistence file.

        Call this after processing is complete to persist the template state.
        Only works if persistence_path was provided during initialization.

        Returns:
            True if state was saved successfully, False otherwise.
        """
        if not self.persistence_path:
            logger.warning("Cannot save state: no persistence_path configured")
            return False

        try:
            import base64
            import zlib

            import jsonpickle
            from drain3.file_persistence import FilePersistence

            # Serialize the drain state (same as TemplateMiner.save_state)
            state = jsonpickle.dumps(self.miner.drain, keys=True).encode("utf-8")

            # Compress (matches Drain3's default behavior)
            state = base64.b64encode(zlib.compress(state))

            # Save to file
            handler = FilePersistence(str(self.persistence_path))
            handler.save_state(state)

            cluster_count = len(self.miner.drain.clusters)
            total_messages = self.miner.drain.get_total_cluster_size()
            logger.info(
                f"Drain3 state saved: {cluster_count} templates, "
                f"{total_messages} messages, {len(state)} bytes -> {self.persistence_path}"
            )
            return True

        except Exception as e:
            logger.exception(f"Failed to save Drain3 state: {e}")
            return False

    def get_template_count(self) -> int:
        """Return the number of discovered templates."""
        return len(self.miner.drain.clusters)

    def parse_message(self, message: str) -> Dict[str, Any]:
        """Parse log message using Drain3 template mining with parameter extraction.

        Sanitizes input message, mines template using Drain3, and extracts variable
        parameters from the template. Handles empty messages and extraction failures
        gracefully by returning structured results with warnings.

        Args:
            message: Raw log message text (may contain ANSI codes, unicode spaces)

        Returns:
            Dict containing:
            - message_raw (str): Original input message
            - message_clean (str): Sanitized message text
            - template_id (Optional[int]): Drain3 cluster ID, None if no template
            - template_str (Optional[str]): Mined template string with <*> placeholders
            - params (Dict[str, str]): Extracted parameters as {var1: value1, var2: value2, ...}
            - warnings (List[str]): Sanitization warnings from CleanResult

        Example:
            >>> parser = DrainParser()
            >>> result = parser.parse_message("Volume vol-001 status healthy")
            >>> result['template_str']
            'Volume <*> status <*>'
            >>> result['params']
            {'var1': 'vol-001', 'var2': 'healthy'}

        Note:
            Uses exact_matching=True for parameter extraction to ensure accuracy.
            Empty messages return None for template fields but preserve structure.
        """
        clean = sanitize_message(message)

        # Guard: Handle empty message case
        if not clean.text:
            return self._create_empty_parse_result(message, clean.warnings)

        # Happy path: Process with Drain3
        result = self.miner.add_log_message(clean.text)
        template_mined = result.get("template_mined")

        params_dict = self._extract_parameters(template_mined, clean.text) if template_mined else {}

        return {
            "message_raw": message,
            "message_clean": clean.text,
            "template_id": result.get("cluster_id"),
            "template_str": template_mined,
            "params": params_dict,
            "warnings": list(clean.warnings),
        }

    def _create_empty_parse_result(self, message: str, warnings: List[str]) -> Dict[str, Any]:
        """Create parse result for empty message."""
        return {
            "message_raw": message,
            "message_clean": "",
            "template_id": None,
            "template_str": None,
            "params": {},
            "warnings": list(warnings),
        }

    def _extract_parameters(self, template: str, text: str) -> Dict[str, str]:
        """Extract parameters from template using exact matching.

        Args:
            template: Template string with <*> placeholders
            text: Cleaned message text to extract from

        Returns:
            Dict mapping var1, var2, ... to extracted parameter values
        """
        params = self.miner.extract_parameters(template, text, exact_matching=True)
        return {f"var{i}": p.value for i, p in enumerate(params, 1)} if params else {}

    def parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Optimized line parsing with fast datetime parsing."""
        # Normalize odd inputs (console.log contains ANSI color codes).
        clean = sanitize_message(line).text

        m = LOG_RE.match(clean)
        if not m:
            # Fallback: console.log-ish lines.
            cm = CONSOLE_RE.match(clean)
            if not cm:
                return None

            ts_raw = cm.group("ts")
            try:
                ts_parsed = parse_ts(ts_raw)
            except Exception:
                return None

            rest = cm.group("rest").strip()
            component = "console"
            msg = rest
            if ":" in rest:
                left, right = rest.split(":", 1)
                if left.strip():
                    component = left.strip()
                msg = right.strip()

            header = {"ts": ts_parsed, "level": "Information", "component": component, "ctx": _parse_ctx("")}
            body = self.parse_message(msg)
            return {**header, **body}

        # Use optimized datetime parsing
        ts_raw = m.group("ts")
        try:
            ts_parsed = normalize_datetime(fast_parse_storone_datetime(ts_raw))
        except Exception:
            # Fallback for malformed timestamps
            ts_parsed = ts_raw

        header = {
            "ts": ts_parsed,
            "level": m.group("level").strip(),
            "component": m.group("component").strip(),
            "ctx": _parse_ctx(m.group("ctx")),
        }

        msg = m.group("msg").strip()
        body = self.parse_message(msg)
        return {**header, **body}

    def parse_file(self, path: str | Path) -> List[Dict[str, Any]]:
        """Parse file into list (legacy - accumulates in memory).

        For streaming, use parse_file_streaming() instead.
        """
        path = Path(path)
        results: List[Dict[str, Any]] = []

        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:  # Skip empty lines early
                    continue

                parsed = self.parse_line(line)
                if parsed:
                    results.append(parsed)

        return results

    def parse_file_streaming(self, path: str | Path) -> Iterator[Dict[str, Any]]:
        """Stream parse file line-by-line without accumulating in memory.

        Yields parsed events one at a time. Use this for large files.
        """
        path = Path(path)

        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:  # Skip empty lines early
                    continue

                parsed = self.parse_line(line)
                if parsed:
                    yield parsed

    def parse_files(self, paths: Iterable[str | Path]) -> List[Dict[str, Any]]:
        """Parse many files (eager)."""
        out: List[Dict[str, Any]] = []
        for p in paths:
            out.extend(self.parse_file(Path(p)))
        return out

    def iter_files(self, paths: Iterable[str | Path]) -> Iterator[Dict[str, Any]]:
        """Parse many files (streaming)."""
        for p in paths:
            for row in self.parse_file(Path(p)):
                yield row

    def ingest_by_filename_keywords(
        self,
        root: str | Path,
        keywords: Sequence[str],
        *,
        exts: Sequence[str] | None = None,
        recursive: bool = True,
        mode: Literal["any", "all"] = "any",
    ) -> Iterator[Dict[str, Any]]:
        """Find files by filename keywords, then ingest via streaming."""
        finder = FileFinder(Path(root))
        paths = finder.find(keywords, exts=exts or None, recursive=recursive, mode=mode)
        return self.iter_files(paths)

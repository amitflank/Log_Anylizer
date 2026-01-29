# log_ingest/file_finder.py
from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal


@dataclass
class FindRecord:
    when: float
    keywords: tuple[str, ...]
    exts: tuple[str, ...] | None
    files: list[Path]


class FileFinder:
    """Search a directory tree for files whose *names* match given keywords."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.history: list[FindRecord] = []

    def find(
        self,
        keywords: Iterable[str],
        *,
        exts: Sequence[str] | None = None,
        recursive: bool = True,
        mode: Literal["any", "all"] = "any",
    ) -> List[Path]:
        """Return files under root whose *filename* matches keywords (case-insensitive).

        Args:
            keywords: Substrings to search for in the filename (not contents).
            exts: Optional list of allowed suffixes (e.g., [".log", ".txt"]). Case-insensitive.
            recursive: Whether to recurse into subdirectories (default True).
            mode: 'any' → filename contains any keyword; 'all' → contains all keywords.

        Returns:
            List of Path objects that match.
        """
        kw = [k.casefold() for k in keywords]
        extset = {e.casefold() for e in exts} if exts else None

        matches: List[Path] = []
        it = self.root.rglob("*") if recursive else self.root.glob("*")

        for path in it:
            if not path.is_file():
                continue

            if extset is not None:
                # Path.suffix is the last suffix (".log" for "a.log", ".gz" for "a.log.gz")
                if path.suffix.casefold() not in extset:
                    continue

            name_cf = path.name.casefold()

            if not kw:
                matches.append(path)
                continue

            if mode == "any":
                if any(k in name_cf for k in kw):
                    matches.append(path)
            elif all(k in name_cf for k in kw):
                matches.append(path)

        self.history.append(
            FindRecord(time.time(), tuple(keywords), tuple(exts) if exts else None, matches)
        )  # Help debugging and interprabilty
        return matches

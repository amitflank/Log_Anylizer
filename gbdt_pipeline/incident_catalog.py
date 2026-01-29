"""Persistence helpers for incident target configurations."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(eq=True, frozen=True)
class IncidentTarget:
    """Describe a single prediction target."""

    name: str
    template_hashes: list[str]
    initial_lookback_minutes: int
    max_lookback_minutes: int
    filters: dict[str, Any]


class IncidentCatalog:
    """JSON-backed store for prediction targets."""

    def __init__(self, catalog_path: Path) -> None:
        """Initialise the catalog, loading existing entries if present."""
        self._path = Path(catalog_path)
        self._data: dict[str, Any] = {"targets": {}}
        if self._path.exists():
            loaded = json.loads(self._path.read_text())
            self._data["targets"] = loaded.get("targets", {})

    def list_targets(self) -> list[IncidentTarget]:
        """Return all registered targets."""
        return [self._decode(name, cfg) for name, cfg in self._data["targets"].items()]

    def get_target(self, name: str) -> IncidentTarget:
        """Fetch a specific target by name."""
        if name not in self._data["targets"]:
            raise KeyError(name)
        return self._decode(name, self._data["targets"][name])

    def add_or_update_target(self, target: IncidentTarget) -> None:
        """Insert or replace a target definition."""
        self._data["targets"][target.name] = asdict(target)

    def save(self) -> None:
        """Persist the catalog to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(self._data, handle, indent=2)

    @staticmethod
    def _decode(name: str, payload: dict[str, Any]) -> IncidentTarget:
        """Convert raw JSON payload to an IncidentTarget."""
        return IncidentTarget(
            name=name,
            template_hashes=list(payload.get("template_hashes", [])),
            initial_lookback_minutes=int(payload.get("initial_lookback_minutes", 15)),
            max_lookback_minutes=int(payload.get("max_lookback_minutes", 60)),
            filters=dict(payload.get("filters", {})),
        )

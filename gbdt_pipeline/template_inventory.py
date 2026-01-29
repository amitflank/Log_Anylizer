"""Aggregate metadata about templates and their variables."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from gbdt_pipeline.exclusion import TemplateExclusion
from gbdt_pipeline.window_extractor import LookbackWindow

MAX_VARIABLES_PER_TEMPLATE = 20


@dataclass
class TemplateInfo:
    """Summary information for a single template."""

    template_str: str | None
    total_instances: int
    variable_types: dict[str, str]
    top_values: dict[str, list[str]]


@dataclass
class TemplateSummary:
    """Mapping of template identifiers to their metadata."""

    templates: dict[str, TemplateInfo]


class TemplateInventory:
    """Collect statistics about templates observed in windows."""

    def __init__(self, exclusion: TemplateExclusion | None = None) -> None:
        """Initialise empty inventory."""
        self._templates: dict[str, dict[str, object]] = {}
        self._exclusion = exclusion

    def record_window(self, window: LookbackWindow) -> None:
        """Accumulate template occurrences from a window."""
        for bucket in window.buckets:
            templates = (bucket.templates or {}).items()
            for template_id, payload in templates:
                if self._exclusion and self._exclusion.matches(template_id):
                    continue
                self._update_template_entry(template_id, payload)

    def _update_template_entry(self, template_id: str, payload: dict[str, object]) -> None:
        entry = self._templates.setdefault(
            template_id,
            {
                "template_str": payload.get("template_str"),
                "count": 0,
                "vars": {},
            },
        )
        instances = payload.get("instances", [])
        entry["count"] = entry.get("count", 0) + (len(instances) or 1)
        for instance in instances:
            self._collect_instance_variables(entry["vars"], instance)

    @staticmethod
    def _collect_instance_variables(
        store: dict[str, list[object]],
        instance: dict[str, object],
    ) -> None:
        for key, value in instance.items():
            if key in {"timestamp", "ts"}:
                continue
            store.setdefault(key, []).append(value)

    def summarise(self) -> TemplateSummary:
        """Create a summary view of all recorded templates."""
        summary: dict[str, TemplateInfo] = {}
        for template_id, payload in self._templates.items():
            vars_payload = payload.get("vars", {})
            ranked_vars = sorted(
                vars_payload.items(), key=lambda item: len(item[1]), reverse=True
            )[:MAX_VARIABLES_PER_TEMPLATE]
            pruned_vars = dict(ranked_vars)
            var_types = {
                name: self._infer_type(values) for name, values in pruned_vars.items()
            }
            top_values = {
                name: self._top_categorical_values(values)
                for name, values in pruned_vars.items()
            }
            summary[template_id] = TemplateInfo(
                template_str=payload.get("template_str"),
                total_instances=int(payload.get("count", 0)),
                variable_types=var_types,
                top_values=top_values,
            )
        return TemplateSummary(summary)

    @staticmethod
    def _infer_type(values: list[object]) -> str:
        """Infer a coarse type for variable values."""
        cleaned = [value for value in values if value is not None]
        if not cleaned:
            return "unknown"

        try:
            for value in cleaned:
                float(value)
        except (TypeError, ValueError):
            pass
        else:
            return "numeric"

        lowered = {str(value).lower() for value in cleaned}
        if lowered <= {"true", "false"}:
            return "boolean"

        if TemplateInventory._all_datetimes(cleaned):
            return "datetime"

        return "categorical"

    @staticmethod
    def _top_categorical_values(values: list[object], limit: int = 10) -> list[str]:
        """Return the most frequent categorical values observed."""
        if not values:
            return []
        counts = Counter(str(value) for value in values if value is not None)
        return [value for value, _ in counts.most_common(limit)]

    @staticmethod
    def _all_datetimes(values: list[object]) -> bool:
        """Return True if every value parses as a datetime."""
        for value in values:
            if isinstance(value, datetime):
                continue
            try:
                datetime.fromisoformat(str(value))
            except ValueError:
                return False
        return True

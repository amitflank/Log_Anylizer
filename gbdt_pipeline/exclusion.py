"""Helpers for excluding templates from the pipeline."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class TemplateExclusion:
    """Determines whether a template should be ignored."""

    explicit: frozenset[str]
    patterns: tuple[re.Pattern[str], ...]

    @classmethod
    def from_lists(
        cls,
        templates: Iterable[str] | None = None,
        pattern_strings: Sequence[str] | None = None,
    ) -> TemplateExclusion:
        """Build an exclusion object from optional collections."""
        compiled = tuple(re.compile(pattern) for pattern in (pattern_strings or []))
        return cls(frozenset(templates or ()), compiled)

    def matches(self, template_id: str) -> bool:
        """Return True when a template ID is excluded."""
        if template_id in self.explicit:
            return True
        return any(pattern.match(template_id) for pattern in self.patterns)

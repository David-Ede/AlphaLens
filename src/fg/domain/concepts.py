"""Concept-map helpers for SEC taxonomy resolution."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from fg.domain.enums import Confidence


def get_preferred_concepts(metric_code: str, concept_map: dict[str, Any]) -> list[str]:
    """Return preferred concept tags for a metric code."""
    concepts = concept_map.get("concepts", {})
    return [str(tag) for tag in concepts.get(metric_code, {}).get("preferred", [])]


def resolve_concept(
    metric_code: str,
    available_concepts: Iterable[str],
    concept_map: dict[str, Any],
) -> tuple[str | None, Confidence]:
    """Resolve best concept from available tags using configured priority."""
    available = {item for item in available_concepts}
    preferred = get_preferred_concepts(metric_code, concept_map)
    for concept in preferred:
        if concept in available:
            return concept, Confidence.REPORTED
    if preferred and available:
        # A non-preferred but still accepted tag.
        return sorted(available)[0], Confidence.FALLBACK_TAG
    return None, Confidence.DERIVED


def validate_concept_map(concept_map: dict[str, Any]) -> None:
    """Validate concept map shape according to v1 rules."""
    concepts = concept_map.get("concepts", {})
    for metric_code, config in concepts.items():
        preferred = config.get("preferred", [])
        if any(not str(tag).strip() for tag in preferred):
            msg = f"Blank preferred concept for metric {metric_code}"
            raise ValueError(msg)

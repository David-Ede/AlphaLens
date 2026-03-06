"""Unit tests for concept map logic."""

from __future__ import annotations

from fg.domain.concepts import resolve_concept, validate_concept_map
from fg.domain.enums import Confidence


def test_resolve_concept_prefers_config_order() -> None:
    concept_map = {
        "concepts": {"eps_diluted_actual": {"preferred": ["us-gaap:EarningsPerShareDiluted"], "fallback": []}}
    }
    concept, confidence = resolve_concept(
        metric_code="eps_diluted_actual",
        available_concepts=["us-gaap:EarningsPerShareDiluted", "us-gaap:Other"],
        concept_map=concept_map,
    )
    assert concept == "us-gaap:EarningsPerShareDiluted"
    assert confidence == Confidence.REPORTED


def test_resolve_concept_uses_fallback_when_preferred_absent() -> None:
    concept_map = {"concepts": {"revenue_actual": {"preferred": ["us-gaap:Revenues"], "fallback": []}}}
    concept, confidence = resolve_concept(
        metric_code="revenue_actual",
        available_concepts=["custom:Revenue"],
        concept_map=concept_map,
    )
    assert concept == "custom:Revenue"
    assert confidence == Confidence.FALLBACK_TAG


def test_validate_concept_map_rejects_blank() -> None:
    concept_map = {"concepts": {"eps_diluted_actual": {"preferred": [""]}}}
    try:
        validate_concept_map(concept_map)
    except ValueError:
        assert True
    else:
        raise AssertionError("Expected ValueError for blank preferred concept")

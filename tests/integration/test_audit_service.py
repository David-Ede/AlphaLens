"""Integration tests for audit service assembly."""

from __future__ import annotations

from fg.services.audit_service import build_audit_view
from fg.settings import Settings


def test_audit_view_empty(settings: Settings) -> None:
    payload = build_audit_view(settings, "AAPL")
    assert payload["lineage"] == []
    assert payload["quality"] == []


def test_audit_view_seeded(seeded_settings: Settings) -> None:
    payload = build_audit_view(seeded_settings, "AAPL")
    assert isinstance(payload["lineage"], list)
    assert isinstance(payload["quality"], list)
    assert isinstance(payload["source_meta"], list)

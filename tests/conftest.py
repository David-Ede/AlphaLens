"""Pytest fixtures for offline AlphaLens tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from fg.settings import Settings, get_settings


@pytest.fixture()
def settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Settings:
    """Build isolated demo settings with temp data root."""
    data_root = tmp_path / "data"
    monkeypatch.setenv("APP_ENV", "demo")
    monkeypatch.setenv("FMP_API_KEY", "")
    monkeypatch.setenv("SEC_USER_AGENT", "Example Company example@example.com")
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("DUCKDB_PATH", str(data_root / "fg.duckdb"))
    get_settings.cache_clear()
    cfg = get_settings()
    yield cfg
    get_settings.cache_clear()


@pytest.fixture()
def seeded_settings(settings: Settings) -> Settings:
    """Seed fixture tickers into isolated storage."""
    from fg.services.refresh_service import RefreshService

    service = RefreshService(settings)
    service.demo_seed(["AAPL", "MSFT", "KO"])
    return settings

"""Offline integration tests for fixture-backed pipeline."""

from __future__ import annotations

from fg.domain.enums import PEMethod
from fg.domain.models import RefreshRequest
from fg.services.refresh_service import RefreshService
from fg.settings import Settings
from fg.storage.repositories import read_table


def test_refresh_pipeline_for_reference_tickers(settings: Settings) -> None:
    service = RefreshService(settings)
    for ticker in ["AAPL", "MSFT", "KO"]:
        result = service.refresh_ticker(
            RefreshRequest(ticker=ticker, lookback_years=20, pe_method=PEMethod.STATIC_15),
            fixture_mode=True,
        )
        assert result["rows"]["series"] > 0
        company_key = result["company_key"]
        kpi = read_table(settings, "gold", "mart_kpi_snapshot", key=company_key)
        valuation = read_table(settings, "gold", "mart_valuation_series", key=f"{company_key}_20_static_15")
        audit = read_table(settings, "gold", "mart_audit_grid", key=company_key)
        assert not kpi.empty
        assert not valuation.empty
        assert not audit.empty


def test_build_gold_from_existing_silver(seeded_settings: Settings) -> None:
    service = RefreshService(seeded_settings)
    result = service.build_gold("AAPL", lookback_years=20, pe_method="normal_pe")
    assert result["ticker"] == "AAPL"
    assert result["series_rows"] > 0

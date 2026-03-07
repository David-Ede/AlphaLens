from __future__ import annotations

import pandas as pd
import pytest

from fg.domain.enums import PEMethod
from fg.domain.models import CompanyRef, RefreshRequest
from fg.pipelines.historical_loader import HistoricalDataLoader, canonicalize_companyfacts_payload
from fg.services.refresh_service import RefreshService
from fg.settings import Settings


def test_canonicalize_companyfacts_payload_builds_standalone_quarters(settings: Settings) -> None:
    company = CompanyRef(
        company_key="0000000001",
        ticker="TEST",
        issuer_name="Test Co",
        exchange="NASDAQ",
        fiscal_year_end_mmdd="0930",
    )
    raw_payload = {
        "cik": "0000000001",
        "entityName": "Test Co",
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            _fact("2023-10-01", "2023-12-30", 200.0, 2024, "Q1", "10-Q", "2024-02-01"),
                            _fact("2023-10-01", "2024-03-30", 450.0, 2024, "Q2", "10-Q", "2024-05-01"),
                            _fact("2023-10-01", "2024-06-29", 700.0, 2024, "Q3", "10-Q", "2024-08-01"),
                            _fact("2023-10-01", "2024-09-28", 1000.0, 2024, "FY", "10-K", "2024-11-01"),
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            _fact("2023-10-01", "2023-12-30", 20.0, 2024, "Q1", "10-Q", "2024-02-01"),
                            _fact("2023-10-01", "2024-03-30", 45.0, 2024, "Q2", "10-Q", "2024-05-01"),
                            _fact("2023-10-01", "2024-06-29", 70.0, 2024, "Q3", "10-Q", "2024-08-01"),
                            _fact("2023-10-01", "2024-09-28", 100.0, 2024, "FY", "10-K", "2024-11-01"),
                        ]
                    }
                },
                "WeightedAverageNumberOfDilutedSharesOutstanding": {
                    "units": {
                        "shares": [
                            _fact("2023-10-01", "2023-12-30", 50.0, 2024, "Q1", "10-Q", "2024-02-01"),
                            _fact("2023-12-31", "2024-03-30", 50.0, 2024, "Q2", "10-Q", "2024-05-01"),
                            _fact("2024-03-31", "2024-06-29", 50.0, 2024, "Q3", "10-Q", "2024-08-01"),
                            _fact("2023-10-01", "2024-09-28", 50.0, 2024, "FY", "10-K", "2024-11-01"),
                        ]
                    }
                },
            }
        },
    }

    canonical = canonicalize_companyfacts_payload(
        payload=raw_payload,
        company=company,
        concept_map=settings.concept_map_config,
        metrics_config=settings.metrics_config,
    )

    annual = pd.DataFrame(canonical["annual_facts"])
    quarterly = pd.DataFrame(canonical["quarterly_facts"])

    assert sorted(annual["metric_code"].unique().tolist()) == [
        "eps_diluted_actual",
        "net_income_actual",
        "revenue_actual",
        "shares_diluted_actual",
    ]
    annual_eps = annual[annual["metric_code"] == "eps_diluted_actual"].iloc[-1]
    assert annual_eps["confidence"] == "derived"
    assert annual_eps["value"] == pytest.approx(2.0)

    quarterly_eps = quarterly[quarterly["metric_code"] == "eps_diluted_actual"].sort_values("fiscal_quarter")
    quarterly_revenue = (
        quarterly[quarterly["metric_code"] == "revenue_actual"]
        .sort_values("fiscal_quarter")["value"]
        .tolist()
    )
    assert quarterly_eps["fiscal_quarter"].tolist() == [1, 2, 3]
    assert quarterly_eps["value"].tolist() == pytest.approx([0.4, 0.5, 0.5])
    assert quarterly_revenue == pytest.approx([200.0, 250.0, 250.0, 300.0])


def test_historical_loader_fixture_mode_populates_storage_and_duckdb_views(settings: Settings) -> None:
    loader = HistoricalDataLoader(settings)

    result = loader.load_ticker("AAPL", fixture_mode=True, build_gold=True)
    inventory = loader.table_inventory()
    created_views = loader.register_duckdb_views()
    dim_count = loader.query_duckdb("SELECT COUNT(*) AS row_count FROM v_dim_company")

    assert result["status"] == "ok"
    assert result["rows"]["silver_annual"] > 0
    assert "v_dim_company" in created_views
    assert not inventory[inventory["table_name"] == "dim_company"].empty
    assert int(dim_count.iloc[0]["row_count"]) == 1


def test_historical_loader_handles_fmp_failure(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_fmp(*args, **kwargs):
        raise RuntimeError("FMP unavailable")

    monkeypatch.setattr("fg.pipelines.historical_loader.ingest_fmp", _raise_fmp)
    loader = HistoricalDataLoader(settings)

    result = loader.load_ticker("AAPL", fixture_mode=True, build_gold=False)

    assert result["status"] == "ok"
    assert result["rows"]["silver_estimate_snapshot"] == 0
    assert any("Forward estimates unavailable" in message for message in result["warnings"])


def test_refresh_service_handles_fmp_failure(settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_fmp(*args, **kwargs):
        raise RuntimeError("FMP unavailable")

    monkeypatch.setattr("fg.services.refresh_service.ingest_fmp", _raise_fmp)
    service = RefreshService(settings)

    result = service.refresh_ticker(
        RefreshRequest(ticker="AAPL", lookback_years=20, pe_method=PEMethod.STATIC_15),
        fixture_mode=True,
    )

    assert result["ticker"] == "AAPL"
    assert result["rows"]["estimates"] == 0


def _fact(
    start: str,
    end: str,
    value: float,
    fiscal_year: int,
    fiscal_period: str,
    form: str,
    filed: str,
) -> dict[str, object]:
    return {
        "start": start,
        "end": end,
        "val": value,
        "fy": fiscal_year,
        "fp": fiscal_period,
        "form": form,
        "filed": filed,
        "accn": f"0000000001-{fiscal_year}-{fiscal_period.lower()}",
    }

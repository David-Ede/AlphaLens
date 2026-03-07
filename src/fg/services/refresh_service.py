"""Refresh orchestration from ingestion to gold marts."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from fg.domain.enums import PEMethod
from fg.domain.models import RefreshRequest, ViewModel, ViewModelMeta
from fg.ingestion.fmp_ingest import empty_estimate_payload, ingest_fmp
from fg.ingestion.resolve_company import resolve_company
from fg.ingestion.sec_ingest import ingest_sec
from fg.ingestion.yahoo_ingest import ingest_yahoo
from fg.marts.audit import build_audit_mart
from fg.marts.eps_bars import build_eps_bars_mart
from fg.marts.kpi_snapshot import build_kpi_snapshot_mart
from fg.marts.source_freshness import build_source_freshness_mart
from fg.marts.valuation_series import build_valuation_series_mart
from fg.normalization.estimates import normalize_estimates
from fg.normalization.market_data import normalize_market_data
from fg.normalization.quality_checks import run_quality_checks
from fg.normalization.sec_actuals_annual import normalize_sec_annual
from fg.normalization.sec_actuals_quarterly import normalize_sec_quarterly
from fg.settings import Settings, get_settings
from fg.storage.repositories import read_table, upsert_table


class RefreshService:
    """Run deterministic refresh/build steps for one ticker request."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def refresh_ticker(
        self,
        request: RefreshRequest,
        fixture_mode: bool | None = None,
    ) -> dict[str, Any]:
        """Refresh one ticker end-to-end."""
        request_id = str(uuid.uuid4())
        use_fixtures = self.settings.is_demo_mode if fixture_mode is None else fixture_mode
        company = resolve_company(
            settings=self.settings,
            ticker=request.ticker,
            fixture_mode=use_fixtures,
        )
        _submissions, companyfacts = ingest_sec(
            settings=self.settings,
            company=company,
            fixture_mode=use_fixtures,
        )
        prices, actions = ingest_yahoo(
            settings=self.settings,
            company=company,
            fixture_mode=use_fixtures,
        )
        estimate_available = True
        try:
            estimate_payload = ingest_fmp(
                settings=self.settings,
                company=company,
                fixture_mode=use_fixtures or not self.settings.fmp_api_key,
            )
        except Exception:
            estimate_payload = empty_estimate_payload(company.ticker)
            estimate_available = False
        self._update_dim_pulls(company.company_key, include_fmp=estimate_available)

        annual = normalize_sec_annual(self.settings, company.company_key, companyfacts)
        quarterly, _ttm = normalize_sec_quarterly(self.settings, company.company_key, companyfacts)
        daily, _actions, _monthly = normalize_market_data(
            self.settings,
            company.company_key,
            company.ticker,
            prices,
            actions,
        )
        estimates = normalize_estimates(self.settings, company.company_key, estimate_payload)

        warnings: list[str] = []
        series, selected_pe, valuation_warnings = build_valuation_series_mart(
            settings=self.settings,
            company_key=company.company_key,
            ticker=company.ticker,
            lookback_years=request.lookback_years,
            pe_method=request.pe_method.value,
            manual_pe=request.manual_pe,
            show_estimates=request.show_estimates,
        )
        warnings.extend(valuation_warnings)
        issues = run_quality_checks(
            settings=self.settings,
            company_key=company.company_key,
            annual_df=annual[annual["metric_code"] == "eps_diluted_actual"] if not annual.empty else annual,
            price_df=daily,
            estimate_df=estimates,
            warnings=warnings,
        )
        eps_bars = build_eps_bars_mart(self.settings, company.company_key, request.lookback_years)
        kpi = build_kpi_snapshot_mart(self.settings, company.company_key, company.ticker, selected_pe)
        freshness = build_source_freshness_mart(self.settings, company.company_key)
        audit = build_audit_mart(self.settings, company.company_key)

        result = {
            "request_id": request_id,
            "ticker": company.ticker,
            "company_key": company.company_key,
            "issuer_name": company.issuer_name,
            "warnings": warnings,
            "rows": {
                "annual": len(annual),
                "quarterly": len(quarterly),
                "prices": len(daily),
                "estimates": len(estimates),
                "series": len(series),
                "eps_bars": len(eps_bars),
                "issues": len(issues),
                "freshness": len(freshness),
                "audit": len(audit),
                "kpi": len(kpi),
            },
            "built_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        # Persist nested values as JSON strings for compatibility with legacy CSV rows.
        status_row = {
            **result,
            "warnings": json.dumps(result["warnings"]),
            "rows": json.dumps(result["rows"], separators=(",", ":")),
        }
        upsert_table(
            settings=self.settings,
            layer="gold",
            table_name="mart_refresh_status",
            key=company.company_key,
            df=pd.DataFrame([status_row]),
            dedupe_keys=["company_key"],
        )
        return result

    def build_gold(self, ticker: str, lookback_years: int = 20, pe_method: str = "static_15") -> dict[str, Any]:
        """Rebuild gold marts from existing silver data."""
        dim = read_table(self.settings, "silver", "dim_company")
        if dim.empty:
            raise ValueError("No companies available in silver layer.")
        row = dim[dim["ticker"] == ticker.upper()]
        if row.empty:
            raise ValueError(f"Ticker not found in silver layer: {ticker}")
        company_key = str(row.iloc[-1]["company_key"])
        series, selected_pe, warnings = build_valuation_series_mart(
            settings=self.settings,
            company_key=company_key,
            ticker=ticker.upper(),
            lookback_years=lookback_years,
            pe_method=pe_method,
            manual_pe=None,
            show_estimates=True,
        )
        build_eps_bars_mart(self.settings, company_key, lookback_years)
        build_kpi_snapshot_mart(self.settings, company_key, ticker.upper(), selected_pe)
        build_source_freshness_mart(self.settings, company_key)
        build_audit_mart(self.settings, company_key)
        return {"ticker": ticker.upper(), "company_key": company_key, "series_rows": len(series), "warnings": warnings}

    def load_view_model(
        self,
        ticker: str,
        lookback_years: int,
        pe_method: str,
    ) -> dict[str, Any]:
        """Load compact view-model from gold tables."""
        ticker_upper = ticker.upper()
        dim = read_table(self.settings, "silver", "dim_company")
        row = dim[dim["ticker"] == ticker_upper]
        if row.empty:
            return {
                "meta": {"ticker": ticker_upper},
                "kpis": {},
                "series": {"price": [], "fair_value_actual": [], "fair_value_estimate": [], "normal_pe_value": [], "eps_bars": []},
                "tables": {"annual": [], "quarterly": [], "audit": [], "quality_issues": []},
                "warnings": ["No data loaded yet. Select a ticker and click Refresh."],
            }
        company_key = str(row.iloc[-1]["company_key"])
        issuer_name = str(row.iloc[-1]["issuer_name"])
        series = read_table(
            self.settings,
            "gold",
            "mart_valuation_series",
            key=f"{company_key}_{lookback_years}_{pe_method}",
        )
        eps_bars = read_table(self.settings, "gold", "mart_eps_bars", key=f"{company_key}_{lookback_years}")
        annual = read_table(self.settings, "silver", "fact_fundamental_annual", key=company_key)
        quarterly = read_table(self.settings, "silver", "fact_fundamental_quarterly", key=company_key)
        audit = read_table(self.settings, "gold", "mart_audit_grid", key=company_key)
        issues = read_table(self.settings, "silver", "fact_quality_issue", key=company_key)
        kpi = read_table(self.settings, "gold", "mart_kpi_snapshot", key=company_key)
        kpi_payload = kpi.iloc[-1].to_dict() if not kpi.empty else {}

        payload = {
            "meta": ViewModelMeta(
                ticker=ticker_upper,
                company_key=company_key,
                issuer_name=issuer_name,
                lookback_years=lookback_years,
                pe_method=pe_method,
                built_at=datetime.now(tz=timezone.utc).isoformat(),
            ).model_dump(),
            "kpis": kpi_payload,
            "series": {
                "price": _series_records(series, "price"),
                "fair_value_actual": _series_records(series, "fair_value_actual"),
                "fair_value_estimate": _series_records(series, "fair_value_estimate"),
                "normal_pe_value": _series_records(series, "normal_pe_value"),
                "eps_bars": eps_bars.to_dict(orient="records"),
            },
            "tables": {
                "annual": annual.to_dict(orient="records"),
                "quarterly": quarterly.to_dict(orient="records"),
                "audit": audit.to_dict(orient="records"),
                "quality_issues": issues.to_dict(orient="records"),
            },
            "warnings": [str(m) for m in issues["message"].tolist()] if not issues.empty else [],
        }
        ViewModel.model_validate(payload)
        out_dir = Path("tests/fixtures/expected/view_models")
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{ticker_upper}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload

    def demo_seed(self, tickers: list[str] | None = None) -> list[dict[str, Any]]:
        """Seed demo data from frozen fixtures into all layers."""
        run_tickers = tickers or ["AAPL", "MSFT", "KO"]
        results: list[dict[str, Any]] = []
        for ticker in run_tickers:
            req = RefreshRequest(ticker=ticker, lookback_years=20, pe_method=PEMethod.STATIC_15)
            results.append(self.refresh_ticker(req, fixture_mode=True))
        return results

    def _update_dim_pulls(self, company_key: str, include_fmp: bool = True) -> None:
        dim = read_table(self.settings, "silver", "dim_company", key=company_key)
        if dim.empty:
            return
        now = datetime.now(tz=timezone.utc).date().isoformat()
        dim.loc[:, "last_sec_pull_at"] = now
        dim.loc[:, "last_yahoo_pull_at"] = now
        if include_fmp:
            dim.loc[:, "last_fmp_pull_at"] = now
        upsert_table(
            settings=self.settings,
            layer="silver",
            table_name="dim_company",
            key=company_key,
            df=dim,
            dedupe_keys=["company_key"],
        )


def _series_records(series: pd.DataFrame, name: str) -> list[dict[str, Any]]:
    if series.empty:
        return []
    return series[series["series_name"] == name].to_dict(orient="records")



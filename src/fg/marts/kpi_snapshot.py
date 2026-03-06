"""Build KPI snapshot mart."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from fg.domain.quality import compute_quality_score, evaluate_freshness
from fg.domain.valuation import compute_kpis
from fg.settings import Settings
from fg.storage.repositories import read_table, upsert_table


def build_kpi_snapshot_mart(
    settings: Settings,
    company_key: str,
    ticker: str,
    selected_pe: float,
) -> pd.DataFrame:
    """Build one-row KPI snapshot for overview cards."""
    prices = read_table(settings, "silver", "fact_price_daily", key=ticker)
    annual = read_table(settings, "silver", "fact_fundamental_annual", key=company_key)
    annual_eps = annual[annual["metric_code"] == "eps_diluted_actual"] if not annual.empty else annual
    estimates = read_table(settings, "silver", "fact_estimate_snapshot", key=company_key)
    issues = read_table(settings, "silver", "fact_quality_issue", key=company_key)
    dim_company = read_table(settings, "silver", "dim_company", key=company_key)

    sec_pull = str(dim_company.iloc[-1]["last_sec_pull_at"]) if not dim_company.empty else None
    yahoo_pull = str(dim_company.iloc[-1]["last_yahoo_pull_at"]) if not dim_company.empty else None
    fmp_pull = str(dim_company.iloc[-1]["last_fmp_pull_at"]) if not dim_company.empty else None

    sec_fresh = evaluate_freshness(sec_pull, int(settings.app_config["freshness"]["sec_days"]))
    price_fresh = evaluate_freshness(yahoo_pull, int(settings.app_config["freshness"]["price_days"]))
    est_fresh = evaluate_freshness(fmp_pull, int(settings.app_config["freshness"]["estimate_days"]))
    latest_conf = (
        str(annual_eps.sort_values("period_end_date").iloc[-1]["confidence"])
        if not annual_eps.empty
        else None
    )
    quality_score = compute_quality_score(issues, latest_conf, sec_fresh, price_fresh, est_fresh)
    kpis = compute_kpis(prices, annual_eps, estimates, selected_pe, quality_score)
    row = pd.DataFrame(
        [
            {
                "company_key": company_key,
                "as_of_date": datetime.now(tz=timezone.utc).date().isoformat(),
                "last_price": kpis["last_price"],
                "last_price_date": str(prices.sort_values("trade_date").iloc[-1]["trade_date"]) if not prices.empty else None,
                "latest_actual_eps": kpis["latest_actual_eps"],
                "latest_actual_eps_period_end": str(annual_eps.sort_values("period_end_date").iloc[-1]["period_end_date"])
                if not annual_eps.empty
                else None,
                "current_pe": kpis["current_pe"],
                "selected_pe": kpis["selected_pe"],
                "fair_value_now": kpis["fair_value_now"],
                "valuation_gap_pct": kpis["valuation_gap_pct"],
                "last_filing_date": kpis["last_filing_date"],
                "last_estimate_snapshot_date": kpis["last_estimate_snapshot_date"],
                "data_quality_score": kpis["data_quality_score"],
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            }
        ]
    )
    upsert_table(
        settings=settings,
        layer="gold",
        table_name="mart_kpi_snapshot",
        key=company_key,
        df=row,
        dedupe_keys=["company_key"],
    )
    return row



"""Build chart-ready valuation series marts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from fg.domain.valuation import (
    build_fair_value_series,
    build_observed_year_end_prices,
    compute_normal_pe,
)
from fg.settings import Settings
from fg.storage.repositories import read_table, upsert_table


def build_valuation_series_mart(
    settings: Settings,
    company_key: str,
    ticker: str,
    lookback_years: int,
    pe_method: str,
    manual_pe: float | None,
    show_estimates: bool,
) -> tuple[pd.DataFrame, float, list[str]]:
    """Build and persist `mart_valuation_series` for request state."""
    annual = read_table(settings, "silver", "fact_fundamental_annual", key=company_key)
    annual_eps = annual[annual["metric_code"] == "eps_diluted_actual"].copy() if not annual.empty else annual
    annual_eps = annual_eps.sort_values("fiscal_year").tail(lookback_years)

    monthly = read_table(settings, "silver", "fact_price_monthly", key=ticker)
    estimates = read_table(settings, "silver", "fact_estimate_snapshot", key=company_key)
    if not show_estimates:
        estimates = pd.DataFrame(columns=estimates.columns if not estimates.empty else [])

    warnings: list[str] = []
    selected_pe = float(settings.valuation_defaults.get("static_pe_default", 15.0))
    if manual_pe is not None:
        selected_pe = float(manual_pe)
    if pe_method == "normal_pe":
        prices = build_observed_year_end_prices(annual_eps, monthly)
        selected_pe, warnings, _observed = compute_normal_pe(
            observed_eps=annual_eps,
            year_end_prices=prices,
            min_years=int(settings.valuation_defaults.get("normal_pe_min_years", 3)),
            clip_quantiles=tuple(settings.valuation_defaults.get("normal_pe_clip_quantiles", [0.05, 0.95])),
        )

    fair = build_fair_value_series(
        company_key=company_key,
        lookback_years=lookback_years,
        pe_method=pe_method,
        selected_pe=selected_pe,
        annual_actual_df=annual_eps,
        estimate_df=estimates,
    )

    price_rows: list[dict[str, Any]] = []
    for row in monthly.itertuples(index=False):
        price_rows.append(
            {
                "company_key": company_key,
                "lookback_years": lookback_years,
                "pe_method": pe_method,
                "series_name": "price",
                "x_date": str(row.trade_date),
                "y_value": float(row.split_adjusted_close),
                "fiscal_year": None,
                "is_estimate": False,
                "display_style": "solid",
                "tooltip_payload_json": {
                    "metric": "price_close_split_adjusted",
                    "value": float(row.split_adjusted_close),
                    "trade_date": str(row.trade_date),
                    "source": "yahoo",
                },
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            }
        )
    series = pd.concat([pd.DataFrame(price_rows), fair], ignore_index=True) if not fair.empty else pd.DataFrame(price_rows)
    if series.empty:
        return series, selected_pe, warnings
    series["trace_order"] = series["series_name"].map(
        {
            "price": 1,
            "fair_value_actual": 2,
            "fair_value_estimate": 3,
            "normal_pe_value": 4,
        }
    ).fillna(99)
    series = series.sort_values(["trace_order", "x_date"]).drop(columns=["trace_order"])
    upsert_table(
        settings=settings,
        layer="gold",
        table_name="mart_valuation_series",
        key=f"{company_key}_{lookback_years}_{pe_method}",
        df=series,
        dedupe_keys=["series_name", "x_date", "pe_method", "lookback_years"],
    )
    return series.reset_index(drop=True), selected_pe, warnings



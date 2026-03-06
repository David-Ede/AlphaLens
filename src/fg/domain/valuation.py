"""Valuation formulas and KPI calculations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


def compute_static_15(eps: float, pe: float = 15.0) -> float:
    """Compute fair value under static P/E method."""
    return eps * pe


def compute_normal_pe(
    observed_eps: pd.DataFrame,
    year_end_prices: pd.DataFrame,
    min_years: int = 3,
    clip_quantiles: tuple[float, float] = (0.05, 0.95),
) -> tuple[float, list[str], pd.DataFrame]:
    """Compute normal P/E from historical observed annual P/E values."""
    merged = observed_eps.merge(
        year_end_prices[["fiscal_year", "price_on_or_before_period_end"]],
        on="fiscal_year",
        how="inner",
    ).copy()
    merged["observed_pe"] = merged["price_on_or_before_period_end"] / merged["value"]
    merged = merged[merged["value"] > 0].copy()
    issues: list[str] = []
    if len(merged) < min_years:
        issues.append("normal_pe_fallback_insufficient_history")
        merged["observed_pe_clipped"] = merged.get("observed_pe", pd.Series(dtype=float))
        return 15.0, issues, merged
    p05 = merged["observed_pe"].quantile(clip_quantiles[0])
    p95 = merged["observed_pe"].quantile(clip_quantiles[1])
    merged["observed_pe_clipped"] = merged["observed_pe"].clip(lower=p05, upper=p95)
    normal_pe = float(merged["observed_pe_clipped"].median())
    return normal_pe, issues, merged


def _price_on_or_before(price_df: pd.DataFrame, period_end_date: str) -> float | None:
    """Return last split-adjusted close on or before period end date."""
    if price_df.empty:
        return None
    point = price_df[price_df["trade_date"] <= period_end_date]
    if point.empty:
        return None
    return float(point.sort_values("trade_date").iloc[-1]["split_adjusted_close"])


def build_observed_year_end_prices(
    annual_actual_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build observed year-end price table for annual P/E calculations."""
    records: list[dict[str, Any]] = []
    for row in annual_actual_df.itertuples(index=False):
        records.append(
            {
                "fiscal_year": int(row.fiscal_year),
                "price_on_or_before_period_end": _price_on_or_before(price_df, str(row.period_end_date)),
            }
        )
    return pd.DataFrame(records)


def build_fair_value_series(
    company_key: str,
    lookback_years: int,
    pe_method: str,
    selected_pe: float,
    annual_actual_df: pd.DataFrame,
    estimate_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build fair value series for actual and estimate periods."""
    rows: list[dict[str, Any]] = []
    actual = annual_actual_df.sort_values("period_end_date").copy()
    for row in actual.itertuples(index=False):
        fair = float(row.value) * selected_pe if float(row.value) > 0 else None
        rows.append(
            {
                "company_key": company_key,
                "lookback_years": lookback_years,
                "pe_method": pe_method,
                "series_name": "fair_value_actual",
                "x_date": str(row.period_end_date),
                "y_value": fair,
                "fiscal_year": int(row.fiscal_year),
                "is_estimate": False,
                "display_style": "solid",
                "tooltip_payload_json": {
                    "metric": "eps_diluted_actual",
                    "value": float(row.value),
                    "period_end": str(row.period_end_date),
                    "concept": str(row.concept),
                    "confidence": str(row.confidence),
                    "filed_at": str(row.filed_at),
                    "accession_no": str(row.accession_no),
                },
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            }
        )
    estimates = estimate_df.sort_values("target_period_end_date").copy()
    for row in estimates.itertuples(index=False):
        fair = float(row.mean_value) * selected_pe if float(row.mean_value) > 0 else None
        rows.append(
            {
                "company_key": company_key,
                "lookback_years": lookback_years,
                "pe_method": pe_method,
                "series_name": "fair_value_estimate",
                "x_date": str(row.target_period_end_date),
                "y_value": fair,
                "fiscal_year": int(row.target_fiscal_year),
                "is_estimate": True,
                "display_style": "dashed",
                "tooltip_payload_json": {
                    "metric": "eps_estimate_mean",
                    "value": float(row.mean_value),
                    "period_end": str(row.target_period_end_date),
                    "snapshot_date": str(row.as_of_date),
                    "source": str(row.source_name),
                    "confidence": "estimate",
                },
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def compute_kpis(
    price_df: pd.DataFrame,
    annual_actual_df: pd.DataFrame,
    estimate_df: pd.DataFrame,
    selected_pe: float,
    quality_score: int,
) -> dict[str, Any]:
    """Compute overview KPI snapshot."""
    if price_df.empty:
        return {
            "last_price": None,
            "latest_actual_eps": None,
            "current_pe": None,
            "selected_pe": selected_pe,
            "fair_value_now": None,
            "valuation_gap_pct": None,
            "last_filing_date": None,
            "last_estimate_snapshot_date": None,
            "data_quality_score": quality_score,
        }
    latest_price_row = price_df.sort_values("trade_date").iloc[-1]
    last_price = float(latest_price_row["split_adjusted_close"])
    latest_eps_row = annual_actual_df.sort_values("period_end_date").iloc[-1] if not annual_actual_df.empty else None
    latest_eps = float(latest_eps_row["value"]) if latest_eps_row is not None else None
    fair_value_now = latest_eps * selected_pe if latest_eps is not None and latest_eps > 0 else None
    current_pe = (last_price / latest_eps) if latest_eps is not None and latest_eps > 0 else None
    valuation_gap = ((last_price - fair_value_now) / fair_value_now) if fair_value_now else None
    last_filing = (
        str(annual_actual_df.sort_values("filed_at").iloc[-1]["filed_at"])
        if not annual_actual_df.empty
        else None
    )
    last_estimate_snapshot = (
        str(estimate_df.sort_values("as_of_date").iloc[-1]["as_of_date"])
        if not estimate_df.empty
        else None
    )
    return {
        "last_price": last_price,
        "latest_actual_eps": latest_eps,
        "current_pe": current_pe,
        "selected_pe": selected_pe,
        "fair_value_now": fair_value_now,
        "valuation_gap_pct": valuation_gap,
        "last_filing_date": last_filing,
        "last_estimate_snapshot_date": last_estimate_snapshot,
        "data_quality_score": quality_score,
    }



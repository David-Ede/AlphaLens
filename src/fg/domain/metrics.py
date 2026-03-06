"""Canonical metric catalog."""

from __future__ import annotations

CORE_METRICS: dict[str, str] = {
    "eps_diluted_actual": "Diluted EPS",
    "revenue_actual": "Revenue",
    "net_income_actual": "Net Income",
    "shares_diluted_actual": "Diluted Shares",
    "price_close_split_adjusted": "Split Adjusted Close",
    "dividend_cash": "Cash Dividend",
    "eps_estimate_mean": "EPS Estimate Mean",
}

OPTIONAL_METRICS: dict[str, str] = {
    "free_cash_flow_actual": "Free Cash Flow",
    "operating_cash_flow_actual": "Operating Cash Flow",
    "dividend_per_share_actual": "Dividend Per Share",
    "revenue_estimate_mean": "Revenue Estimate Mean",
}


def is_known_metric(metric_code: str) -> bool:
    """Return True if metric code is known by config/domain."""
    return metric_code in CORE_METRICS or metric_code in OPTIONAL_METRICS

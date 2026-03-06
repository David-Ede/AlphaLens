"""Period classification and quarterly derivation helpers."""

from __future__ import annotations

from datetime import date

import pandas as pd

ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
QUARTERLY_FORMS = {"10-Q", "10-Q/A"}


def is_annual_form(form_type: str, duration_days: int) -> bool:
    """Return True if filing is annual under v1 rules."""
    return form_type in ANNUAL_FORMS and 330 <= duration_days <= 400


def is_quarterly_form(form_type: str, duration_days: int) -> bool:
    """Return True if filing is quarterly under v1 rules."""
    return form_type in QUARTERLY_FORMS and 75 <= duration_days <= 110


def fiscal_quarter_from_period_end(period_end: str, fiscal_year_end_mmdd: str = "1231") -> int:
    """Map period end date to fiscal quarter using simple month offsets."""
    period = date.fromisoformat(period_end)
    fy_end_month = int(fiscal_year_end_mmdd[:2])
    delta = (period.month - fy_end_month) % 12
    if delta in (0, 1, 2):
        return 4
    if delta in (3, 4, 5):
        return 1
    if delta in (6, 7, 8):
        return 2
    return 3


def derive_standalone_quarters(ytd_df: pd.DataFrame) -> pd.DataFrame:
    """Derive standalone quarterly values from YTD values per fiscal year."""
    if ytd_df.empty:
        return ytd_df.copy()
    data = ytd_df.sort_values(["fiscal_year", "fiscal_quarter"]).copy()
    values: list[float] = []
    prev_by_year: dict[int, float] = {}
    for row in data.itertuples(index=False):
        year = int(row.fiscal_year)
        current = float(row.value)
        previous = prev_by_year.get(year, 0.0)
        values.append(current - previous)
        prev_by_year[year] = current
    data["value"] = values
    data["confidence"] = "derived"
    return data


def derive_q4_from_annual(
    annual_value: float,
    q1_value: float,
    q2_value: float,
    q3_value: float,
) -> float:
    """Derive Q4 standalone value from annual total and first three quarters."""
    return annual_value - (q1_value + q2_value + q3_value)


def build_ttm_from_quarters(quarterly_df: pd.DataFrame) -> pd.DataFrame:
    """Build TTM series from standalone quarterly values."""
    if quarterly_df.empty:
        return pd.DataFrame(columns=list(quarterly_df.columns))
    data = quarterly_df.sort_values("period_end_date").copy()
    data["ttm_value"] = data["value"].rolling(window=4).sum()
    ttm = data.dropna(subset=["ttm_value"]).copy()
    ttm["period_type"] = "ttm"
    ttm["value"] = ttm["ttm_value"]
    return ttm.drop(columns=["ttm_value"])

"""Unit tests for period helpers."""

from __future__ import annotations

import pandas as pd

from fg.domain.periods import (
    build_ttm_from_quarters,
    derive_q4_from_annual,
    fiscal_quarter_from_period_end,
    is_annual_form,
    is_quarterly_form,
)


def test_period_classification_rules() -> None:
    assert is_annual_form("10-K", 365)
    assert not is_annual_form("10-Q", 365)
    assert is_quarterly_form("10-Q", 90)
    assert not is_quarterly_form("10-K", 90)


def test_derive_q4_from_annual() -> None:
    q4 = derive_q4_from_annual(10.0, 2.0, 2.5, 2.0)
    assert q4 == 3.5


def test_build_ttm_from_quarters() -> None:
    df = pd.DataFrame(
        [
            {"period_end_date": "2024-03-31", "value": 1.0},
            {"period_end_date": "2024-06-30", "value": 1.5},
            {"period_end_date": "2024-09-30", "value": 2.0},
            {"period_end_date": "2024-12-31", "value": 2.5},
            {"period_end_date": "2025-03-31", "value": 3.0},
        ]
    )
    ttm = build_ttm_from_quarters(df)
    assert len(ttm) == 2
    assert float(ttm.iloc[0]["value"]) == 7.0
    assert float(ttm.iloc[1]["value"]) == 9.0


def test_fiscal_quarter_from_period_end() -> None:
    assert fiscal_quarter_from_period_end("2024-12-31", "1231") == 4

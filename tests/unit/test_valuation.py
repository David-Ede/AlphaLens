"""Unit tests for valuation formulas."""

from __future__ import annotations

import pandas as pd

from fg.domain.valuation import compute_kpis, compute_normal_pe, compute_static_15


def test_compute_static_15() -> None:
    assert compute_static_15(5.0, 15.0) == 75.0


def test_compute_normal_pe_fallback() -> None:
    eps = pd.DataFrame([{"fiscal_year": 2023, "value": 1.5}, {"fiscal_year": 2024, "value": 2.0}])
    prices = pd.DataFrame(
        [
            {"fiscal_year": 2023, "price_on_or_before_period_end": 30.0},
            {"fiscal_year": 2024, "price_on_or_before_period_end": 40.0},
        ]
    )
    value, issues, observed = compute_normal_pe(eps, prices, min_years=3)
    assert value == 15.0
    assert "normal_pe_fallback_insufficient_history" in issues
    assert not observed.empty


def test_compute_normal_pe_median_clipped() -> None:
    eps = pd.DataFrame(
        [
            {"fiscal_year": 2020, "value": 2.0},
            {"fiscal_year": 2021, "value": 2.5},
            {"fiscal_year": 2022, "value": 3.0},
            {"fiscal_year": 2023, "value": 3.5},
        ]
    )
    prices = pd.DataFrame(
        [
            {"fiscal_year": 2020, "price_on_or_before_period_end": 40.0},
            {"fiscal_year": 2021, "price_on_or_before_period_end": 50.0},
            {"fiscal_year": 2022, "price_on_or_before_period_end": 75.0},
            {"fiscal_year": 2023, "price_on_or_before_period_end": 70.0},
        ]
    )
    value, issues, _observed = compute_normal_pe(eps, prices, min_years=3)
    assert issues == []
    assert round(value, 2) > 10.0


def test_negative_eps_suppresses_pe() -> None:
    prices = pd.DataFrame([{"trade_date": "2025-12-31", "split_adjusted_close": 100.0}])
    annual = pd.DataFrame(
        [
            {
                "period_end_date": "2025-12-31",
                "value": -2.0,
                "filed_at": "2026-01-31",
            }
        ]
    )
    estimates = pd.DataFrame()
    kpis = compute_kpis(prices, annual, estimates, selected_pe=15.0, quality_score=90)
    assert kpis["current_pe"] is None
    assert kpis["fair_value_now"] is None

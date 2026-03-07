"""Unit tests for overview callback helpers."""

from __future__ import annotations

import pandas as pd

from fg.ui.callbacks.overview import resolve_price_history_start_date


def test_resolve_price_history_start_date_uses_latest_trade_date() -> None:
    prices = pd.DataFrame(
        [
            {"trade_date": "2024-01-15"},
            {"trade_date": "2026-02-28"},
            {"trade_date": "not-a-date"},
        ]
    )

    assert resolve_price_history_start_date(prices, 5) == "2021-02-28"


def test_resolve_price_history_start_date_falls_back_to_today_for_empty_frame() -> None:
    expected = str((pd.Timestamp.today().normalize() - pd.DateOffset(years=1)).date())

    assert resolve_price_history_start_date(pd.DataFrame(), 1) == expected

"""Unit tests for overview control helpers."""

from __future__ import annotations

import pandas as pd

from fg.ui.components.controls import build_ticker_options


def test_build_ticker_options_prefers_loaded_companies_and_fallbacks() -> None:
    companies = pd.DataFrame(
        [
            {"ticker": "AAPL", "issuer_name": "Apple Computer"},
            {"ticker": "AAPL", "issuer_name": "Apple Inc."},
            {"ticker": "TSLA", "issuer_name": "Tesla, Inc."},
        ]
    )

    options = build_ticker_options(
        companies=companies,
        fallback_tickers=["MSFT", " ", "AAPL"],
        default_ticker="NVDA",
    )

    assert options == [
        {"label": "AAPL - Apple Inc.", "value": "AAPL"},
        {"label": "MSFT", "value": "MSFT"},
        {"label": "NVDA", "value": "NVDA"},
        {"label": "TSLA - Tesla, Inc.", "value": "TSLA"},
    ]


def test_build_ticker_options_handles_empty_company_table() -> None:
    options = build_ticker_options(pd.DataFrame(), fallback_tickers=["msft"], default_ticker="aapl")

    assert options == [
        {"label": "AAPL", "value": "AAPL"},
        {"label": "MSFT", "value": "MSFT"},
    ]

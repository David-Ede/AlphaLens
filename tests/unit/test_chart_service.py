"""Unit tests for chart service helpers."""

from __future__ import annotations

import pandas as pd

from fg.services.chart_service import (
    build_eps_bar_chart,
    build_historical_price_chart,
    figure_to_json_dict,
)


def test_build_historical_price_chart_filters_before_start_date() -> None:
    frame = pd.DataFrame(
        [
            {"trade_date": "1999-12-31", "split_adjusted_close": 59.5},
            {"trade_date": "2000-01-03", "split_adjusted_close": 60.0},
            {"trade_date": "not-a-date", "split_adjusted_close": 61.0},
            {"trade_date": "2000-01-04", "split_adjusted_close": None},
        ]
    )
    fig = build_historical_price_chart("MSFT", frame)

    assert fig.layout.title.text == "MSFT Price History Since 2000"
    assert len(fig.data) == 1
    assert list(fig.data[0].x) == [pd.Timestamp("2000-01-03")]
    assert list(fig.data[0].y) == [60.0]


def test_build_historical_price_chart_handles_empty_frame() -> None:
    payload = build_historical_price_chart("MSFT", pd.DataFrame()).to_plotly_json()
    assert len(payload["data"]) == 0


def test_build_eps_bar_chart_handles_empty_frame() -> None:
    payload = build_eps_bar_chart(pd.DataFrame()).to_plotly_json()
    assert payload["layout"]["title"]["text"] == "EPS Bars"
    assert len(payload["data"]) == 0


def test_figure_to_json_dict_returns_serialized_payload() -> None:
    fig = build_historical_price_chart("MSFT", pd.DataFrame())
    payload = figure_to_json_dict(fig)
    assert set(payload.keys()) == {"data", "layout"}

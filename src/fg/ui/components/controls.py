"""Overview controls builders."""

from __future__ import annotations

from typing import Any

import pandas as pd
from dash import dcc, html


def build_ticker_options(
    companies: pd.DataFrame,
    fallback_tickers: list[str] | None = None,
    default_ticker: str = "",
) -> list[dict[str, Any]]:
    """Build searchable dropdown options from loaded companies and fallback tickers."""
    labels_by_ticker: dict[str, str] = {}

    if not companies.empty and "ticker" in companies.columns:
        frame = companies.copy()
        frame["ticker"] = frame["ticker"].astype(str).str.upper().str.strip()
        if "issuer_name" not in frame.columns:
            frame["issuer_name"] = ""
        frame["issuer_name"] = frame["issuer_name"].fillna("").astype(str).str.strip()
        frame = frame[frame["ticker"] != ""].drop_duplicates(subset=["ticker"], keep="last")
        for row in frame.sort_values("ticker").itertuples(index=False):
            ticker = str(getattr(row, "ticker", "")).upper().strip()
            issuer_name = str(getattr(row, "issuer_name", "")).strip()
            if not ticker:
                continue
            labels_by_ticker[ticker] = ticker if not issuer_name else f"{ticker} - {issuer_name}"

    for ticker in fallback_tickers or []:
        normalized = str(ticker).upper().strip()
        if normalized and normalized not in labels_by_ticker:
            labels_by_ticker[normalized] = normalized

    normalized_default = default_ticker.upper().strip()
    if normalized_default and normalized_default not in labels_by_ticker:
        labels_by_ticker[normalized_default] = normalized_default

    return [
        {"label": labels_by_ticker[ticker], "value": ticker}
        for ticker in sorted(labels_by_ticker)
    ]


def build_overview_controls(
    default_ticker: str,
    default_lookback: int,
    ticker_options: list[dict[str, Any]],
) -> html.Div:
    """Build overview control row with fixed component IDs."""
    return html.Div(
        className="row control-panel",
        children=[
            html.Div(
                className="control-row",
                children=[
                    html.Div(
                        className="control-group control-group-wide",
                        children=[
                            html.Label("Ticker", htmlFor="overview-ticker-input", className="control-label"),
                            dcc.Dropdown(
                                id="overview-ticker-input",
                                options=ticker_options,
                                value=default_ticker.upper().strip() or None,
                                placeholder="Search loaded stocks",
                                clearable=True,
                                searchable=True,
                                style={"minWidth": "320px"},
                            ),
                        ],
                    ),
                    html.Div(
                        className="control-group",
                        children=[
                            html.Label(
                                "Valuation lookback (years)",
                                htmlFor="overview-lookback-dropdown",
                                className="control-label",
                            ),
                            dcc.Dropdown(
                                id="overview-lookback-dropdown",
                                options=[{"label": str(v), "value": v} for v in [5, 10, 15, 20]],
                                value=default_lookback,
                                clearable=False,
                                style={"width": "180px"},
                            ),
                        ],
                    ),
                    html.Div(
                        className="control-group",
                        children=[
                            html.Label("P/E method", htmlFor="overview-pe-method-radio", className="control-label"),
                            dcc.RadioItems(
                                id="overview-pe-method-radio",
                                options=[
                                    {"label": "static_15", "value": "static_15"},
                                    {"label": "normal_pe", "value": "normal_pe"},
                                ],
                                value="static_15",
                                inline=True,
                                style={"minWidth": "220px"},
                            ),
                        ],
                    ),
                    html.Div(
                        id="overview-manual-pe-group",
                        className="control-group",
                        children=[
                            html.Label("Manual P/E", htmlFor="overview-manual-pe-input", className="control-label"),
                            dcc.Input(
                                id="overview-manual-pe-input",
                                type="number",
                                placeholder="Manual P/E",
                                value=15.0,
                                min=0,
                                step=0.01,
                                style={"width": "120px"},
                            ),
                        ],
                    ),
                    html.Div(
                        className="control-group",
                        children=[
                            html.Label("Forecasts", htmlFor="overview-show-estimates-toggle", className="control-label"),
                            dcc.Checklist(
                                id="overview-show-estimates-toggle",
                                options=[{"label": "Show estimates", "value": "on"}],
                                value=["on"],
                                inline=True,
                                style={"minWidth": "140px"},
                            ),
                        ],
                    ),
                    html.Div(
                        className="control-actions",
                        children=[
                            html.Button("Refresh", id="overview-refresh-button", n_clicks=0),
                            html.Button("Cancel", id="overview-cancel-button", n_clicks=0),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="control-hint",
                children=(
                    "Valuation lookback controls how many years are used in the fair value model. "
                    "The historical price chart has its own years slider below."
                ),
            ),
        ],
    )

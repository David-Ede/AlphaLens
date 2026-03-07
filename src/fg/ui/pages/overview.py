"""Overview page layout."""

from __future__ import annotations

import dash
from dash import html

from fg.settings import get_settings
from fg.storage.repositories import read_table
from fg.ui.components.controls import build_overview_controls, build_ticker_options
from fg.ui.components.graphs import (
    eps_graph_component,
    historical_price_graph_component,
    main_graph_component,
)


def layout() -> html.Div:
    """Render overview page layout in required order."""
    settings = get_settings()
    default_ticker = settings.ui_defaults.get("demo_default_ticker", "AAPL") if settings.is_demo_mode else ""
    default_lookback = int(settings.ui_defaults.get("default_lookback_years", 20))
    ticker_options = build_ticker_options(
        read_table(settings, "silver", "dim_company"),
        fallback_tickers=settings.watchlist,
        default_ticker=default_ticker,
    )
    return html.Div(
        children=[
            html.Div(
                className="row",
                children=[
                    html.H2("AlphaLens Overview"),
                    html.Div(
                        id="overview-demo-badge",
                        className="badge",
                        children=(
                            "Demo mode: using frozen fixtures where live data is unavailable"
                            if settings.is_demo_mode
                            else ""
                        ),
                        style={"marginBottom": "10px"},
                    ),
                ],
            ),
            build_overview_controls(
                default_ticker=default_ticker,
                default_lookback=default_lookback,
                ticker_options=ticker_options,
            ),
            html.Div(
                id="overview-status-banner",
                className="row",
                children="No data loaded yet. Select a ticker and click Refresh.",
            ),
            html.Div(id="overview-kpi-grid", className="row"),
            main_graph_component(),
            historical_price_graph_component(default_year_window=default_lookback),
            eps_graph_component(),
            html.Div(id="overview-freshness-badges", className="row"),
            html.Div(
                className="row",
                children=[
                    html.Button("Export CSV", id="overview-export-csv-button", n_clicks=0),
                    html.Button("Export XLSX", id="overview-export-xlsx-button", n_clicks=0, style={"marginLeft": "6px"}),
                ],
            ),
        ]
    )


dash.register_page(__name__, path="/", name="Overview", layout=layout)

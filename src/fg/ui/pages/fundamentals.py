"""Fundamentals page layout."""

from __future__ import annotations

import dash
from dash import dcc, html

from fg.ui.components.tables import build_table


def layout() -> html.Div:
    """Render fundamentals page."""
    return html.Div(
        children=[
            html.H2("Fundamentals"),
            html.Div(
                className="row",
                children=[
                    dcc.Dropdown(
                        id="fundamentals-metric-selector",
                        options=[
                            {"label": "EPS", "value": "eps_diluted_actual"},
                            {"label": "Revenue", "value": "revenue_actual"},
                            {"label": "Net Income", "value": "net_income_actual"},
                            {"label": "Diluted Shares", "value": "shares_diluted_actual"},
                        ],
                        value="eps_diluted_actual",
                        clearable=False,
                        style={"width": "240px", "display": "inline-block"},
                    ),
                    dcc.Dropdown(
                        id="fundamentals-period-selector",
                        options=[
                            {"label": "All", "value": "all"},
                            {"label": "Annual", "value": "annual"},
                            {"label": "Quarterly", "value": "quarterly"},
                        ],
                        value="all",
                        clearable=False,
                        style={"width": "180px", "display": "inline-block", "marginLeft": "8px"},
                    ),
                    dcc.Dropdown(
                        id="fundamentals-confidence-filter",
                        options=[
                            {"label": "All", "value": "all"},
                            {"label": "reported", "value": "reported"},
                            {"label": "fallback_tag", "value": "fallback_tag"},
                            {"label": "derived", "value": "derived"},
                        ],
                        value="all",
                        clearable=False,
                        style={"width": "180px", "display": "inline-block", "marginLeft": "8px"},
                    ),
                ],
            ),
            html.Div(
                id="fundamentals-concept-summary",
                className="row",
                children="No fundamentals loaded yet. Refresh a ticker from the Overview page.",
            ),
            build_table(
                "fundamentals-annual-table",
                columns=[
                    "metric_code",
                    "fiscal_year",
                    "period_end_date",
                    "value",
                    "unit",
                    "confidence",
                    "concept",
                    "filed_at",
                ],
            ),
            html.Div(style={"height": "12px"}),
            build_table(
                "fundamentals-quarterly-table",
                columns=[
                    "metric_code",
                    "fiscal_year",
                    "fiscal_quarter",
                    "period_end_date",
                    "value",
                    "unit",
                    "confidence",
                    "concept",
                    "filed_at",
                ],
            ),
        ]
    )


dash.register_page(__name__, path="/fundamentals", name="Fundamentals", layout=layout)

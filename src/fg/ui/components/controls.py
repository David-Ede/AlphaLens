"""Overview controls builders."""

from __future__ import annotations

from dash import dcc, html


def build_overview_controls(default_ticker: str, default_lookback: int) -> html.Div:
    """Build overview control row with fixed component IDs."""
    return html.Div(
        className="row",
        children=[
            dcc.Input(
                id="overview-ticker-input",
                type="text",
                value=default_ticker,
                placeholder="Ticker (e.g., AAPL)",
                debounce=True,
            ),
            dcc.Dropdown(
                id="overview-lookback-dropdown",
                options=[{"label": str(v), "value": v} for v in [5, 10, 15, 20]],
                value=default_lookback,
                clearable=False,
                style={"width": "140px", "display": "inline-block", "marginLeft": "8px"},
            ),
            dcc.RadioItems(
                id="overview-pe-method-radio",
                options=[
                    {"label": "static_15", "value": "static_15"},
                    {"label": "normal_pe", "value": "normal_pe"},
                ],
                value="static_15",
                inline=True,
                style={"display": "inline-block", "marginLeft": "12px"},
            ),
            dcc.Input(
                id="overview-manual-pe-input",
                type="number",
                placeholder="Manual P/E",
                value=15.0,
                min=0,
                step=0.01,
                style={"marginLeft": "8px", "width": "120px"},
            ),
            dcc.Checklist(
                id="overview-show-estimates-toggle",
                options=[{"label": "Show estimates", "value": "on"}],
                value=["on"],
                inline=True,
                style={"display": "inline-block", "marginLeft": "10px"},
            ),
            html.Button("Refresh", id="overview-refresh-button", n_clicks=0, style={"marginLeft": "8px"}),
            html.Button("Cancel", id="overview-cancel-button", n_clicks=0, style={"marginLeft": "6px"}),
        ],
    )

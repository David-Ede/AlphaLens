"""Graph component wrappers."""

from __future__ import annotations

from dash import dcc, html


def main_graph_component() -> html.Div:
    """Main valuation graph wrapper."""
    return html.Div(
        className="row chart-panel",
        children=[
            dcc.Loading(
                type="default",
                children=[dcc.Graph(id="overview-main-graph", figure={})],
            )
        ],
    )


def historical_price_graph_component(default_year_window: int = 20) -> html.Div:
    """Historical daily split-adjusted price chart wrapper."""
    return html.Div(
        className="row chart-panel",
        children=[
            html.Div(
                className="chart-toolbar",
                children=[
                    html.Div(
                        children=[
                            html.Div("Price history window (years)", className="control-label"),
                            html.Div(
                                "Use the slider to focus on the most recent years of trading history.",
                                className="control-hint",
                            ),
                        ]
                    ),
                    html.Div(id="overview-price-history-window-value", className="slider-readout"),
                ],
            ),
            dcc.Slider(
                id="overview-price-history-years-slider",
                min=1,
                max=30,
                step=1,
                value=default_year_window,
                marks={
                    1: "1",
                    3: "3",
                    5: "5",
                    10: "10",
                    15: "15",
                    20: "20",
                    30: "30",
                },
                tooltip={"placement": "bottom", "always_visible": True},
            ),
            dcc.Loading(
                type="default",
                children=[dcc.Graph(id="overview-historical-price-graph", figure={})],
            )
        ],
    )


def eps_graph_component() -> html.Div:
    """EPS bar chart wrapper."""
    return html.Div(
        className="row chart-panel",
        children=[
            dcc.Loading(
                type="default",
                children=[dcc.Graph(id="overview-eps-bars", figure={})],
            )
        ],
    )

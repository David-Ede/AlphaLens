"""Graph component wrappers."""

from __future__ import annotations

from dash import dcc, html


def main_graph_component() -> html.Div:
    """Main valuation graph wrapper."""
    return html.Div(
        className="row",
        children=[
            dcc.Loading(
                type="default",
                children=[dcc.Graph(id="overview-main-graph", figure={})],
            )
        ],
    )


def historical_price_graph_component() -> html.Div:
    """Historical daily split-adjusted price chart wrapper."""
    return html.Div(
        className="row",
        children=[
            dcc.Loading(
                type="default",
                children=[dcc.Graph(id="overview-historical-price-graph", figure={})],
            )
        ],
    )


def eps_graph_component() -> html.Div:
    """EPS bar chart wrapper."""
    return html.Div(
        className="row",
        children=[
            dcc.Loading(
                type="default",
                children=[dcc.Graph(id="overview-eps-bars", figure={})],
            )
        ],
    )

"""Deterministic Plotly figure builders."""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.graph_objects as go


def build_main_chart(
    ticker: str,
    issuer_name: str,
    series_df: pd.DataFrame,
    theme: str = "plotly_white",
) -> go.Figure:
    """Build main valuation chart with deterministic trace order."""
    fig = go.Figure()
    trace_order = ["price", "fair_value_actual", "fair_value_estimate", "normal_pe_value"]
    style_map = {
        "price": {"color": "#1f2937", "dash": "solid"},
        "fair_value_actual": {"color": "#1d4ed8", "dash": "solid"},
        "fair_value_estimate": {"color": "#1d4ed8", "dash": "dash"},
        "normal_pe_value": {"color": "#059669", "dash": "solid"},
    }
    for name in trace_order:
        subset = series_df[series_df["series_name"] == name] if not series_df.empty else pd.DataFrame()
        if subset.empty:
            continue
        subset = subset.sort_values("x_date")
        fig.add_trace(
            go.Scatter(
                x=subset["x_date"],
                y=subset["y_value"],
                mode="lines",
                name=name,
                line={
                    "color": style_map[name]["color"],
                    "dash": style_map[name]["dash"],
                    "width": 2,
                },
                customdata=subset["tooltip_payload_json"],
                hovertemplate="%{x}<br>%{y:.2f}<extra>%{fullData.name}</extra>",
            )
        )
    fig.update_layout(
        template=theme,
        title=f"{ticker.upper()} - {issuer_name}",
        margin={"l": 40, "r": 20, "t": 50, "b": 40},
        legend={"orientation": "h", "y": 1.05, "x": 0},
        xaxis_title="Date",
        yaxis_title="Price / Fair Value",
    )
    return fig


def build_eps_bar_chart(eps_bars_df: pd.DataFrame, theme: str = "plotly_white") -> go.Figure:
    """Build EPS companion bar chart."""
    fig = go.Figure()
    if eps_bars_df.empty:
        fig.update_layout(template=theme, title="EPS Bars")
        return fig
    actual = eps_bars_df[eps_bars_df["is_estimate"] == False].sort_values("fiscal_year")  # noqa: E712
    estimate = eps_bars_df[eps_bars_df["is_estimate"] == True].sort_values("fiscal_year")  # noqa: E712
    if not actual.empty:
        fig.add_trace(
            go.Bar(
                x=actual["fiscal_year"],
                y=actual["eps_actual"],
                name="actual",
                marker_color="#111827",
                customdata=actual[["confidence", "concept", "filed_at"]].values,
                hovertemplate=(
                    "FY %{x}<br>EPS %{y:.2f}<br>Confidence %{customdata[0]}<br>"
                    "Concept %{customdata[1]}<br>Filed %{customdata[2]}<extra></extra>"
                ),
            )
        )
    if not estimate.empty:
        fig.add_trace(
            go.Bar(
                x=estimate["fiscal_year"],
                y=estimate["eps_estimate"],
                name="estimate",
                marker_color="#2563eb",
                customdata=estimate[["confidence", "concept", "snapshot_date"]].values,
                hovertemplate=(
                    "FY %{x}<br>EPS %{y:.2f}<br>Confidence %{customdata[0]}<br>"
                    "Concept %{customdata[1]}<br>Snapshot %{customdata[2]}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        template=theme,
        barmode="group",
        title="Fiscal Year EPS",
        xaxis_title="Fiscal Year",
        yaxis_title="EPS",
        margin={"l": 40, "r": 20, "t": 50, "b": 40},
    )
    return fig


def figure_to_json_dict(figure: go.Figure) -> dict[str, Any]:
    """Serialize figure to deterministic JSON dict."""
    return figure.to_plotly_json()

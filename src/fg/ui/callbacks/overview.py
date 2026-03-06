"""Overview callbacks: request serialization and figure rendering."""

from __future__ import annotations

from typing import Any

import pandas as pd
from dash import Input, Output, callback, html

from fg.services.chart_service import build_eps_bar_chart, build_main_chart
from fg.services.refresh_service import RefreshService
from fg.settings import get_settings
from fg.ui.components.cards import build_kpi_cards


def register_callbacks() -> None:
    """Register overview callbacks."""

    @callback(
        Output("store-request", "data"),
        Input("overview-ticker-input", "value"),
        Input("overview-lookback-dropdown", "value"),
        Input("overview-pe-method-radio", "value"),
        Input("overview-show-estimates-toggle", "value"),
        Input("overview-manual-pe-input", "value"),
    )
    def serialize_request(
        ticker: str | None,
        lookback: int | None,
        pe_method: str | None,
        show_estimates_values: list[str] | None,
        manual_pe: float | None,
    ) -> dict[str, Any]:
        use_ticker = (ticker or "").upper().strip()
        return {
            "ticker": use_ticker,
            "lookback_years": int(lookback or 20),
            "pe_method": pe_method or "static_15",
            "manual_pe": manual_pe,
            "show_estimates": bool(show_estimates_values and "on" in show_estimates_values),
        }

    @callback(
        Output("overview-manual-pe-input", "style"),
        Input("overview-pe-method-radio", "value"),
    )
    def toggle_manual_pe(pe_method: str) -> dict[str, str]:
        if pe_method == "static_15":
            return {"display": "inline-block", "marginLeft": "8px", "width": "120px"}
        return {"display": "none"}

    @callback(
        Output("store-valuation-dataset", "data"),
        Input("store-request", "data"),
        Input("store-refresh-status", "data"),
    )
    def load_view_model(request_data: dict[str, Any] | None, _refresh_status: dict[str, Any] | None) -> dict[str, Any]:
        if not request_data or not request_data.get("ticker"):
            return {
                "meta": {},
                "kpis": {},
                "series": {"price": [], "fair_value_actual": [], "fair_value_estimate": [], "normal_pe_value": [], "eps_bars": []},
                "tables": {"annual": [], "quarterly": [], "audit": [], "quality_issues": []},
                "warnings": ["No data loaded yet. Enter a ticker and click Refresh."],
            }
        service = RefreshService(get_settings())
        return service.load_view_model(
            ticker=str(request_data["ticker"]),
            lookback_years=int(request_data.get("lookback_years", 20)),
            pe_method=str(request_data.get("pe_method", "static_15")),
        )

    @callback(
        Output("overview-kpi-grid", "children"),
        Input("store-valuation-dataset", "data"),
    )
    def render_kpis(dataset: dict[str, Any] | None) -> html.Div:
        if not dataset:
            return html.Div("No data loaded yet. Enter a ticker and click Refresh.")
        kpis = dataset.get("kpis", {})
        return build_kpi_cards(kpis)

    @callback(
        Output("overview-main-graph", "figure"),
        Input("store-valuation-dataset", "data"),
    )
    def render_main_chart(dataset: dict[str, Any] | None) -> dict[str, Any]:
        if not dataset:
            return {}
        meta = dataset.get("meta", {})
        records = (
            dataset.get("series", {}).get("price", [])
            + dataset.get("series", {}).get("fair_value_actual", [])
            + dataset.get("series", {}).get("fair_value_estimate", [])
            + dataset.get("series", {}).get("normal_pe_value", [])
        )
        frame = pd.DataFrame(records)
        fig = build_main_chart(
            ticker=str(meta.get("ticker", "")),
            issuer_name=str(meta.get("issuer_name", "")),
            series_df=frame,
            theme="plotly_white",
        )
        return fig.to_plotly_json()

    @callback(
        Output("overview-eps-bars", "figure"),
        Input("store-valuation-dataset", "data"),
    )
    def render_eps_bars(dataset: dict[str, Any] | None) -> dict[str, Any]:
        if not dataset:
            return {}
        frame = pd.DataFrame(dataset.get("series", {}).get("eps_bars", []))
        fig = build_eps_bar_chart(frame, theme="plotly_white")
        return fig.to_plotly_json()

    @callback(
        Output("overview-freshness-badges", "children"),
        Input("store-valuation-dataset", "data"),
    )
    def render_freshness_badges(dataset: dict[str, Any] | None) -> html.Div:
        if not dataset:
            return html.Div()
        quality = dataset.get("tables", {}).get("quality_issues", [])
        if not quality:
            return html.Div("Freshness: no active warnings")
        return html.Div(f"Warnings: {len(quality)}")

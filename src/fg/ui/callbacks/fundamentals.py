"""Fundamentals table callbacks."""

from __future__ import annotations

from typing import Any

from dash import Input, Output, callback


def register_callbacks() -> None:
    """Register fundamentals callbacks."""

    @callback(
        Output("fundamentals-annual-table", "data"),
        Output("fundamentals-quarterly-table", "data"),
        Output("fundamentals-concept-summary", "children"),
        Input("fundamentals-metric-selector", "value"),
        Input("fundamentals-period-selector", "value"),
        Input("fundamentals-confidence-filter", "value"),
        Input("store-valuation-dataset", "data"),
    )
    def render_fundamentals(
        metric_code: str,
        period_selector: str,
        confidence_filter: str,
        dataset: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        if not dataset:
            msg = "No fundamentals loaded yet. Refresh a ticker from the Overview page."
            return [], [], msg
        annual = list(dataset.get("tables", {}).get("annual", []))
        quarterly = list(dataset.get("tables", {}).get("quarterly", []))
        annual = [row for row in annual if row.get("metric_code") == metric_code]
        quarterly = [row for row in quarterly if row.get("metric_code") == metric_code]
        if confidence_filter != "all":
            annual = [row for row in annual if row.get("confidence") == confidence_filter]
            quarterly = [row for row in quarterly if row.get("confidence") == confidence_filter]
        if period_selector == "annual":
            quarterly = []
        elif period_selector == "quarterly":
            annual = []
        concept_set = sorted(
            {
                str(row.get("concept", ""))
                for row in (annual + quarterly)
                if str(row.get("concept", "")).strip()
            }
        )
        summary = "Concepts used: " + (", ".join(concept_set) if concept_set else "none")
        return annual, quarterly, summary

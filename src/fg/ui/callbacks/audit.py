"""Audit callbacks."""

from __future__ import annotations

from typing import Any

from dash import Input, Output, callback, html

from fg.services.audit_service import build_audit_view
from fg.settings import get_settings


def register_callbacks() -> None:
    """Register audit callbacks."""

    @callback(
        Output("audit-lineage-table", "data"),
        Output("audit-quality-table", "data"),
        Output("audit-source-meta-panel", "children"),
        Output("audit-methodology-card", "children"),
        Input("store-valuation-dataset", "data"),
    )
    def render_audit(dataset: dict[str, Any] | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Any, str]:
        if not dataset or not dataset.get("meta", {}).get("ticker"):
            msg = "No audit data loaded yet. Refresh a ticker from the Overview page."
            return [], [], html.Div(), msg
        ticker = str(dataset["meta"]["ticker"])
        payload = build_audit_view(get_settings(), ticker=ticker)
        source_children = html.Ul(
            [
                html.Li(
                    f"{row.get('source_name')}: {row.get('freshness_status')} (last {row.get('last_pull_at')})"
                )
                for row in payload.get("source_meta", [])
            ]
        )
        return (
            list(payload.get("lineage", [])),
            list(payload.get("quality", [])),
            source_children,
            str(payload.get("methodology", "")),
        )

"""Audit page layout."""

from __future__ import annotations

import dash
from dash import html

from fg.ui.components.tables import build_table


def layout() -> html.Div:
    """Render audit page."""
    return html.Div(
        children=[
            html.H2("Audit"),
            html.Div(
                id="audit-methodology-card",
                className="row",
                children="No audit data loaded yet. Refresh a ticker from the Overview page.",
            ),
            build_table(
                "audit-lineage-table",
                columns=[
                    "metric",
                    "period",
                    "value",
                    "unit",
                    "confidence",
                    "taxonomy",
                    "concept",
                    "form_type",
                    "filed_at",
                    "accession_no",
                    "source_name",
                ],
            ),
            html.Div(style={"height": "12px"}),
            build_table(
                "audit-quality-table",
                columns=["severity", "issue_code", "metric_code", "period_key", "message", "detected_at"],
            ),
            html.Div(id="audit-source-meta-panel", className="row"),
        ]
    )


dash.register_page(__name__, path="/audit", name="Audit", layout=layout)

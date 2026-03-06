"""KPI card builders."""

from __future__ import annotations

from typing import Any

from dash import html


def _fmt_currency(value: Any) -> str:
    return "—" if value is None else f"${float(value):,.2f}"


def _fmt_eps(value: Any) -> str:
    return "—" if value is None else f"{float(value):.2f}"


def _fmt_pe(value: Any) -> str:
    return "N/M" if value is None else f"{float(value):.2f}"


def _fmt_pct(value: Any) -> str:
    return "—" if value is None else f"{float(value) * 100:.1f}%"


def build_kpi_cards(kpis: dict[str, Any]) -> html.Div:
    """Build KPI card grid."""
    cards = [
        ("Last Price", _fmt_currency(kpis.get("last_price"))),
        ("Latest Actual EPS", _fmt_eps(kpis.get("latest_actual_eps"))),
        ("Current P/E", _fmt_pe(kpis.get("current_pe"))),
        ("Selected P/E", _fmt_pe(kpis.get("selected_pe"))),
        ("Fair Value Now", _fmt_currency(kpis.get("fair_value_now"))),
        ("Valuation Gap", _fmt_pct(kpis.get("valuation_gap_pct"))),
        ("Last Filing Date", str(kpis.get("last_filing_date") or "—")),
        ("Last Estimate Snapshot Date", str(kpis.get("last_estimate_snapshot_date") or "—")),
        ("Data Quality Score", str(kpis.get("data_quality_score") or "—")),
    ]
    return html.Div(
        className="card-grid",
        children=[
            html.Div(className="kpi-card", children=[html.Div(label), html.Strong(value)])
            for label, value in cards
        ],
    )

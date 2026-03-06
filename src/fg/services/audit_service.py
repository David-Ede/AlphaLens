"""Audit-page view assembly helpers."""

from __future__ import annotations

from typing import Any

from fg.settings import Settings
from fg.storage.repositories import read_table


def build_audit_view(settings: Settings, ticker: str) -> dict[str, Any]:
    """Build audit page payload for ticker."""
    dim = read_table(settings, "silver", "dim_company")
    if dim.empty or "ticker" not in dim.columns:
        return {
            "lineage": [],
            "quality": [],
            "source_meta": {},
            "methodology": "No audit data loaded yet. Refresh a ticker from the Overview page.",
        }
    row = dim[dim["ticker"] == ticker.upper()]
    if row.empty:
        return {
            "lineage": [],
            "quality": [],
            "source_meta": {},
            "methodology": "No audit data loaded yet. Refresh a ticker from the Overview page.",
        }
    company_key = str(row.iloc[-1]["company_key"])
    audit = read_table(settings, "gold", "mart_audit_grid", key=company_key)
    quality = read_table(settings, "silver", "fact_quality_issue", key=company_key)
    source = read_table(settings, "gold", "mart_source_freshness", key=company_key)
    return {
        "lineage": audit.to_dict(orient="records"),
        "quality": quality.to_dict(orient="records"),
        "source_meta": source.to_dict(orient="records"),
        "methodology": (
            "Valuation uses SEC annual EPS actuals plus selected P/E method. "
            "Lineage rows include concept, filed date, accession number, and source."
        ),
    }

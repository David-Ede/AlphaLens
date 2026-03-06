"""Lineage helpers for audit datasets."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


def build_lineage_row(
    entity_type: str,
    entity_id: str,
    source_name: str,
    source_endpoint: str,
    source_locator: str,
    raw_record_hash: str,
    transform_version: str = "0.1.0",
) -> dict[str, Any]:
    """Build one lineage row."""
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "source_name": source_name,
        "source_endpoint": source_endpoint,
        "source_locator": source_locator,
        "raw_record_hash": raw_record_hash,
        "transform_version": transform_version,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def build_audit_grid(facts_df: pd.DataFrame) -> pd.DataFrame:
    """Build UI-friendly audit grid from canonical facts."""
    if facts_df.empty:
        return pd.DataFrame(
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
            ]
        )
    grid = pd.DataFrame(
        {
            "metric": facts_df["metric_code"],
            "period": facts_df["period_end_date"],
            "value": facts_df["value"],
            "unit": facts_df["unit"],
            "confidence": facts_df["confidence"],
            "taxonomy": facts_df["taxonomy"],
            "concept": facts_df["concept"],
            "form_type": facts_df["form_type"],
            "filed_at": facts_df["filed_at"],
            "accession_no": facts_df["accession_no"],
            "source_name": facts_df["source_name"],
        }
    )
    return grid.sort_values(["period", "metric"]).reset_index(drop=True)



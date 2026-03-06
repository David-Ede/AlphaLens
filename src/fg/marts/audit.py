"""Build audit mart from canonical facts and quality issues."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from fg.domain.lineage import build_audit_grid
from fg.settings import Settings
from fg.storage.repositories import read_table, upsert_table


def build_audit_mart(settings: Settings, company_key: str) -> pd.DataFrame:
    """Build UI-ready audit grid with lineage columns."""
    annual = read_table(settings, "silver", "fact_fundamental_annual", key=company_key)
    quarterly = read_table(settings, "silver", "fact_fundamental_quarterly", key=company_key)
    frames = [frame for frame in [annual, quarterly] if not frame.empty]
    if not frames:
        audit = pd.DataFrame(
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
                "built_at",
            ]
        )
    else:
        facts = pd.concat(frames, ignore_index=True)
        audit = build_audit_grid(facts)
        audit["built_at"] = datetime.now(tz=timezone.utc).isoformat()
    upsert_table(
        settings=settings,
        layer="gold",
        table_name="mart_audit_grid",
        key=company_key,
        df=audit,
        dedupe_keys=["metric", "period", "concept", "accession_no"],
    )
    return audit



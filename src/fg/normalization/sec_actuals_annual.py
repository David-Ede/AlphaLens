"""Normalize SEC companyfacts payload into annual canonical facts."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import pandas as pd

from fg.domain.periods import is_annual_form
from fg.settings import Settings
from fg.storage.repositories import upsert_table


def _iter_annual_rows(companyfacts: dict[str, Any]) -> list[dict[str, Any]]:
    annual = companyfacts.get("annual_facts", [])
    if annual:
        return [row for row in annual if isinstance(row, dict)]
    generic = companyfacts.get("facts", [])
    return [row for row in generic if row.get("period_type") == "annual"]


def normalize_sec_annual(
    settings: Settings,
    company_key: str,
    companyfacts: dict[str, Any],
) -> pd.DataFrame:
    """Normalize annual SEC facts and persist to silver."""
    rows: list[dict[str, Any]] = []
    ingested_at = datetime.now(tz=timezone.utc).isoformat()
    for record in _iter_annual_rows(companyfacts):
        duration_days = int(record.get("duration_days", 365))
        form_type = str(record.get("form_type", "10-K"))
        if not is_annual_form(form_type, duration_days):
            continue
        raw_hash = sha256(str(record).encode("utf-8")).hexdigest()
        rows.append(
            {
                "company_key": company_key,
                "metric_code": str(record["metric_code"]),
                "fiscal_year": int(record["fiscal_year"]),
                "period_end_date": str(record["period_end_date"]),
                "duration_days": duration_days,
                "value": float(record["value"]),
                "unit": str(record.get("unit", "USD")),
                "form_type": form_type,
                "filed_at": str(record.get("filed_at", record["period_end_date"])),
                "accession_no": str(record.get("accession_no", "")),
                "taxonomy": str(record.get("taxonomy", "us-gaap")),
                "concept": str(record.get("concept", "")),
                "confidence": str(record.get("confidence", "reported")),
                "amended": bool(record.get("amended", False)),
                "source_name": "sec",
                "raw_record_hash": raw_hash,
                "ingested_at": ingested_at,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["metric_code", "fiscal_year", "filed_at"]).drop_duplicates(
            subset=["company_key", "metric_code", "fiscal_year"],
            keep="last",
        )
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_fundamental_annual",
        key=company_key,
        df=df,
        dedupe_keys=["company_key", "metric_code", "fiscal_year"],
    )
    return df



"""Estimate normalization from FMP snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import pandas as pd

from fg.settings import Settings
from fg.storage.repositories import upsert_table

ESTIMATE_COLUMNS = [
    "company_key",
    "as_of_date",
    "target_period_type",
    "target_fiscal_year",
    "target_period_end_date",
    "metric_code",
    "mean_value",
    "high_value",
    "low_value",
    "analyst_count",
    "unit",
    "currency",
    "source_name",
    "raw_record_hash",
    "ingested_at",
]


def normalize_estimates(
    settings: Settings,
    company_key: str,
    payload: dict[str, Any],
) -> pd.DataFrame:
    """Normalize annual estimate snapshot payload into silver facts."""
    as_of_date = str(payload.get("as_of_date", datetime.now(tz=timezone.utc).date().isoformat()))
    rows = payload.get("rows", [])
    normalized: list[dict[str, Any]] = []
    for row in rows:
        fiscal_year = int(row.get("target_fiscal_year", row.get("fiscalYear", 0)))
        if fiscal_year == 0:
            continue
        period_end = str(
            row.get(
                "target_period_end_date",
                row.get("periodEndDate", f"{fiscal_year}-12-31"),
            )
        )
        mean_value = float(row.get("mean_value", row.get("epsMean", 0.0)))
        normalized.append(
            {
                "company_key": company_key,
                "as_of_date": as_of_date,
                "target_period_type": "annual",
                "target_fiscal_year": fiscal_year,
                "target_period_end_date": period_end,
                "metric_code": "eps_estimate_mean",
                "mean_value": mean_value,
                "high_value": row.get("high_value", row.get("epsHigh")),
                "low_value": row.get("low_value", row.get("epsLow")),
                "analyst_count": row.get("analyst_count", row.get("analystCount")),
                "unit": "USD/share",
                "currency": "USD",
                "source_name": "fmp",
                "raw_record_hash": sha256(str(row).encode("utf-8")).hexdigest(),
                "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
            }
        )
    df = pd.DataFrame(normalized)
    if df.empty:
        df = pd.DataFrame(columns=ESTIMATE_COLUMNS)
    if not df.empty:
        df = df[df["target_fiscal_year"] >= datetime.now(tz=timezone.utc).year - 1]
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_estimate_snapshot",
        key=company_key,
        df=df,
        dedupe_keys=["company_key", "as_of_date", "target_period_end_date", "metric_code"],
    )
    return df



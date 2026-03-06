"""Build source freshness mart."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from fg.domain.quality import evaluate_freshness
from fg.settings import Settings
from fg.storage.repositories import read_table, upsert_table


def build_source_freshness_mart(settings: Settings, company_key: str) -> pd.DataFrame:
    """Build freshness rows from dim_company timestamps."""
    dim = read_table(settings, "silver", "dim_company", key=company_key)
    if dim.empty:
        return pd.DataFrame()
    row = dim.iloc[-1]
    thresholds = settings.app_config["freshness"]
    data = pd.DataFrame(
        [
            {
                "company_key": company_key,
                "source_name": "sec",
                "last_pull_at": row.get("last_sec_pull_at"),
                "freshness_status": evaluate_freshness(
                    str(row.get("last_sec_pull_at")), int(thresholds["sec_days"])
                ).value,
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            },
            {
                "company_key": company_key,
                "source_name": "yahoo",
                "last_pull_at": row.get("last_yahoo_pull_at"),
                "freshness_status": evaluate_freshness(
                    str(row.get("last_yahoo_pull_at")), int(thresholds["price_days"])
                ).value,
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            },
            {
                "company_key": company_key,
                "source_name": "fmp",
                "last_pull_at": row.get("last_fmp_pull_at"),
                "freshness_status": evaluate_freshness(
                    str(row.get("last_fmp_pull_at")), int(thresholds["estimate_days"])
                ).value,
                "built_at": datetime.now(tz=timezone.utc).isoformat(),
            },
        ]
    )
    upsert_table(
        settings=settings,
        layer="gold",
        table_name="mart_source_freshness",
        key=company_key,
        df=data,
        dedupe_keys=["company_key", "source_name"],
    )
    return data



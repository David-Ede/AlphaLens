"""Build annual EPS bar mart for actuals and estimates."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from fg.settings import Settings
from fg.storage.repositories import read_table, upsert_table


def build_eps_bars_mart(
    settings: Settings,
    company_key: str,
    lookback_years: int,
) -> pd.DataFrame:
    """Build `mart_eps_bars` for selected company/lookback."""
    annual = read_table(settings, "silver", "fact_fundamental_annual", key=company_key)
    annual = annual[annual["metric_code"] == "eps_diluted_actual"] if not annual.empty else annual
    annual = annual.sort_values("fiscal_year").tail(lookback_years)
    estimates = read_table(settings, "silver", "fact_estimate_snapshot", key=company_key)

    rows: list[dict[str, object]] = []
    built_at = datetime.now(tz=timezone.utc).isoformat()
    for row in annual.itertuples(index=False):
        rows.append(
            {
                "company_key": company_key,
                "lookback_years": lookback_years,
                "fiscal_year": int(row.fiscal_year),
                "period_end_date": str(row.period_end_date),
                "eps_actual": float(row.value),
                "eps_estimate": None,
                "is_estimate": False,
                "confidence": str(row.confidence),
                "concept": str(row.concept),
                "filed_at": str(row.filed_at),
                "snapshot_date": None,
                "built_at": built_at,
            }
        )
    for row in estimates.itertuples(index=False):
        rows.append(
            {
                "company_key": company_key,
                "lookback_years": lookback_years,
                "fiscal_year": int(row.target_fiscal_year),
                "period_end_date": str(row.target_period_end_date),
                "eps_actual": None,
                "eps_estimate": float(row.mean_value),
                "is_estimate": True,
                "confidence": "estimate",
                "concept": "vendor_estimate",
                "filed_at": None,
                "snapshot_date": str(row.as_of_date),
                "built_at": built_at,
            }
        )
    df = pd.DataFrame(rows).sort_values(["fiscal_year", "is_estimate"])
    upsert_table(
        settings=settings,
        layer="gold",
        table_name="mart_eps_bars",
        key=f"{company_key}_{lookback_years}",
        df=df,
        dedupe_keys=["fiscal_year", "is_estimate"],
    )
    return df



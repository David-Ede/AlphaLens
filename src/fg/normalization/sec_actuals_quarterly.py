"""Normalize SEC payload into quarterly canonical facts and derived TTM rows."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import pandas as pd

from fg.domain.periods import build_ttm_from_quarters, derive_q4_from_annual, is_quarterly_form
from fg.settings import Settings
from fg.storage.repositories import read_table, upsert_table


def _iter_quarterly_rows(companyfacts: dict[str, Any]) -> list[dict[str, Any]]:
    quarterly = companyfacts.get("quarterly_facts", [])
    if quarterly:
        return [row for row in quarterly if isinstance(row, dict)]
    generic = companyfacts.get("facts", [])
    return [row for row in generic if row.get("period_type") == "quarterly"]


def normalize_sec_quarterly(
    settings: Settings,
    company_key: str,
    companyfacts: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize quarterly SEC facts and derive TTM from standalone quarters."""
    rows: list[dict[str, Any]] = []
    ingested_at = datetime.now(tz=timezone.utc).isoformat()
    for record in _iter_quarterly_rows(companyfacts):
        duration_days = int(record.get("duration_days", 90))
        form_type = str(record.get("form_type", "10-Q"))
        if not is_quarterly_form(form_type, duration_days):
            continue
        rows.append(
            {
                "company_key": company_key,
                "metric_code": str(record["metric_code"]),
                "fiscal_year": int(record["fiscal_year"]),
                "fiscal_quarter": int(record["fiscal_quarter"]),
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
                "raw_record_hash": sha256(str(record).encode("utf-8")).hexdigest(),
                "ingested_at": ingested_at,
            }
        )
    qdf = pd.DataFrame(rows)
    if not qdf.empty:
        qdf = qdf.sort_values(["metric_code", "fiscal_year", "fiscal_quarter", "filed_at"]).drop_duplicates(
            subset=["company_key", "metric_code", "fiscal_year", "fiscal_quarter"],
            keep="last",
        )
    # Derive Q4 if annual exists and Q4 is missing for EPS.
    annual = read_table(settings, "silver", "fact_fundamental_annual", key=company_key)
    if not qdf.empty and not annual.empty:
        qdf = _derive_q4_rows(qdf, annual)
    ttm = build_ttm_from_quarters(
        qdf[qdf["metric_code"] == "eps_diluted_actual"].copy() if not qdf.empty else qdf
    )
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_fundamental_quarterly",
        key=company_key,
        df=qdf,
        dedupe_keys=["company_key", "metric_code", "fiscal_year", "fiscal_quarter"],
    )
    if not ttm.empty:
        upsert_table(
            settings=settings,
            layer="silver",
            table_name="fact_fundamental_ttm",
            key=company_key,
            df=ttm,
            dedupe_keys=["company_key", "metric_code", "period_end_date"],
        )
    return qdf, ttm


def _derive_q4_rows(quarterly_df: pd.DataFrame, annual_df: pd.DataFrame) -> pd.DataFrame:
    """Derive Q4 standalone row where annual and Q1-Q3 exist but Q4 missing."""
    derived_rows: list[dict[str, Any]] = []
    eps_annual = annual_df[annual_df["metric_code"] == "eps_diluted_actual"]
    eps_quarterly = quarterly_df[quarterly_df["metric_code"] == "eps_diluted_actual"]
    for year in sorted(set(eps_annual["fiscal_year"].tolist())):
        year_annual = eps_annual[eps_annual["fiscal_year"] == year]
        year_quarterly = eps_quarterly[eps_quarterly["fiscal_year"] == year]
        if year_annual.empty or len(year_quarterly) < 3:
            continue
        present = set(int(q) for q in year_quarterly["fiscal_quarter"].tolist())
        if 4 in present:
            continue
        if not {1, 2, 3}.issubset(present):
            continue
        annual_val = float(year_annual.iloc[-1]["value"])
        q1 = float(year_quarterly[year_quarterly["fiscal_quarter"] == 1].iloc[-1]["value"])
        q2 = float(year_quarterly[year_quarterly["fiscal_quarter"] == 2].iloc[-1]["value"])
        q3 = float(year_quarterly[year_quarterly["fiscal_quarter"] == 3].iloc[-1]["value"])
        q4_val = derive_q4_from_annual(annual_val, q1, q2, q3)
        template = year_quarterly.iloc[-1].to_dict()
        template["fiscal_quarter"] = 4
        template["value"] = q4_val
        template["confidence"] = "derived"
        template["raw_record_hash"] = sha256(
            f"{template['company_key']}-{template['metric_code']}-{year}-Q4".encode()
        ).hexdigest()
        derived_rows.append(template)
    if not derived_rows:
        return quarterly_df
    return pd.concat([quarterly_df, pd.DataFrame(derived_rows)], ignore_index=True)



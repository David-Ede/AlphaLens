"""Typed schema validation helpers for persisted tables."""

from __future__ import annotations

from typing import Any

import pandas as pd

SCHEMA_COLUMNS: dict[str, list[str]] = {
    "dim_company": [
        "company_key",
        "cik",
        "ticker",
        "issuer_name",
        "exchange",
        "fiscal_year_end_mmdd",
        "currency",
        "last_sec_pull_at",
        "last_yahoo_pull_at",
        "last_fmp_pull_at",
    ],
    "fact_fundamental_annual": [
        "company_key",
        "metric_code",
        "fiscal_year",
        "period_end_date",
        "duration_days",
        "value",
        "unit",
        "form_type",
        "filed_at",
        "accession_no",
        "taxonomy",
        "concept",
        "confidence",
        "amended",
        "source_name",
        "raw_record_hash",
        "ingested_at",
    ],
    "fact_fundamental_quarterly": [
        "company_key",
        "metric_code",
        "fiscal_year",
        "fiscal_quarter",
        "period_end_date",
        "duration_days",
        "value",
        "unit",
        "form_type",
        "filed_at",
        "accession_no",
        "taxonomy",
        "concept",
        "confidence",
        "amended",
        "source_name",
        "raw_record_hash",
        "ingested_at",
    ],
}


def validate_columns(df: pd.DataFrame, table_name: str) -> None:
    """Validate dataframe contains required columns."""
    required = SCHEMA_COLUMNS.get(table_name, [])
    missing = [col for col in required if col not in df.columns]
    if missing:
        msg = f"Table {table_name} missing required columns: {missing}"
        raise ValueError(msg)


def build_empty(table_name: str) -> pd.DataFrame:
    """Build empty dataframe for known table."""
    return pd.DataFrame(columns=SCHEMA_COLUMNS.get(table_name, []))


def dedupe_by_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Drop duplicates by keys, keeping latest row."""
    if df.empty:
        return df
    return df.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)


def to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert dataframe to records."""
    return df.to_dict(orient="records")

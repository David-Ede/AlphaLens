"""CSV/XLSX export builders."""

from __future__ import annotations

from io import BytesIO
from typing import Literal

import pandas as pd

from fg.settings import Settings
from fg.storage.repositories import read_table, write_export

EXPORT_COLUMNS = [
    "ticker",
    "company_key",
    "metric_code",
    "period_type",
    "fiscal_year",
    "fiscal_quarter",
    "period_end_date",
    "value",
    "unit",
    "source_name",
    "confidence",
    "concept",
    "filed_at",
    "accession_no",
]


def build_export_frame(settings: Settings, ticker: str) -> pd.DataFrame:
    """Build merged export dataframe with fixed column order."""
    ticker_up = ticker.upper()
    dim = read_table(settings, "silver", "dim_company")
    row = dim[dim["ticker"] == ticker_up]
    if row.empty:
        return pd.DataFrame(columns=EXPORT_COLUMNS)
    company_key = str(row.iloc[-1]["company_key"])
    annual = read_table(settings, "silver", "fact_fundamental_annual", key=company_key).copy()
    quarterly = read_table(settings, "silver", "fact_fundamental_quarterly", key=company_key).copy()
    annual["period_type"] = "annual"
    annual["fiscal_quarter"] = None
    quarterly["period_type"] = "quarterly"
    combined = pd.concat([annual, quarterly], ignore_index=True) if not quarterly.empty else annual
    if combined.empty:
        return pd.DataFrame(columns=EXPORT_COLUMNS)
    combined["ticker"] = ticker_up
    return combined[EXPORT_COLUMNS].sort_values(["metric_code", "period_type", "fiscal_year", "fiscal_quarter"])


def export_bytes(frame: pd.DataFrame, output_format: Literal["csv", "xlsx"]) -> bytes:
    """Build in-memory bytes for export format."""
    if output_format == "csv":
        return frame.to_csv(index=False).encode("utf-8")
    buffer = BytesIO()
    frame.to_excel(buffer, index=False, engine="openpyxl")
    return buffer.getvalue()


def write_export_file(
    settings: Settings,
    ticker: str,
    output_format: Literal["csv", "xlsx"],
) -> str:
    """Write export payload to data/exports and return output path."""
    frame = build_export_frame(settings, ticker)
    payload = export_bytes(frame, output_format)
    path = write_export(settings, f"{ticker.upper()}_export.{output_format}", payload)
    return str(path)

"""Normalization-stage quality checks."""

from __future__ import annotations

from typing import Any

import pandas as pd

from fg.domain.enums import Severity
from fg.domain.quality import create_quality_issue
from fg.settings import Settings
from fg.storage.repositories import upsert_table


def run_quality_checks(
    settings: Settings,
    company_key: str,
    annual_df: pd.DataFrame,
    price_df: pd.DataFrame,
    estimate_df: pd.DataFrame,
    warnings: list[str] | None = None,
) -> pd.DataFrame:
    """Run hard/soft data quality checks and persist issues."""
    issues: list[dict[str, Any]] = []
    if annual_df.empty:
        issues.append(
            create_quality_issue(
                company_key=company_key,
                issue_code="missing_annual_eps",
                metric_code="eps_diluted_actual",
                message="No annual EPS actuals were found for the selected lookback.",
                severity=Severity.ERROR,
            )
        )
    if price_df.empty:
        issues.append(
            create_quality_issue(
                company_key=company_key,
                issue_code="missing_price_history",
                metric_code="price_close_split_adjusted",
                message="No market price history is available for this ticker.",
                severity=Severity.ERROR,
            )
        )
    if estimate_df.empty:
        issues.append(
            create_quality_issue(
                company_key=company_key,
                issue_code="missing_forward_estimates",
                metric_code="eps_estimate_mean",
                message="Forward estimates unavailable; showing actuals only.",
                severity=Severity.WARNING,
            )
        )
    for code in warnings or []:
        issues.append(
            create_quality_issue(
                company_key=company_key,
                issue_code=code,
                metric_code="eps_diluted_actual",
                message=code.replace("_", " "),
                severity=Severity.WARNING,
            )
        )
    issue_df = pd.DataFrame(issues)
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_quality_issue",
        key=company_key,
        df=issue_df,
        dedupe_keys=["company_key", "issue_code", "period_key"],
    )
    return issue_df

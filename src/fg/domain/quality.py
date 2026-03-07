"""Quality issue and freshness helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd

from fg.domain.enums import FreshnessStatus, Severity


def create_quality_issue(
    company_key: str,
    issue_code: str,
    metric_code: str,
    message: str,
    severity: Severity = Severity.WARNING,
    period_key: str = "global",
) -> dict[str, Any]:
    """Create a quality issue dict."""
    return {
        "company_key": company_key,
        "severity": severity.value,
        "issue_code": issue_code,
        "metric_code": metric_code,
        "period_key": period_key,
        "message": message,
        "detected_at": datetime.now(tz=timezone.utc).date().isoformat(),
        "resolved_at": None,
    }


def evaluate_freshness(last_pull_at: str | None, max_age_days: int) -> FreshnessStatus:
    """Evaluate freshness status against age threshold."""
    if not last_pull_at or str(last_pull_at).strip().lower() in {"none", "nan", "nat"}:
        return FreshnessStatus.UNKNOWN
    pull_date = date.fromisoformat(last_pull_at[:10])
    age = (datetime.now(tz=timezone.utc).date() - pull_date).days
    return FreshnessStatus.FRESH if age <= max_age_days else FreshnessStatus.STALE


def compute_quality_score(
    issues_df: pd.DataFrame,
    latest_eps_confidence: str | None,
    sec_freshness: FreshnessStatus,
    price_freshness: FreshnessStatus,
    estimate_freshness: FreshnessStatus,
) -> int:
    """Compute quality score with deterministic penalties."""
    score = 100
    if latest_eps_confidence == "derived":
        score -= 30
    if sec_freshness == FreshnessStatus.STALE:
        score -= 20
    if price_freshness == FreshnessStatus.STALE:
        score -= 15
    if estimate_freshness == FreshnessStatus.STALE:
        score -= 10
    unresolved_warnings = 0 if issues_df.empty else int((issues_df["severity"] != "error").sum())
    if unresolved_warnings > 1:
        score -= 10
    return max(score, 0)



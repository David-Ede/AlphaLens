"""Unit tests for quality helpers and audit service."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from fg.domain.enums import FreshnessStatus
from fg.domain.metrics import is_known_metric
from fg.domain.quality import compute_quality_score, evaluate_freshness


def test_evaluate_freshness_statuses() -> None:
    assert evaluate_freshness(None, 10) == FreshnessStatus.UNKNOWN
    fresh_date = date.today().isoformat()
    stale_date = (date.today() - timedelta(days=30)).isoformat()
    assert evaluate_freshness(fresh_date, 10) == FreshnessStatus.FRESH
    assert evaluate_freshness(stale_date, 10) == FreshnessStatus.STALE


def test_compute_quality_score_penalties() -> None:
    issues = pd.DataFrame([{"severity": "warning"}, {"severity": "warning"}])
    score = compute_quality_score(
        issues_df=issues,
        latest_eps_confidence="derived",
        sec_freshness=FreshnessStatus.STALE,
        price_freshness=FreshnessStatus.STALE,
        estimate_freshness=FreshnessStatus.STALE,
    )
    assert score <= 15


def test_known_metric_lookup() -> None:
    assert is_known_metric("eps_diluted_actual")
    assert not is_known_metric("unknown_metric")

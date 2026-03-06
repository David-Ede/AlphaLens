"""Pydantic domain models for AlphaLens."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from fg.domain.enums import Confidence, PEMethod, PeriodType, Severity


class CompanyRef(BaseModel):
    """Issuer reference keyed by CIK."""

    company_key: str
    ticker: str
    issuer_name: str
    exchange: str = "UNKNOWN"
    fiscal_year_end_mmdd: str = "1231"
    currency: str = "USD"
    active: bool = True


class CanonicalFact(BaseModel):
    """Canonical fact row used by annual/quarterly/ttm tables."""

    company_key: str
    metric_code: str
    period_type: PeriodType
    fiscal_year: int
    fiscal_quarter: int | None = None
    period_end_date: str
    duration_days: int
    value: float
    unit: str
    source_name: str = "sec"
    taxonomy: str = "us-gaap"
    concept: str
    form_type: str
    filed_at: str
    accession_no: str
    confidence: Confidence = Confidence.REPORTED
    amended: bool = False
    raw_record_hash: str


class PriceBar(BaseModel):
    """Daily price bar."""

    company_key: str
    ticker: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    split_adjusted_close: float
    volume: float
    currency: str = "USD"
    source_name: str = "yahoo"


class CorporateAction(BaseModel):
    """Corporate action row."""

    company_key: str
    action_type: str
    action_date: str
    cash_value: float | None = None
    split_ratio: float | None = None
    source_name: str = "yahoo"


class EstimateSnapshot(BaseModel):
    """Estimate snapshot row."""

    company_key: str
    as_of_date: str
    target_period_type: str = "annual"
    target_fiscal_year: int
    target_period_end_date: str
    metric_code: str = "eps_estimate_mean"
    mean_value: float
    high_value: float | None = None
    low_value: float | None = None
    analyst_count: int | None = None
    unit: str = "USD/share"
    currency: str = "USD"
    source_name: str = "fmp"
    raw_record_hash: str


class ValuationPoint(BaseModel):
    """One chart point in a valuation series."""

    company_key: str
    series_name: str
    x_date: str
    y_value: float | None
    is_estimate: bool = False
    lookback_years: int
    pe_method: PEMethod
    display_style: str
    tooltip_payload: dict[str, Any] = Field(default_factory=dict)


class QualityIssue(BaseModel):
    """Quality issue raised during transforms."""

    company_key: str
    severity: Severity
    issue_code: str
    metric_code: str
    period_key: str
    message: str
    detected_at: str = Field(default_factory=lambda: datetime.now(tz=timezone.utc).date().isoformat())


class RefreshRequest(BaseModel):
    """Canonical refresh request object."""

    ticker: str
    lookback_years: int = 20
    pe_method: PEMethod = PEMethod.STATIC_15
    manual_pe: float | None = None
    show_estimates: bool = True


class KpiSnapshot(BaseModel):
    """KPI payload used by overview cards."""

    last_price: float | None = None
    latest_actual_eps: float | None = None
    current_pe: float | None = None
    selected_pe: float | None = None
    fair_value_now: float | None = None
    valuation_gap_pct: float | None = None
    last_filing_date: str | None = None
    last_estimate_snapshot_date: str | None = None
    data_quality_score: int = 100


class ViewModelMeta(BaseModel):
    """Top-level view model metadata."""

    ticker: str
    company_key: str
    issuer_name: str
    lookback_years: int
    pe_method: PEMethod
    built_at: str


class ViewModel(BaseModel):
    """Full page dataset used by Dash callbacks."""

    meta: ViewModelMeta
    kpis: KpiSnapshot
    series: dict[str, list[dict[str, Any]]]
    tables: dict[str, list[dict[str, Any]]]
    warnings: list[str] = Field(default_factory=list)



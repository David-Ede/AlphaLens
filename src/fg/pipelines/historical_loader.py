"""Notebook-friendly historical data loading pipeline."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Any

import pandas as pd

from fg.domain.enums import PEMethod
from fg.domain.models import CompanyRef
from fg.domain.periods import is_annual_form
from fg.ingestion.fmp_ingest import empty_estimate_payload, ingest_fmp
from fg.ingestion.resolve_company import resolve_company
from fg.ingestion.sec_ingest import ingest_sec
from fg.ingestion.yahoo_ingest import ingest_yahoo
from fg.marts.audit import build_audit_mart
from fg.marts.eps_bars import build_eps_bars_mart
from fg.marts.kpi_snapshot import build_kpi_snapshot_mart
from fg.marts.source_freshness import build_source_freshness_mart
from fg.marts.valuation_series import build_valuation_series_mart
from fg.normalization.estimates import normalize_estimates
from fg.normalization.market_data import normalize_market_data
from fg.normalization.quality_checks import run_quality_checks
from fg.normalization.sec_actuals_annual import normalize_sec_annual
from fg.normalization.sec_actuals_quarterly import normalize_sec_quarterly
from fg.settings import Settings, get_settings
from fg.storage.duckdb import get_connection, query_df
from fg.storage.paths import all_table_files
from fg.storage.repositories import read_table, upsert_table

DEFAULT_DUCKDB_VIEWS: tuple[tuple[str, str, str], ...] = (
    ("bronze", "bronze_sec_submissions", "v_sec_submissions"),
    ("bronze", "bronze_sec_companyfacts", "v_sec_companyfacts"),
    ("bronze", "bronze_yahoo_prices", "v_yahoo_prices"),
    ("bronze", "bronze_yahoo_actions", "v_yahoo_actions"),
    ("bronze", "bronze_fmp_estimates", "v_fmp_estimates"),
    ("silver", "dim_company", "v_dim_company"),
    ("silver", "fact_fundamental_annual", "v_fundamental_annual"),
    ("silver", "fact_fundamental_quarterly", "v_fundamental_quarterly"),
    ("silver", "fact_fundamental_ttm", "v_fundamental_ttm"),
    ("silver", "fact_price_daily", "v_price_daily"),
    ("silver", "fact_price_monthly", "v_price_monthly"),
    ("silver", "fact_corporate_action", "v_corporate_action"),
    ("silver", "fact_estimate_snapshot", "v_estimate_snapshot"),
    ("silver", "fact_quality_issue", "v_quality_issue"),
    ("gold", "mart_valuation_series", "v_valuation_series"),
    ("gold", "mart_eps_bars", "v_eps_bars"),
    ("gold", "mart_kpi_snapshot", "v_kpi_snapshot"),
    ("gold", "mart_source_freshness", "v_source_freshness"),
    ("gold", "mart_audit_grid", "v_audit_grid"),
)

_ADDITIVE_QUARTERLY_METRICS = {"eps_diluted_actual", "revenue_actual", "net_income_actual"}


@dataclass(slots=True)
class _SecObservation:
    metric_code: str
    concept: str
    taxonomy: str
    unit: str
    value: float
    fiscal_year: int
    fiscal_quarter: int | None
    period_end_date: str
    duration_days: int
    form_type: str
    filed_at: str
    accession_no: str
    confidence: str
    amended: bool
    preferred_rank: int
    source_name: str = "sec"


def canonicalize_companyfacts_payload(
    payload: dict[str, Any],
    company: CompanyRef,
    concept_map: dict[str, Any],
    metrics_config: dict[str, Any],
) -> dict[str, Any]:
    """Convert raw SEC companyfacts JSON into the canonical fact shape used by silver normalizers."""
    if _companyfacts_is_canonical(payload):
        return payload

    observations = _extract_sec_observations(
        payload=payload,
        company=company,
        concept_map=concept_map,
        metrics_config=metrics_config,
    )
    annual_facts = _build_annual_facts(company.company_key, observations)
    quarterly_facts = _build_quarterly_facts(company.company_key, observations, annual_facts)
    annual_facts = _derive_missing_annual_eps(company.company_key, annual_facts, concept_map)
    quarterly_facts = _derive_missing_quarterly_eps(company.company_key, quarterly_facts, concept_map)

    return {
        "cik": str(payload.get("cik", company.company_key)).zfill(10),
        "entityName": str(payload.get("entityName", company.issuer_name)),
        "annual_facts": annual_facts,
        "quarterly_facts": quarterly_facts,
    }


class HistoricalDataLoader:
    """Notebook-facing loader that populates bronze, silver, and optionally gold layers."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def load_ticker(
        self,
        ticker: str,
        *,
        fixture_mode: bool = False,
        include_estimates: bool = True,
        build_gold: bool = False,
        lookback_years: int = 20,
        pe_method: PEMethod | str = PEMethod.STATIC_15,
        show_estimates: bool = True,
        manual_pe: float | None = None,
    ) -> dict[str, Any]:
        """Load one ticker end-to-end into local storage."""
        ticker_upper = ticker.upper().strip()
        pe_method_value = pe_method.value if isinstance(pe_method, PEMethod) else str(pe_method)

        company = resolve_company(
            settings=self.settings,
            ticker=ticker_upper,
            fixture_mode=fixture_mode,
        )
        submissions, companyfacts = ingest_sec(
            settings=self.settings,
            company=company,
            fixture_mode=fixture_mode,
        )
        company = self._refresh_company_metadata(company=company, submissions=submissions)
        canonical_facts = canonicalize_companyfacts_payload(
            payload=companyfacts,
            company=company,
            concept_map=self.settings.concept_map_config,
            metrics_config=self.settings.metrics_config,
        )
        prices, actions = ingest_yahoo(
            settings=self.settings,
            company=company,
            fixture_mode=fixture_mode,
        )
        estimate_warning: str | None = None
        estimate_available = include_estimates
        estimate_ingested = False
        if include_estimates:
            try:
                estimate_payload = ingest_fmp(
                    settings=self.settings,
                    company=company,
                    fixture_mode=fixture_mode or not self.settings.fmp_api_key,
                )
                estimate_ingested = True
            except Exception as exc:
                estimate_payload = empty_estimate_payload(company.ticker)
                estimate_warning = f"Forward estimates unavailable for {company.ticker}: {exc}"
                estimate_available = False
        else:
            estimate_payload = empty_estimate_payload(company.ticker)
            estimate_available = False
        self._update_pull_timestamps(company.company_key, include_fmp=estimate_available)

        annual = normalize_sec_annual(self.settings, company.company_key, canonical_facts)
        quarterly, ttm = normalize_sec_quarterly(self.settings, company.company_key, canonical_facts)
        daily, action_fact, monthly = normalize_market_data(
            self.settings,
            company.company_key,
            company.ticker,
            prices,
            actions,
        )
        estimates = normalize_estimates(self.settings, company.company_key, estimate_payload)
        issues = run_quality_checks(
            settings=self.settings,
            company_key=company.company_key,
            annual_df=annual[annual["metric_code"] == "eps_diluted_actual"] if not annual.empty else annual,
            price_df=daily,
            estimate_df=estimates,
            warnings=[],
        )

        gold_counts: dict[str, int] = {}
        if build_gold:
            series, selected_pe, valuation_warnings = build_valuation_series_mart(
                settings=self.settings,
                company_key=company.company_key,
                ticker=company.ticker,
                lookback_years=lookback_years,
                pe_method=pe_method_value,
                manual_pe=manual_pe,
                show_estimates=show_estimates,
            )
            if valuation_warnings:
                issues = run_quality_checks(
                    settings=self.settings,
                    company_key=company.company_key,
                    annual_df=annual[annual["metric_code"] == "eps_diluted_actual"] if not annual.empty else annual,
                    price_df=daily,
                    estimate_df=estimates,
                    warnings=valuation_warnings,
                )
            eps_bars = build_eps_bars_mart(self.settings, company.company_key, lookback_years)
            kpi = build_kpi_snapshot_mart(self.settings, company.company_key, company.ticker, selected_pe)
            freshness = build_source_freshness_mart(self.settings, company.company_key)
            audit = build_audit_mart(self.settings, company.company_key)
            gold_counts = {
                "gold_valuation_series": len(series),
                "gold_eps_bars": len(eps_bars),
                "gold_kpi_snapshot": len(kpi),
                "gold_source_freshness": len(freshness),
                "gold_audit_grid": len(audit),
            }

        warning_messages = []
        if not issues.empty and "message" in issues.columns:
            warning_messages = [
                str(message)
                for message in issues[issues["severity"] != "error"]["message"].dropna().tolist()
            ]
        if estimate_warning:
            warning_messages.append(estimate_warning)

        result = {
            "status": "ok",
            "ticker": company.ticker,
            "company_key": company.company_key,
            "issuer_name": company.issuer_name,
            "built_at": datetime.now(tz=timezone.utc).isoformat(),
            "warnings": warning_messages,
            "rows": {
                "bronze_sec_submissions": 1 if submissions else 0,
                "bronze_sec_companyfacts": 1 if companyfacts else 0,
                "bronze_yahoo_prices": len(prices),
                "bronze_yahoo_actions": len(actions),
                "bronze_fmp_estimates": 1 if estimate_ingested else 0,
                "silver_annual": len(annual),
                "silver_quarterly": len(quarterly),
                "silver_ttm": len(ttm),
                "silver_price_daily": len(daily),
                "silver_price_monthly": len(monthly),
                "silver_corporate_action": len(action_fact),
                "silver_estimate_snapshot": len(estimates),
                "silver_quality_issue": len(issues),
                **gold_counts,
            },
        }
        return result

    def load_tickers(
        self,
        tickers: list[str],
        *,
        fixture_mode: bool = False,
        include_estimates: bool = True,
        build_gold: bool = False,
        lookback_years: int = 20,
        pe_method: PEMethod | str = PEMethod.STATIC_15,
        show_estimates: bool = True,
        manual_pe: float | None = None,
        continue_on_error: bool = True,
    ) -> pd.DataFrame:
        """Load multiple tickers and return one summary row per ticker."""
        rows: list[dict[str, Any]] = []
        for raw_ticker in tickers:
            ticker = raw_ticker.upper().strip()
            if not ticker:
                continue
            try:
                result = self.load_ticker(
                    ticker=ticker,
                    fixture_mode=fixture_mode,
                    include_estimates=include_estimates,
                    build_gold=build_gold,
                    lookback_years=lookback_years,
                    pe_method=pe_method,
                    show_estimates=show_estimates,
                    manual_pe=manual_pe,
                )
                rows.append(
                    {
                        "ticker": result["ticker"],
                        "company_key": result["company_key"],
                        "status": result["status"],
                        "issuer_name": result["issuer_name"],
                        "warning_count": len(result["warnings"]),
                        **result["rows"],
                    }
                )
            except Exception as exc:
                if not continue_on_error:
                    raise
                rows.append(
                    {
                        "ticker": ticker,
                        "company_key": None,
                        "status": "error",
                        "issuer_name": None,
                        "warning_count": 0,
                        "error_message": str(exc),
                    }
                )
        return pd.DataFrame(rows)

    def table_inventory(self) -> pd.DataFrame:
        """Return row and file counts for the main persisted tables."""
        rows: list[dict[str, Any]] = []
        for layer, table_name, _view_name in DEFAULT_DUCKDB_VIEWS:
            files = all_table_files(self.settings, layer, table_name)
            frame = read_table(self.settings, layer, table_name) if files else pd.DataFrame()
            rows.append(
                {
                    "layer": layer,
                    "table_name": table_name,
                    "file_count": len(files),
                    "row_count": len(frame),
                }
            )
        return pd.DataFrame(rows).sort_values(["layer", "table_name"]).reset_index(drop=True)

    def register_duckdb_views(self) -> list[str]:
        """Register helper views over parquet/csv tables in the configured DuckDB database."""
        created: list[str] = []
        conn = get_connection(self.settings.data_dirs["duckdb_path"])
        try:
            for layer, table_name, view_name in DEFAULT_DUCKDB_VIEWS:
                files = all_table_files(self.settings, layer, table_name)
                if not files:
                    continue
                frame = read_table(self.settings, layer, table_name)
                if frame.columns.empty:
                    continue
                has_parquet = any(path.suffix == ".parquet" for path in files)
                source_pattern = (
                    self.settings.data_dirs[layer] / table_name / ("*.parquet" if has_parquet else "*.csv")
                ).as_posix()
                reader = (
                    f"read_parquet('{source_pattern}')"
                    if has_parquet
                    else f"read_csv_auto('{source_pattern}', header=true)"
                )
                conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {reader}")
                created.append(view_name)
        finally:
            conn.close()
        return created

    def query_duckdb(self, sql: str) -> pd.DataFrame:
        """Execute a DuckDB query against the configured database."""
        return query_df(self.settings.data_dirs["duckdb_path"], sql)

    def _refresh_company_metadata(self, company: CompanyRef, submissions: dict[str, Any]) -> CompanyRef:
        fiscal_year_end = _normalize_fiscal_year_end(
            submissions.get("fiscalYearEnd") or submissions.get("fiscal_year_end_mmdd"),
            company.fiscal_year_end_mmdd,
        )
        issuer_name = str(
            submissions.get("issuer_name")
            or submissions.get("name")
            or submissions.get("entityName")
            or company.issuer_name
        )
        exchange = str(submissions.get("exchange") or company.exchange or "UNKNOWN")
        updated = company.model_copy(
            update={
                "issuer_name": issuer_name,
                "exchange": exchange,
                "fiscal_year_end_mmdd": fiscal_year_end,
            }
        )
        dim = read_table(self.settings, "silver", "dim_company", key=company.company_key)
        if not dim.empty:
            dim.loc[:, "issuer_name"] = updated.issuer_name
            dim.loc[:, "exchange"] = updated.exchange
            dim.loc[:, "fiscal_year_end_mmdd"] = updated.fiscal_year_end_mmdd
            upsert_table(
                settings=self.settings,
                layer="silver",
                table_name="dim_company",
                key=company.company_key,
                df=dim,
                dedupe_keys=["company_key"],
            )
        return updated

    def _update_pull_timestamps(self, company_key: str, include_fmp: bool = True) -> None:
        dim = read_table(self.settings, "silver", "dim_company", key=company_key)
        if dim.empty:
            return
        today = datetime.now(tz=timezone.utc).date().isoformat()
        dim.loc[:, "last_sec_pull_at"] = today
        dim.loc[:, "last_yahoo_pull_at"] = today
        if include_fmp:
            dim.loc[:, "last_fmp_pull_at"] = today
        upsert_table(
            settings=self.settings,
            layer="silver",
            table_name="dim_company",
            key=company_key,
            df=dim,
            dedupe_keys=["company_key"],
        )


def _companyfacts_is_canonical(payload: dict[str, Any]) -> bool:
    annual = payload.get("annual_facts", [])
    quarterly = payload.get("quarterly_facts", [])
    if annual or quarterly:
        return True
    generic = payload.get("facts")
    return bool(generic) and isinstance(generic, list) and "metric_code" in generic[0]


def _extract_sec_observations(
    payload: dict[str, Any],
    company: CompanyRef,
    concept_map: dict[str, Any],
    metrics_config: dict[str, Any],
) -> list[_SecObservation]:
    facts_tree = payload.get("facts", {})
    if not isinstance(facts_tree, dict):
        return []

    expected_units = {
        str(metric["code"]): str(metric.get("unit", "")).strip()
        for metric in metrics_config.get("metrics", [])
        if isinstance(metric, dict) and metric.get("code")
    }
    concept_defs = concept_map.get("concepts", {})
    rows: list[_SecObservation] = []

    for metric_code, expected_unit in expected_units.items():
        config = concept_defs.get(metric_code, {})
        concepts = [
            str(tag)
            for tag in [*config.get("preferred", []), *config.get("fallback", [])]
            if str(tag).strip()
        ]
        for preferred_rank, tag in enumerate(concepts):
            taxonomy, concept_name = _split_concept_tag(tag)
            if taxonomy is None or concept_name is None:
                continue
            concept_payload = facts_tree.get(taxonomy, {}).get(concept_name)
            if not isinstance(concept_payload, dict):
                continue
            units = concept_payload.get("units", {})
            if not isinstance(units, dict):
                continue
            for unit_key, entries in units.items():
                normalized_unit = _normalize_unit_for_metric(str(unit_key), expected_unit)
                if normalized_unit is None or not isinstance(entries, list):
                    continue
                for entry in entries:
                    observation = _build_sec_observation(
                        company=company,
                        metric_code=metric_code,
                        taxonomy=taxonomy,
                        concept_name=concept_name,
                        normalized_unit=normalized_unit,
                        preferred_rank=preferred_rank,
                        entry=entry,
                    )
                    if observation is not None:
                        rows.append(observation)
    return rows


def _build_sec_observation(
    company: CompanyRef,
    metric_code: str,
    taxonomy: str,
    concept_name: str,
    normalized_unit: str,
    preferred_rank: int,
    entry: Any,
) -> _SecObservation | None:
    if not isinstance(entry, dict):
        return None
    value = _safe_float(entry.get("val"))
    period_end = str(entry.get("end", "")).strip()
    form_type = str(entry.get("form", "")).strip()
    filed_at = str(entry.get("filed", period_end)).strip()
    if value is None or not period_end or not form_type:
        return None
    fiscal_year = _safe_int(entry.get("fy"))
    if fiscal_year is None:
        fiscal_year = _derive_fiscal_year(period_end, company.fiscal_year_end_mmdd)
    fiscal_quarter = _parse_fiscal_quarter(entry.get("fp"), period_end, company.fiscal_year_end_mmdd)
    duration_days = _duration_days(entry.get("start"), period_end)
    return _SecObservation(
        metric_code=metric_code,
        concept=f"{taxonomy}:{concept_name}",
        taxonomy=taxonomy,
        unit=normalized_unit,
        value=value,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        period_end_date=period_end,
        duration_days=duration_days,
        form_type=form_type,
        filed_at=filed_at,
        accession_no=str(entry.get("accn", "")).strip(),
        confidence="reported",
        amended=form_type.endswith("/A"),
        preferred_rank=preferred_rank,
    )


def _build_annual_facts(company_key: str, observations: list[_SecObservation]) -> list[dict[str, Any]]:
    annual_candidates = [
        observation
        for observation in observations
        if is_annual_form(observation.form_type, observation.duration_days)
    ]
    if not annual_candidates:
        return []
    frame = pd.DataFrame([_observation_to_record(company_key, obs) for obs in annual_candidates])
    frame["filed_at_sort"] = pd.to_datetime(frame["filed_at"], errors="coerce")
    frame = frame.sort_values(
        ["metric_code", "fiscal_year", "preferred_rank", "filed_at_sort"],
        ascending=[True, True, True, False],
    )
    frame = frame.drop_duplicates(subset=["company_key", "metric_code", "fiscal_year"], keep="first")
    frame = frame.drop(columns=["preferred_rank", "filed_at_sort"])
    return frame.to_dict(orient="records")


def _build_quarterly_facts(
    company_key: str,
    observations: list[_SecObservation],
    annual_facts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    quarter_candidates = [
        observation
        for observation in observations
        if observation.form_type.startswith("10-Q") and observation.fiscal_quarter in {1, 2, 3}
    ]
    if not quarter_candidates:
        return []

    base = pd.DataFrame([_observation_to_record(company_key, obs) for obs in quarter_candidates])
    base["filed_at_sort"] = pd.to_datetime(base["filed_at"], errors="coerce")
    base = base.sort_values(
        ["metric_code", "fiscal_year", "fiscal_quarter", "preferred_rank", "filed_at_sort"],
        ascending=[True, True, True, True, False],
    )
    base = base.drop_duplicates(
        subset=["company_key", "metric_code", "fiscal_year", "fiscal_quarter"],
        keep="first",
    )

    standalone_rows: list[dict[str, Any]] = []
    for (_metric_code, _fiscal_year), group in base.groupby(["metric_code", "fiscal_year"], sort=True):
        previous_ytd: float | None = None
        previous_duration: int | None = None
        for row in group.sort_values("fiscal_quarter").to_dict(orient="records"):
            duration_days = int(row["duration_days"])
            is_ytd_row = duration_days > 110
            if is_ytd_row and str(row["metric_code"]) not in _ADDITIVE_QUARTERLY_METRICS:
                continue
            if is_ytd_row and previous_ytd is None:
                continue
            record = dict(row)
            if is_ytd_row and previous_duration is not None:
                record["value"] = float(row["value"]) - float(previous_ytd)
                record["duration_days"] = max(duration_days - previous_duration, 75)
                record["confidence"] = "derived"
                record["raw_record_hash"] = _hash_record(
                    record["company_key"],
                    record["metric_code"],
                    record["fiscal_year"],
                    record["fiscal_quarter"],
                    record["period_end_date"],
                    record["value"],
                )
            standalone_rows.append(record)
            previous_ytd = float(row["value"])
            previous_duration = duration_days

    quarterly = pd.DataFrame(standalone_rows)
    if quarterly.empty:
        return []
    quarterly = pd.concat(
        [quarterly, pd.DataFrame(_derive_q4_rows(company_key, quarterly, annual_facts))],
        ignore_index=True,
    )
    quarterly = quarterly.drop(columns=["preferred_rank", "filed_at_sort"], errors="ignore")
    quarterly = quarterly.sort_values(["metric_code", "fiscal_year", "fiscal_quarter"])
    return quarterly.to_dict(orient="records")


def _derive_q4_rows(
    company_key: str,
    quarterly: pd.DataFrame,
    annual_facts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if quarterly.empty or not annual_facts:
        return []
    annual = pd.DataFrame(annual_facts)
    if annual.empty:
        return []

    derived_rows: list[dict[str, Any]] = []
    existing = {
        (str(row.metric_code), int(row.fiscal_year), int(row.fiscal_quarter))
        for row in quarterly.itertuples(index=False)
    }
    for annual_row in annual.to_dict(orient="records"):
        metric_code = str(annual_row["metric_code"])
        if metric_code not in _ADDITIVE_QUARTERLY_METRICS:
            continue
        fiscal_year = int(annual_row["fiscal_year"])
        year_quarters = quarterly[
            (quarterly["metric_code"] == metric_code) & (quarterly["fiscal_year"] == fiscal_year)
        ].sort_values("fiscal_quarter")
        present = set(int(value) for value in year_quarters["fiscal_quarter"].tolist())
        if (metric_code, fiscal_year, 4) in existing or not {1, 2, 3}.issubset(present):
            continue
        q3_end = str(year_quarters[year_quarters["fiscal_quarter"] == 3].iloc[-1]["period_end_date"])
        duration_days = _duration_between(q3_end, str(annual_row["period_end_date"])) or 90
        annual_value = float(annual_row["value"])
        q4_value = annual_value - float(year_quarters["value"].sum())
        derived_rows.append(
            {
                "company_key": company_key,
                "metric_code": metric_code,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": 4,
                "period_end_date": annual_row["period_end_date"],
                "duration_days": duration_days,
                "value": q4_value,
                "unit": annual_row["unit"],
                "form_type": annual_row["form_type"],
                "filed_at": annual_row["filed_at"],
                "accession_no": annual_row["accession_no"],
                "taxonomy": annual_row["taxonomy"],
                "concept": annual_row["concept"],
                "confidence": "derived",
                "amended": bool(annual_row["amended"]),
                "source_name": "sec",
                "raw_record_hash": _hash_record(
                    company_key,
                    metric_code,
                    fiscal_year,
                    4,
                    annual_row["period_end_date"],
                    q4_value,
                ),
            }
        )
    return derived_rows


def _derive_missing_annual_eps(
    company_key: str,
    annual_facts: list[dict[str, Any]],
    concept_map: dict[str, Any],
) -> list[dict[str, Any]]:
    return _derive_missing_eps(company_key, annual_facts, concept_map, period_type="annual")


def _derive_missing_quarterly_eps(
    company_key: str,
    quarterly_facts: list[dict[str, Any]],
    concept_map: dict[str, Any],
) -> list[dict[str, Any]]:
    return _derive_missing_eps(company_key, quarterly_facts, concept_map, period_type="quarterly")


def _derive_missing_eps(
    company_key: str,
    rows: list[dict[str, Any]],
    concept_map: dict[str, Any],
    *,
    period_type: str,
) -> list[dict[str, Any]]:
    if not rows:
        return rows

    frame = pd.DataFrame(rows)
    join_keys = ["fiscal_year"] if period_type == "annual" else ["fiscal_year", "fiscal_quarter"]
    eps_keys = set(
        tuple(item[key] for key in join_keys)
        for item in frame[frame["metric_code"] == "eps_diluted_actual"][join_keys].to_dict(orient="records")
    )
    numerator = frame[frame["metric_code"] == "net_income_actual"]
    denominator = frame[frame["metric_code"] == "shares_diluted_actual"]
    if numerator.empty or denominator.empty:
        return rows

    derive_cfg = concept_map.get("concepts", {}).get("eps_diluted_actual", {}).get("derive", {})
    concept_label = f"derived:{derive_cfg.get('numerator', 'net_income')}/{derive_cfg.get('denominator', 'shares')}"
    merged = numerator.merge(denominator, on=join_keys, suffixes=("_num", "_den"))
    derived_rows: list[dict[str, Any]] = []
    for row in merged.to_dict(orient="records"):
        key = tuple(row[key] for key in join_keys)
        shares = float(row["value_den"])
        if key in eps_keys or shares == 0:
            continue
        period_end_date = str(row["period_end_date_num"])
        fiscal_quarter = _safe_int(row.get("fiscal_quarter"))
        derived_value = float(row["value_num"]) / shares
        derived_rows.append(
            {
                "company_key": company_key,
                "metric_code": "eps_diluted_actual",
                "fiscal_year": int(row["fiscal_year"]),
                "fiscal_quarter": fiscal_quarter,
                "period_end_date": period_end_date,
                "duration_days": int(row["duration_days_num"]),
                "value": derived_value,
                "unit": "USD/share",
                "form_type": str(row["form_type_num"]),
                "filed_at": max(str(row["filed_at_num"]), str(row["filed_at_den"])),
                "accession_no": str(row["accession_no_num"] or row["accession_no_den"]),
                "taxonomy": "derived",
                "concept": concept_label,
                "confidence": "derived",
                "amended": bool(row["amended_num"]) or bool(row["amended_den"]),
                "source_name": "sec",
                "raw_record_hash": _hash_record(
                    company_key,
                    "eps_diluted_actual",
                    int(row["fiscal_year"]),
                    fiscal_quarter,
                    period_end_date,
                    derived_value,
                ),
            }
        )
    if not derived_rows:
        return rows
    combined = pd.concat([frame, pd.DataFrame(derived_rows)], ignore_index=True)
    sort_keys = ["metric_code", "fiscal_year"]
    if period_type == "quarterly":
        sort_keys.append("fiscal_quarter")
    combined = combined.sort_values(sort_keys)
    return combined.to_dict(orient="records")


def _observation_to_record(company_key: str, observation: _SecObservation) -> dict[str, Any]:
    return {
        "company_key": company_key,
        "metric_code": observation.metric_code,
        "fiscal_year": observation.fiscal_year,
        "fiscal_quarter": observation.fiscal_quarter,
        "period_end_date": observation.period_end_date,
        "duration_days": observation.duration_days,
        "value": observation.value,
        "unit": observation.unit,
        "form_type": observation.form_type,
        "filed_at": observation.filed_at,
        "accession_no": observation.accession_no,
        "taxonomy": observation.taxonomy,
        "concept": observation.concept,
        "confidence": observation.confidence,
        "amended": observation.amended,
        "source_name": observation.source_name,
        "raw_record_hash": _hash_record(
            company_key,
            observation.metric_code,
            observation.fiscal_year,
            observation.fiscal_quarter,
            observation.period_end_date,
            observation.value,
        ),
        "preferred_rank": observation.preferred_rank,
    }


def _normalize_fiscal_year_end(value: Any, default: str) -> str:
    text = "".join(ch for ch in str(value or default) if ch.isdigit())
    return text if len(text) == 4 else default


def _split_concept_tag(tag: str) -> tuple[str | None, str | None]:
    if ":" not in tag:
        return None, None
    taxonomy, concept_name = tag.split(":", maxsplit=1)
    return taxonomy.strip(), concept_name.strip()


def _normalize_unit_for_metric(unit_key: str, expected_unit: str) -> str | None:
    normalized = unit_key.strip().lower().replace(" ", "")
    expected = expected_unit.strip().lower().replace(" ", "")
    if expected == "usd":
        return "USD" if normalized == "usd" else None
    if expected == "usd/share":
        return "USD/share" if normalized in {"usd/share", "usd/shares"} else None
    if expected == "shares":
        return "shares" if normalized in {"share", "shares"} else None
    return expected_unit or unit_key


def _parse_fiscal_quarter(fp_value: Any, period_end_date: str, fiscal_year_end_mmdd: str) -> int | None:
    text = str(fp_value or "").upper().strip()
    if text.startswith("Q") and text[1:].isdigit():
        quarter = int(text[1:])
        return quarter if quarter in {1, 2, 3, 4} else None
    return _derive_fiscal_quarter(period_end_date, fiscal_year_end_mmdd)


def _derive_fiscal_year(period_end_date: str, fiscal_year_end_mmdd: str) -> int:
    period_end = date.fromisoformat(period_end_date)
    fiscal_month = int(fiscal_year_end_mmdd[:2])
    fiscal_day = int(fiscal_year_end_mmdd[2:])
    fiscal_day = min(fiscal_day, monthrange(period_end.year, fiscal_month)[1])
    fiscal_year_end = date(period_end.year, fiscal_month, fiscal_day)
    if period_end > fiscal_year_end:
        return period_end.year + 1
    return period_end.year


def _derive_fiscal_quarter(period_end_date: str, fiscal_year_end_mmdd: str) -> int:
    period_end = date.fromisoformat(period_end_date)
    fiscal_month = int(fiscal_year_end_mmdd[:2])
    month_offset = (period_end.month - fiscal_month) % 12
    if month_offset in {0, 1, 2}:
        return 4
    if month_offset in {3, 4, 5}:
        return 1
    if month_offset in {6, 7, 8}:
        return 2
    return 3


def _duration_days(start_value: Any, end_value: str) -> int:
    if not start_value:
        return 0
    try:
        start = date.fromisoformat(str(start_value))
        end = date.fromisoformat(end_value)
    except ValueError:
        return 0
    return max((end - start).days, 0)


def _duration_between(start_value: str, end_value: str) -> int | None:
    try:
        start = date.fromisoformat(start_value)
        end = date.fromisoformat(end_value)
    except ValueError:
        return None
    return max((end - start).days, 0)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _hash_record(
    company_key: str,
    metric_code: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_end_date: str,
    value: float,
) -> str:
    payload = f"{company_key}|{metric_code}|{fiscal_year}|{fiscal_quarter}|{period_end_date}|{value:.12f}"
    return sha256(payload.encode("utf-8")).hexdigest()

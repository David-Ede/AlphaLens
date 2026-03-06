> **Single source of truth.** This document supersedes the earlier repo-ready technical spec and is intended to be handed to **one autonomous agent builder** to create the repository, implementation, dashboard, tests, CI, demo fixtures, and documentation **in one uninterrupted build pass**.
>
> **Normative precedence.** If any earlier section is ambiguous, optional, or presents multiple choices, **Sections 31–42 override and resolve that ambiguity**.
>
> **Success definition.** The build is complete only when the repository is runnable locally in demo mode, tests pass offline, quality gates pass, the dashboard renders from fixture-backed data, and the final delivery artifacts listed in Section 42 exist.


# FAST Graphs Clone (Core Valuation View) — Single-Agent Build Instructions and Delivery Spec

## 1) Goal

Build a Python project that recreates the core FAST Graphs workflow:

- show long-term stock price history
- show an earnings-based fair value line
- optionally show a “normal P/E” fair value line
- extend the fair value line forward with analyst EPS estimates
- expose the underlying cleaned fundamentals, estimates, and source lineage in an audit-friendly Dash UI

This spec uses the following source policy:

- **SEC EDGAR** = actual historical fundamentals
- **Yahoo Finance via `yfinance`** = price history, dividends, splits/actions
- **Financial Modeling Prep (FMP)** = forward estimates only

## 2) Source assumptions and constraints

- SEC `data.sec.gov` public APIs do not require API keys, expose submissions plus XBRL company facts, update throughout the day, provide nightly bulk ZIP archives, and do **not** support browser CORS. All SEC access should therefore be server-side. The SEC also asks clients to declare a `User-Agent` and stay within fair-access guidance of at most 10 requests/second.  
- SEC company facts aggregate non-custom taxonomy, entity-wide facts, which makes them the preferred source for issuer-level historical actuals; avoid the SEC `frames` endpoint for the main company history because it aligns facts to calendar frames rather than the issuer’s own fiscal periods.  
- `yfinance` exposes price history, dividends, splits, actions, and estimate-related helpers; in this project it is the market-data adapter, not the source of truth for actual fundamentals.  
- FMP’s Financial Estimates API provides forward revenue/EPS consensus data by annual or quarterly period. FMP’s own documentation also notes that this endpoint does **not** provide a historical revision timeline, so the project must snapshot estimates on a schedule if you want revision history later.  
- Dash Pages is the right structure for the UI, `dcc.Store` is the right primitive for sharing processed data across callbacks, background callbacks are the right primitive for long refresh jobs, `dash_table.DataTable` is the right primitive for fundamentals/audit tables, and `dcc.Download` is the right primitive for CSV/XLSX export. For local development use Diskcache; for production use Celery + Redis.

## 3) Non-goals for v1

- real-time intraday market data
- full non-U.S. coverage parity
- portfolio accounting / tax lots
- complete estimate revision history out of the box
- broker integrations or order execution
- total return as the primary chart (can be a later feature)

## 4) Opinionated stack

- **Python** 3.12
- **Dash + Plotly** for UI
- **DuckDB + Parquet + PyArrow** for storage
- **Pydantic v2** for settings and domain validation
- **httpx** for SEC/FMP HTTP clients
- **yfinance** for Yahoo adapter
- **pandas or polars** for transforms (pick one; do not mix in business logic)
- **Tenacity** for retries
- **structlog** or stdlib logging with JSON formatter
- **pytest** for tests
- **ruff + mypy** for linting/type checks
- **Typer** for CLI commands
- **Diskcache** for local Dash background jobs
- **Celery + Redis** for production background jobs

## 5) Repository layout

```text
fastgraphs-clone/
├── README.md
├── pyproject.toml
├── .env.example
├── Makefile
├── docker-compose.yml
├── conf/
│   ├── app.yml
│   ├── metrics.yml
│   ├── concept_map.yml
│   └── watchlists.yml
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── cache/
│   └── exports/
├── docs/
│   ├── technical-spec.md
│   ├── methodology.md
│   ├── runbook.md
│   └── adr/
├── sql/
│   ├── views/
│   └── checks/
├── src/fg/
│   ├── __init__.py
│   ├── settings.py
│   ├── logging.py
│   ├── cli.py
│   ├── clients/
│   │   ├── sec.py
│   │   ├── yahoo.py
│   │   └── fmp.py
│   ├── domain/
│   │   ├── enums.py
│   │   ├── models.py
│   │   ├── metrics.py
│   │   ├── concepts.py
│   │   ├── periods.py
│   │   ├── valuation.py
│   │   ├── quality.py
│   │   └── lineage.py
│   ├── storage/
│   │   ├── paths.py
│   │   ├── duckdb.py
│   │   ├── repositories.py
│   │   └── schemas.py
│   ├── ingestion/
│   │   ├── resolve_company.py
│   │   ├── sec_ingest.py
│   │   ├── yahoo_ingest.py
│   │   └── fmp_ingest.py
│   ├── normalization/
│   │   ├── sec_actuals_annual.py
│   │   ├── sec_actuals_quarterly.py
│   │   ├── market_data.py
│   │   ├── estimates.py
│   │   └── quality_checks.py
│   ├── marts/
│   │   ├── valuation_series.py
│   │   ├── eps_bars.py
│   │   ├── kpi_snapshot.py
│   │   ├── source_freshness.py
│   │   └── audit.py
│   ├── services/
│   │   ├── refresh_service.py
│   │   ├── chart_service.py
│   │   ├── export_service.py
│   │   └── audit_service.py
│   └── ui/
│       ├── app.py
│       ├── assets/
│       ├── components/
│       │   ├── controls.py
│       │   ├── cards.py
│       │   ├── graphs.py
│       │   └── tables.py
│       ├── pages/
│       │   ├── overview.py
│       │   ├── fundamentals.py
│       │   └── audit.py
│       └── callbacks/
│           ├── overview.py
│           ├── fundamentals.py
│           ├── audit.py
│           ├── refresh.py
│           └── export.py
└── tests/
    ├── fixtures/
    ├── unit/
    ├── integration/
    └── snapshots/
```

## 6) Configuration

### Required environment variables
- `SEC_USER_AGENT`  
  Format: `Company Name email@domain.com`
- `FMP_API_KEY`
- `DATA_ROOT`
- `DUCKDB_PATH`

### Recommended environment variables
- `APP_ENV`
- `MAX_SEC_RPS` (default `8` to stay under SEC guidance)
- `DEFAULT_STATIC_PE` (default `15`)
- `DEFAULT_LOOKBACK_YEARS` (default `20`)
- `PRICE_CACHE_TTL_HOURS`
- `ESTIMATE_CACHE_TTL_HOURS`
- `REDIS_URL` (prod)
- `WARM_TICKERS` (comma-separated)
- `DASH_DEBUG`
- `LOG_LEVEL`

## 7) Canonical keys and identifiers

- Primary issuer key = **CIK**
- Human entrypoint = ticker
- One ticker may map to one CIK; maintain alias history if ticker changes
- Persist both `ticker_input` and `resolved_cik`
- Every downstream table must carry `company_key = cik`

Rationale:
- CIK is stable at the issuer level
- ticker is a presentation identifier
- storage, joins, and dedupe logic should not be keyed on ticker alone

## 8) Domain models (logical)

### `CompanyRef`
- `company_key` (CIK string)
- `ticker`
- `issuer_name`
- `exchange`
- `fiscal_year_end_mmdd`
- `currency`
- `active`

### `CanonicalFact`
- `company_key`
- `metric_code`
- `period_type` (`annual`, `quarterly`, `ttm`)
- `fiscal_year`
- `fiscal_quarter` nullable
- `period_end_date`
- `duration_days`
- `value`
- `unit`
- `source_name`
- `taxonomy`
- `concept`
- `form_type`
- `filed_at`
- `accession_no`
- `confidence` (`reported`, `fallback_tag`, `derived`)
- `amended`
- `raw_record_hash`

### `PriceBar`
- `company_key`
- `ticker`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `split_adjusted_close`
- `volume`
- `currency`
- `source_name`

### `CorporateAction`
- `company_key`
- `action_type` (`dividend`, `split`)
- `action_date`
- `cash_value` nullable
- `split_ratio` nullable
- `source_name`

### `EstimateSnapshot`
- `company_key`
- `as_of_date`
- `target_period_type` (`annual`, `quarterly`)
- `target_fiscal_year`
- `target_period_end_date`
- `metric_code` (`eps_estimate_mean`, `revenue_estimate_mean`)
- `mean_value`
- `high_value` nullable
- `low_value` nullable
- `analyst_count` nullable
- `unit`
- `currency`
- `source_name`
- `raw_record_hash`

### `ValuationPoint`
- `company_key`
- `series_name`
- `x_date`
- `y_value`
- `is_estimate`
- `lookback_years`
- `pe_method`
- `display_style`
- `tooltip_payload`

### `QualityIssue`
- `company_key`
- `severity`
- `issue_code`
- `metric_code`
- `period_key`
- `message`
- `detected_at`

## 9) Storage schema

Use Parquet files partitioned by source/date where helpful, plus DuckDB views over them.

### Bronze tables
Raw, append-only, no cleaning.

#### `bronze_sec_submissions`
- `cik`
- `ticker_requested`
- `payload_json`
- `pulled_at`
- `endpoint`
- `payload_hash`

#### `bronze_sec_companyfacts`
- `cik`
- `payload_json`
- `pulled_at`
- `endpoint`
- `payload_hash`

#### `bronze_yahoo_prices`
- `ticker`
- `price_frame_parquet_path`
- `pulled_at`
- `payload_hash`

#### `bronze_yahoo_actions`
- `ticker`
- `actions_frame_parquet_path`
- `pulled_at`
- `payload_hash`

#### `bronze_fmp_estimates`
- `ticker`
- `period`
- `payload_json`
- `pulled_at`
- `endpoint`
- `payload_hash`

### Silver tables
Cleaned, deduped, source-aware.

#### `dim_company`
- `company_key`
- `cik`
- `ticker`
- `issuer_name`
- `exchange`
- `fiscal_year_end_mmdd`
- `currency`
- `last_sec_pull_at`
- `last_yahoo_pull_at`
- `last_fmp_pull_at`

#### `fact_fundamental_annual`
- `company_key`
- `metric_code`
- `fiscal_year`
- `period_end_date`
- `duration_days`
- `value`
- `unit`
- `form_type`
- `filed_at`
- `accession_no`
- `taxonomy`
- `concept`
- `confidence`
- `amended`
- `source_name`
- `raw_record_hash`
- `ingested_at`

Logical uniqueness:
`company_key + metric_code + fiscal_year`

#### `fact_fundamental_quarterly`
- `company_key`
- `metric_code`
- `fiscal_year`
- `fiscal_quarter`
- `period_end_date`
- `duration_days`
- `value`
- `unit`
- `form_type`
- `filed_at`
- `accession_no`
- `taxonomy`
- `concept`
- `confidence`
- `amended`
- `source_name`
- `raw_record_hash`
- `ingested_at`

Logical uniqueness:
`company_key + metric_code + fiscal_year + fiscal_quarter`

#### `fact_price_daily`
- `company_key`
- `ticker`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `split_adjusted_close`
- `volume`
- `currency`
- `source_name`
- `ingested_at`

Logical uniqueness:
`company_key + trade_date`

#### `fact_corporate_action`
- `company_key`
- `action_type`
- `action_date`
- `cash_value`
- `split_ratio`
- `source_name`
- `ingested_at`

#### `fact_estimate_snapshot`
- `company_key`
- `as_of_date`
- `target_period_type`
- `target_fiscal_year`
- `target_period_end_date`
- `metric_code`
- `mean_value`
- `high_value`
- `low_value`
- `analyst_count`
- `unit`
- `currency`
- `source_name`
- `raw_record_hash`
- `ingested_at`

Logical uniqueness:
`company_key + as_of_date + target_period_end_date + metric_code`

#### `fact_lineage`
- `entity_type`
- `entity_id`
- `source_name`
- `source_endpoint`
- `source_locator`
- `raw_record_hash`
- `transform_version`
- `created_at`

#### `fact_quality_issue`
- `company_key`
- `severity`
- `issue_code`
- `metric_code`
- `period_key`
- `message`
- `detected_at`
- `resolved_at`

### Gold marts
UI-facing, precomputed, parameterized by lookback/method.

#### `mart_valuation_series`
- `company_key`
- `lookback_years`
- `pe_method`
- `series_name`
- `x_date`
- `y_value`
- `fiscal_year` nullable
- `is_estimate`
- `display_style`
- `tooltip_payload_json`
- `built_at`

#### `mart_eps_bars`
- `company_key`
- `lookback_years`
- `fiscal_year`
- `period_end_date`
- `eps_actual` nullable
- `eps_estimate` nullable
- `is_estimate`
- `confidence`
- `built_at`

#### `mart_kpi_snapshot`
- `company_key`
- `as_of_date`
- `last_price`
- `last_price_date`
- `latest_actual_eps`
- `latest_actual_eps_period_end`
- `current_pe`
- `selected_pe`
- `fair_value_now`
- `valuation_gap_pct`
- `last_filing_date`
- `last_estimate_snapshot_date`
- `data_quality_score`
- `built_at`

#### `mart_source_freshness`
- `company_key`
- `source_name`
- `last_pull_at`
- `freshness_status`
- `built_at`

#### `mart_audit_grid`
Prejoined audit rows used directly by the UI:
- metric
- period
- value
- unit
- confidence
- taxonomy
- concept
- form_type
- filed_at
- accession_no
- source_name

## 10) Source adapter contracts

### SEC client (`clients/sec.py`)
Responsibilities:
- resolve CIK/ticker metadata
- fetch `submissions/CIK##########.json`
- fetch `api/xbrl/companyfacts/CIK##########.json`
- optionally fetch `companyfacts.zip` and `submissions.zip` for batch warm-loads

Guardrails:
- mandatory `User-Agent`
- rate limiter
- retry with backoff
- no browser-side use
- keep raw payloads

### Yahoo client (`clients/yahoo.py`)
Responsibilities:
- fetch price history
- fetch dividends
- fetch splits/actions
- never be the source of truth for actual annual SEC fundamentals

### FMP client (`clients/fmp.py`)
Responsibilities:
- fetch annual estimates
- optionally fetch quarterly estimates
- timestamp every fetch
- treat every fetch as a snapshot because the endpoint does not give historical revision history

## 11) Metric catalog

### Core metrics required for MVP
- `eps_diluted_actual`
- `revenue_actual`
- `net_income_actual`
- `shares_diluted_actual`
- `price_close_split_adjusted`
- `dividend_cash`
- `eps_estimate_mean`

### Optional v2 metrics
- `free_cash_flow_actual`
- `operating_cash_flow_actual`
- `dividend_per_share_actual`
- `revenue_estimate_mean`

## 12) Concept map policy

Store concept fallbacks in `conf/concept_map.yml`.

### Proposed concept priority

#### EPS diluted
1. `us-gaap:EarningsPerShareDiluted`
2. `ifrs-full:BasicAndDilutedEarningsLossPerShare`
3. derived from `NetIncomeLoss / WeightedAverageNumberOfDilutedSharesOutstanding`

#### Revenue
1. `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax`
2. `us-gaap:SalesRevenueNet`
3. `us-gaap:Revenues`

#### Net income
1. `us-gaap:NetIncomeLoss`

#### Diluted shares
1. `us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding`

Confidence rules:
- direct preferred concept = `reported`
- accepted fallback concept = `fallback_tag`
- derived math = `derived`

## 13) Period classification rules

### Annual
Accept if:
- form is one of `10-K`, `10-K/A`, `20-F`, `20-F/A`, `40-F`, `40-F/A`
- duration is between 330 and 400 days

### Quarterly
Accept if:
- form is one of `10-Q`, `10-Q/A`, `6-K` (only if clearly quarterly financials), or annual-derived Q4
- duration is between 75 and 110 days

### TTM
Derived from the most recent four standalone quarters.

## 14) Deduplication and restatement rules

When multiple facts map to the same canonical metric and fiscal period:

1. prefer direct preferred concept
2. prefer latest filed amendment if it supersedes prior value
3. prefer latest `filed_at`
4. prefer annual forms over interim forms for annual actuals
5. preserve all losers in lineage/audit, but publish one winner into the canonical silver fact table

## 15) Quarterly derivation rules

Some filings provide year-to-date values instead of standalone quarter values.

Rules:
- if Q2/Q3 values are YTD, derive standalone quarter by differencing current YTD minus prior YTD
- if standalone Q4 is unavailable, derive Q4 as FY minus Q1–Q3
- mark any derived quarter fact with `confidence=derived`
- derive TTM only from standalone quarterly facts, never from mixed cumulative periods

## 16) Market-data normalization rules

- use Yahoo daily history as raw market series
- compute `split_adjusted_close` for the main valuation chart
- keep cash dividends in a separate action table
- price used for observed P/E on a fiscal period end = last market close on or before `period_end_date`
- do **not** use dividend-adjusted total-return prices for the main price-vs-fundamentals chart

## 17) Estimate normalization rules

- ingest FMP estimates as snapshots with `as_of_date`
- map estimate periods onto issuer fiscal years
- use actual SEC values for completed years
- use estimates only for future or still-open years
- store quarterly estimates separately from annual estimates
- never overwrite prior estimate snapshots

## 18) Valuation engine

Implement in `domain/valuation.py` and `marts/valuation_series.py`.

### Supported P/E methods
1. `static_15`
2. `normal_pe`

### `static_15`
- fair value = actual/estimated EPS × 15

### `normal_pe`
- observed P/E per year = price at fiscal year end ÷ actual EPS
- exclude years with EPS <= 0
- winsorize or trim outliers
- normal P/E = median observed P/E over selected lookback
- fair value = actual/estimated EPS × normal P/E

### Current KPIs
- last price
- current price date
- latest actual EPS
- current P/E
- selected P/E
- fair value now
- valuation gap %
- EPS CAGR over selected lookback
- last filing date
- last estimate snapshot date

### Negative EPS behavior
- display EPS
- suppress P/E and fair-value output as `N/M`
- show a UI badge: `Valuation line hidden because EPS <= 0`

## 19) Chart methodology

### Main chart
- Series 1: monthly price line from `split_adjusted_close`
- Series 2: annual actual fair-value line (solid)
- Series 3: annual estimated fair-value line (dashed)
- Series 4: optional normal-P/E line (solid secondary color or toggle)
- vertical markers for filings optional

### Lower chart / companion chart
- fiscal-year EPS bars
- actual bars separate from estimated bars
- hover shows confidence, concept, form, and filing date

### Tooltip contract
Each visible point should expose:
- displayed value
- source
- period end
- filed date / estimate snapshot date
- concept used
- confidence
- accession number or vendor endpoint ref if available

## 20) Dash UI specification

### Pages
- `overview`
- `fundamentals`
- `audit`

### Shared stores
- `store-request`
- `store-valuation-dataset`
- `store-refresh-status`
- `store-export-payload`

### Overview page components
- ticker input
- lookback dropdown
- P/E method radio
- manual P/E input (only visible for static mode if user wants override)
- show estimates toggle
- refresh button
- cancel button
- progress bar / status text
- KPI cards
- main valuation graph
- EPS bar chart
- freshness badge row
- export CSV / XLSX buttons

### Fundamentals page components
- annual fundamentals table
- quarterly fundamentals table
- concept summary panel
- metric selector
- period selector
- confidence filter

### Audit page components
- lineage DataTable
- quality issues DataTable
- raw source metadata panel
- method explanation card

## 21) Callback responsibilities

### Callback A — request serialization
Inputs:
- ticker
- lookback
- P/E method
- show estimates
- manual P/E
Outputs:
- `store-request.data`

Purpose:
- create one canonical request object for downstream callbacks

### Callback B — refresh orchestration (background)
Inputs:
- `store-request.data`
- refresh button
Outputs:
- `store-refresh-status.data`
- progress components
- disabled state for refresh/cancel buttons

Behavior:
- check freshness of gold mart
- if stale or missing, run pipeline:
  1. resolve company
  2. ingest SEC
  3. ingest Yahoo
  4. ingest FMP
  5. normalize
  6. rebuild gold marts
- update progress along the way
- support cancel

### Callback C — load view model
Inputs:
- `store-request.data`
- `store-refresh-status.data`
Outputs:
- `store-valuation-dataset.data`

Behavior:
- read the gold marts
- assemble a compact page view-model JSON

### Callback D — render KPI cards
Inputs:
- `store-valuation-dataset.data`
Outputs:
- KPI card children

### Callback E — render main chart
Inputs:
- `store-valuation-dataset.data`
Outputs:
- `overview-main-graph.figure`

### Callback F — render EPS bars
Inputs:
- `store-valuation-dataset.data`
Outputs:
- `overview-eps-bars.figure`

### Callback G — render fundamentals tables
Inputs:
- selected metric/filters
- `store-valuation-dataset.data`
Outputs:
- annual and quarterly tables

### Callback H — render audit tables
Inputs:
- `store-valuation-dataset.data`
Outputs:
- lineage table
- quality issues table

### Callback I — export
Inputs:
- export button clicks
- `store-valuation-dataset.data`
Outputs:
- `dcc.Download.data`

## 22) View-model JSON shape

```json
{
  "meta": {
    "ticker": "AAPL",
    "company_key": "0000320193",
    "issuer_name": "Apple Inc.",
    "lookback_years": 20,
    "pe_method": "static_15",
    "built_at": "2026-03-06T00:00:00Z"
  },
  "kpis": {
    "last_price": 0.0,
    "latest_actual_eps": 0.0,
    "current_pe": 0.0,
    "selected_pe": 15.0,
    "fair_value_now": 0.0,
    "valuation_gap_pct": 0.0,
    "last_filing_date": "YYYY-MM-DD",
    "last_estimate_snapshot_date": "YYYY-MM-DD",
    "data_quality_score": 100
  },
  "series": {
    "price": [],
    "fair_value_actual": [],
    "fair_value_estimate": [],
    "normal_pe_value": [],
    "eps_bars": []
  },
  "tables": {
    "annual": [],
    "quarterly": [],
    "audit": [],
    "quality_issues": []
  }
}
```

## 23) Refresh and scheduling policy

### On-demand
- from UI refresh button for current ticker

### Scheduled
- nightly: Yahoo prices/actions for warm watchlist
- nightly: FMP estimate snapshots for warm watchlist
- daily or filing-driven: SEC submissions/companyfacts refresh
- rebuild affected gold marts after any upstream change

### Scale-up path
- use SEC nightly `companyfacts.zip` and `submissions.zip` to warm a universe cache
- rebuild gold marts incrementally only for tickers with changed raw payload hashes

## 24) Quality checks

### Hard failures
- no CIK resolved
- no price history
- no annual EPS actuals for lookback window
- malformed estimate periods

### Soft warnings
- EPS derived instead of reported
- stale estimates
- stale prices
- missing diluted shares
- mismatched fiscal year mapping
- large discontinuity around split dates

### Suggested data quality score
Start at 100 and subtract:
- 30 if latest EPS is derived
- 20 if last SEC filing is stale beyond expected cadence
- 15 if last price is stale
- 10 if last estimate snapshot is stale
- 10 if multiple quality warnings remain unresolved

## 25) Testing plan

### Unit tests
- concept mapping
- annual/quarter classification
- YTD-to-standalone-quarter derivation
- dedupe selection
- normal P/E calculation
- valuation gap formula
- negative EPS handling

### Contract tests
Use frozen raw payloads for:
- SEC submissions
- SEC companyfacts
- yfinance price/actions
- FMP estimates

### Integration tests
- full refresh/build for 3 reference tickers:
  - AAPL
  - MSFT
  - KO
- verify marts populate and KPI snapshot is non-null where expected

### Snapshot tests
- Plotly figure JSON snapshots for overview charts
- rendered audit table schema snapshots

### Smoke tests
- Dash routes load
- refresh callback returns progress
- export callback returns file payload

## 26) Deployment topology

### Local development
- one Dash web process
- Diskcache background callback manager
- local DuckDB
- local `data/` volume

### Production
- Dash web container
- Celery worker container
- Redis container
- shared data volume or object store mount
- scheduled refresh runner (cron or orchestration)

## 27) CLI surface

Recommended commands:

- `fg run-dashboard`
- `fg refresh-ticker --ticker AAPL`
- `fg refresh-watchlist --name core`
- `fg build-gold --ticker AAPL`
- `fg quality-report --ticker AAPL`
- `fg export --ticker AAPL --format csv`

## 28) Build order

### Milestone 1 — end-to-end MVP
- ticker -> CIK resolution
- SEC annual actual EPS/revenue/shares
- Yahoo price/dividends/splits
- FMP annual EPS estimates
- static 15x fair-value line
- overview page only

### Milestone 2 — analytical credibility
- quarterly/TTM support
- normal-P/E mode
- audit page
- quality scoring

### Milestone 3 — usability
- exports
- warm watchlists
- background refresh with progress/cancel
- fundamentals page filters

## 29) Most important architectural decisions

1. **Use CIK as the primary company key.**
2. **Use SEC only for actual fundamentals.**
3. **Use Yahoo only for market series and actions.**
4. **Use FMP only for forward estimates.**
5. **Snapshot estimates; never overwrite them.**
6. **Keep bronze raw payloads forever for reproducibility.**
7. **Publish only one canonical value per metric-period, but retain losers in lineage.**
8. **Serve the UI from gold marts, not raw pipelines.**
9. **Do all SEC access server-side.**
10. **Treat the audit view as a first-class feature, not a debugging afterthought.**

## 30) Acceptance criteria

The repo is “ready” when an engineer can:

- run one command to start the Dash app locally
- enter a ticker
- trigger a refresh
- see price vs fair value with actual and estimated periods
- inspect the exact EPS values used and where they came from
- export the visible dataset
- reproduce the same chart from persisted silver/gold data without re-fetching sources


## 31) AI Agent execution contract

This section is **normative**. The builder must follow it exactly.

### Execution mode
- Build the project in **one pass** and as **one single agent**.
- Do **not** ask clarifying questions.
- Do **not** stop at scaffolding, wireframes, or stubs for required features.
- Prefer a complete, working implementation over a partially elegant one.
- If the environment lacks live API keys or outbound network, finish the build in **demo mode** using fixtures and local data.

### Required builder behavior
- Create the repository from scratch if it does not already exist.
- Initialize a git repository with default branch `main` **if the environment permits**. If `.git` cannot be created in the environment, still create a repository-ready directory structure and all repo metadata files.
- Create every file listed in the repository manifest, even if some are initially thin wrappers.
- Implement the full local development path, test path, and dashboard path.
- Implement the full fixture-backed demo path.
- Run formatting, linting, type checking, unit tests, integration tests, snapshot tests, and smoke tests before declaring completion.
- Update `README.md`, `docs/methodology.md`, and `docs/runbook.md` so they match the implementation that was actually built.
- Record any implementation deviations in `docs/adr/0001-implementation-deviations.md`.
- Produce a final handoff report at `docs/final-implementation-report.md`.

### Prohibited builder behavior
- No unresolved `TODO` markers in required v1 files.
- No empty required modules except `__init__.py`.
- No code paths that require live internet access for default local startup or default test execution.
- No secrets checked into source control.
- No browser-side SEC requests.
- No silent fallback from one data source to another without surfacing lineage and warnings.

### Completion rule
The build is not complete until all items in Section 42 are satisfied.

## 32) Mandatory defaults and resolved choices

This section resolves all implementation branches. These defaults **must** be used.

### Repository identity
- Repository name: `fastgraphs-clone`
- Python package name: `fg`
- CLI command name: `fg`
- Default branch name: `main`
- Initial version: `0.1.0`
- License: `MIT`

### Runtime and library choices
- Python version: **3.12**
- Dataframe engine: **pandas only**
- Logging: **standard library logging with a custom JSON formatter**
- Chart construction: **`plotly.graph_objects`**, not Plotly Express
- Configuration loading: **Pydantic Settings + YAML**
- Storage: **DuckDB + Parquet + PyArrow**
- Excel export library: **openpyxl**
- Testing framework: **pytest**
- Network blocking in offline tests: **pytest-socket**
- HTTP mocking for `httpx`: **respx**
- Dash route smoke tests: use the Dash/Flask server test client; do **not** require Selenium, Playwright, or a real browser in v1

### Source usage policy
- SEC is the only source of truth for **actual historical fundamentals**
- Yahoo via `yfinance` is the only source of truth for **prices, dividends, and splits**
- FMP is the only source of truth for **forward estimates**
- If FMP is unavailable, the app must enter **demo estimates mode** using fixture-backed estimate snapshots; the UI must state that estimates are demo/frozen

### Resolved analytical defaults
- Default lookback: **20 years**
- Supported lookbacks: **5, 10, 15, 20**
- Default P/E method: **`static_15`**
- Supported P/E methods in v1: **`static_15`** and **`normal_pe`**
- Manual P/E override is allowed only as an input that modifies the selected P/E while the method remains `static_15`
- Default static P/E: **15.0**
- Forecast horizon: **2 forward fiscal years**
- Show estimates by default: **on**
- Show filing markers by default: **off**
- Default chart theme: **`plotly_white`**
- Default timezone for stored timestamps: **UTC**
- Date display format: **`YYYY-MM-DD`**
- Currency display precision: **2 decimals**
- Percentage display precision: **1 decimal**
- EPS display precision: **2 decimals**
- Large integer display uses comma separators
- Non-meaningful valuation output must display **`N/M`** exactly

### Resolved period and filing rules
- **Ignore `6-K` in v1**. Earlier references to possibly accepting `6-K` are overridden.
- Annual actuals come from `10-K`, `10-K/A`, `20-F`, `20-F/A`, `40-F`, `40-F/A`
- Quarterly actuals come from `10-Q`, `10-Q/A`, or annual-derived Q4
- TTM is derived from the most recent four standalone quarters only

### Resolved market-series rules
- Monthly price series uses the **last trading day of each calendar month**
- If the current month is partial, include the latest available trading day for that month
- Observed annual P/E uses the last market close on or before the fiscal year end
- Main chart uses **split-adjusted price**, not dividend-adjusted total return

### Resolved `normal_pe` calculation
1. Build annual observed P/E values for the selected lookback years:
   - `observed_pe_y = price_on_or_before_fiscal_year_end_y / annual_diluted_eps_y`
2. Exclude years where annual diluted EPS is `<= 0`
3. If fewer than **3** valid years remain:
   - fallback to `15.0`
   - add a soft quality issue: `normal_pe_fallback_insufficient_history`
4. For the remaining positive-EPS years:
   - compute the 5th percentile `p05`
   - compute the 95th percentile `p95`
   - clip each observed P/E into `[p05, p95]`
5. `normal_pe = median(clipped_observed_pe_values)`
6. Store full precision; display rounded to 2 decimals

### Resolved startup behavior
- If no local gold data exists and the app is launched without live keys, the app must start in **demo mode** and auto-seed fixture-backed data for:
  - `AAPL`
  - `MSFT`
  - `KO`
- In demo mode, the Overview page defaults to ticker `AAPL`
- In live mode, the ticker input starts blank

## 33) Repo bootstrap and required project files

The builder must create the following repository-level files in addition to the earlier repository layout.

### Required root files
- `.gitignore`
- `.editorconfig`
- `.python-version`
- `.pre-commit-config.yaml`
- `LICENSE`
- `Dockerfile`
- `.github/workflows/ci.yml`
- `.github/workflows/nightly-live.yml`
- `pytest.ini` **or** equivalent pytest configuration inside `pyproject.toml`
- `docs/final-implementation-report.md`
- `docs/adr/0001-implementation-deviations.md`

### Required initialization steps
1. Create the repo directory `fastgraphs-clone`
2. Initialize git with branch `main` if possible
3. Create a Python package under `src/fg`
4. Configure the package as installable via `pyproject.toml`
5. Create all data directories, docs directories, SQL directories, and test directories
6. Create fixture payloads and golden snapshots for `AAPL`, `MSFT`, and `KO`

### Required `pyproject.toml` content
The project must define:
- package metadata
- runtime dependencies
- dev dependencies
- console script entry point `fg = fg.cli:app`
- Ruff configuration
- mypy configuration
- pytest configuration
- coverage configuration

### Required runtime dependencies
At minimum include:
- `dash`
- `plotly`
- `duckdb`
- `pyarrow`
- `pandas`
- `pydantic`
- `pydantic-settings`
- `httpx`
- `yfinance`
- `tenacity`
- `typer`
- `diskcache`
- `openpyxl`

### Required development dependencies
At minimum include:
- `pytest`
- `pytest-cov`
- `pytest-socket`
- `respx`
- `mypy`
- `ruff`
- `pre-commit`

### Required `.gitignore`
The gitignore must exclude at least:
- `.env`
- `.venv/`
- `__pycache__/`
- `.pytest_cache/`
- `.mypy_cache/`
- `.ruff_cache/`
- `data/bronze/`
- `data/silver/`
- `data/gold/`
- `data/cache/`
- `data/exports/`
- `data/logs/`
- `.DS_Store`

### Required `README.md` sections
The README must include:
- project purpose
- architecture summary
- data source policy
- demo mode
- setup instructions
- commands
- testing instructions
- dashboard screenshots or placeholder description for where screenshots belong
- known limitations
- license

## 34) File-by-file implementation contract

Every file listed below must exist and must implement the stated responsibility.

### Root and meta files
- `README.md` — concise project overview, setup, commands, architecture, demo mode, screenshots section, limitations.
- `pyproject.toml` — packaging, dependencies, lint/type/test/coverage configuration, console script.
- `.env.example` — all required and optional environment variables with safe placeholder values.
- `Makefile` — developer commands defined in Section 37.
- `docker-compose.yml` — local multi-service composition for app + Redis, with Redis optional in local dev.
- `Dockerfile` — buildable container for the Dash app and CLI.
- `.gitignore` — ignore secrets, caches, and generated data.
- `.editorconfig` — standard formatting defaults.
- `.python-version` — pin to Python 3.12.
- `.pre-commit-config.yaml` — run Ruff and basic hygiene checks.
- `.github/workflows/ci.yml` — CI pipeline from Section 38.
- `.github/workflows/nightly-live.yml` — optional nightly live refresh/tests, skipped unless secrets are set.
- `LICENSE` — MIT license text.

### Config files
- `conf/app.yml` — application, storage, cache, freshness, UI, scheduling defaults.
- `conf/metrics.yml` — canonical metric catalog and display metadata.
- `conf/concept_map.yml` — taxonomy preference lists and derivation rules.
- `conf/watchlists.yml` — named watchlists; include `core: [AAPL, MSFT, KO]`.

### Documentation
- `docs/technical-spec.md` — this final document or a pointer to it.
- `docs/methodology.md` — actual methodology implemented, including lineage and valuation formulas.
- `docs/runbook.md` — operator runbook for local and CI usage.
- `docs/adr/0001-implementation-deviations.md` — explicit deviation log, even if it says “none”.
- `docs/final-implementation-report.md` — final build report populated at the end.

### SQL
- `sql/views/` — DuckDB helper views for silver and gold marts.
- `sql/checks/` — SQL-based quality checks where practical.

### Package root
- `src/fg/__init__.py` — package version and package marker only.
- `src/fg/settings.py` — `Settings` model, YAML loader, env merge logic, demo-mode detection.
- `src/fg/logging.py` — JSON logging formatter and `configure_logging()` function.
- `src/fg/cli.py` — Typer app with all commands from Section 37.

### Source clients
- `src/fg/clients/sec.py` — `SECClient` with `resolve_ticker_to_cik()`, `fetch_submissions()`, `fetch_companyfacts()`, rate limiting, retries, redacted request logging.
- `src/fg/clients/yahoo.py` — `YahooClient` with `fetch_price_history()`, `fetch_actions()`, adapter normalization, retries.
- `src/fg/clients/fmp.py` — `FMPClient` with `fetch_annual_estimates()` and optional `fetch_quarterly_estimates()` plus snapshot timestamping and redacted logging.

### Domain
- `src/fg/domain/enums.py` — enums for period type, confidence, action type, P/E method, freshness status, severity.
- `src/fg/domain/models.py` — Pydantic models for company refs, facts, prices, actions, estimates, requests, KPIs, quality issues, view models.
- `src/fg/domain/metrics.py` — canonical metric codes and labels.
- `src/fg/domain/concepts.py` — helpers for concept-map resolution.
- `src/fg/domain/periods.py` — annual, quarterly, fiscal period classification helpers and quarter-derivation helpers.
- `src/fg/domain/valuation.py` — `compute_static_15()`, `compute_normal_pe()`, `build_fair_value_series()`, `compute_kpis()`.
- `src/fg/domain/quality.py` — quality issue creation, scoring, freshness evaluation.
- `src/fg/domain/lineage.py` — lineage row builders and audit formatting helpers.

### Storage
- `src/fg/storage/paths.py` — canonical directory and file-path helpers.
- `src/fg/storage/duckdb.py` — connection factory and SQL execution helpers.
- `src/fg/storage/repositories.py` — read/write repository functions for bronze, silver, and gold layers.
- `src/fg/storage/schemas.py` — typed schema definitions and validation rules for stored tables.

### Ingestion
- `src/fg/ingestion/resolve_company.py` — ticker-to-CIK resolution flow and company dimension creation.
- `src/fg/ingestion/sec_ingest.py` — raw SEC ingestion to bronze with hashes and pull timestamps.
- `src/fg/ingestion/yahoo_ingest.py` — Yahoo price/actions ingestion to bronze.
- `src/fg/ingestion/fmp_ingest.py` — FMP estimate snapshot ingestion to bronze.

### Normalization
- `src/fg/normalization/sec_actuals_annual.py` — canonical annual fact extraction from SEC companyfacts.
- `src/fg/normalization/sec_actuals_quarterly.py` — quarterly extraction, YTD-to-quarter logic, TTM derivation.
- `src/fg/normalization/market_data.py` — daily price and action normalization, monthly aggregation helpers.
- `src/fg/normalization/estimates.py` — estimate period mapping, future-year filtering, snapshot handling.
- `src/fg/normalization/quality_checks.py` — table-level quality checks and warning generation.

### Gold marts
- `src/fg/marts/valuation_series.py` — build chart-ready time series for price and fair value lines.
- `src/fg/marts/eps_bars.py` — build annual EPS bar dataset for actuals and estimates.
- `src/fg/marts/kpi_snapshot.py` — build one-row KPI snapshot per company/request state.
- `src/fg/marts/source_freshness.py` — freshness mart for data badges.
- `src/fg/marts/audit.py` — build UI-ready audit and lineage grid.

### Services
- `src/fg/services/refresh_service.py` — end-to-end refresh orchestration from request to marts.
- `src/fg/services/chart_service.py` — deterministic Plotly figure builders for main chart and EPS bars.
- `src/fg/services/export_service.py` — CSV/XLSX export builders with fixed column order.
- `src/fg/services/audit_service.py` — audit-page view assembly and explanation text generation.

### UI
- `src/fg/ui/app.py` — Dash app factory, page registration, background callback manager wiring.
- `src/fg/ui/assets/` — CSS only; keep styling minimal and deterministic.
- `src/fg/ui/components/controls.py` — control-row builders with fixed component IDs.
- `src/fg/ui/components/cards.py` — KPI card builders.
- `src/fg/ui/components/graphs.py` — graph component wrappers.
- `src/fg/ui/components/tables.py` — standardized DataTable builders.
- `src/fg/ui/pages/overview.py` — Overview page layout.
- `src/fg/ui/pages/fundamentals.py` — Fundamentals page layout.
- `src/fg/ui/pages/audit.py` — Audit page layout.
- `src/fg/ui/callbacks/overview.py` — request serialization and rendering callbacks.
- `src/fg/ui/callbacks/fundamentals.py` — fundamentals filters/table callbacks.
- `src/fg/ui/callbacks/audit.py` — audit and quality table callbacks.
- `src/fg/ui/callbacks/refresh.py` — background refresh callback and progress state.
- `src/fg/ui/callbacks/export.py` — CSV/XLSX export callback.

### Tests
- `tests/conftest.py` — common fixtures, network blocking, temp data roots.
- `tests/fixtures/` — raw payloads, normalized expectations, and demo seeds.
- `tests/unit/` — unit tests for domain logic.
- `tests/integration/` — offline end-to-end pipeline tests.
- `tests/snapshots/` — approved Plotly JSON and export schema snapshots.

### Required file quality rules
- Every file must contain a module docstring.
- Public functions must have type hints.
- Required v1 modules may not be empty.
- Circular imports are not allowed.
- Business logic must live outside Dash callback bodies where practical.
- Figure generation must be deterministic for snapshot testing.

## 35) Config file schemas and example contents

The builder must create these config files with the following schemas.

### `.env.example`
```dotenv
APP_ENV=local
SEC_USER_AGENT=Example Company example@example.com
FMP_API_KEY=
DATA_ROOT=./data
DUCKDB_PATH=./data/fg.duckdb
MAX_SEC_RPS=8
DEFAULT_STATIC_PE=15
DEFAULT_LOOKBACK_YEARS=20
PRICE_CACHE_TTL_HOURS=24
ESTIMATE_CACHE_TTL_HOURS=24
DASH_DEBUG=1
LOG_LEVEL=INFO
REDIS_URL=redis://redis:6379/0
WARM_TICKERS=AAPL,MSFT,KO
```

### `conf/app.yml`
```yaml
app:
  name: fastgraphs-clone
  package: fg
  version: 0.1.0
  env: local
  demo_mode_default: true

data:
  root: ./data
  duckdb_path: ./data/fg.duckdb
  bronze_dir: ./data/bronze
  silver_dir: ./data/silver
  gold_dir: ./data/gold
  cache_dir: ./data/cache
  export_dir: ./data/exports
  log_dir: ./data/logs

cache:
  price_ttl_hours: 24
  estimate_ttl_hours: 24

freshness:
  price_days: 3
  sec_days: 120
  estimate_days: 14

ui:
  default_lookback_years: 20
  lookback_options: [5, 10, 15, 20]
  default_pe_method: static_15
  default_show_estimates: true
  default_theme: plotly_white
  demo_default_ticker: AAPL

valuation:
  static_pe_default: 15.0
  forecast_years: 2
  normal_pe_min_years: 3
  normal_pe_clip_quantiles: [0.05, 0.95]

scheduling:
  warm_watchlist_name: core
  nightly_refresh_enabled: true
```

### `conf/metrics.yml`
```yaml
metrics:
  - code: eps_diluted_actual
    label: Diluted EPS
    unit: USD/share
    display_order: 10
    exportable: true
    chart_role: fair_value_driver

  - code: revenue_actual
    label: Revenue
    unit: USD
    display_order: 20
    exportable: true
    chart_role: fundamentals_table

  - code: net_income_actual
    label: Net Income
    unit: USD
    display_order: 30
    exportable: true
    chart_role: fundamentals_table

  - code: shares_diluted_actual
    label: Diluted Shares
    unit: shares
    display_order: 40
    exportable: true
    chart_role: fundamentals_table

  - code: dividend_cash
    label: Cash Dividend
    unit: USD/share
    display_order: 50
    exportable: true
    chart_role: actions_table

  - code: eps_estimate_mean
    label: EPS Estimate Mean
    unit: USD/share
    display_order: 60
    exportable: true
    chart_role: estimate_driver
```

### `conf/concept_map.yml`
```yaml
concepts:
  eps_diluted_actual:
    preferred:
      - us-gaap:EarningsPerShareDiluted
      - ifrs-full:BasicAndDilutedEarningsLossPerShare
    fallback: []
    derive:
      numerator: us-gaap:NetIncomeLoss
      denominator: us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding

  revenue_actual:
    preferred:
      - us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax
      - us-gaap:SalesRevenueNet
      - us-gaap:Revenues
    fallback: []
    derive: null

  net_income_actual:
    preferred:
      - us-gaap:NetIncomeLoss
    fallback: []
    derive: null

  shares_diluted_actual:
    preferred:
      - us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding
    fallback: []
    derive: null
```

### `conf/watchlists.yml`
```yaml
watchlists:
  core:
    - AAPL
    - MSFT
    - KO
```

### Config validation rules
- Missing `SEC_USER_AGENT` is a hard error in live SEC mode
- Missing `FMP_API_KEY` is **not** a hard error; it triggers demo-estimates mode
- Invalid lookback options are a hard startup error
- Unknown metric codes in config are a hard startup error
- Concept maps must not reference blank preferred tags

## 36) Fixture, demo mode, and no-network test policy

### Default policy
- All default tests must run **without outbound network access**
- The builder must block network access in offline test runs using `pytest-socket`
- Live tests, if implemented, must be explicitly marked with `@pytest.mark.live` and skipped by default

### Required reference fixtures
The repository must include frozen fixture payloads for:
- `AAPL`
- `MSFT`
- `KO`

For each ticker, store at minimum:
- SEC submissions payload
- SEC companyfacts payload
- Yahoo price history payload or normalized frame fixture
- Yahoo corporate actions fixture
- FMP estimate snapshot fixture

### Suggested fixture layout
```text
tests/fixtures/
├── raw/
│   ├── sec/
│   │   ├── submissions/
│   │   │   ├── AAPL.json
│   │   │   ├── MSFT.json
│   │   │   └── KO.json
│   │   └── companyfacts/
│   │       ├── AAPL.json
│   │       ├── MSFT.json
│   │       └── KO.json
│   ├── yahoo/
│   │   ├── prices/
│   │   │   ├── AAPL.parquet
│   │   │   ├── MSFT.parquet
│   │   │   └── KO.parquet
│   │   └── actions/
│   │       ├── AAPL.parquet
│   │       ├── MSFT.parquet
│   │       └── KO.parquet
│   └── fmp/
│       ├── annual_estimates/
│       │   ├── AAPL.json
│       │   ├── MSFT.json
│       │   └── KO.json
├── expected/
│   ├── view_models/
│   ├── exports/
│   └── quality_reports/
└── demo_seed/
```

### Demo mode behavior
Demo mode is active when **any** of the following is true:
- `FMP_API_KEY` is empty
- the app is started with `APP_ENV=demo`
- live refresh is unavailable in the environment
- the builder intentionally seeds only fixtures for local demo

When demo mode is active:
- the app must still start successfully
- the dashboard must load without internet
- the UI must show a visible badge: `Demo mode: using frozen fixtures where live data is unavailable`
- estimates must be read from fixture snapshots if live FMP is unavailable
- `AAPL`, `MSFT`, and `KO` demo seeds must be available

### Demo seeding requirement
Implement `make demo-seed` and `fg demo-seed` to populate local bronze/silver/gold data from fixture sources for the reference tickers.

### Offline testing rule
- Unit, integration, snapshot, and smoke tests use fixtures only
- No default test may call SEC, Yahoo, or FMP live endpoints
- Any accidental live network access in offline tests must fail the test suite

## 37) Command contract for setup, run, refresh, export, and test

The builder must implement these commands exactly, either through `Makefile`, `fg`, or both as described below.

### Makefile commands
- `make setup`
  - create virtual environment if needed
  - install the package with dev dependencies
  - install pre-commit hooks
- `make lint`
  - run Ruff
- `make typecheck`
  - run mypy
- `make test`
  - run the full offline test suite with coverage and network blocked
- `make test-live`
  - run live tests only when explicitly enabled and secrets are present
- `make demo-seed`
  - populate demo data from fixtures
- `make run`
  - start the Dash app locally
- `make refresh TICKER=AAPL`
  - run end-to-end refresh for a single ticker
- `make build-gold TICKER=AAPL`
  - rebuild gold marts for a single ticker from existing bronze/silver data
- `make export TICKER=AAPL FORMAT=csv`
  - export the current ticker dataset to the selected format
- `make quality-report TICKER=AAPL`
  - emit a quality report
- `make ci`
  - run lint, typecheck, test in sequence

### CLI commands
The Typer CLI must expose:
- `fg run-dashboard`
- `fg demo-seed`
- `fg refresh-ticker --ticker AAPL`
- `fg refresh-watchlist --name core`
- `fg build-gold --ticker AAPL`
- `fg quality-report --ticker AAPL`
- `fg export --ticker AAPL --format csv`

### Command behavior requirements
- Every command must return exit code `0` on success and non-zero on failure
- Every command must log a concise success/failure message
- `fg run-dashboard` must start successfully in demo mode without live keys
- `fg refresh-ticker` must support offline fixture mode and live mode
- `fg export` must write files into `data/exports/`

## 38) CI/CD workflow and quality gates

### Required CI workflow
Create `.github/workflows/ci.yml` with at least these jobs:
1. `lint`
   - checkout
   - set up Python 3.12
   - install dev dependencies
   - run Ruff
2. `typecheck`
   - run mypy
3. `test`
   - run offline pytest suite with coverage and network blocked
4. `package`
   - build the distribution
5. `docker-build`
   - build the Docker image

### Required quality gates
The default CI pipeline must fail if any of the following are true:
- Ruff reports violations
- mypy reports errors in `src/fg`
- any offline test fails
- coverage for non-UI business logic is below **85%**
- the Dash smoke test fails
- the package build fails
- the Docker image build fails

### Required artifacts
On CI failure, upload at least:
- `pytest` output or JUnit XML
- coverage report
- snapshot diff artifacts if snapshot tests fail

### Optional nightly live workflow
Create `.github/workflows/nightly-live.yml` that:
- runs only on a schedule or manual dispatch
- is skipped if secrets are not configured
- optionally performs a live refresh for `AAPL`, `MSFT`, and `KO`
- writes a simple artifact/report
- does **not** block normal CI

## 39) UI layout, states, and formatting contract

This section defines the required dashboard behavior in detail.

### Global UI requirements
- Use Dash Pages with routes:
  - `/` for Overview
  - `/fundamentals`
  - `/audit`
- Use deterministic component IDs
- Keep CSS minimal
- Keep the UI functional without custom JavaScript

### Required component IDs

#### Shared stores
- `store-request`
- `store-valuation-dataset`
- `store-refresh-status`
- `store-export-payload`

#### Overview controls
- `overview-ticker-input`
- `overview-lookback-dropdown`
- `overview-pe-method-radio`
- `overview-manual-pe-input`
- `overview-show-estimates-toggle`
- `overview-refresh-button`
- `overview-cancel-button`

#### Overview display
- `overview-status-banner`
- `overview-demo-badge`
- `overview-freshness-badges`
- `overview-kpi-grid`
- `overview-main-graph`
- `overview-eps-bars`
- `overview-export-csv-button`
- `overview-export-xlsx-button`

#### Fundamentals
- `fundamentals-metric-selector`
- `fundamentals-period-selector`
- `fundamentals-confidence-filter`
- `fundamentals-annual-table`
- `fundamentals-quarterly-table`
- `fundamentals-concept-summary`

#### Audit
- `audit-methodology-card`
- `audit-lineage-table`
- `audit-quality-table`
- `audit-source-meta-panel`

### Overview page layout order
1. Title and mode badge row
2. Control row
3. Status banner / progress row
4. KPI grid
5. Main valuation graph
6. EPS bar chart
7. Freshness badges row
8. Export buttons row

### Default UI state
- In demo mode:
  - ticker input prefilled with `AAPL`
  - lookback `20`
  - P/E method `static_15`
  - show estimates `true`
- In live mode:
  - ticker input blank
  - same remaining defaults

### Control visibility rules
- `overview-manual-pe-input` is hidden unless the P/E method is `static_15`
- if the manual P/E field is blank, use the configured default static P/E
- if the manual P/E field is populated, it overrides the configured static P/E for the current request only

### Required loading states
- While refresh is running, show:
  - banner text: `Refreshing data…`
  - refresh button disabled
  - cancel button enabled
- While figures are rendering, show Dash loading spinners

### Required empty states
- Overview page with no request yet:
  - `No data loaded yet. Enter a ticker and click Refresh.`
- Fundamentals page with no dataset:
  - `No fundamentals loaded yet. Refresh a ticker from the Overview page.`
- Audit page with no dataset:
  - `No audit data loaded yet. Refresh a ticker from the Overview page.`

### Required error states
- Ticker resolution failure:
  - `Unable to resolve ticker. Check the symbol and try again.`
- Missing SEC fundamentals:
  - `No annual EPS actuals were found for the selected lookback.`
- Missing Yahoo prices:
  - `No market price history is available for this ticker.`
- Missing FMP estimates:
  - non-blocking warning: `Forward estimates unavailable; showing actuals only.`

### KPI cards
Display at least these cards:
- Last Price
- Latest Actual EPS
- Current P/E
- Selected P/E
- Fair Value Now
- Valuation Gap
- Last Filing Date
- Last Estimate Snapshot Date
- Data Quality Score

### Formatting rules
- Price values: `$123.45`
- EPS values: `12.34`
- P/E values: `15.00`
- Percentages: `12.3%`
- Dates: `2026-03-06`
- Missing non-blocking numeric values: em dash `—`
- Non-meaningful valuation values when EPS <= 0: `N/M`

### Main chart contract
- Trace order must be deterministic:
  1. price
  2. fair_value_actual
  3. fair_value_estimate
  4. normal_pe_value (only when selected)
- Actual fair value line is solid
- Estimate fair value line is dashed
- Current-month price point is included if available
- Plot title includes ticker and issuer name
- Use `plotly_white`
- No animated transitions in v1

### EPS bar chart contract
- Separate actual and estimated bars
- Hover must include:
  - fiscal year
  - EPS value
  - confidence
  - concept
  - filed date or estimate snapshot date

## 40) Error handling, logging, partial-failure rules, and secrets handling

### Logging
All runtime logging must be JSON-formatted and include, where relevant:
- timestamp
- level
- event
- request_id
- ticker
- cik
- source_name
- stage
- status
- duration_ms
- error_type
- message

Log destinations:
- stdout
- optional local file under `data/logs/app.jsonl`

### Request IDs
Every refresh request must generate a `request_id` that propagates through:
- ingestion
- normalization
- mart building
- export

### Retry policy
- SEC client: 3 attempts with exponential backoff
- FMP client: 3 attempts with exponential backoff
- Yahoo adapter: 2 attempts with exponential backoff
- Retry only on transient/network-like failures, not on permanent validation errors

### Partial-failure rules
- **SEC failure**
  - block valuation build
  - show blocking error
  - do not silently substitute another fundamentals source
- **Yahoo failure**
  - block valuation chart build
  - show blocking error
- **FMP failure**
  - continue with actuals-only dataset
  - hide estimate line
  - show non-blocking warning
  - allow export
- **Quality-check failure**
  - surface warning unless it invalidates required core data

### Data-lineage rule
Every displayed fact used in a chart, KPI, or table must be traceable to:
- source name
- period end
- concept
- filing date or snapshot date
- accession number or source reference
- confidence

### Secret handling
- Never commit `.env`
- Keep `.env.example` committed
- Redact API keys and sensitive query strings in logs
- Do not persist raw FMP URLs containing API keys
- Do not log full `SEC_USER_AGENT` values; log that it is configured, not its full literal content
- Bronze payload metadata must store a redacted endpoint name or URL without secrets

### Security boundaries
- SEC access must be server-side only
- Client browser must not call SEC directly
- The app must not expose raw secrets in any UI component, exported file, or log artifact

## 41) Manual QA script, golden outputs, and milestone completion criteria

### Manual QA script
The builder must perform this sequence and record the outcome in the final implementation report.

1. Run `make setup`
2. Run `make demo-seed`
3. Run `make test`
4. Run `make run`
5. Open the dashboard in demo mode
6. Verify Overview loads with `AAPL`
7. Verify KPI cards render and main chart displays price plus fair-value line
8. Toggle P/E method to `normal_pe` and verify the chart updates
9. Navigate to Fundamentals and verify annual and quarterly tables load
10. Navigate to Audit and verify lineage and quality tables load
11. Export CSV and XLSX
12. Confirm files are written under `data/exports/`
13. Refresh `MSFT` and `KO` in demo mode or fixture-backed mode
14. Confirm the quality-report command returns a non-empty report

### Golden output expectations
Golden outputs must be structural, not dependent on today’s live prices.

#### `AAPL`
- Overview dataset exists
- positive latest actual EPS
- non-null price series
- non-null actual fair-value series
- estimate series present in live or demo-estimate mode
- audit table contains lineage rows with accession numbers

#### `MSFT`
- same structural expectations as `AAPL`

#### `KO`
- long-history fundamentals and price series must produce a valid chart
- should be a useful sanity case for long lookback

### Required approved artifacts
Store approved outputs for at least one ticker:
- one view-model JSON snapshot
- one main chart Plotly JSON snapshot
- one EPS bar chart Plotly JSON snapshot
- one export header snapshot
- one quality-report snapshot

### Example export columns
The exported fundamentals/audit-friendly dataset must include at least:
- `ticker`
- `company_key`
- `metric_code`
- `period_type`
- `fiscal_year`
- `fiscal_quarter`
- `period_end_date`
- `value`
- `unit`
- `source_name`
- `confidence`
- `concept`
- `filed_at`
- `accession_no`

### Milestone completion criteria

#### Milestone 1 — end-to-end MVP
Done when all of the following are true:
- repo bootstraps successfully
- `make demo-seed` works
- `make run` starts the app
- Overview page renders `AAPL`
- static 15x chart works
- offline tests for core valuation logic pass

#### Milestone 2 — analytical credibility
Done when all of the following are true:
- quarterly and TTM logic exists
- `normal_pe` mode works
- audit page renders lineage and quality issues
- data quality score is computed
- snapshot tests exist and pass

#### Milestone 3 — usability and delivery
Done when all of the following are true:
- exports work
- background refresh path works
- CI passes
- Docker build succeeds
- final docs and implementation report exist

## 42) Final delivery and handoff contract

The builder must deliver a repository that includes **all** of the following.

### Required artifacts
- complete repository structure
- installable Python package
- working Dash app
- CLI commands from Section 37
- fixture-backed demo mode
- offline tests
- CI workflows
- Dockerfile and docker-compose
- docs:
  - `README.md`
  - `docs/technical-spec.md`
  - `docs/methodology.md`
  - `docs/runbook.md`
  - `docs/adr/0001-implementation-deviations.md`
  - `docs/final-implementation-report.md`

### Final implementation report requirements
`docs/final-implementation-report.md` must include:
1. build summary
2. repository tree summary
3. commands executed
4. test results
5. coverage result
6. CI/local quality gate status
7. demo mode status
8. live mode status, if attempted
9. known limitations
10. deviations from spec
11. next recommended improvements

### Definition of done
The project is done only if:
- `make ci` passes locally
- `make demo-seed` passes
- `make run` starts the dashboard
- the manual QA script from Section 41 has been completed and recorded
- required docs exist and are populated
- required exports and snapshots exist
- no required v1 file is an empty placeholder
- no required feature remains documented only as a TODO

### Final builder message expectation
When the builder finishes, it should be able to truthfully report:
- the repo was created
- the app runs locally in demo mode
- offline tests passed
- charts render from persisted data
- exports work
- known limitations are documented

### Final note
This document is intended to be sufficient for a **single autonomous agent** to build the repository end to end. Where Sections 1–30 describe architecture and methodology, **Sections 31–42 convert the spec into a deterministic execution brief**. If there is any conflict, the later sections win.

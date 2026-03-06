"""Ticker-to-CIK resolution and company dimension persistence."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from fg.clients.sec import SECClient
from fg.domain.models import CompanyRef
from fg.settings import Settings
from fg.storage.repositories import upsert_table


def _fixture_submission_path(ticker: str) -> Path:
    return Path("tests/fixtures/raw/sec/submissions") / f"{ticker.upper()}.json"


def resolve_company(
    settings: Settings,
    ticker: str,
    sec_client: SECClient | None = None,
    fixture_mode: bool = False,
) -> CompanyRef:
    """Resolve ticker to company reference and upsert dim_company."""
    normalized = ticker.upper().strip()
    payload: dict[str, Any]
    if fixture_mode:
        path = _fixture_submission_path(normalized)
        if not path.exists():
            raise ValueError(f"Unable to resolve ticker. Missing fixture for {normalized}")
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        if sec_client is None:
            sec_client = SECClient(user_agent=settings.sec_user_agent, max_rps=settings.max_sec_rps)
        payload = sec_client.resolve_ticker_to_cik(normalized)
    cik = str(payload.get("cik", "")).zfill(10)
    issuer_name = str(payload.get("issuer_name", payload.get("name", normalized)))
    fiscal_mmdd = str(payload.get("fiscal_year_end_mmdd", "1231"))
    exchange = str(payload.get("exchange", "UNKNOWN"))
    company = CompanyRef(
        company_key=cik,
        ticker=normalized,
        issuer_name=issuer_name,
        exchange=exchange,
        fiscal_year_end_mmdd=fiscal_mmdd,
        currency="USD",
        active=True,
    )
    row = pd.DataFrame(
        [
            {
                "company_key": cik,
                "cik": cik,
                "ticker": normalized,
                "issuer_name": issuer_name,
                "exchange": exchange,
                "fiscal_year_end_mmdd": fiscal_mmdd,
                "currency": "USD",
                "last_sec_pull_at": datetime.now(tz=timezone.utc).date().isoformat(),
                "last_yahoo_pull_at": None,
                "last_fmp_pull_at": None,
            }
        ]
    )
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="dim_company",
        key=cik,
        df=row,
        dedupe_keys=["company_key"],
    )
    return company



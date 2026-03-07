"""FMP estimate ingestion into bronze storage."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from fg.clients.fmp import FMPClient
from fg.domain.models import CompanyRef
from fg.settings import Settings
from fg.storage.repositories import upsert_table


def _load_fixture(ticker: str) -> dict[str, Any]:
    path = Path("tests/fixtures/raw/fmp/annual_estimates") / f"{ticker.upper()}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def empty_estimate_payload(ticker: str) -> dict[str, Any]:
    """Build an empty annual estimate payload when FMP is unavailable."""
    return {
        "ticker": ticker.upper(),
        "as_of_date": datetime.now(tz=timezone.utc).date().isoformat(),
        "period": "annual",
        "rows": [],
    }


def ingest_fmp(
    settings: Settings,
    company: CompanyRef,
    fmp_client: FMPClient | None = None,
    fixture_mode: bool = False,
) -> dict[str, Any]:
    """Ingest annual estimate snapshots from FMP or fixtures."""
    ticker = company.ticker.upper()
    if fixture_mode or not settings.fmp_api_key:
        payload = _load_fixture(ticker)
    else:
        fmp_client = fmp_client or FMPClient(api_key=settings.fmp_api_key)
        payload = fmp_client.fetch_annual_estimates(ticker=ticker)

    pulled_at = datetime.now(tz=timezone.utc).isoformat()
    payload_hash = sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    row = pd.DataFrame(
        [
            {
                "ticker": ticker,
                "period": "annual",
                "payload_json": json.dumps(payload, separators=(",", ":")),
                "pulled_at": pulled_at,
                "endpoint": "fmp/analyst-estimates",
                "payload_hash": payload_hash,
                "company_key": company.company_key,
            }
        ]
    )
    upsert_table(
        settings=settings,
        layer="bronze",
        table_name="bronze_fmp_estimates",
        key=ticker,
        df=row,
        dedupe_keys=["ticker", "payload_hash"],
    )
    return payload



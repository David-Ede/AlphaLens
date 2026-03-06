"""SEC raw payload ingestion into bronze storage."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from fg.clients.sec import SECClient
from fg.domain.models import CompanyRef
from fg.settings import Settings
from fg.storage.repositories import write_json_payload


def _read_fixture(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ingest_sec(
    settings: Settings,
    company: CompanyRef,
    sec_client: SECClient | None = None,
    fixture_mode: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Ingest SEC submissions and companyfacts into bronze tables."""
    ticker = company.ticker.upper()
    if fixture_mode:
        sub_path = Path("tests/fixtures/raw/sec/submissions") / f"{ticker}.json"
        facts_path = Path("tests/fixtures/raw/sec/companyfacts") / f"{ticker}.json"
        submissions = _read_fixture(sub_path)
        companyfacts = _read_fixture(facts_path)
    else:
        if sec_client is None:
            sec_client = SECClient(settings.sec_user_agent, max_rps=settings.max_sec_rps)
        submissions = sec_client.fetch_submissions(company.company_key)
        companyfacts = sec_client.fetch_companyfacts(company.company_key)

    pulled_at = datetime.now(tz=timezone.utc).isoformat()
    sub_hash = sha256(json.dumps(submissions, sort_keys=True).encode("utf-8")).hexdigest()
    facts_hash = sha256(json.dumps(companyfacts, sort_keys=True).encode("utf-8")).hexdigest()

    write_json_payload(
        settings=settings,
        layer="bronze",
        table_name="bronze_sec_submissions",
        key=company.company_key,
        payload=submissions,
        metadata={
            "cik": company.company_key,
            "ticker_requested": ticker,
            "pulled_at": pulled_at,
            "endpoint": "sec/submissions",
            "payload_hash": sub_hash,
        },
    )
    write_json_payload(
        settings=settings,
        layer="bronze",
        table_name="bronze_sec_companyfacts",
        key=company.company_key,
        payload=companyfacts,
        metadata={
            "cik": company.company_key,
            "pulled_at": pulled_at,
            "endpoint": "sec/companyfacts",
            "payload_hash": facts_hash,
        },
    )
    return submissions, companyfacts



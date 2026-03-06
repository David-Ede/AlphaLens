"""Yahoo raw market data ingestion into bronze storage."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import pandas as pd

from fg.clients.yahoo import YahooClient
from fg.domain.models import CompanyRef
from fg.settings import Settings
from fg.storage.repositories import upsert_table


def ingest_yahoo(
    settings: Settings,
    company: CompanyRef,
    yahoo_client: YahooClient | None = None,
    fixture_mode: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ingest Yahoo prices and actions into bronze tables."""
    ticker = company.ticker.upper()
    if fixture_mode:
        price_parquet = Path("tests/fixtures/raw/yahoo/prices") / f"{ticker}.parquet"
        price_csv = Path("tests/fixtures/raw/yahoo/prices") / f"{ticker}.csv"
        action_parquet = Path("tests/fixtures/raw/yahoo/actions") / f"{ticker}.parquet"
        action_csv = Path("tests/fixtures/raw/yahoo/actions") / f"{ticker}.csv"
        prices = pd.read_parquet(price_parquet) if price_parquet.exists() else pd.read_csv(price_csv)
        actions = pd.read_parquet(action_parquet) if action_parquet.exists() else pd.read_csv(action_csv)
    else:
        yahoo_client = yahoo_client or YahooClient()
        prices = yahoo_client.fetch_price_history(ticker)
        actions = yahoo_client.fetch_actions(ticker)

    pulled_at = datetime.now(tz=timezone.utc).isoformat()
    prices = prices.copy()
    prices["company_key"] = company.company_key
    prices["ticker"] = ticker
    prices["pulled_at"] = pulled_at
    prices["payload_hash"] = sha256(
        prices.to_json(date_format="iso", orient="records").encode("utf-8")
    ).hexdigest()

    actions = actions.copy()
    if actions.empty:
        actions = pd.DataFrame(
            columns=["action_date", "action_type", "cash_value", "split_ratio", "source_name"]
        )
    actions["company_key"] = company.company_key
    actions["ticker"] = ticker
    actions["pulled_at"] = pulled_at
    actions["payload_hash"] = sha256(
        actions.to_json(date_format="iso", orient="records").encode("utf-8")
    ).hexdigest()

    upsert_table(
        settings=settings,
        layer="bronze",
        table_name="bronze_yahoo_prices",
        key=ticker,
        df=prices,
        dedupe_keys=["company_key", "trade_date"],
    )
    upsert_table(
        settings=settings,
        layer="bronze",
        table_name="bronze_yahoo_actions",
        key=ticker,
        df=actions,
        dedupe_keys=["company_key", "action_date", "action_type"],
    )
    return prices, actions



"""Market-data normalization and monthly aggregation."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from fg.settings import Settings
from fg.storage.repositories import upsert_table


def normalize_market_data(
    settings: Settings,
    company_key: str,
    ticker: str,
    prices_bronze: pd.DataFrame,
    actions_bronze: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Normalize bronze Yahoo data into silver facts."""
    ingested_at = datetime.now(tz=timezone.utc).isoformat()
    prices = prices_bronze.copy()
    if prices.empty:
        daily = pd.DataFrame()
    else:
        daily = prices[
            [
                "company_key",
                "ticker",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "split_adjusted_close",
                "volume",
                "currency",
                "source_name",
            ]
        ].copy()
        daily["ingested_at"] = ingested_at
    actions = actions_bronze.copy()
    if actions.empty:
        action_fact = pd.DataFrame()
    else:
        action_fact = actions[
            ["company_key", "action_type", "action_date", "cash_value", "split_ratio", "source_name"]
        ].copy()
        action_fact["ingested_at"] = ingested_at
    monthly = build_monthly_price_series(daily)
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_price_daily",
        key=ticker,
        df=daily,
        dedupe_keys=["company_key", "trade_date"],
    )
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_corporate_action",
        key=ticker,
        df=action_fact,
        dedupe_keys=["company_key", "action_date", "action_type"],
    )
    upsert_table(
        settings=settings,
        layer="silver",
        table_name="fact_price_monthly",
        key=ticker,
        df=monthly,
        dedupe_keys=["company_key", "trade_date"],
    )
    return daily, action_fact, monthly


def build_monthly_price_series(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Build monthly price series using last trading day of each calendar month."""
    if daily_df.empty:
        return daily_df.copy()
    df = daily_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["month_key"] = df["trade_date"].dt.to_period("M")
    idx = df.sort_values("trade_date").groupby("month_key")["trade_date"].idxmax()
    monthly = df.loc[idx].drop(columns=["month_key"]).sort_values("trade_date")
    monthly["trade_date"] = monthly["trade_date"].dt.date.astype(str)
    return monthly.reset_index(drop=True)



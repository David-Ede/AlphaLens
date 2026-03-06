"""Yahoo Finance adapter via yfinance."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

LOGGER = logging.getLogger(__name__)


class YahooClient:
    """Fetch split-adjusted market data from Yahoo."""

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=3),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def fetch_price_history(self, ticker: str) -> pd.DataFrame:
        """Fetch full daily price history."""
        import yfinance as yf

        frame = yf.Ticker(ticker).history(period="max", auto_adjust=False)
        if frame.empty:
            raise ValueError(f"No Yahoo price history for {ticker}")
        frame = frame.reset_index().rename(
            columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "split_adjusted_close",
                "Volume": "volume",
            }
        )
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date.astype(str)
        frame["currency"] = "USD"
        frame["source_name"] = "yahoo"
        frame["pulled_at"] = datetime.now(tz=timezone.utc).isoformat()
        LOGGER.info(
            "yahoo_prices_ok",
            extra={"source_name": "yahoo", "ticker": ticker, "stage": "prices", "status": "ok"},
        )
        return frame[
            [
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "split_adjusted_close",
                "volume",
                "currency",
                "source_name",
                "pulled_at",
            ]
        ]

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=3),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def fetch_actions(self, ticker: str) -> pd.DataFrame:
        """Fetch dividend/split actions."""
        import yfinance as yf

        actions = yf.Ticker(ticker).actions
        if actions is None or actions.empty:
            return pd.DataFrame(columns=["action_date", "action_type", "cash_value", "split_ratio", "source_name"])
        rows: list[dict[str, object]] = []
        for idx, row in actions.reset_index().iterrows():
            action_date = str(pd.to_datetime(row.iloc[0]).date())
            dividend = float(row.get("Dividends", 0.0))
            split = float(row.get("Stock Splits", 0.0))
            if dividend > 0:
                rows.append(
                    {
                        "action_date": action_date,
                        "action_type": "dividend",
                        "cash_value": dividend,
                        "split_ratio": None,
                        "source_name": "yahoo",
                    }
                )
            if split > 0:
                rows.append(
                    {
                        "action_date": action_date,
                        "action_type": "split",
                        "cash_value": None,
                        "split_ratio": split,
                        "source_name": "yahoo",
                    }
                )
            _ = idx
        return pd.DataFrame(rows)



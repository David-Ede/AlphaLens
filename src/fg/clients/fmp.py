"""Financial Modeling Prep client for estimate snapshots."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

LOGGER = logging.getLogger(__name__)


class FMPClient:
    """Fetch annual/quarterly estimate snapshots from FMP."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    def _fetch(self, ticker: str, period: str) -> list[dict[str, Any]]:
        if not self.api_key:
            raise ValueError("FMP_API_KEY is not configured.")
        url = (
            "https://financialmodelingprep.com/stable/analyst-estimates?"
            f"symbol={ticker}&period={period}&apikey={self.api_key}"
        )
        with httpx.Client(timeout=20.0) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
        LOGGER.info(
            "fmp_request_ok",
            extra={
                "source_name": "fmp",
                "ticker": ticker,
                "stage": "http",
                "status": "ok",
                "message": f"Fetched endpoint hash={sha256(url.encode()).hexdigest()[:12]}",
            },
        )
        if not isinstance(payload, list):
            return []
        return payload

    def fetch_annual_estimates(self, ticker: str) -> dict[str, Any]:
        """Fetch annual estimates with snapshot timestamp."""
        data = self._fetch(ticker=ticker, period="annual")
        return {
            "ticker": ticker.upper(),
            "as_of_date": datetime.now(tz=timezone.utc).date().isoformat(),
            "period": "annual",
            "rows": data,
        }

    def fetch_quarterly_estimates(self, ticker: str) -> dict[str, Any]:
        """Fetch quarterly estimates with snapshot timestamp."""
        data = self._fetch(ticker=ticker, period="quarter")
        return {
            "ticker": ticker.upper(),
            "as_of_date": datetime.now(tz=timezone.utc).date().isoformat(),
            "period": "quarterly",
            "rows": data,
        }



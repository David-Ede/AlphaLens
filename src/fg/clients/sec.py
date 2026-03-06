"""SEC HTTP client with rate limiting and retries."""

from __future__ import annotations

import logging
import time
from hashlib import sha256
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

LOGGER = logging.getLogger(__name__)

_STATIC_TICKER_MAP: dict[str, tuple[str, str]] = {
    "AAPL": ("0000320193", "Apple Inc."),
    "MSFT": ("0000789019", "Microsoft Corp"),
    "KO": ("0000021344", "Coca-Cola Co"),
}


class SECClient:
    """SEC client for submissions/companyfacts and ticker resolution."""

    def __init__(self, user_agent: str, max_rps: int = 8) -> None:
        self.user_agent = user_agent.strip()
        self.max_rps = max_rps
        self._last_request_monotonic = 0.0
        if not self.user_agent:
            raise ValueError("SEC_USER_AGENT is required for live SEC access.")

    def _sleep_for_rate_limit(self) -> None:
        min_delay = 1 / max(self.max_rps, 1)
        elapsed = time.monotonic() - self._last_request_monotonic
        if elapsed < min_delay:
            time.sleep(min_delay - elapsed)
        self._last_request_monotonic = time.monotonic()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        reraise=True,
    )
    def _get_json(self, url: str) -> dict[str, Any]:
        self._sleep_for_rate_limit()
        headers = {"User-Agent": self.user_agent, "Accept": "application/json"}
        with httpx.Client(timeout=20.0, headers=headers) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
        LOGGER.info(
            "sec_request_ok",
            extra={
                "source_name": "sec",
                "stage": "http",
                "status": "ok",
                "message": f"Fetched endpoint hash={sha256(url.encode()).hexdigest()[:12]}",
            },
        )
        return payload

    def resolve_ticker_to_cik(self, ticker: str) -> dict[str, str]:
        """Resolve ticker to CIK and issuer name."""
        normalized = ticker.upper().strip()
        if normalized in _STATIC_TICKER_MAP:
            cik, name = _STATIC_TICKER_MAP[normalized]
            return {"ticker": normalized, "cik": cik, "issuer_name": name}
        data = self._get_json("https://www.sec.gov/files/company_tickers.json")
        for item in data.values():
            if str(item.get("ticker", "")).upper() == normalized:
                cik = str(item.get("cik_str", "")).zfill(10)
                return {
                    "ticker": normalized,
                    "cik": cik,
                    "issuer_name": str(item.get("title", normalized)),
                }
        raise ValueError(f"Unable to resolve ticker {normalized}")

    def fetch_submissions(self, cik: str) -> dict[str, Any]:
        """Fetch SEC submissions payload for CIK."""
        padded = str(cik).zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{padded}.json"
        return self._get_json(url)

    def fetch_companyfacts(self, cik: str) -> dict[str, Any]:
        """Fetch SEC companyfacts payload for CIK."""
        padded = str(cik).zfill(10)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json"
        return self._get_json(url)

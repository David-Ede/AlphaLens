"""Domain enums."""

from __future__ import annotations

from enum import Enum


class PeriodType(str, Enum):
    """Supported period types."""

    ANNUAL = "annual"
    QUARTERLY = "quarterly"
    TTM = "ttm"


class Confidence(str, Enum):
    """Data confidence tags."""

    REPORTED = "reported"
    FALLBACK_TAG = "fallback_tag"
    DERIVED = "derived"
    ESTIMATE = "estimate"


class ActionType(str, Enum):
    """Corporate action types."""

    DIVIDEND = "dividend"
    SPLIT = "split"


class PEMethod(str, Enum):
    """Valuation P/E methods."""

    STATIC_15 = "static_15"
    NORMAL_PE = "normal_pe"


class FreshnessStatus(str, Enum):
    """Freshness status for source pulls."""

    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    """Quality issue severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

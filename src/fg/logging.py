"""JSON logging for runtime events."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format log records as JSON lines."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "event": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "request_id",
            "ticker",
            "cik",
            "source_name",
            "stage",
            "status",
            "duration_ms",
            "error_type",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", log_path: Path | None = None) -> None:
    """Configure root logger with JSON output."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=handlers,
        format="%(message)s",
        force=True,
    )
    formatter = JsonFormatter()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)



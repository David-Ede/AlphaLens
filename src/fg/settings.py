"""Application settings and YAML/env config loading."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings merged with YAML config files."""

    app_env: str = Field(default="local", alias="APP_ENV")
    sec_user_agent: str = Field(default="", alias="SEC_USER_AGENT")
    fmp_api_key: str = Field(default="", alias="FMP_API_KEY")
    data_root: str = Field(default="./data", alias="DATA_ROOT")
    duckdb_path: str = Field(default="./data/fg.duckdb", alias="DUCKDB_PATH")
    max_sec_rps: int = Field(default=8, alias="MAX_SEC_RPS")
    default_static_pe: float = Field(default=15.0, alias="DEFAULT_STATIC_PE")
    default_lookback_years: int = Field(default=20, alias="DEFAULT_LOOKBACK_YEARS")
    price_cache_ttl_hours: int = Field(default=24, alias="PRICE_CACHE_TTL_HOURS")
    estimate_cache_ttl_hours: int = Field(default=24, alias="ESTIMATE_CACHE_TTL_HOURS")
    dash_debug: bool = Field(default=True, alias="DASH_DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    redis_url: str = Field(default="redis://redis:6379/0", alias="REDIS_URL")
    warm_tickers: str = Field(default="AAPL,MSFT,KO", alias="WARM_TICKERS")

    app_config: dict[str, Any] = Field(default_factory=dict)
    metrics_config: dict[str, Any] = Field(default_factory=dict)
    concept_map_config: dict[str, Any] = Field(default_factory=dict)
    watchlists_config: dict[str, Any] = Field(default_factory=dict)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @field_validator("default_lookback_years")
    @classmethod
    def _validate_default_lookback(cls, value: int) -> int:
        if value not in {5, 10, 15, 20}:
            msg = "DEFAULT_LOOKBACK_YEARS must be one of 5, 10, 15, 20."
            raise ValueError(msg)
        return value

    def load_yaml(self, root: Path | None = None) -> None:
        """Load YAML config files into settings."""
        base = root or Path.cwd()
        self.app_config = _read_yaml(base / "conf" / "app.yml")
        self.metrics_config = _read_yaml(base / "conf" / "metrics.yml")
        self.concept_map_config = _read_yaml(base / "conf" / "concept_map.yml")
        self.watchlists_config = _read_yaml(base / "conf" / "watchlists.yml")
        self._validate_yaml_contract()

    def _validate_yaml_contract(self) -> None:
        lookbacks = self.app_config.get("ui", {}).get("lookback_options", [])
        if sorted(lookbacks) != [5, 10, 15, 20]:
            msg = "conf/app.yml ui.lookback_options must be [5, 10, 15, 20]"
            raise ValueError(msg)
        metric_codes = {item["code"] for item in self.metrics_config.get("metrics", [])}
        required_codes = {
            "eps_diluted_actual",
            "revenue_actual",
            "net_income_actual",
            "shares_diluted_actual",
            "dividend_cash",
            "eps_estimate_mean",
        }
        if not required_codes.issubset(metric_codes):
            missing = required_codes.difference(metric_codes)
            msg = f"conf/metrics.yml missing required metrics: {sorted(missing)}"
            raise ValueError(msg)
        concepts = self.concept_map_config.get("concepts", {})
        for metric, mapping in concepts.items():
            preferred = mapping.get("preferred", [])
            if any(not str(tag).strip() for tag in preferred):
                msg = f"conf/concept_map.yml contains blank preferred concept for {metric}"
                raise ValueError(msg)

    @property
    def watchlist(self) -> list[str]:
        """Return warm tickers list."""
        from_env = [t.strip().upper() for t in self.warm_tickers.split(",") if t.strip()]
        if from_env:
            return from_env
        core = self.watchlists_config.get("watchlists", {}).get("core", [])
        return [str(t).upper() for t in core]

    @property
    def is_demo_mode(self) -> bool:
        """Return whether demo mode should be active."""
        if self.app_env.lower() == "demo":
            return True
        if not self.fmp_api_key.strip():
            return True
        return bool(self.app_config.get("app", {}).get("demo_mode_default", True))

    @property
    def ui_defaults(self) -> dict[str, Any]:
        """Expose UI defaults from config."""
        return self.app_config.get("ui", {})

    @property
    def valuation_defaults(self) -> dict[str, Any]:
        """Expose valuation defaults from config."""
        return self.app_config.get("valuation", {})

    @property
    def data_dirs(self) -> dict[str, Path]:
        """Return canonical data directories."""
        root = Path(self.data_root)
        return {
            "root": root,
            "duckdb_path": Path(self.duckdb_path),
            "bronze": root / "bronze",
            "silver": root / "silver",
            "gold": root / "gold",
            "cache": root / "cache",
            "exports": root / "exports",
            "logs": root / "logs",
        }

    def ensure_data_dirs(self) -> None:
        """Create data directories if they do not already exist."""
        for path in self.data_dirs.values():
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
            else:
                path.mkdir(parents=True, exist_ok=True)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        msg = f"Invalid YAML object at {path}"
        raise ValueError(msg)
    return data


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Create and cache settings instance."""
    settings = Settings()
    settings.load_yaml()
    settings.ensure_data_dirs()
    if settings.sec_user_agent.strip() == "" and settings.app_env.lower() != "demo":
        # Live SEC mode requires explicit User-Agent.
        pass
    return settings


class RequestState(BaseModel):
    """Serialized overview request state used in shared Dash store."""

    ticker: str
    lookback_years: int = 20
    pe_method: str = "static_15"
    manual_pe: float | None = None
    show_estimates: bool = True

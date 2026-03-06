"""Canonical filesystem path helpers."""

from __future__ import annotations

from pathlib import Path

from fg.settings import Settings


def table_dir(settings: Settings, layer: str, table_name: str) -> Path:
    """Return table directory for layer/table."""
    root = settings.data_dirs[layer]
    path = root / table_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def table_file(
    settings: Settings,
    layer: str,
    table_name: str,
    key: str,
    suffix: str = ".parquet",
) -> Path:
    """Return deterministic parquet file path for keyed writes."""
    safe_key = key.replace("/", "_").replace("\\", "_").upper()
    return table_dir(settings, layer, table_name) / f"{safe_key}{suffix}"


def all_table_files(settings: Settings, layer: str, table_name: str) -> list[Path]:
    """Return all parquet files for table."""
    path = table_dir(settings, layer, table_name)
    return sorted(list(path.glob("*.parquet")) + list(path.glob("*.csv")))

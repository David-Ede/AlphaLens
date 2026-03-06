"""Parquet-backed repositories for bronze/silver/gold layers."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from fg.settings import Settings
from fg.storage.paths import all_table_files, table_file


def _parquet_engine_available() -> bool:
    return bool(importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"))


def _string_converter(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return "" if text.lower() == "nan" else text


def _read_csv_with_key_converters(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        converters={
            "company_key": _string_converter,
            "cik": _string_converter,
            "ticker": _string_converter,
            "accession_no": _string_converter,
            "fiscal_year_end_mmdd": _string_converter,
        },
    )


def write_table(
    settings: Settings,
    layer: str,
    table_name: str,
    key: str,
    df: pd.DataFrame,
) -> Path:
    """Write a keyed parquet table file."""
    use_parquet = _parquet_engine_available()
    suffix = ".parquet" if use_parquet else ".csv"
    path = table_file(settings, layer, table_name, key, suffix=suffix)
    path.parent.mkdir(parents=True, exist_ok=True)
    if use_parquet:
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    return path


def read_table(
    settings: Settings,
    layer: str,
    table_name: str,
    key: str | None = None,
) -> pd.DataFrame:
    """Read one keyed parquet file or all files in a table."""
    if key is not None:
        parquet_path = table_file(settings, layer, table_name, key, suffix=".parquet")
        csv_path = table_file(settings, layer, table_name, key, suffix=".csv")
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
        if csv_path.exists():
            try:
                return _read_csv_with_key_converters(csv_path)
            except EmptyDataError:
                return pd.DataFrame()
        return pd.DataFrame()
    files = all_table_files(settings, layer, table_name)
    if not files:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for file in files:
        if file.suffix == ".parquet":
            frames.append(pd.read_parquet(file))
        else:
            try:
                frames.append(_read_csv_with_key_converters(file))
            except EmptyDataError:
                frames.append(pd.DataFrame())
    return pd.concat(frames, ignore_index=True)


def upsert_table(
    settings: Settings,
    layer: str,
    table_name: str,
    key: str,
    df: pd.DataFrame,
    dedupe_keys: list[str] | None = None,
) -> Path:
    """Upsert by appending existing file then deduping."""
    existing = read_table(settings, layer, table_name, key=key)
    merged = pd.concat([existing, df], ignore_index=True) if not existing.empty else df
    if dedupe_keys and not merged.empty:
        merged = merged.drop_duplicates(subset=dedupe_keys, keep="last")
    return write_table(settings, layer, table_name, key, merged)


def write_json_payload(
    settings: Settings,
    layer: str,
    table_name: str,
    key: str,
    payload: dict[str, Any],
    metadata: dict[str, Any],
) -> Path:
    """Write raw JSON payload metadata row into parquet bronze table."""
    row = {**metadata, "payload_json": json.dumps(payload, separators=(",", ":"))}
    return upsert_table(
        settings=settings,
        layer=layer,
        table_name=table_name,
        key=key,
        df=pd.DataFrame([row]),
    )


def read_json_payload(
    settings: Settings,
    layer: str,
    table_name: str,
    key: str,
) -> dict[str, Any] | None:
    """Read latest JSON payload from keyed bronze table."""
    df = read_table(settings, layer, table_name, key=key)
    if df.empty:
        return None
    payload = str(df.sort_values(df.columns[0]).iloc[-1]["payload_json"])
    return json.loads(payload)


def write_export(
    settings: Settings,
    file_name: str,
    content: bytes,
) -> Path:
    """Write export payload under data/exports."""
    output = settings.data_dirs["exports"] / file_name
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(content)
    return output

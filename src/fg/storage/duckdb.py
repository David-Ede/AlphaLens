"""DuckDB connection and SQL helpers."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def get_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def run_sql(db_path: Path, sql: str) -> None:
    """Execute SQL statement."""
    conn = get_connection(db_path)
    try:
        conn.execute(sql)
    finally:
        conn.close()


def query_df(db_path: Path, sql: str) -> pd.DataFrame:
    """Execute SQL query and return dataframe."""
    conn = get_connection(db_path)
    try:
        return conn.execute(sql).df()
    finally:
        conn.close()

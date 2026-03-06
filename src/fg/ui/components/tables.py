"""Standardized Dash DataTable builders."""

from __future__ import annotations

from dash import dash_table


def build_table(table_id: str, columns: list[str]) -> dash_table.DataTable:
    """Build a deterministic table with minimal styling."""
    return dash_table.DataTable(
        id=table_id,
        columns=[{"name": col, "id": col} for col in columns],
        data=[],
        sort_action="native",
        filter_action="native",
        page_size=15,
        style_table={"overflowX": "auto"},
        style_cell={"fontSize": 12, "padding": "6px", "textAlign": "left"},
    )

"""Structural snapshot tests for view model, charts, and exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fg.services.chart_service import build_eps_bar_chart, build_main_chart
from fg.services.export_service import build_export_frame
from fg.services.refresh_service import RefreshService
from fg.settings import Settings
from fg.storage.repositories import read_table

SNAPSHOT_DIR = Path("tests/snapshots")


def _load_snapshot(name: str) -> Any:
    return json.loads((SNAPSHOT_DIR / name).read_text(encoding="utf-8"))


def test_view_model_snapshot(seeded_settings: Settings) -> None:
    payload = RefreshService(seeded_settings).load_view_model("AAPL", 20, "static_15")
    expected = _load_snapshot("aapl_view_model.json")
    assert sorted(payload.keys()) == sorted(expected["top_level_keys"])
    assert sorted(payload["meta"].keys()) == sorted(expected["meta_keys"])
    assert sorted(payload["series"].keys()) == sorted(expected["series_keys"])
    assert sorted(payload["tables"].keys()) == sorted(expected["table_keys"])


def test_chart_snapshots(seeded_settings: Settings) -> None:
    payload = RefreshService(seeded_settings).load_view_model("AAPL", 20, "static_15")
    expected_main = _load_snapshot("aapl_main_chart.json")
    expected_eps = _load_snapshot("aapl_eps_chart.json")
    series_records = (
        payload["series"]["price"]
        + payload["series"]["fair_value_actual"]
        + payload["series"]["fair_value_estimate"]
        + payload["series"]["normal_pe_value"]
    )
    main = build_main_chart("AAPL", "Apple Inc.", pd.DataFrame(series_records)).to_plotly_json()
    eps = build_eps_bar_chart(pd.DataFrame(payload["series"]["eps_bars"])).to_plotly_json()
    trace_names = [trace["name"] for trace in main["data"]]
    eps_trace_names = [trace["name"] for trace in eps["data"]]
    assert trace_names == expected_main["trace_order"]
    assert eps_trace_names == expected_eps["trace_order"]


def test_export_header_and_quality_snapshot(seeded_settings: Settings) -> None:
    frame = build_export_frame(seeded_settings, "AAPL")
    assert list(frame.columns) == _load_snapshot("export_headers.json")
    dim = read_table(seeded_settings, "silver", "dim_company")
    company_key = str(dim[dim["ticker"] == "AAPL"].iloc[-1]["company_key"])
    quality = read_table(seeded_settings, "silver", "fact_quality_issue", key=company_key)
    payload = {
        "ticker": "AAPL",
        "issue_count": len(quality),
        "issue_codes": sorted(quality["issue_code"].tolist()) if not quality.empty else [],
    }
    expected = _load_snapshot("aapl_quality_report.json")
    assert payload["ticker"] == expected["ticker"]
    assert payload["issue_count"] >= expected["min_issue_count"]

"""Dash smoke tests without browser automation."""

from __future__ import annotations

from fg.services.export_service import write_export_file
from fg.services.refresh_service import RefreshService
from fg.settings import Settings
from fg.ui.app import create_app


def test_dash_routes_load(seeded_settings: Settings) -> None:
    app = create_app(seeded_settings)
    client = app.server.test_client()
    for route in ["/", "/fundamentals", "/audit"]:
        response = client.get(route)
        assert response.status_code == 200


def test_load_view_model_and_export(seeded_settings: Settings) -> None:
    service = RefreshService(seeded_settings)
    payload = service.load_view_model("AAPL", 20, "static_15")
    assert payload["meta"]["ticker"] == "AAPL"
    assert len(payload["series"]["price"]) > 0
    csv_path = write_export_file(seeded_settings, "AAPL", "csv")
    xlsx_path = write_export_file(seeded_settings, "AAPL", "xlsx")
    assert csv_path.endswith(".csv")
    assert xlsx_path.endswith(".xlsx")

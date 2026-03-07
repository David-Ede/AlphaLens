"""Refresh orchestration callback."""

from __future__ import annotations

from typing import Any

from dash import Input, Output, State, callback, no_update

from fg.domain.enums import PEMethod
from fg.domain.models import RefreshRequest
from fg.services.refresh_service import RefreshService
from fg.settings import get_settings


def register_callbacks() -> None:
    """Register refresh callback."""

    @callback(
        Output("store-refresh-status", "data"),
        Output("overview-status-banner", "children"),
        Output("overview-refresh-button", "disabled"),
        Output("overview-cancel-button", "disabled"),
        Input("overview-refresh-button", "n_clicks"),
        Input("overview-cancel-button", "n_clicks"),
        State("store-request", "data"),
        prevent_initial_call=True,
    )
    def run_refresh(
        refresh_clicks: int,
        cancel_clicks: int,
        request_data: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], str, bool, bool]:
        if cancel_clicks and cancel_clicks > refresh_clicks:
            return {"status": "cancelled"}, "Refresh cancelled.", False, True
        if not refresh_clicks:
            return no_update, no_update, False, True
        if not request_data or not request_data.get("ticker"):
            return {"status": "error"}, "Select a ticker from the list and try again.", False, True
        service = RefreshService(get_settings())
        request = RefreshRequest(
            ticker=str(request_data["ticker"]),
            lookback_years=int(request_data.get("lookback_years", 20)),
            pe_method=PEMethod(str(request_data.get("pe_method", "static_15"))),
            manual_pe=request_data.get("manual_pe"),
            show_estimates=bool(request_data.get("show_estimates", True)),
        )
        status = service.refresh_ticker(request=request)
        return status, "Refresh complete.", False, True

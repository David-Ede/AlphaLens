"""Export callbacks."""

from __future__ import annotations

from dash import Input, Output, State, callback, ctx, dcc, no_update

from fg.services.export_service import write_export_file
from fg.settings import get_settings


def register_callbacks() -> None:
    """Register export callback."""

    @callback(
        Output("download-export", "data"),
        Input("overview-export-csv-button", "n_clicks"),
        Input("overview-export-xlsx-button", "n_clicks"),
        State("store-valuation-dataset", "data"),
        prevent_initial_call=True,
    )
    def export_payload(
        csv_clicks: int,
        xlsx_clicks: int,
        dataset: dict | None,
    ) -> dict | None:
        _ = (csv_clicks, xlsx_clicks)
        if not dataset or not dataset.get("meta", {}).get("ticker"):
            return no_update
        ticker = str(dataset["meta"]["ticker"])
        button = ctx.triggered_id
        output_format = "xlsx" if button == "overview-export-xlsx-button" else "csv"
        path = write_export_file(get_settings(), ticker=ticker, output_format=output_format)
        return dcc.send_file(path)

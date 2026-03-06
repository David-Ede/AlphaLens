"""Dash app factory and callback wiring."""

from __future__ import annotations

from dash import Dash, dcc, html, page_container, page_registry

from fg.logging import configure_logging
from fg.services.refresh_service import RefreshService
from fg.settings import Settings, get_settings
from fg.ui.callbacks import audit as audit_callbacks
from fg.ui.callbacks import export as export_callbacks
from fg.ui.callbacks import fundamentals as fundamentals_callbacks
from fg.ui.callbacks import overview as overview_callbacks
from fg.ui.callbacks import refresh as refresh_callbacks


def create_app(settings: Settings | None = None) -> Dash:
    """Create configured Dash application."""
    runtime = settings or get_settings()
    configure_logging(level=runtime.log_level, log_path=runtime.data_dirs["logs"] / "app.jsonl")
    _ensure_demo_seed(runtime)
    app = Dash(
        __name__,
        use_pages=True,
        pages_folder="",
        suppress_callback_exceptions=True,
        title="AlphaLens",
    )
    # Register pages after app instantiation.
    from fg.ui.pages import audit, fundamentals, overview  # noqa: F401

    app.layout = html.Div(
        className="app-shell",
        children=[
            dcc.Store(id="store-request"),
            dcc.Store(id="store-valuation-dataset"),
            dcc.Store(id="store-refresh-status"),
            dcc.Store(id="store-export-payload"),
            dcc.Download(id="download-export"),
            html.Div(
                className="nav-links row",
                children=[
                    dcc.Link(page["name"], href=page["path"])
                    for page in page_registry.values()
                    if page["module"].startswith("fg.ui.pages")
                ],
            ),
            page_container,
        ],
    )
    overview_callbacks.register_callbacks()
    refresh_callbacks.register_callbacks()
    fundamentals_callbacks.register_callbacks()
    audit_callbacks.register_callbacks()
    export_callbacks.register_callbacks()
    return app


def _ensure_demo_seed(settings: Settings) -> None:
    """Auto-seed fixtures on startup if demo mode has no gold data."""
    gold_dir = settings.data_dirs["gold"]
    has_gold = gold_dir.exists() and any(gold_dir.rglob("*.parquet"))
    if has_gold:
        return
    if not settings.is_demo_mode:
        return
    service = RefreshService(settings)
    service.demo_seed(tickers=["AAPL", "MSFT", "KO"])


def run_dashboard(host: str = "127.0.0.1", port: int = 8050, debug: bool | None = None) -> None:
    """Run Dash development server."""
    settings = get_settings()
    app = create_app(settings)
    app.run(host=host, port=port, debug=settings.dash_debug if debug is None else debug)

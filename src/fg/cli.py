"""Typer CLI entry points."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import typer

from fg.domain.enums import PEMethod
from fg.domain.models import RefreshRequest
from fg.services.export_service import write_export_file
from fg.services.refresh_service import RefreshService
from fg.settings import get_settings
from fg.storage.repositories import read_table
from fg.ui.app import run_dashboard

app = typer.Typer(help="AlphaLens CLI")


@app.command("run-dashboard")
def run_dashboard_cmd() -> None:
    """Start Dash app in local mode."""
    run_dashboard()


@app.command("demo-seed")
def demo_seed_cmd() -> None:
    """Seed demo tickers from fixture payloads."""
    service = RefreshService(get_settings())
    result = service.demo_seed(["AAPL", "MSFT", "KO"])
    typer.echo(f"Demo seed complete for {len(result)} tickers.")


@app.command("refresh-ticker")
def refresh_ticker_cmd(
    ticker: str = typer.Option(..., "--ticker", "-t"),
    lookback_years: int = typer.Option(20, "--lookback-years"),
    pe_method: PEMethod = typer.Option(PEMethod.STATIC_15, "--pe-method"),
    show_estimates: bool = typer.Option(True, "--show-estimates/--no-show-estimates"),
    manual_pe: float | None = typer.Option(None, "--manual-pe"),
) -> None:
    """Run end-to-end refresh for one ticker."""
    service = RefreshService(get_settings())
    req = RefreshRequest(
        ticker=ticker.upper(),
        lookback_years=lookback_years,
        pe_method=pe_method,
        show_estimates=show_estimates,
        manual_pe=manual_pe,
    )
    result = service.refresh_ticker(req)
    typer.echo(json.dumps(result, indent=2))


@app.command("refresh-watchlist")
def refresh_watchlist_cmd(name: str = typer.Option("core", "--name")) -> None:
    """Refresh all tickers in configured watchlist."""
    settings = get_settings()
    watchlists = settings.watchlists_config.get("watchlists", {})
    tickers = [str(t).upper() for t in watchlists.get(name, [])]
    if not tickers:
        raise typer.BadParameter(f"Watchlist not found or empty: {name}")
    service = RefreshService(settings)
    for ticker in tickers:
        req = RefreshRequest(ticker=ticker, lookback_years=20, pe_method=PEMethod.STATIC_15)
        service.refresh_ticker(req)
    typer.echo(f"Refreshed watchlist '{name}' ({len(tickers)} tickers).")


@app.command("build-gold")
def build_gold_cmd(ticker: str = typer.Option(..., "--ticker", "-t")) -> None:
    """Rebuild gold marts from existing silver data for ticker."""
    service = RefreshService(get_settings())
    result = service.build_gold(ticker=ticker.upper())
    typer.echo(json.dumps(result, indent=2))


@app.command("quality-report")
def quality_report_cmd(ticker: str = typer.Option(..., "--ticker", "-t")) -> None:
    """Emit quality report to stdout and fixture snapshot path."""
    settings = get_settings()
    dim = read_table(settings, "silver", "dim_company")
    row = dim[dim["ticker"] == ticker.upper()]
    if row.empty:
        raise typer.BadParameter(f"Ticker has no seeded data: {ticker}")
    company_key = str(row.iloc[-1]["company_key"])
    issues = read_table(settings, "silver", "fact_quality_issue", key=company_key)
    payload = {
        "ticker": ticker.upper(),
        "company_key": company_key,
        "issue_count": len(issues),
        "issues": issues.to_dict(orient="records"),
    }
    out_dir = Path("tests/fixtures/expected/quality_reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{ticker.upper()}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    typer.echo(json.dumps(payload, indent=2))


@app.command("export")
def export_cmd(
    ticker: str = typer.Option(..., "--ticker", "-t"),
    format: Literal["csv", "xlsx"] = typer.Option("csv", "--format"),
) -> None:
    """Export canonical fundamentals dataset."""
    path = write_export_file(get_settings(), ticker=ticker.upper(), output_format=format)
    typer.echo(f"Export written: {path}")

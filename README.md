# AlphaLens

AlphaLens is a Python + Dash implementation of the FAST Graphs core valuation workflow:
split-adjusted price history, earnings-based fair value, optional normal P/E, and forward EPS extension.

## Project Purpose

Build an audit-friendly valuation dashboard where each displayed number can be traced to source lineage.

## Architecture Summary

- `src/fg/clients`: SEC, Yahoo, and FMP adapters
- `src/fg/ingestion` and `src/fg/normalization`: bronze -> silver pipeline
- `src/fg/marts`: gold datasets for UI
- `src/fg/services`: orchestration, charting, exports, and audit assembly
- `src/fg/ui`: Dash Pages app (`/`, `/fundamentals`, `/audit`)
- `DuckDB + Parquet`: local storage
- `tests/fixtures`: frozen demo fixtures for offline execution

## Data Source Policy

- SEC: only source of truth for historical fundamentals
- Yahoo (`yfinance`): only source of truth for prices/dividends/splits
- FMP: only source of truth for forward estimates

## Demo Mode

Demo mode is enabled when `APP_ENV=demo`, `FMP_API_KEY` is empty, or only local fixtures are available.
It auto-seeds `AAPL`, `MSFT`, and `KO` and shows a visible demo badge in the dashboard.

## Setup Instructions

```bash
make setup
cp .env.example .env
make demo-seed
```

## No-Terminal Launch In VS Code

Use this when you do not want to type commands in a terminal:

1. Open `run_dashboard.py` in VS Code.
2. Click the **Run Python File** button.
3. The launcher will:
   - verify Python 3.12+
   - install app dependencies once if missing
   - create `.env` from `.env.example` if needed
   - auto-select an open localhost port (starting from `8050`)
   - open your browser to the exact selected URL
4. In the dashboard, enter `MSFT` on Overview and click **Refresh**.

The launcher forces `debug=False` to avoid duplicate browser tabs from the dev reloader.

## Commands

- `make run`
- `make refresh TICKER=AAPL`
- `make build-gold TICKER=AAPL`
- `make export TICKER=AAPL FORMAT=csv`
- `make quality-report TICKER=AAPL`
- `make ci`

Equivalent CLI commands are under `fg --help`.

## Testing Instructions

```bash
make test
```

Offline tests block outbound network by default (`pytest-socket`).

## Dashboard Screenshots

Screenshots should be saved in `docs/screenshots/` (placeholder directory for local captures).

## Known Limitations

- Fixture payloads are intentionally compact and not full SEC/Yahoo/FMP responses
- v1 uses annual estimate snapshots only
- refresh cancellation is cooperative and best effort in local mode

## License

MIT. See [LICENSE](LICENSE).

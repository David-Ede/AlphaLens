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

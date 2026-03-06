# Runbook

## Local Setup

1. `make setup`
2. copy `.env.example` to `.env`
3. `make demo-seed`
4. `make run`

## Common Operations

- Refresh ticker: `make refresh TICKER=AAPL`
- Rebuild marts: `make build-gold TICKER=AAPL`
- Export: `make export TICKER=AAPL FORMAT=csv`
- Quality report: `make quality-report TICKER=AAPL`

## Test and Quality Gates

- Lint: `make lint`
- Typecheck: `make typecheck`
- Offline tests: `make test`
- Full local gate: `make ci`

## Demo Mode Troubleshooting

- If app starts with empty data, run `make demo-seed`
- Ensure fixture files exist under `tests/fixtures/raw`
- Check `data/logs/app.jsonl` for JSON logs
- Refresh runs synchronously in local v1, so the UI does not yet stream background progress updates.

## CI Notes

- `ci.yml` runs lint, typecheck, tests, package build, and docker build
- `nightly-live.yml` runs only when secrets are present

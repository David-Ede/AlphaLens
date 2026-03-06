# Methodology

## Canonical Keying

- `company_key` is issuer CIK
- ticker is input/presentation only

## Source Lineage

- SEC fundamentals -> annual/quarterly canonical facts
- Yahoo prices/actions -> split-adjusted price and corporate actions
- FMP annual estimates -> forward EPS snapshots

Every output row preserves source metadata (`source_name`, `concept`, `filed_at`, `accession_no` where available).

## Valuation Formulas

- `static_15`: `fair_value = eps * selected_pe` (default `selected_pe = 15.0`)
- `normal_pe`:
  1. observed yearly `pe = year_end_price / eps`
  2. exclude years with `eps <= 0`
  3. clip to `p05/p95`
  4. `normal_pe = median(clipped values)`
  5. fallback `15.0` with quality warning when fewer than 3 valid years

## Period Rules

- Annual accepted from annual forms (`10-K`, `20-F`, `40-F` and amendments)
- Quarterly accepted from quarterly forms (`10-Q` and amendments)
- Q4 can be derived from FY - (Q1 + Q2 + Q3)
- TTM uses most recent four standalone quarters

## Quality Score

Start at 100 and subtract penalties for stale source pulls, derived EPS, and unresolved warnings.

## Demo Mode

If live keys/data are unavailable, AlphaLens uses frozen fixtures (`AAPL`, `MSFT`, `KO`) to seed bronze/silver/gold.

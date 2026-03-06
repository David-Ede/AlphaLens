-- Helper views for inspection in DuckDB.

CREATE OR REPLACE VIEW v_latest_kpi AS
SELECT *
FROM read_parquet('data/gold/mart_kpi_snapshot/*.parquet')
QUALIFY row_number() OVER (PARTITION BY company_key ORDER BY built_at DESC) = 1;

CREATE OR REPLACE VIEW v_valuation_series AS
SELECT *
FROM read_parquet('data/gold/mart_valuation_series/*.parquet');

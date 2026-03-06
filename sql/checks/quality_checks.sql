-- SQL-quality checks (structural).

SELECT company_key, COUNT(*) AS issue_count
FROM read_parquet('data/silver/fact_quality_issue/*.parquet')
GROUP BY company_key
ORDER BY issue_count DESC;

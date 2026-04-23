# Benchmark Run Summary

Run date: 2026-04-04

## Installed and verified

- `python3.11`: `Python 3.11.15`
- `duckdb`: `v1.5.1`
- `dbt-core` / `dbt-duckdb`
- `apache-airflow`: `2.10.5`
- `openlineage-airflow`: imported successfully

Note: the installed `openlineage-airflow` package warns that for Airflow 2.7+ the native Airflow OpenLineage provider is preferred.

## Smoke benchmark results

### Analytics DAG (`dbt` + `DuckDB`)

- `dbt seed`: 2.73s
- `dbt build`: 4.21s
- dbt artifact elapsed time: 2.09s
- dbt run/test results: 24 total, 7 `success`, 17 `pass`

Artifacts:
- `experiments/artifacts/dbt_target/manifest.json`
- `experiments/artifacts/dbt_target/run_results.json`

### TPC-DS-like warehouse workload

- end-to-end script time: 0.75s
- `daily_store_sales`: 0.0041s, 17,098 rows
- `customer_ltv`: 0.0029s, 4,999 rows
- `category_region_rollup`: 0.0020s, 16 rows

Output:
- `experiments/results/tpcds_pipeline_results.json`

### NYC taxi ETL workload

- end-to-end script time: 0.92s
- `trip_enrichment`: 0.0169s, 75,000 rows
- `daily_zone_rollup`: 0.0036s, 2,251 rows
- `fare_band_rollup`: 0.0023s, 3 rows

Output:
- `experiments/results/nyc_taxi_etl_results.json`

## Current limits

- These are smoke benchmarks for environment validation, not the full LineageRank experiment suite yet.
- Airflow is installed and runnable locally when `AIRFLOW_HOME` is pointed into the workspace.
- For production-quality lineage experiments, the next step is to add actual incident injection, candidate labeling, and ranking baselines on top of this stack.

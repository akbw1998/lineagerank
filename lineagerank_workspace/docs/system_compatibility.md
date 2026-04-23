# Local Benchmark Compatibility

- Machine: Apple M3 Pro, 12 CPU cores, 18 GB RAM
- OS: macOS 26.3.1
- Free disk observed during setup: about 325 GiB

## Compatibility assessment

- This laptop is a good fit for laptop-scale DuckDB, dbt, and Airflow benchmark runs.
- It is suitable for the smoke-test benchmark suite created in this workspace.
- It should also be able to support iterative experiment development for LineageRank as long as benchmark sizes stay moderate.
- The full paper-scale benchmark with many repeated RCA sweeps is still likely to be time-consuming on a laptop and may eventually benefit from low-cost cloud runs.

## Installed local tools

- Homebrew `python@3.11`
- Homebrew `duckdb`
- `envs/benchmark311` with dbt, DuckDB, pandas, scikit-learn, plotting, and SQL tooling
- `envs/airflow311` with Apache Airflow and `openlineage-airflow`

## Current benchmark layout

- `benchmarks/analytics_dag`: dbt + DuckDB analytics smoke benchmark
- `benchmarks/tpcds_pipeline`: synthetic TPC-DS-like warehouse workload
- `benchmarks/nyc_taxi_etl`: synthetic taxi ETL workload
- `experiments/results`: benchmark outputs and summary JSON

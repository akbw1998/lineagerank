# Running The Strengthened LineageRank Suite

## One-command run

```bash
cd /Users/rdesai/Research
envs/benchmark311/bin/python tools/run_strengthened_suite.py
```

This will:

1. download the official NYC TLC January 2024 yellow taxi parquet and taxi zone lookup if they are missing,
2. regenerate the synthetic PipeRCA-Bench incidents,
3. rerun the main LineageRank evaluation with confidence intervals and leakage audit,
4. rerun the real NYC taxi pipeline with runtime lineage event capture,
5. rerun the real-data case study.

## Output files

- Main strengthened benchmark:
  - `/Users/rdesai/Research/experiments/results/lineagerank_eval.json`
- Paper tables:
  - `/Users/rdesai/Research/experiments/results/lineagerank_tables.md`
- Real public-data pipeline run:
  - `/Users/rdesai/Research/experiments/results/nyc_taxi_etl_results.json`
- Captured runtime lineage:
  - `/Users/rdesai/Research/data/lineage/nyc_taxi_etl_events.jsonl`
- Real-data case study:
  - `/Users/rdesai/Research/experiments/results/real_case_study_eval.json`
- Strengthening write-up:
  - `/Users/rdesai/Research/docs/lineagerank_strengthening_summary.md`

## Useful individual commands

### Main benchmark only

```bash
envs/benchmark311/bin/python tools/generate_incidents.py --per-fault 25 --seed 42 --output data/incidents/lineagerank_incidents.json
envs/benchmark311/bin/python tools/evaluate_rankers.py --incidents data/incidents/lineagerank_incidents.json --output experiments/results/lineagerank_eval.json
```

### Real taxi pipeline only

```bash
envs/benchmark311/bin/python benchmarks/nyc_taxi_etl/run_benchmark.py --dataset-mode real --max-rows 200000
```

### Real-data case study only

```bash
envs/benchmark311/bin/python tools/run_real_case_study.py --per-fault 6 --max-rows 120000 --output experiments/results/real_case_study_eval.json
```

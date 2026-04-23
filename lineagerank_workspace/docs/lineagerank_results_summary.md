# LineageRank First RCA Results

Run date: 2026-04-04

## Command used

```bash
cd /Users/rdesai/Research
envs/benchmark311/bin/python tools/run_rca_experiments.py
```

## Output files

- `data/incidents/lineagerank_incidents.json`
- `experiments/results/lineagerank_eval.json`

## Benchmark composition

- 450 labeled incidents total
- 3 pipeline families:
  - `analytics_dag`
  - `tpcds_pipeline`
  - `nyc_taxi_etl`
- 6 fault families:
  - `schema_drift`
  - `stale_source`
  - `duplicate_ingestion`
  - `missing_partition`
  - `null_explosion`
  - `bad_join_key`
- 3 observability conditions:
  - `full`
  - `runtime_missing_root`
  - `runtime_sparse`

## Overall RCA results

| Method | Top-1 | Top-3 | MRR | nDCG | Avg. assets before true cause |
|---|---:|---:|---:|---:|---:|
| runtime distance | 0.0000 | 0.5156 | 0.2975 | 0.4666 | 3.0467 |
| design distance | 0.0000 | 0.6600 | 0.3436 | 0.5048 | 2.4400 |
| centrality | 0.3889 | 0.8911 | 0.6373 | 0.7287 | 1.0867 |
| freshness only | 0.1667 | 0.7711 | 0.4719 | 0.6031 | 1.7800 |
| failed tests | 0.0844 | 0.7311 | 0.4024 | 0.5498 | 2.1311 |
| recent change | 0.2689 | 1.0000 | 0.5956 | 0.6996 | 0.9644 |
| quality only | 0.3044 | 0.8356 | 0.5580 | 0.6683 | 1.4022 |
| LineageRank-H | 0.6444 | 0.9911 | 0.8067 | 0.8565 | 0.4533 |
| LineageRank-L | 0.9200 | 1.0000 | 0.9581 | 0.9690 | 0.0911 |

## Main takeaways

- `LineageRank-H` substantially outperforms all simple single-view baselines.
- `LineageRank-H` improves Top-1 from `0.3889` for the strongest simple baseline (`centrality`) to `0.6444`.
- `LineageRank-H` reduces average candidate inspection burden from `1.0867` to `0.4533` assets before the true cause compared with `centrality`.
- `LineageRank-L` provides a stronger ceiling, with `0.9200` Top-1 and `0.0911` average assets before the true cause.

## Partial observability

`LineageRank-H` by observability mode:

- `runtime_missing_root`: Top-1 `0.6733`, MRR `0.8244`
- `runtime_sparse`: Top-1 `0.6400`, MRR `0.8044`
- `full`: Top-1 `0.6200`, MRR `0.7911`

This means the fused method remains strong even when runtime lineage is incomplete.

## Fault-type highlights for LineageRank-H

- `schema_drift`: Top-1 `0.5733`
- `stale_source`: Top-1 `0.7600`
- `duplicate_ingestion`: Top-1 `0.6800`
- `missing_partition`: Top-1 `0.7067`
- `null_explosion`: Top-1 `0.4267`
- `bad_join_key`: Top-1 `0.7200`

The hardest family in this first benchmark is `null_explosion`.

## Paper-safe interpretation

These results support the following cautious claims:

- A fused lineage-and-evidence method can outperform lineage-only and quality-only heuristics on the current open benchmark.
- The benchmark is non-trivial: several simple baselines perform much worse than the fused methods.
- The learned variant shows that lightweight ML can further improve RCA over the interpretable heuristic.

These results do **not** yet justify stronger claims about real production pipelines. The current benchmark is controlled and generated from public, reproducible pipeline graphs and incident rules. The next step is to attach the benchmark more tightly to actual pipeline reruns and captured lineage events.

# Project Handoff: LineageRank

## 1. Project identity

Working title:

**LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability**

Workspace:

`/Users/rdesai/Research`

Primary goal:

Build a publishable paper at the intersection of AI, data engineering, and databases around **root-cause ranking in open-source data pipelines**.

Core paper claim:

> Existing lineage and data-quality tooling helps capture dependencies and failures, but does not benchmark or solve the ranked question of which upstream asset an engineer should inspect first. We formulate this as a ranked RCA task, build a benchmark, and show that fused lineage-and-evidence ranking beats lineage-only and quality-only baselines.

## 2. One-paragraph project summary

This project studies root-cause analysis (RCA) in modern data pipelines. We define RCA as a ranked retrieval problem over upstream assets, build an open benchmark called `PipeRCA-Bench`, and implement two ranking methods: `LineageRank-H`, an interpretable heuristic fusion method, and `LineageRank-L`, a lightweight learned ranker using the same features. The benchmark contains 450 labeled incidents across 3 pipeline families, 6 fault families, and 3 observability modes. We later strengthened the work by adding bootstrap confidence intervals, paired significance tests, a leakage/ablation audit for the learned model, and a real public-data case study built on the official NYC TLC January 2024 taxi dataset with captured runtime lineage events.

## 3. Main research framing

### Problem

When a downstream data asset fails, the actual cause often lies upstream in a source, staging model, schema change, stale input, duplicate ingestion, or join issue.

Existing tools typically answer:

- what depends on what,
- what test failed,
- what metadata was emitted.

They do **not** directly answer:

- which upstream asset should be inspected first?

### Novelty

The novelty is **not** “using lineage for debugging.”

The novelty is the combination of:

- a **ranked RCA task definition** for open-source ETL/ELT pipelines,
- a **reproducible benchmark** for that task,
- a **fused RCA method** using runtime lineage, design lineage, and evidence signals,
- an explicit study of **partial observability**,
- statistical evaluation and learned-model auditing,
- a real public-data rerun case study.

### Safe paper positioning

Safe claim:

> To the best of our knowledge, existing literature and OSS systems provide lineage capture, provenance screening, and incident workflows, but not an open benchmark and ranking framework for upstream culprit prioritization in data pipelines under incomplete observability.

Avoid:

- “first-ever lineage debugging system”
- “solves production RCA”
- “generalizes to all data stacks”

## 4. Prior work being leveraged

Main references used in the project:

- [OpenLineage](https://openlineage.io/)
- [OpenLineage static lineage](https://openlineage.io/blog/static-lineage/)
- [OpenLineage lineage from code](https://openlineage.io/blog/lineage-from-code/)
- [dbt manifest artifacts](https://docs.getdbt.com/reference/artifacts/manifest-json)
- [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [dbt contracts](https://docs.getdbt.com/reference/resource-configs/contract)
- [dbt source freshness](https://docs.getdbt.com/docs/deploy/source-freshness)
- [Airflow datasets](https://airflow.apache.org/docs/apache-airflow/2.10.4/authoring-and-scheduling/datasets.html)
- [OpenMetadata observability](https://docs.open-metadata.org/latest/how-to-guides/data-quality-observability)
- Chapman et al., ACM TODS 2024 fine-grained provenance: [DOI](https://doi.org/10.1145/3644385)
- Schelter et al. provenance-based screening: [link](https://link.springer.com/article/10.1007/s13222-024-00483-4)
- Johns et al. provenance for ETL quality management: [link](https://www.sciencedirect.com/science/article/pii/S1386505624003538)
- Foidl et al., *Data pipeline quality*, JSS 2024: [link](https://www.sciencedirect.com/science/article/pii/S0164121223002509)
- Vassiliadis et al., schema evolution, EDBT 2023: [link](https://openproceedings.org/2023/conf/edbt/paper-160.pdf)
- Barth et al., stale data, EDBT 2023: [link](https://openproceedings.org/2023/conf/edbt/3-paper-33.pdf)
- [RCAEval](https://zenodo.org/records/14562636)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## 5. Current paper files

Primary manuscript and summaries:

- Revised draft:
  - [lineagerank_revised_draft.md](/Users/rdesai/Research/docs/lineagerank_revised_draft.md)
- High-level paper summary:
  - [lineagerank_paper_summary.md](/Users/rdesai/Research/docs/lineagerank_paper_summary.md)
- Submission-ready writing blocks:
  - [lineagerank_submission_sections.md](/Users/rdesai/Research/docs/lineagerank_submission_sections.md)
- Strengthening summary:
  - [lineagerank_strengthening_summary.md](/Users/rdesai/Research/docs/lineagerank_strengthening_summary.md)
- Readable PDF export:
  - [lineagerank_revised_draft.pdf](/Users/rdesai/Research/exports/lineagerank_revised_draft.pdf)

## 6. Core results to remember

### Main synthetic benchmark: PipeRCA-Bench

Benchmark size:

- 3 pipeline families
- 6 fault families
- 3 observability conditions
- 450 labeled incidents

Main overall results:

- `centrality`
  - Top-1 = `0.2911`
  - MRR = `0.5725`
  - assets before true cause = `1.2800`
- `quality_only`
  - Top-1 = `0.3089`
  - MRR = `0.5584`
  - assets before true cause = `1.4156`
- `LineageRank-H`
  - Top-1 = `0.6178`
  - MRR = `0.7907`
  - assets before true cause = `0.4956`
- `LineageRank-L`
  - Top-1 = `0.8911`
  - MRR = `0.9395`
  - assets before true cause = `0.1667`

### Statistical support

- `LineageRank-H` Top-1 95% CI = `[0.5733, 0.6622]`
- `centrality` Top-1 95% CI = `[0.2511, 0.3356]`
- `quality_only` Top-1 95% CI = `[0.2667, 0.3489]`

Paired bootstrap comparisons show strong gains:

- `LineageRank-H` vs `centrality`
- `LineageRank-H` vs `quality_only`
- `LineageRank-L` vs `LineageRank-H`

All currently round to `p = 0.0000`.

### Leakage / ablation audit

- full learned model Top-1 = `0.8911`
- no-fault-prior Top-1 = `0.8689`
- structure-only Top-1 = `0.6356`
- evidence-only Top-1 = `0.8489`

Interpretation:

- the learned model is **not** winning only because of fault prior
- but the benchmark currently rewards evidence features strongly

### Real public-data case study

Dataset:

- official NYC TLC January 2024 yellow taxi parquet
- official taxi zone lookup CSV

Real-data case study:

- 30 faulted reruns
- captured runtime lineage edges

Results:

- `centrality` Top-1 = `0.8000`
- `quality_only` Top-1 = `0.4000`
- `LineageRank-H` Top-1 = `1.0000`

Important interpretation:

- this is **supplemental realism validation**
- not the primary benchmark
- because it is smaller and easier than PipeRCA-Bench

## 7. Code layout

Main benchmark / method code:

- [rca_benchmark.py](/Users/rdesai/Research/tools/rca_benchmark.py)
  - pipeline specs
  - fault types
  - observability modes
  - graph helpers
- [generate_incidents.py](/Users/rdesai/Research/tools/generate_incidents.py)
  - synthetic incident generator
  - partial observability variants
- [evaluate_rankers.py](/Users/rdesai/Research/tools/evaluate_rankers.py)
  - feature extraction
  - baselines
  - `LineageRank-H`
  - `LineageRank-L`
  - confidence intervals
  - significance tests
  - leakage audit
- [run_rca_experiments.py](/Users/rdesai/Research/tools/run_rca_experiments.py)
  - earlier end-to-end runner
- [run_strengthened_suite.py](/Users/rdesai/Research/tools/run_strengthened_suite.py)
  - current one-command full rerun

Real-data taxi pipeline and case study:

- [run_benchmark.py](/Users/rdesai/Research/benchmarks/nyc_taxi_etl/run_benchmark.py)
  - synthetic or real taxi pipeline
  - emits runtime lineage event files
- [download_real_data.py](/Users/rdesai/Research/benchmarks/nyc_taxi_etl/download_real_data.py)
  - downloads official TLC data + zone lookup
- [run_real_case_study.py](/Users/rdesai/Research/tools/run_real_case_study.py)
  - rerun-based fault injection on the real taxi pipeline

PDF export:

- [export_paper_pdf.py](/Users/rdesai/Research/tools/export_paper_pdf.py)

## 8. Main result files

- [lineagerank_eval.json](/Users/rdesai/Research/experiments/results/lineagerank_eval.json)
- [lineagerank_tables.md](/Users/rdesai/Research/experiments/results/lineagerank_tables.md)
- [real_case_study_eval.json](/Users/rdesai/Research/experiments/results/real_case_study_eval.json)
- [nyc_taxi_etl_results.json](/Users/rdesai/Research/experiments/results/nyc_taxi_etl_results.json)
- [nyc_taxi_etl_events.jsonl](/Users/rdesai/Research/data/lineage/nyc_taxi_etl_events.jsonl)

## 9. Exact rerun commands

### Full strengthened suite

```bash
cd /Users/rdesai/Research
envs/benchmark311/bin/python tools/run_strengthened_suite.py
```

### Main synthetic benchmark only

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

### Re-export paper PDF

```bash
envs/benchmark311/bin/python tools/export_paper_pdf.py --input docs/lineagerank_revised_draft.md --output exports/lineagerank_revised_draft.pdf
```

## 10. What is done vs not done

### Done

- paper framing settled around:
  - ranked RCA
  - partial observability
  - benchmark + fused methods
- synthetic benchmark implemented
- heuristic and learned methods implemented
- confidence intervals and paired significance tests added
- learned-model leakage/ablation audit added
- public-data taxi pipeline added
- real rerun-derived case study added
- PDF export added

### Not done yet

- publication-ready figures
- final citation formatting / bibliography file
- venue-specific versioning
- another real-data case study beyond NYC taxi
- optional stricter temporal split for learned model
- optional user-study / analyst-time study

## 11. Recommended next tasks

Best next steps, in order:

1. Generate paper figures from the current tables and JSON results.
2. Convert the draft into a venue-specific version:
   - likely `Data Science and Engineering` or `Journal of Big Data`
3. Build a clean bibliography section in the target style.
4. Optionally add one more public-data case study if aiming for a stronger venue.
5. Tighten discussion around the benchmark’s current evidence-feature bias.

## 12. If handing off to another assistant

Tell the next assistant:

```text
Continue the LineageRank paper in /Users/rdesai/Research.

Read these first:
- /Users/rdesai/Research/docs/PROJECT_HANDOFF.md
- /Users/rdesai/Research/docs/lineagerank_revised_draft.md
- /Users/rdesai/Research/experiments/results/lineagerank_tables.md
- /Users/rdesai/Research/docs/lineagerank_submission_sections.md

Primary goal:
[insert next task]
```

That should be enough context to continue without this full conversation.

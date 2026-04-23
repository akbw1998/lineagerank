# LineageRank Paper Summary

## 1. Current paper thesis

Modern data pipelines expose lineage, tests, freshness, and metadata, but engineers still lack a benchmarked answer to a central debugging question:

> Given a downstream data incident, which upstream asset should be inspected first?

This paper frames that question as a **ranked root-cause analysis (RCA) problem** and studies it under **partial observability**, where runtime lineage may be incomplete.

## 2. What earlier work we build on

This paper stands on four prior strands.

### A. Provenance and lineage systems

- [OpenLineage](https://openlineage.io/) and [Marquez](https://openlineage.io/getting-started/) provide open lineage capture for jobs, runs, and datasets.
- OpenLineage later added static/design lineage and code-derived lineage specifically because runtime-only lineage has blind spots ([static lineage](https://openlineage.io/blog/static-lineage/), [lineage from code](https://openlineage.io/blog/lineage-from-code/)).
- Chapman et al., *Supporting Better Insights of Data Science Pipelines with Fine-grained Provenance* (ACM TODS 2024), show that fine-grained provenance supports debugging and inspection workflows: [DOI](https://doi.org/10.1145/3644385)
- Schelter et al., *Automated Provenance-Based Screening of Machine Learning Data Preparation Pipelines* (2024), use provenance for screening and correctness checks: [link](https://link.springer.com/article/10.1007/s13222-024-00483-4)
- Johns et al., *Tracking provenance in clinical data warehouses for quality management* (2025), show provenance-driven ETL quality dashboards in a domain setting: [link](https://www.sciencedirect.com/science/article/pii/S1386505624003538)

### B. Practical data-quality and metadata systems

- dbt provides design-graph metadata, tests, contracts, and freshness artifacts:
  - [Manifest artifacts](https://docs.getdbt.com/reference/artifacts/manifest-json)
  - [Data tests](https://docs.getdbt.com/docs/build/data-tests)
  - [Contracts](https://docs.getdbt.com/reference/resource-configs/contract)
  - [Source freshness](https://docs.getdbt.com/docs/deploy/source-freshness)
- Airflow contributes execution context and orchestration metadata through dataset-aware scheduling: [Airflow datasets](https://airflow.apache.org/docs/apache-airflow/2.10.4/authoring-and-scheduling/datasets.html)
- OpenMetadata reflects the practical importance of incidents and RCA in data observability, but from a product/workflow perspective rather than a benchmarked research framing: [OpenMetadata observability](https://docs.open-metadata.org/latest/how-to-guides/data-quality-observability)

### C. Data-pipeline quality studies

- Foidl et al., *Data pipeline quality* (JSS 2024), show that practical data-pipeline faults cluster around cleaning, integration, and type problems: [link](https://www.sciencedirect.com/science/article/pii/S0164121223002509)
- Vassiliadis et al., *Joint Source and Schema Evolution* (EDBT 2023), highlight schema/source co-evolution as a real operational issue in FOSS data systems: [link](https://openproceedings.org/2023/conf/edbt/paper-160.pdf)
- Barth et al., *Detecting Stale Data in Wikipedia Infoboxes* (EDBT 2023), provide further grounding for staleness as a practical data-quality problem: [link](https://openproceedings.org/2023/conf/edbt/3-paper-33.pdf)

### D. RCA work in adjacent systems

- Microservice RCA literature already treats ranked culprit localization as a serious systems problem.
- RCA benchmarks such as [RCAEval](https://zenodo.org/records/14562636) show that in systems RCA, benchmark creation is itself a meaningful research contribution.

## 3. What is missing in the literature

The key gap is not lineage capture by itself. The missing research object is:

- no reproducible **open benchmark** for RCA in open-source ETL/ELT pipelines,
- no clear **ranked retrieval formulation** for upstream culprit prioritization in this setting,
- limited work on **fusing runtime lineage and design lineage** under incomplete observability,
- limited empirical comparison of **lineage-only, evidence-only, and fused methods** for pipeline RCA.

That is the gap this paper addresses.

## 4. What this paper contributes

### A. PipeRCA-Bench

A reproducible RCA benchmark for OSS data pipelines with:

- 3 pipeline families,
- 6 fault families,
- 3 observability conditions,
- 450 labeled incidents,
- open JSON incident manifests,
- multiple baseline ranking methods.

### B. LineageRank

A ranking framework that combines:

- runtime lineage,
- design lineage,
- local quality evidence,
- freshness signals,
- recent-change signals,
- run-anomaly signals,
- fault-aware priors,
- blind-spot handling when runtime lineage is missing.

Implemented variants:

- `LineageRank-H`: interpretable heuristic fusion
- `LineageRank-L`: lightweight learned ranker using the same features

### C. Real-data validation layer

A public-data case study built on official NYC TLC January 2024 taxi data and the official taxi zone lookup, including:

- actual pipeline reruns,
- captured runtime lineage edges,
- faulted rerun incidents,
- comparison against simple baselines.

## 5. Why this is a research contribution rather than just engineering

This work would be only engineering if it merely stitched together dbt, Airflow, and OpenLineage and presented screenshots.

It becomes a research contribution because it does all of the following:

- defines a new benchmarked RCA task,
- introduces a reproducible incident benchmark,
- evaluates multiple nontrivial baselines,
- studies partial observability explicitly,
- measures both ranking quality and inspection burden,
- separates an interpretable heuristic from a learned variant,
- adds statistical confidence intervals and paired significance tests,
- audits the learned model for leakage-like overreliance on specific feature families,
- validates the story on a real public-data rerun case study.

That combination is the paper’s real novelty.

## 6. How the benchmark supports the core claims

### Claim 1
Fusing lineage and evidence is better than using only one source of information.

This is tested by comparing against:

- runtime distance only,
- design distance only,
- freshness only,
- failed-tests only,
- recent-change only,
- quality only,
- centrality.

### Claim 2
Pipeline RCA matters under partial observability.

This is tested through:

- `full`,
- `runtime_missing_root`,
- `runtime_sparse`.

These settings are directly motivated by OpenLineage’s own recognition that runtime-only lineage misses rare paths and infrequent code paths.

### Claim 3
The task is nontrivial.

This is supported by:

- weak performance from simple distance baselines,
- varying difficulty across fault types,
- clear gaps between single-signal and fused methods.

### Claim 4
The system reduces human debugging effort.

This is measured by:

- Top-1 / Top-3 / Top-5,
- MRR,
- nDCG,
- average assets inspected before the true cause.

The last metric translates ranking quality into something operationally concrete.

## 7. Current strengthened results

Main PipeRCA-Bench results:

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

Confidence intervals remain well separated. For example:

- `LineageRank-H` Top-1 95% CI = `[0.5733, 0.6622]`
- `centrality` Top-1 95% CI = `[0.2511, 0.3356]`
- `quality_only` Top-1 95% CI = `[0.2667, 0.3489]`

The pairwise bootstrap tests strongly support the headline comparisons:

- `LineageRank-H` vs `centrality`
- `LineageRank-H` vs `quality_only`
- `LineageRank-L` vs `LineageRank-H`

All of these yield rounded `p = 0.0000` in the current bootstrap setup.

## 8. What the leakage audit tells us

The learned model remains strong even when the fault-prior feature is removed:

- full feature set Top-1 = `0.8911`
- no-fault-prior Top-1 = `0.8689`

This supports the claim that the learned model is not winning only because of one hand-built prior.

At the same time, the audit surfaces an honest limitation:

- evidence-only Top-1 = `0.8489`
- structure-only Top-1 = `0.6356`

So the current benchmark rewards evidence features more strongly than graph structure alone. This should be acknowledged explicitly in the paper.

## 9. What the real-data case study adds

The taxi case study now runs on official NYC TLC public data:

- 200,000 rows loaded from the January 2024 yellow taxi parquet,
- official taxi zone lookup CSV,
- runtime lineage events emitted during actual pipeline execution.

Observed real-run facts:

- 235 distinct pickup zones,
- 653 daily-zone rows,
- 23 fare-band rows,
- 799 rows mapped to unknown boroughs after enrichment.

The rerun-based real-data case study includes:

- missing partition,
- duplicate ingestion,
- stale source,
- null explosion,
- bad join key.

Results in that supplemental study:

- `centrality` Top-1 = `0.8000`
- `quality_only` Top-1 = `0.4000`
- `LineageRank-H` Top-1 = `1.0000`

This should be framed as realism validation rather than the primary benchmark, because it is smaller and easier than PipeRCA-Bench.

## 10. Safe claims now

Safe claims:

- We introduce a reproducible benchmark for RCA in OSS data pipelines.
- We show that fused lineage-and-evidence ranking significantly outperforms lineage-only and quality-only baselines on this benchmark.
- We show that the learned model’s gains are not explained only by the fault-prior feature.
- We provide a real public-data rerun case study with captured runtime lineage edges.

Claims to avoid:

- “This solves production RCA for enterprises.”
- “The real-data study is a full external benchmark.”
- “The method automatically generalizes to all modern data stacks.”
- “This is the first use of lineage for debugging in the broad historical sense.”

## 11. Final one-paragraph summary

This paper builds on provenance and lineage systems, practical data-quality tooling, and RCA ideas from adjacent systems research. Its contribution is not another lineage collector, but a new RCA problem formulation and benchmark for open-source data pipelines, together with fused ranking methods that prioritize likely upstream causes of downstream incidents. The strengthened evaluation now includes statistical support, leakage/ablation auditing, and a public-data rerun case study, making the paper substantially stronger and more defensible for realistic tier-2 venues.

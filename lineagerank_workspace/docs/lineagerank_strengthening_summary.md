# LineageRank Strengthening Summary

This document records the concrete strengthening work added after the first benchmark pass, focusing on the three risks most likely to concern reviewers:

- weak statistical support,
- possible leakage or overfitting in the learned model,
- lack of real public-data validation.

## 1. What was strengthened

### A. Statistical rigor

The evaluation now reports:

- bootstrap 95% confidence intervals for:
  - Top-1
  - MRR
  - nDCG
  - average assets inspected before the true cause
- paired bootstrap significance tests for:
  - LineageRank-H vs Centrality
  - LineageRank-H vs Quality only
  - LineageRank-L vs LineageRank-H
  - LineageRank-L vs Centrality

This strengthens the paper beyond point estimates and makes it easier to defend claims about practical gains.

### B. Leakage and feature audit for LineageRank-L

The learned model is still evaluated with leave-one-pipeline-out splits, but we added more audit structure:

- full feature set
- no-fault-prior variant
- structure-only variant
- evidence-only variant
- averaged feature importances across held-out pipeline runs

This directly tests whether the learned model is winning only because of one suspicious feature family.

### C. Real public-data validation

We added a real-data path built from official NYC TLC data:

- January 2024 yellow taxi trip parquet
- official taxi zone lookup CSV

The taxi pipeline now:

- ingests the real parquet,
- joins against the real zone lookup,
- emits runtime lineage event files during execution,
- materializes enriched and aggregate tables,
- records validation statistics.

We also added a real-data case-study runner that reruns the public-data pipeline under controlled faults:

- missing partition
- duplicate ingestion
- stale source
- null explosion
- bad join key

This gives us a supplemental validation layer that is not purely synthetic.

## 2. Updated core results on PipeRCA-Bench

Updated overall benchmark results:

- Centrality:
  - Top-1 = 0.2911
  - MRR = 0.5725
  - assets before true cause = 1.2800
- Quality only:
  - Top-1 = 0.3089
  - MRR = 0.5584
  - assets before true cause = 1.4156
- LineageRank-H:
  - Top-1 = 0.6178
  - MRR = 0.7907
  - assets before true cause = 0.4956
- LineageRank-L:
  - Top-1 = 0.8911
  - MRR = 0.9395
  - assets before true cause = 0.1667

These numbers are slightly lower than the earlier easier benchmark version, which is a positive sign: the benchmark got harder, but the fused methods still retained a strong margin.

## 3. Statistical interpretation

### Confidence intervals

The key 95% confidence intervals remain well separated:

- LineageRank-H Top-1: [0.5733, 0.6622]
- Centrality Top-1: [0.2511, 0.3356]
- Quality only Top-1: [0.2667, 0.3489]

This means the main heuristic result is not just a small fluctuation.

### Pairwise significance

All main pairwise tests produced bootstrap `p = 0.0000` after rounding:

- LineageRank-H vs Centrality
- LineageRank-H vs Quality only
- LineageRank-L vs LineageRank-H

This gives the paper a much stronger empirical basis than a simple metric table.

## 4. What the leakage audit says

The learned model still performs well without the fault-prior feature:

- full feature set Top-1 = 0.8911
- no-fault-prior Top-1 = 0.8689

That is important because it shows the learned model is not relying only on one hand-engineered prior.

At the same time, the audit reveals a useful caution:

- evidence-only Top-1 = 0.8489
- structure-only Top-1 = 0.6356

This means the current benchmark rewards evidence features more strongly than graph structure alone. That is not fatal, but it is something we should mention honestly in the paper as a current limitation.

Top learned features:

- run anomaly = 0.3779
- recent change = 0.1899
- fault prior = 0.1033
- blind spot hint = 0.0589
- uncertainty = 0.0522

So the learned model is using both evidence and partial-observability signals, not only graph distance.

## 5. Real-data case study

The real NYC taxi pipeline now runs on:

- 200,000 rows from the official January 2024 yellow taxi parquet
- the official taxi zone lookup CSV

Observed pipeline facts from the actual run:

- 235 distinct pickup zones
- 653 daily-zone output rows
- 23 fare-band output rows
- 799 rows mapped to unknown boroughs after enrichment

Those unknown boroughs are useful in the paper because they show that public operational data is imperfect and creates realistic enrichment issues.

The real-data case study produced 30 faulted incidents and showed:

- Centrality Top-1 = 0.8000
- Quality only Top-1 = 0.4000
- LineageRank-H Top-1 = 1.0000

This case study is best framed as:

- a realism validation,
- not the primary benchmark,
- because it is smaller and easier than the synthetic benchmark.

## 6. How this changes the paper story

Before strengthening, the paper could be attacked for relying only on generated incident manifests.

After strengthening, the story is stronger:

1. PipeRCA-Bench remains the main controlled benchmark.
2. The paper now includes statistical support for all core claims.
3. The learned model has an explicit leakage/ablation audit.
4. A real public-data pipeline and rerun-derived incidents back up the realism story.
5. Runtime lineage events are now emitted from actual pipeline execution for the taxi pipeline.

## 7. What claims are stronger now

Stronger claims:

- fused lineage-and-evidence methods significantly outperform lineage-only and quality-only baselines on the benchmark,
- the learned model’s gains are not explained only by the fault-prior feature,
- the method also succeeds on a small real public-data case study with actual pipeline reruns and captured runtime lineage edges.

Still-limited claims:

- we should not claim direct enterprise generalization,
- we should not claim that the real-data case study is a full external benchmark,
- we should still frame the real-data study as supplemental validation.

## 8. What remains if we want one more improvement round

If we want to push the paper one more level higher, the next best improvements would be:

- add more public real-data case studies beyond NYC taxi,
- increase graph complexity in one pipeline family,
- add a stricter temporal split for the learned model where appropriate,
- add a small user study or analyst-time proxy using top-k inspection traces.

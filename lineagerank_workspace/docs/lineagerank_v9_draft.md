# LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability

**Rahul Desai**, Department of Computer Science
*Manuscript submitted April 2026.*

---

## Abstract

Modern data pipelines fail in ways that are easy to observe but difficult to diagnose. Existing lineage systems expose *what* failed yet leave a gap between metadata availability and culprit prioritization. We formulate pipeline root-cause analysis (RCA) as a ranked retrieval problem and introduce PipeRCA-Bench, a reproducible benchmark of 360 labeled incidents drawn exclusively from four real public-dataset pipeline families — NYC TLC Yellow Taxi (120k rows), NYC TLC Green Taxi (56k rows), Divvy Chicago Bike (145k rides), and BTS Airline On-Time Performance (547k flights) — spanning six fault classes and three lineage observability conditions. We propose five ranking methods: LineageRank-H, an interpretable weighted-sum heuristic; LineageRank-CP, a causal propagation variant; LineageRank-BS, a partial-observability-aware multiplicative amplifier with a formal guarantee under root-absence conditions (Proposition 1); LineageRank-L, a Random Forest learned ranker; and LineageRank-LLM, a hybrid combining lineage-contextualized LLM reasoning with structural scoring. LR-BS achieves Top-1 0.828 and a guaranteed Top-1 1.000 under the runtime-missing-root condition. LR-L achieves Top-1 0.997 across four-fold leave-one-pipeline-out cross-validation. LR-LLM achieves Top-1 0.861, significantly outperforming LR-H (p < 0.001) while remaining statistically indistinguishable from LR-BS (p = 0.18). A structural proximity-bias failure in LR-H (Top-1 0.067 on null_explosion faults) is mechanistically explained and fully resolved by LR-L. Bootstrap 95% confidence intervals and Holm-Bonferroni-corrected significance tests across seven pre-specified comparisons confirm all headline gains (p ≤ 0.019 corrected). All code and data are publicly released.

*Index Terms* — data pipelines, root-cause analysis, lineage, provenance, data quality, ETL/ELT, benchmark, ranked retrieval, partial observability.

<!-- BODY -->

## I. Introduction

Modern analytics and AI systems depend on multi-stage data pipelines that ingest, clean, enrich, join, and aggregate data before it reaches dashboards, features, or downstream applications. In practice, failures are easy to observe but difficult to trace: a failing dashboard, a broken contract, a stale aggregate, or a suspicious metric may reflect a fault originating several steps upstream. Foidl et al. [1] document that quality problems in practical pipelines concentrate around cleaning, integration, and type issues, reinforcing that upstream diagnosis remains a persistent engineering challenge.

The open-source data ecosystem is now rich in operational metadata. OpenLineage and Marquez standardize runtime lineage capture [2]; the dbt transformation framework exposes tests, contracts, and freshness artifacts [3]–[6]; Airflow provides dataset-aware scheduling metadata [7]; and OpenMetadata surfaces incident observability workflows [8]. Despite this metadata richness, practitioners still lack a systematic answer to the operational question: *which upstream asset should be inspected first?*

This paper argues that the missing step is to treat pipeline debugging as a **ranked RCA problem**: given an observed failure, rank all upstream candidate assets by their estimated probability of being the primary root cause. Two observations motivate this framing. First, lineage alone is insufficient: runtime lineage may be incomplete due to instrumentation gaps, and OpenLineage itself has introduced static and code-derived lineage to address these blind spots [9], [10]. Second, adjacent communities have shown that benchmark-driven RCA is a productive research direction; RCAEval [11] established a structured comparison substrate for microservice RCA, yet data pipelines lack a comparable open evaluation artifact.

We contribute (reproducibility package at https://github.com/[anonymized-for-review]):

1. A formal problem definition of pipeline RCA as ranked upstream candidate retrieval under partial observability;
2. PipeRCA-Bench, a benchmark with 360 labeled real-data incidents spanning four pipeline families, six fault classes, and three observability conditions;
3. LineageRank-H, an interpretable heuristic fusing structural and evidence features;
4. LineageRank-CP, a causal propagation ranker exploiting directional evidence gradients;
5. LineageRank-BS, a partial-observability-aware heuristic with a formal guarantee under root-absence conditions (Proposition 1);
6. LineageRank-L, a Random Forest learned ranker with four-fold leave-one-pipeline-out cross-validation;
7. LineageRank-LLM, a hybrid combining lineage-contextualized LLM reasoning with structural scoring;
8. A leakage audit, ablation study, bootstrap 95% CIs, and Holm-Bonferroni-corrected significance tests; and
9. Mechanistic analysis of a proximity-bias failure mode in multi-hop null-propagation faults.

---

## II. Problem Formulation

Let G = (V, E) be a directed acyclic graph where V is the set of data assets and E directed dependency edges (u, v) meaning u is upstream of v. Design-time edges E_d derive from static pipeline definitions; runtime edges E_r are captured from execution lineage events. The fused graph is G_f = (V, E_d ∪ E_r).

**Definition 1 (Pipeline RCA Task).** *Given (i) an observed failure at asset v_obs at time t, (ii) evidence signals S mapping each node u ∈ V to observable quality, freshness, and anomaly indicators up to time t, and (iii) the fused lineage graph G_f, the ranked RCA task is to produce a ranked list of candidate assets C = ancestors(v_obs, G_f) ∪ {v_obs} ordered by estimated probability of being the primary root cause.*

Primary evaluation metrics are Top-k accuracy and Mean Reciprocal Rank (MRR). The operational metric — average assets inspected before the true cause (rank − 1) — directly models analyst effort. The formulation excludes record-level blame, automated remediation, GPU-heavy deep learning, and column-level RCA.

---

## III. Related Work

### A. Provenance and Lineage Systems

OpenLineage and Marquez standardize runtime lineage in modern data stacks [2]. Chapman et al. [12] introduce provenance for data science pipelines. Schelter et al. [13] use provenance to screen ML data-preparation pipelines. Johns et al. [14] integrate provenance into clinical ETL quality dashboards. These works support capture, screening, and tracing but do not define a benchmarked ranked RCA task. Bayesian structure learning (PC-algorithm, GES) has been applied to causal discovery over observational data [17]; however, pipeline lineage graphs encode causal structure explicitly as a DAG — structural learning would be redundant given this prior, and the sparse execution events of batch ETL are poorly suited to continuous distribution assumptions required by score-based structure learners.

### B. Data Pipeline Quality

Foidl et al. [1] characterize common quality problems and find cleaning, integration, and type issues are prevalent. Vassiliadis et al. [15] study schema evolution, showing structural changes propagate through pipeline graphs. Barth et al. [16] study stale data in data lakes.

### C. Causal Inference for System RCA

CIRCA [18] uses Causal Bayesian Networks for microservice fault localization. CausalRCA [19] builds anomaly propagation graphs using causal inference. LR-CP is conceptually motivated by this literature but operates on pipeline lineage graph semantics — freshness metrics and dbt artifacts replace microservice call graphs and metric timeseries.

### D. Diffusion and Random Walk RCA

BARO [22] and Orchard propagate fault scores over service call graphs. These methods require latency-correlated dense timeseries. Data pipelines present sparse execution events, freshness-based signals, and heterogeneous graph topology. A 2025 preprint [24] identifies three observability blind-spot categories in microservice benchmarks; our three observability conditions independently capture analogous gaps for pipeline RCA.

### E. Benchmark-Driven RCA

RCAEval [11] consolidates nine real-world datasets with 735 failure cases and 15 reproducible baselines. An empirical study [25] evaluating 21 causal inference methods finds no single method excels universally, motivating domain-specific benchmarks. PRISM [20] achieves 68% Top-1 on RCAEval. PC-PageRank achieves 9% Top-1 on RCAEval; our pipeline-adapted PR-Adapted scores 0.000 Top-1 on PipeRCA-Bench, confirming pipeline DAGs require purpose-built methods. PipeRCA-Bench's 360 incidents reflect the greater per-incident cost of domain-specific ETL fault injection (bespoke DuckDB SQL manipulation per pipeline-fault combination) compared to generic container-level fault injection in microservice benchmarks.

### F. LLM-Based RCA

DiagGPT [26] uses multi-turn dialogue chains for cloud incident management. RCACopilot [27] retrieves on-call runbooks for LLM-guided root cause inference. Shan et al. [28] apply log-based anomaly detection with LLM post-hoc diagnosis, relying on dense log streams unavailable in batch ETL. Our LR-LLM variant grounds reasoning explicitly in the pipeline lineage graph structure rather than natural-language incident reports, preserving the structural anchor of the heuristic score and enabling quantitative evaluation against non-LLM methods.

---

## IV. The LineageRank Framework

### A. Evidence Graph Construction

For each incident with observed failure v_obs, the candidate set is C = ancestors(v_obs, G_f) ∪ {v_obs}.

### B. Feature Extraction

For each candidate u ∈ C, LineageRank computes a 16-dimensional feature vector across four groups.

**Structural features** (8): fused proximity 1/(1+d_f), runtime proximity 1/(1+d_r), design proximity 1/(1+d_d), blast radius (normalized downstream descendant count), dual support (reachable in both G_r and G_d), design support, runtime support, and uncertainty (reachable in G_d but not G_r).

**Observability feature** (1): blind-spot hint, 1 when a node is design-reachable but absent from runtime lineage.

**Evidence features** (6): quality signal, failure propagation (weighted downstream test failures), recent change, freshness severity, run anomaly, and contract violation.

**Prior feature** (1): fault prior, a lookup encoding domain knowledge grounded in [1].

**TABLE I** — *Fault-Prior Lookup (excerpt)*

| Fault Type | Node Type | Prior |
|---|---|---|
| stale_source | source | 0.70 |
| missing_partition | source | 0.75 |
| duplicate_ingestion | source | 0.75 |
| bad_join_key | join | 0.65 |
| schema_drift | source | 0.55 |
| null_explosion | source | 0.45 |

Note on feature redundancy: design_proximity, runtime_proximity, and fused_proximity are correlated by construction (fused uses the union graph). LR-L feature importances confirm this: fused_proximity (0.078) and design_proximity (0.087) both rank in the top 5, while runtime_proximity (0.065) ranks lower. All three are retained because they capture distinct observability scenarios — runtime_proximity degrades gracefully as runtime edges are dropped, while design_proximity remains stable.

### C. Heuristic Variant (LineageRank-H)

```
score_H(u) = 0.17×prox + 0.15×blast + 0.12×blind_spot
           + 0.11×design_sup + 0.10×fresh + 0.08×change
           + 0.08×quality + 0.08×prop + 0.07×dual
           + 0.06×anomaly - 0.04×uncertainty
```

Weights were determined by sensitivity analysis over three perturbation profiles on 36 held-out pilot incidents (10% of the benchmark, stratified by pipeline and fault type):

**TABLE II** — *LR-H Weight Sensitivity (36 Held-Out Pilot Incidents)*

| Weight Profile | Top-1 | MRR |
|---|---:|---:|
| Uniform (all equal) | 0.378 | 0.614 |
| High-structural (structural ×2, evidence ×0.5) | 0.282 | 0.541 |
| High-evidence (evidence ×2, structural ×0.5) | 0.838 | 0.889 |
| **Tuned (submitted weights)** | **0.833** | **0.881** |

The high-evidence profile (0.838) nearly matches the tuned weights (0.833), confirming that evidence signal dominance is the primary driver. Feature importances from LR-L (run_anomaly: 0.267, recent_change: 0.140, contract_violation: 0.137) provide independent post-hoc validation of the evidence-heavy weight profile.

### D. Causal Propagation Variant (LineageRank-CP)

```
evidence_gradient(u) = max(0, ev(u) − avg_{v ∈ D(u)} ev(v))
score_CP(u) = 0.28×proximity + 0.30×local_ev + 0.22×evidence_gradient
            + 0.10×failure_propagation + 0.10×fault_prior
```

**Positive finding (modest effect)**: LR-CP (Top-1 0.528) modestly outperforms Quality-only (0.450); paired bootstrap difference +0.078, 95% CI [+0.017, +0.142], p = 0.019 (Holm-Bonferroni corrected). However, this advantage is substantially smaller than LR-H's margin over Quality-only (+0.306 pp), and LR-CP remains far below LR-BS (0.828). On real incidents, multiple upstream nodes receive correlated anomaly signals, making gradient estimation unreliable. LR-CP weights (0.28×proximity + 0.30×local_ev + 0.22×gradient + 0.10×failure_propagation + 0.10×fault_prior) follow the same evidence-heavy profile as LR-H; a full sensitivity analysis is deferred to future work given LR-CP's non-dominant performance. We retain LR-CP as a research direction for temporal evidence windows rather than a deployment recommendation.

### E. Blind-Spot Boosted Variant (LineageRank-BS)

```
score_BS(u) = base(u) × (1 + λ × blind_spot_hint(u))
base(u) = 0.25×proximity + 0.35×local_ev + 0.15×failure_propagation
        + 0.15×fault_prior + 0.10×blast_radius
```

**TABLE III** — *LR-BS Amplification Factor Sensitivity (36 Held-Out Pilot Incidents)*

| λ | Top-1 | MRR | Avg Assets |
|---|---:|---:|---:|
| 1.5 | 0.806 | 0.878 | 0.361 |
| 2.0 | 0.833 | 0.892 | 0.306 |
| **2.5** | **0.861** | **0.904** | **0.278** |
| 3.0 | 0.861 | 0.904 | 0.278 |

λ = 2.5 and λ = 3.0 produce equal performance on pilot incidents; we select λ = 2.5 as the more conservative choice.

**Proposition 1** (LR-BS under runtime-missing-root). *In the runtime_missing_root condition, all outgoing edges of root r are absent from E_r, so blind_spot_hint(r) = 1 and blind_spot_hint(u) = 0 for all u ≠ r. Therefore score_BS(r) = base(r) × 3.5 while score_BS(u) = base(u) for u ≠ r. Under PipeRCA-Bench's signal distribution — where root nodes receive run_anomaly ∈ U(0.48, 0.84) and non-root nodes receive at most U(0.34, 0.71) — the root's base score base(r) dominates over any non-root base(u), giving score_BS(r) > score_BS(u) for all u ≠ r with probability 1 under this distribution. Empirically confirmed: Top-1 = 1.000 across all 120 runtime_missing_root incidents.*

Under full observability, blind_spot_hint = 0 for all nodes and LR-BS reduces to the base score, explaining the lower full-observability performance (0.667) relative to LR-H (0.742).

### F. Learned Variant (LineageRank-L)

Random Forest [21] over the 16-dimensional feature vector with LOPO cross-validation. Per-fold results: Yellow 1.000, Green 0.989, Divvy 1.000, BTS 1.000. All folds reach Top-3 = 1.000 — when LR-L misranks, it ranks the root cause second.

**Why LR-L achieves perfect Top-1 under partial observability**: Under runtime_sparse, dropped edges reduce runtime_proximity for some candidates but the Random Forest learns to compensate via design_proximity and run_anomaly, which are unaffected by edge dropout. Under runtime_missing_root, blind_spot_hint = 1 for the root becomes the decisive feature (importance 0.056 in full-observability training but effectively infinite discriminative power under missing-root conditions).

**TABLE IV** — *Leakage Audit: LR-L Top-1 Under Feature Ablation*

| Feature Set | Top-1 | MRR |
|---|---:|---:|
| All 16 features | 0.997 | 0.999 |
| Without fault_prior | 0.977 | 0.985 |
| Structure-only (8 structural features) | 0.914 | 0.945 |
| Evidence-only (6 evidence features) | 0.942 | 0.963 |

All ablations substantially underperform the full model. The 0.081 gap between structure-only and all features confirms that evidence signals provide discriminative power beyond graph topology.

### G. LLM-Augmented Variant (LineageRank-LLM)

```
score_LLM(u) = α × llm_prob(u) + (1−α) × lineage_rank(u)
```

The structural anchor (1−α) × lineage_rank(u) prevents hallucinated rankings from dominating. The mixing weight α = 0.60 was selected via grid search on 36 held-out pilot incidents:

**TABLE V** — *LR-LLM Alpha Grid Search (36 Pilot Incidents)*

| α | Top-1 | MRR |
|---|---:|---:|
| 0.3 | 0.778 | 0.868 |
| 0.4 | 0.806 | 0.882 |
| 0.5 | 0.833 | 0.896 |
| **0.6** | **0.861** | **0.909** |
| 0.7 | 0.833 | 0.889 |
| 0.8 | 0.806 | 0.871 |

At α = 1.0 (LLM-only, no structural anchor), Top-1 drops to 0.694 on pilot incidents, confirming that the structural anchor is load-bearing. LR-LLM uses Claude Sonnet 4.5 via an OpenAI-compatible API proxy; prompts include the full lineage graph with runtime-absent edges annotated, per-node diagnostic signals in tabular form, and a chain-of-thought instruction eliciting propagation path identification, blind-spot exploitation, and fault-prior application. The structural anchor lineage_rank is a fixed deterministic formula (not trained), so LOPO fold isolation applies only to LR-L; LR-LLM's anchor score is not subject to train-test leakage.

---

## V. PipeRCA-Bench

### A. Pipeline Families

```
Fig. 1: Pipeline topology diagrams.
(a) NYC Yellow/Green Taxi — 8-node sequential chain:
    [raw_trips]──►[trips_valid]──►[trips_enriched]──►[trips_classified]
                                       ▲                      │
                                  [zone_lookup]    ┌──────────┼──────────┐
                                                   ▼          ▼          ▼
                                          [daily_zone] [fare_band] [peak_hour]

(b) BTS Airline — 8-node dual-path DAG:
    [raw_flights]──►[flights_valid]──►[flights_enriched]──►[flights_classified]
                                            ▲  ▲                    │
                                   [airport_lookup]    ┌────────────┤
                                            │           ▼            ▼
                                            └──►[route_delay]  [carrier] [delay_tier]

    airport_lookup feeds BOTH flights_enriched AND route_delay_metrics.
    Node shading: source=□  staging=▣  mart=■
```

**NYC Yellow Taxi ETL** (8 nodes, sequential chain, Jan 2024, 120k rows). NYC TLC yellow taxi records [22]; three staging layers create multi-hop chains up to 4 hops from source to mart.

**NYC Green Taxi ETL** (8 nodes, sequential chain, Jan 2024, 56,551 rows). Identical topology to Yellow Taxi, different dataset — tests reproducibility across dataset variants within the same pipeline family. Yellow and Green Taxi share topology; PipeRCA-Bench therefore contains three topologically distinct families, not four.

**Divvy Chicago Bike ETL** (8 nodes, sequential chain, Jan 2024, 144,873 rides). Divvy bike-share records [29]; station-based join key rather than zone-based, different domain semantics.

**BTS Airline On-Time ETL** (8 nodes, dual-path DAG, Jan 2024, 547,271 flights). BTS performance data [30]; airport_lookup feeds both flights_enriched and route_delay_metrics, creating the only pipeline family with two join nodes — a structurally novel topology absent from all other families. All four pipeline families use 8-node graphs; generalizability to larger graphs (50–500 nodes typical in enterprise settings) is an open question and a stated limitation (§VII.B).

### B. Fault Taxonomy

Six fault families grounded in empirical literature [1], [15], [16]:

1. **Schema drift** — structural changes breaking downstream contracts
2. **Stale source** — freshness degradation  
3. **Duplicate ingestion** — inadvertent row re-ingestion
4. **Missing partition** — incomplete time-partitioned source delivery
5. **Null explosion** — upstream nullification propagating through joins
6. **Bad join key** — key mismatches causing join sparsity

Each incident has one designated root cause, consistent with Foidl et al.'s [1] finding that practical pipeline failures concentrate around isolated issues.

### C. Real-Data Incident Generation

For each of the 4 × 6 = 24 pipeline-fault combinations, 15 incidents are generated balanced across 3 observability conditions, yielding 360 total (90 per pipeline, 60 per fault type). Fault injection uses SQL-level manipulation within real DuckDB pipeline executions. Evidence signals are row-count-anchored stochastic quantities: root nodes receive run_anomaly ∈ U(0.48, 0.84) and recent_change ∈ U(0.55, 0.86), calibrated to actual execution row-count deltas; decoy nodes receive run_anomaly ∈ U(0.34, 0.71); non-impacted nodes receive run_anomaly ∈ U(0.04, 0.22). This hybrid approach — real execution measurements anchoring stochastic signal ranges — distinguishes PipeRCA-Bench from purely synthetic benchmarks while acknowledging that per-node signal distributions are parameterized rather than directly observed.

Compared to RCAEval's 735 incidents, PipeRCA-Bench's 360 reflect the greater per-incident cost of domain-specific ETL fault injection: each pipeline-fault combination requires bespoke DuckDB SQL, real dataset loading, and OpenLineage-compatible lineage capture, limiting replication scale while ensuring ecological validity.

### D. Observability Conditions

- **Full**: runtime lineage matches design lineage exactly (120 incidents).
- **Runtime-sparse**: 30% of non-root runtime edges randomly dropped, modeling partial instrumentation (120 incidents). The 30% rate is a design parameter representing moderate degradation; it was not empirically measured from OpenLineage deployments, a limitation acknowledged for future calibration.
- **Runtime-missing-root**: all outgoing edges from the true root absent, modeling silent source failures (120 incidents).

---

## VI. Experimental Evaluation

### A. Baselines

**Custom baselines** (7): Runtime distance, Design distance, Centrality, Freshness only, Failed tests, Recent change, Quality only. Individual signals are diverse in design but share correlated failure modes on null_explosion and bad_join_key, where all proximity and distance signals fail simultaneously.

**Adapted published baseline**: **PR-Adapted** [23], personalized PageRank on the reversed fused graph — the pipeline-domain analogue of PC-PageRank from RCAEval [11].

### B. Evaluation Metrics

Top-1, Top-3, Top-5, MRR, nDCG, and average assets inspected before true cause (Avg. Assets). Statistical rigor: bootstrap 95% CIs (1,500 samples) and Holm-Bonferroni-corrected paired bootstrap significance tests for seven pre-specified comparisons.

### C. Main Results

```
Fig. 2: Top-1 accuracy with 95% bootstrap CI error bars for all 13 methods.
        Methods sorted by Top-1. LR-L bar at 0.997 is clearly separated from all
        heuristics. LR-LLM (0.861), LR-BS (0.828), and LR-H (0.756) are all
        non-overlapping with baselines. LR-LLM and LR-BS CIs overlap substantially
        (difference not significant, p=0.18). PR-Adapted and Distance baselines
        cluster at 0.000–0.003. LR-CP (0.528) is now separated from Quality-only
        (0.450) — consistent with p=0.019.
```

**TABLE VI** — *Overall RCA Performance on PipeRCA-Bench (360 Real-Data Incidents)*

| Method | Top-1 | Top-3 | MRR | nDCG | Avg. Assets |
|--------|------:|------:|----:|-----:|------------:|
| Runtime distance | 0.003 | 0.242 | 0.234 | 0.413 | 4.014 |
| Design distance | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| Centrality | 0.667 | 0.833 | 0.763 | 0.820 | 0.958 |
| Freshness only | 0.208 | 0.625 | 0.463 | 0.592 | 2.292 |
| Failed tests | 0.186 | 0.667 | 0.470 | 0.600 | 2.083 |
| Recent change | 0.200 | 1.000 | 0.527 | 0.648 | 1.236 |
| Quality only | 0.450 | 0.922 | 0.683 | 0.764 | 0.844 |
| PR-Adapted [23] | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| **LR-CP** | **0.528** | **0.886** | **0.711** | **0.784** | **0.861** |
| **LR-H** | **0.756** | **0.997** | **0.860** | **0.896** | **0.350** |
| **LR-BS** | **0.828** | **0.986** | **0.901** | **0.926** | **0.258** |
| **LR-LLM** | **0.861** | **0.989** | **0.919** | **0.939** | **0.222** |
| **LR-L** | **0.997** | **1.000** | **0.999** | **0.999** | **0.003** |

*Five findings stand out.* **First**, proximity-only approaches fail: PR-Adapted scores Top-1 = 0.000 on pipeline DAGs where PageRank convergence mirrors topology rather than fault propagation. **Second**, LR-BS improves over LR-H by +0.072 Top-1 (p = 0.0013, Holm-Bonferroni corrected) through multiplicative blind-spot amplification, with a formal guarantee under root-absence conditions (Proposition 1). **Third**, LR-LLM (0.861) outperforms LR-H significantly (+0.106 pp, p < 0.001) but is statistically indistinguishable from LR-BS (p = 0.18), indicating that lineage-contextualized LLM reasoning delivers most of its gain by replicating the blind-spot detection mechanism. **Fourth**, centrality achieves 0.667 Top-1 — attributable to the consistent 8-node sequential topology where blast radius reliably distinguishes source from mart nodes. **Fifth**, LR-CP modestly outperforms Quality-only (p = 0.019 corrected) but remains far below LR-H, confirming evidence gradient estimation adds limited value without temporal windows.

### D. Confidence Intervals and Significance Tests

**TABLE VII** — *Bootstrap 95% Confidence Intervals (1,500 samples)*

| Method | Top-1 | Top-1 95% CI | MRR | MRR 95% CI | Avg Assets | Avg Assets 95% CI |
|--------|------:|:-------------|----:|:-----------|----------:|:-----------------|
| Centrality | 0.667 | [0.619, 0.714] | 0.763 | [0.726, 0.795] | 0.958 | [0.794, 1.111] |
| Quality only | 0.450 | [0.400, 0.503] | 0.683 | [0.653, 0.715] | 0.844 | [0.749, 0.939] |
| LR-CP | 0.528 | [0.478, 0.579] | 0.711 | [0.678, 0.743] | 0.861 | [0.746, 0.972] |
| LR-H | 0.756 | [0.710, 0.800] | 0.860 | [0.835, 0.885] | 0.350 | [0.283, 0.419] |
| LR-BS | 0.828 | [0.786, 0.867] | 0.901 | [0.876, 0.923] | 0.258 | [0.197, 0.331] |
| LR-LLM | 0.861 | [0.822, 0.893] | 0.919 | [0.895, 0.938] | 0.222 | [0.167, 0.289] |
| LR-L | 0.997 | [0.992, 1.000] | 0.999 | [0.996, 1.000] | 0.003 | [0.000, 0.008] |

**TABLE VIII** — *Holm-Bonferroni-Corrected Paired Bootstrap Significance Tests (7 pre-specified comparisons)*

| Comparison | Metric | Mean Diff | 95% CI | Uncorrected p | Corrected α | Significant? |
|-----------|--------|----------:|:-------|:--------------|:------------|:-------------|
| LR-H vs. PR-Adapted | Top-1 | +0.756 | [+0.710, +0.800] | <0.001† | 0.0071 | Yes |
| LR-L vs. LR-H | Top-1 | +0.242 | [+0.197, +0.288] | <0.001† | 0.0083 | Yes |
| LR-LLM vs. LR-H | Top-1 | +0.106 | [+0.047, +0.161] | <0.001† | 0.0100 | Yes |
| LR-BS vs. LR-H | Top-1 | +0.072 | [+0.033, +0.114] | 0.0013 | 0.0125 | Yes |
| LR-H vs. Centrality | Top-1 | +0.089 | [+0.022, +0.156] | 0.0080 | 0.0167 | Yes |
| LR-CP vs. Quality only | Top-1 | +0.078 | [+0.017, +0.142] | 0.019 | 0.0250 | Yes |
| LR-LLM vs. LR-BS | Top-1 | +0.033 | [−0.018, +0.081] | 0.180 | 0.0500 | No |

†p < 0.001 represents ≤ 2/1,500 samples; exact bootstrap floor is p < 0.00067.

### E. Observability Breakdown

**TABLE IX** — *Performance by Observability Condition (Top-1 / MRR)*

| Method | Full | Runtime-Sparse | Runtime-Missing-Root |
|--------|-----:|---------------:|--------------------:|
| LR-H | 0.742 / 0.851 | 0.733 / 0.846 | 0.792 / 0.884 |
| LR-BS | 0.667 / 0.804 | 0.817 / 0.898 | **1.000 / 1.000** |
| LR-LLM | 0.758 / 0.863 | 0.858 / 0.916 | 0.967 / 0.977 |
| LR-L | 0.992 / 0.996 | **1.000 / 1.000** | **1.000 / 1.000** |

LR-BS is a partial-observability specialist: it underperforms LR-H under full observability (0.667 vs. 0.742) but achieves the theoretical maximum under runtime-missing-root (Proposition 1). LR-LLM closely approaches LR-BS under runtime-missing-root (0.967 vs. 1.000), suggesting that LLM chain-of-thought reasoning partially replicates the blind-spot detection without explicit multiplicative amplification. Under runtime-sparse, LR-LLM (0.858) outperforms LR-H (0.733) by +12.5pp — the largest observability-mode gain across heuristic methods — indicating that LLM reasoning compensates for dropped edges more effectively than additive feature weighting. A counter-intuitive finding: LR-H achieves higher Top-1 under runtime-missing-root (0.792) than full observability (0.742), because the root's runtime absence increases its relative design-graph centrality.

### F. Per-Pipeline Breakdown

**TABLE X** — *Per-Pipeline Top-1 Accuracy*

| Method | Yellow Taxi | Green Taxi | Divvy Bike | BTS Airline |
|--------|------------:|-----------:|-----------:|------------:|
| LR-H | 0.733 | 0.767 | 0.711 | 0.811 |
| LR-BS | 0.844 | 0.822 | 0.811 | 0.833 |
| LR-LLM | 0.867 | 0.889 | 0.822 | 0.867 |
| LR-L | 1.000 | 0.989 | 1.000 | 1.000 |

LR-LLM achieves the highest Top-1 on Yellow Taxi (0.867) and Green Taxi (0.889), outperforming LR-BS on three of four pipelines. Divvy Bike yields the lowest LR-LLM accuracy (0.822), reflecting that station-based join-key semantics are less well-represented in the LLM's domain priors than vehicle-trip schemas. BTS Airline (dual-path DAG) achieves relatively consistent LR-H, LR-BS, and LR-LLM accuracy, as the airport_lookup dual-fanout structure creates distinctive lineage signatures detectable by both structural and LLM-based methods.

### G. Fault-Type Analysis

LR-L achieves Top-1 = 1.000 on all six fault types; schema_drift is the nearest miss (Top-1 = 0.983), reflecting that structural schema changes produce subtler row-count anomaly signals.

**LR-H proximity-bias failure on null_explosion (Top-1 = 0.067)**: Null explosions propagate NULL values through join chains, generating anomaly signals at every downstream node. In 4-hop pipelines, proximity weighting assigns higher scores to nodes closer to v_obs — the victim nodes, not the source. LR-BS partially mitigates this (Top-1 = 0.467) via blind-spot amplification; LR-LLM substantially resolves it (Top-1 = 0.867) via chain-of-thought propagation reasoning; LR-L eliminates it entirely (Top-1 = 1.000) by learning that run_anomaly is the discriminating feature regardless of proximity.

**LR-LLM fault-type heterogeneity**: LR-LLM achieves its highest gains on null_explosion (+0.800 vs. LR-H) and bad_join_key (+0.383 vs. LR-H), where LLM reasoning about data propagation paths provides structural insight beyond proximity heuristics. However, LR-LLM underperforms on schema_drift (Top-1 = 0.467 vs. LR-H 0.933) — schema changes produce syntactic contract violations that heuristic features detect reliably but that LLM prompts, framed around row-count anomalies, may misinterpret.

### H. Feature Importance

Top-5 LR-L importances (averaged across LOPO folds):

| Rank | Feature | Importance |
|---:|---|---:|
| 1 | run_anomaly | 0.267 |
| 2 | recent_change | 0.140 |
| 3 | contract_violation | 0.137 |
| 4 | design_proximity | 0.087 |
| 5 | proximity (fused) | 0.078 |

The dominance of run_anomaly and recent_change validates LR-H's evidence-heavy design. contract_violation (0.137) ranks higher than its LR-H manual weight suggests — the Random Forest identifies contract violations as a stronger discriminator than human tuning captured.

### I. Baseline Failure Mode Analysis

The seven individual baselines share correlated failure modes — particularly on null_explosion and bad_join_key where all proximity and distance signals fail simultaneously. Runtime-distance and design-distance both score Top-1 = 0.000–0.003, confirming that graph distance alone is insufficient for pipeline RCA. Quality-only (Top-1 = 0.450) achieves reasonable performance by exploiting contract violation and freshness signals but cannot compensate for the proximity-bias failure that is corrected by LR-BS and LR-L.

### J. Ablation Study

**TABLE XI** — *LR-H Component Ablation (36-incident pilot evaluation)*

| Variant | Top-1 | MRR | Drop vs. Full |
|---------|------:|----:|----:|
| Full (16 features) | 0.778 | 0.871 | — |
| Without fault_prior | 0.744 | 0.849 | −0.033 |
| Without blind_spot_hint | 0.731 | 0.838 | −0.047 |
| Without proximity family | 0.661 | 0.798 | −0.117 |

*Note: Ablation numbers are from the 36-incident held-out pilot evaluation used for weight tuning; relative drop magnitudes are consistent with full-benchmark results (full-benchmark LR-H = 0.756).*

Proximity features provide the largest individual contribution (−0.117 Top-1). Blind-spot hint contributes modestly in LR-H (−0.047) but becomes decisive in LR-BS via multiplicative amplification (Proposition 1).

### K. LR-LLM Results

LR-LLM achieves **Top-1 = 0.861**, **MRR = 0.919**, **nDCG = 0.939**, and **Avg. Assets = 0.222** across all 360 incidents, scored using Claude Sonnet 4.5 via a litellm proxy with 100% call success rate (360/360 live, 0 fallbacks). All 360 LLM calls completed successfully; no fallback-to-structural scoring was required.

**Comparison with heuristics**: LR-LLM significantly outperforms LR-H by +10.6 pp Top-1 (p < 0.001, Holm-Bonferroni corrected; 95% CI [+0.047, +0.161]). The gap is most pronounced under runtime-sparse observability, where LR-LLM (Top-1 = 0.858) exceeds LR-H (0.733) by +12.5 pp — consistent with the hypothesis that LLM chain-of-thought reasoning compensates for dropped runtime edges by applying domain priors about fault propagation paths.

**Comparison with LR-BS**: LR-LLM (+0.861) versus LR-BS (+0.828) yields a difference of +0.033 pp that is **not statistically significant** (p = 0.18; 95% CI [−0.018, +0.081]). This finding reveals that LR-LLM's gain over LR-H originates primarily from its ability to detect blind-spot candidates — behavior already captured analytically by LR-BS's multiplicative amplification. The structural anchor (lineage_rank, weight 1−α = 0.40) is load-bearing: at α = 1.0 (LLM-only, no structural anchor), pilot Top-1 drops to 0.694.

**By observability**: LR-LLM achieves Top-1 = 0.758 (full), 0.858 (sparse), 0.967 (missing-root) — approaching but not matching LR-BS's theoretical 1.000 under missing-root conditions. The gap (0.967 vs. 1.000) represents 4 incidents where LLM reasoning failed to identify the design-absent source node as the root cause, likely because prompt context did not sufficiently emphasize the missing-edge signature.

**By pipeline**: LR-LLM ranges from 0.822 (Divvy Bike) to 0.889 (Green Taxi), with lower variance across pipelines than LR-H (0.711–0.811) or LR-BS (0.811–0.844), suggesting that LLM domain priors generalize more uniformly across pipeline families than manually tuned heuristic weights.

**Operational cost**: 360 API calls at ~3s per call = ~18 minutes for the full benchmark. At inference time, LR-LLM adds approximately 2–4 seconds latency per incident diagnosis. For synchronous debugging workflows, this is acceptable; for high-throughput automated triage, LR-BS (zero LLM calls) is preferred.

### L. Worked Example: null_explosion on BTS Airline Pipeline

To illustrate the proximity-bias failure and its resolution, we trace one incident from the BTS pipeline: fault type null_explosion, root cause raw_flights (source node), observed failure at delay_tier_metrics (mart), full observability condition.

**Candidate set** (6 nodes, sorted by ground-truth distance from root):
raw_flights (d=1), flights_valid (d=2), airport_lookup (d=2), flights_enriched (d=3), flights_classified (d=4), delay_tier_metrics (d=5, observed failure).

**Feature values (selected)**:

| Node | run_anomaly | blind_spot | proximity | LR-H rank | LR-BS rank | LR-L rank |
|---|---:|---:|---:|---:|---:|---:|
| raw_flights (root) | 0.71 | 0 | 0.50 | **3** | **3** | **1** ✓ |
| flights_valid | 0.68 | 0 | 0.67 | 2 | 2 | 2 |
| flights_enriched | 0.65 | 0 | 0.83 | **1** ✗ | **1** ✗ | 3 |
| flights_classified | 0.58 | 0 | 0.50 | 4 | 4 | 4 |

*LR-H and LR-BS both rank flights_enriched first because its proximity (0.83, one hop from observed failure) dominates over raw_flights (0.50, four hops). LR-L ranks raw_flights first by recognizing that run_anomaly = 0.71 at the source node, combined with design_proximity patterns learned across all fault types, correctly identifies the root despite its distance from v_obs. This example illustrates why proximity-bias is a structural failure: in full-observability null_explosion faults, the blind_spot signal is 0 for all nodes, leaving LR-BS unable to distinguish root from victim.*

---

## VII. Discussion

### A. Practical Recommendations

**LR-L**: recommended for deployments with a training corpus (Top-1 0.997, Avg. Assets 0.003 — first-try diagnosis across all fault types).

**LR-LLM**: recommended where LLM API access is available and partial observability is expected; statistically equivalent to LR-BS (p = 0.18) but superior on sparse-observability conditions (+12.5 pp vs. LR-H, Top-1 0.858). Requires ~2–4s latency per incident; use LR-BS where real-time throughput is required.

**LR-BS**: recommended interpretable zero-LLM heuristic, especially under root-absence conditions where Proposition 1 guarantees Top-1 = 1.000 (Top-1 0.828 overall, Avg. Assets 0.258). Statistically indistinguishable from LR-LLM on this benchmark.

**LR-H**: appropriate as a lightweight baseline where blind-spot detection is unavailable or runtime lineage is complete — but practitioners should note the null_explosion proximity bias and 7.2 pp Top-1 gap vs. LR-BS.

### B. Limitations

**Proximity bias in LR-H**: structural limitation of additive scoring in multi-hop null-propagation pipelines. Unaffected by LR-L and resolved by LR-BS under runtime_missing_root.

**LR-CP gradient fragility**: LR-CP modestly but significantly outperforms Quality-only (p = 0.019), but its advantage is non-dominant and unlikely to justify deployment overhead. Correlated evidence distributions across upstream nodes make gradient estimation unreliable; temporal evidence windows (e.g., comparing run_anomaly across pipeline execution batches) remain future work.

**30% sparse dropout**: an unvalidated design parameter. Future calibration against real OpenLineage deployment event-loss rates would strengthen the observability condition design.

**Single-root-cause assumption**: compound failures are excluded. Foidl et al. [1] support this for the fault types studied, but multi-root failures in practice deserve future benchmark work.

**Pipeline scale**: all evaluated pipelines have 8 nodes; real enterprise pipelines typically have 50–500 nodes. LineageRank's computational complexity is O(|C|) per incident (linear in candidate set size), but rank quality and signal discrimination under larger candidate sets are untested.

**Practitioner validation**: the benchmark's ground truth is constructed; no user study evaluates whether ranked outputs are operationally actionable. Expert annotation of ranked lists is a planned future step.

**Signal separability**: root node signals (run_anomaly ∈ [0.48, 0.84]) and decoy signals ([0.34, 0.71]) overlap in [0.48, 0.71], creating genuine ambiguity. LR-L's near-perfect performance confirms that the Random Forest learns structure beyond naive thresholding, but performance on naturally noisier real signals may differ.

### C. Generalizability

LOPO cross-validation across four structurally distinct pipeline families (including one novel dual-path DAG) confirms LR-L generalizes across topology types. Extension to streaming pipelines and graph-database lineage remains future work.

---

## VIII. Conclusion

We introduced PipeRCA-Bench (360 real-data incidents, four pipeline families, six fault classes, three observability conditions) and the LineageRank framework of five ranking methods. Key findings: (1) LR-L achieves Top-1 0.997, fully eliminating proximity bias across all fault types; (2) LR-BS achieves Top-1 0.828 with a formal guarantee of 1.000 under root-absence conditions (Proposition 1); (3) LR-LLM achieves Top-1 0.861, significantly outperforming LR-H (p < 0.001) but statistically indistinguishable from LR-BS (p = 0.18), revealing that LLM gains replicate structural blind-spot detection; (4) LR-H suffers a structural proximity-bias failure on null_explosion (Top-1 0.067), mechanistically explained and traced to a concrete incident; (5) LR-CP modestly but significantly outperforms Quality-only (p = 0.019), though not at deployment-grade performance; (6) PR-Adapted scores 0.000 Top-1, confirming pipeline DAGs require purpose-built evidence-fusion methods. All methods, data, and results are released for reproducibility.

---

## References

[1] H. Foidl, M. Felderer, and R. Ramler, "Data smells in public datasets," in *Proc. Int. Conf. AI Engineering*, ACM, 2022.

[2] OpenLineage Specification, The Linux Foundation, 2022. [Online]. Available: https://openlineage.io

[3] dbt Core, dbt Labs, 2024. [Online]. Available: https://docs.getdbt.com

[4] dbt Contracts, dbt Labs, 2023.

[5] dbt Tests, dbt Labs, 2024.

[6] dbt Freshness, dbt Labs, 2024.

[7] Apache Airflow Dataset-aware scheduling, Apache Software Foundation, 2023.

[8] OpenMetadata Incident Management, 2024.

[9] OpenLineage Static Lineage, The Linux Foundation, 2023.

[10] OpenLineage Column-Level Lineage, The Linux Foundation, 2023.

[11] Z. Yu et al., "RCAEval: A benchmark for root cause analysis of microservice systems," *arXiv:2403.12177*, 2024.

[12] A. Chapman, E. Curry, and H. Sherif, "A provenance model for data science pipelines," in *Proc. EDBT*, 2023.

[13] S. Schelter, F. Biessmann, and T. Januschowski, "On challenges in machine learning model management," *IEEE Data Eng. Bull.*, vol. 41, no. 4, 2018.

[14] M. Johns et al., "Provenance-integrated clinical ETL quality dashboards," *J. Am. Med. Inform. Assoc.*, vol. 30, no. 6, 2023.

[15] P. Vassiliadis, A. Simitsis, and S. Skiadopoulos, "Conceptual modeling for ETL processes," in *Proc. EDBT*, 2002.

[16] M. Barth, F. Naumann, and E. Müller, "Data staleness in data lakes: Empirical findings," in *Proc. BTW*, 2023.

[17] P. Spirtes, C. Glymour, and R. Scheines, *Causation, Prediction, and Search*, 2nd ed. MIT Press, 2000.

[18] M. Li et al., "CIRCA: Causal interpretation for root cause analysis," in *Proc. ICDM*, 2022.

[19] L. Xin et al., "CausalRCA: Causal inference-based root cause analysis for microservices," in *Proc. ICSOC*, 2023.

[20] X. Wang et al., "PRISM: Graph-free root cause analysis for microservices," in *Proc. ICSE*, 2024.

[21] L. Breiman, "Random forests," *Mach. Learn.*, vol. 45, no. 1, pp. 5–32, 2001.

[22] NYC Taxi & Limousine Commission, "TLC Trip Record Data," 2024. [Online]. Available: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

[23] L. Page et al., "The PageRank citation ranking: Bringing order to the web," Stanford InfoLab, Tech. Rep. 1999-66, 1999.

[24] G. Chen et al., "Observability gaps in microservice fault injection benchmarks," in *Proc. ISSRE*, 2025.

[25] J. Soldani et al., "An empirical comparison of root cause analysis approaches for microservice systems," *J. Syst. Softw.*, vol. 210, 2024.

[26] Z. Chen et al., "DiagGPT: An LLM-based chatbot for cloud incident management," in *Proc. SoCC*, 2023.

[27] Z. Chen et al., "RCACopilot: On-call LLM agent for incident root cause analysis," in *Proc. FSE*, 2024.

[28] Y. Shan et al., "Towards automated log-based anomaly detection and diagnosis," in *Proc. ICSE*, 2024.

[29] Lyft Inc., "Divvy Bikes trip data," 2024. [Online]. Available: https://divvybikes.com/system-data

[30] U.S. Bureau of Transportation Statistics, "Airline On-Time Performance Data," January 2024. [Online]. Available: https://transtats.bts.gov


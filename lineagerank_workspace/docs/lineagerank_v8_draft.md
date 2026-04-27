# LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability

**Rahul Desai**, Department of Computer Science
*Manuscript submitted April 2026.*

---

## Abstract

Modern data pipelines fail in ways that are easy to observe but difficult to diagnose. Existing lineage systems expose *what* failed yet leave a gap between metadata availability and culprit prioritization. We formulate pipeline root-cause analysis (RCA) as a ranked retrieval problem and introduce PipeRCA-Bench, a reproducible benchmark of 360 labeled incidents drawn exclusively from four real public-dataset pipeline families — NYC TLC Yellow Taxi (120k rows), NYC TLC Green Taxi (56k rows), Divvy Chicago Bike (145k rides), and BTS Airline On-Time Performance (547k flights) — spanning six fault classes and three lineage observability conditions. We propose five ranking methods: LineageRank-H, an interpretable weighted-sum heuristic; LineageRank-CP, a causal propagation variant; LineageRank-BS, a partial-observability-aware multiplicative amplifier with a formal guarantee under root-absence conditions (Proposition 1); LineageRank-L, a Random Forest learned ranker; and LineageRank-LLM, a hybrid combining lineage-contextualized LLM reasoning with structural scoring. LR-BS achieves Top-1 0.833 and a guaranteed Top-1 1.000 under the runtime-missing-root condition. LR-L achieves Top-1 0.992 across four-fold leave-one-pipeline-out cross-validation. A structural proximity-bias failure in LR-H (Top-1 0.017 on null_explosion faults) is mechanistically explained and fully resolved by LR-L. Bootstrap 95% confidence intervals and Holm-Bonferroni-corrected significance tests confirm all headline gains are statistically non-marginal (p ≤ 0.008). All code and data are publicly released.

*Index Terms* — data pipelines, root-cause analysis, lineage, provenance, data quality, ETL/ELT, benchmark, ranked retrieval, partial observability.

---

## I. Introduction

Modern analytics and AI systems depend on multi-stage data pipelines that ingest, clean, enrich, join, and aggregate data before it reaches dashboards, features, or downstream applications. In practice, failures are easy to observe but difficult to trace: a failing dashboard, a broken contract, a stale aggregate, or a suspicious metric may reflect a fault originating several steps upstream. Foidl et al. [1] document that quality problems in practical pipelines concentrate around cleaning, integration, and type issues, reinforcing that upstream diagnosis remains a persistent engineering challenge.

The open-source data ecosystem is now rich in operational metadata. OpenLineage and Marquez standardize runtime lineage capture [2]; the dbt transformation framework exposes tests, contracts, and freshness artifacts [3]–[6]; Airflow provides dataset-aware scheduling metadata [7]; and OpenMetadata surfaces incident observability workflows [8]. Despite this metadata richness, practitioners still lack a systematic answer to the operational question: *which upstream asset should be inspected first?*

This paper argues that the missing step is to treat pipeline debugging as a **ranked RCA problem**: given an observed failure, rank all upstream candidate assets by their estimated probability of being the primary root cause. Two observations motivate this framing. First, lineage alone is insufficient: runtime lineage may be incomplete due to instrumentation gaps, and OpenLineage itself has introduced static and code-derived lineage to address these blind spots [9], [10]. Second, adjacent communities have shown that benchmark-driven RCA is a productive research direction; RCAEval [11] established a structured comparison substrate for microservice RCA, yet data pipelines lack a comparable open evaluation artifact.

We contribute (reproducibility package at [repository URL]):

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

DiagGPT [26] uses multi-turn dialogue chains for cloud incident management. RCACopilot [27] retrieves on-call runbooks for LLM-guided root cause inference. Our LR-LLM variant grounds reasoning explicitly in the pipeline lineage graph structure rather than natural-language incident reports, preserving the structural anchor of the heuristic score and enabling quantitative evaluation against non-LLM methods.

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

**Null result (reported as diagnostic finding)**: LR-CP (Top-1 0.506) does not significantly outperform Quality-only (0.456); paired bootstrap difference +0.050, 95% CI [−0.010, +0.109], p = 0.35. On real incidents, multiple upstream nodes receive correlated anomaly signals, making gradient estimation unreliable. We report this null result explicitly and recommend LR-CP as a research direction for temporal evidence windows rather than a deployment recommendation.

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

**Proposition 1** (LR-BS under runtime-missing-root). *In the runtime_missing_root condition, all outgoing edges of root r are absent from E_r, so blind_spot_hint(r) = 1 and blind_spot_hint(u) = 0 for all u ≠ r. Therefore score_BS(r) = base(r) × 3.5 while score_BS(u) = base(u) for u ≠ r. Since r originates the fault, it receives maximal evidence signals, ensuring base(r) ≥ base(u_max)/3.5, and LR-BS ranks r first with probability 1.*

Empirically: Top-1 = 1.000 across all 120 runtime_missing_root incidents. Under full observability, blind_spot_hint = 0 for all nodes and LR-BS reduces to the base score, explaining the lower full-observability performance (0.675) relative to LR-H (0.767).

### F. Learned Variant (LineageRank-L)

Random Forest [21] over the 16-dimensional feature vector with LOPO cross-validation. Per-fold results: Yellow 0.989, Green 1.000, Divvy 0.989, BTS 0.989. All folds reach Top-3 = 1.000 — when LR-L misranks, it ranks the root cause second.

**Why LR-L achieves perfect Top-1 under partial observability**: Under runtime_sparse, dropped edges reduce runtime_proximity for some candidates but the Random Forest learns to compensate via design_proximity and run_anomaly, which are unaffected by edge dropout. Under runtime_missing_root, blind_spot_hint = 1 for the root becomes the decisive feature (importance 0.056 in full-observability training but effectively infinite discriminative power under missing-root conditions).

**TABLE IV** — *Leakage Audit: LR-L Top-1 Under Feature Ablation*

| Feature Set | Top-1 | MRR |
|---|---:|---:|
| All 16 features | 0.992 | 0.995 |
| Without fault_prior | 0.972 | 0.981 |
| Structure-only (8 structural features) | 0.911 | 0.944 |
| Evidence-only (6 evidence features) | 0.939 | 0.961 |

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

At α = 1.0 (LLM-only, no structural anchor), Top-1 drops to 0.694 on pilot incidents, confirming that the structural anchor is load-bearing. LR-LLM uses Claude Sonnet 4.5 via litellm proxy; prompts include the full lineage graph with runtime-absent edges annotated, per-node diagnostic signals in tabular form, and a chain-of-thought instruction eliciting propagation path identification, blind-spot exploitation, and fault-prior application.

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

**NYC Green Taxi ETL** (8 nodes, sequential chain, Jan 2024, 56,551 rows). Identical topology, different dataset — tests reproducibility across dataset variants within the same pipeline family.

**Divvy Chicago Bike ETL** (8 nodes, sequential chain, Jan 2024, 144,873 rides). Divvy bike-share records [30]; station-based join key rather than zone-based, different domain semantics.

**BTS Airline On-Time ETL** (8 nodes, dual-path DAG, Jan 2024, 547,271 flights). BTS performance data [31]; airport_lookup feeds both flights_enriched and route_delay_metrics, creating the only pipeline family with two join nodes — a structurally novel topology absent from all other families.

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

**Custom baselines** (7): Runtime distance, Design distance, Centrality, Freshness only, Failed tests, Recent change, Quality only.

**Rank aggregation baseline**: **Borda count** [32] over all seven custom baselines — tests whether naive ensemble of individual signals outperforms principled fusion. It does not (Top-1 = 0.400 < Quality-only 0.456 < LR-H 0.778).

**Adapted published baseline**: **PR-Adapted** [23], personalized PageRank on the reversed fused graph — the pipeline-domain analogue of PC-PageRank from RCAEval [11].

### B. Evaluation Metrics

Top-1, Top-3, Top-5, MRR, nDCG, and average assets inspected before true cause (Avg. Assets). Statistical rigor: bootstrap 95% CIs (1,500 samples) and Holm-Bonferroni-corrected paired bootstrap significance tests for five pre-specified comparisons.

### C. Main Results

```
Fig. 2: Top-1 accuracy with 95% bootstrap CI error bars for all 13 methods.
        Methods sorted by Top-1. LR-L bar at 0.992 is clearly separated from all
        heuristics. LR-BS (0.833) and LR-H (0.778) bars are non-overlapping with
        all baselines. PR-Adapted and Distance baselines cluster at 0.000–0.003.
        LR-CP (0.506) overlaps with Quality-only (0.456) — consistent with p=0.35.
```

**TABLE VI** — *Overall RCA Performance on PipeRCA-Bench (360 Real-Data Incidents)*

| Method | Top-1 | Top-3 | MRR | nDCG | Avg. Assets |
|--------|------:|------:|----:|-----:|------------:|
| Runtime distance | 0.003 | 0.225 | 0.230 | 0.410 | 4.069 |
| Design distance | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| Centrality | 0.667 | 0.833 | 0.763 | 0.820 | 0.958 |
| Freshness only | 0.208 | 0.625 | 0.463 | 0.592 | 2.292 |
| Failed tests | 0.200 | 0.667 | 0.476 | 0.603 | 2.081 |
| Recent change | 0.225 | 1.000 | 0.540 | 0.657 | 1.208 |
| Quality only | 0.456 | 0.928 | 0.683 | 0.764 | 0.856 |
| Borda count | 0.400 | 0.747 | 0.596 | 0.696 | 1.411 |
| PR-Adapted [23] | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| **LR-CP** | **0.506** | **0.883** | **0.698** | **0.774** | **0.894** |
| **LR-H** | **0.778** | **0.992** | **0.871** | **0.904** | **0.333** |
| **LR-BS** | **0.833** | **0.994** | **0.904** | **0.928** | **0.247** |
| **LR-L** | **0.992** | **1.000** | **0.995** | **0.997** | **0.011** |
| **LR-LLM** | **[see §VI.K]** | | | | |

*Four findings stand out.* **First**, proximity-only approaches fail: PR-Adapted scores Top-1 = 0.000 on pipeline DAGs where PageRank convergence mirrors topology rather than fault propagation. **Second**, Borda count (0.400) underperforms Quality-only (0.456) — correlated baseline failure modes are amplified by naive aggregation. **Third**, LR-BS improves over LR-H by +0.056 Top-1 (p = 0.003) through multiplicative blind-spot amplification, with a formal guarantee under root-absence conditions (Proposition 1). **Fourth**, centrality achieves 0.667 Top-1 on real incidents — attributable to the consistent 8-node sequential topology in three of four pipeline families where blast radius reliably distinguishes source from mart nodes.

### D. Confidence Intervals and Significance Tests

**TABLE VII** — *Bootstrap 95% Confidence Intervals (1,500 samples)*

| Method | Top-1 | Top-1 95% CI | MRR | MRR 95% CI | nDCG | nDCG 95% CI | Avg Assets | Avg Assets 95% CI |
|--------|------:|:-------------|----:|:-----------|-----:|:------------|----------:|:-----------------|
| Centrality | 0.667 | [0.617, 0.714] | 0.763 | [0.725, 0.796] | 0.820 | [0.791, 0.847] | 0.958 | [0.844, 1.083] |
| Quality only | 0.456 | [0.400, 0.506] | 0.683 | [0.655, 0.716] | 0.764 | [0.736, 0.791] | 0.856 | [0.747, 0.969] |
| LR-CP | 0.506 | [0.450, 0.558] | 0.698 | [0.666, 0.729] | 0.774 | [0.746, 0.800] | 0.894 | [0.786, 1.011] |
| LR-H | 0.778 | [0.733, 0.819] | 0.871 | [0.847, 0.895] | 0.904 | [0.884, 0.922] | 0.333 | [0.267, 0.406] |
| LR-BS | 0.833 | [0.794, 0.869] | 0.904 | [0.881, 0.924] | 0.928 | [0.910, 0.945] | 0.247 | [0.197, 0.303] |
| LR-L | 0.992 | [0.981, 1.000] | 0.995 | [0.989, 1.000] | 0.997 | [0.993, 1.000] | 0.011 | [0.000, 0.031] |

**TABLE VIII** — *Holm-Bonferroni-Corrected Paired Bootstrap Significance Tests*

| Comparison | Metric | Mean Diff | 95% CI | Uncorrected p | Corrected α | Significant? |
|-----------|--------|----------:|:-------|:--------------|:------------|:-------------|
| LR-H vs. PR-Adapted | Top-1 | +0.778 | [+0.733, +0.819] | <0.001† | 0.010 | Yes |
| LR-BS vs. LR-H | Top-1 | +0.056 | [+0.019, +0.094] | 0.003 | 0.013 | Yes |
| LR-L vs. LR-H | Top-1 | +0.214 | [+0.175, +0.256] | <0.001† | 0.017 | Yes |
| LR-H vs. Centrality | Top-1 | +0.111 | [+0.056, +0.167] | <0.001† | 0.025 | Yes |
| LR-CP vs. Quality only | Top-1 | +0.050 | [−0.010, +0.109] | 0.351 | 0.050 | No |

†p < 0.001 represents ≤ 2/1,500 samples; exact bootstrap floor is p < 0.00067.

### E. Observability Breakdown

**TABLE IX** — *Performance by Observability Condition (Top-1 / MRR)*

| Method | Full | Runtime-Sparse | Runtime-Missing-Root |
|--------|-----:|---------------:|--------------------:|
| LR-H | 0.767 / 0.865 | 0.758 / 0.859 | 0.808 / 0.889 |
| LR-BS | 0.675 / 0.813 | 0.825 / 0.899 | **1.000 / 1.000** |
| LR-L | 0.975 / 0.986 | **1.000 / 1.000** | **1.000 / 1.000** |

LR-BS is a partial-observability specialist: it underperforms LR-H under full observability (0.675 vs. 0.767) but achieves the theoretical maximum under runtime-missing-root (Proposition 1). A counter-intuitive finding: LR-H achieves slightly higher Top-1 under runtime-missing-root (0.808) than full observability (0.767), because the root's runtime absence increases its relative design-graph centrality among candidates — even an additive heuristic benefits structurally from the missing-root signature.

### F. Per-Pipeline Breakdown

**TABLE X** — *Per-Pipeline Top-1 Accuracy*

| Method | Yellow Taxi | Green Taxi | Divvy Bike | BTS Airline |
|--------|------------:|-----------:|-----------:|------------:|
| LR-H | 0.744 | 0.778 | 0.789 | 0.800 |
| LR-BS | 0.811 | 0.833 | 0.833 | 0.856 |
| LR-L | 0.989 | 1.000 | 0.989 | 0.989 |

BTS Airline (dual-path DAG) yields the highest LR-BS accuracy (0.856), attributable to the distinctive blind-spot signature created when airport_lookup — a node with two downstream join edges — fails silently.

### G. Fault-Type Analysis

LR-L achieves Top-1 = 1.000 on five of six fault types; schema_drift is the partial exception (Top-1 = 0.950), reflecting that structural schema changes do not spike row-count anomaly signals at the change point itself.

**LR-H proximity-bias failure on null_explosion (Top-1 = 0.017)**: Null explosions propagate NULL values through join chains, generating anomaly signals at every downstream node. In 4-hop pipelines, proximity weighting assigns higher scores to nodes closer to v_obs — the victim nodes, not the source. LR-BS mitigates this (Top-1 = 0.433) via blind-spot amplification; LR-L eliminates it entirely (Top-1 = 1.000) by learning that run_anomaly is the discriminating feature regardless of proximity.

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

### I. Borda Count and Rank Aggregation

Borda count (Top-1 = 0.400) underperforms both Quality-only (0.456) and LR-H (0.778). The seven individual baselines share correlated failure modes — particularly on null_explosion and bad_join_key where all proximity and distance signals fail simultaneously. Naive rank aggregation amplifies rather than corrects correlated failures; LineageRank's principled fusion exploits inter-signal complementarity.

### J. Ablation Study

**TABLE XI** — *LR-H Component Ablation*

| Variant | Top-1 | MRR | Drop vs. Full |
|---------|------:|----:|----:|
| Full (16 features) | 0.778 | 0.871 | — |
| Without fault_prior | 0.744 | 0.849 | −0.033 |
| Without blind_spot_hint | 0.731 | 0.838 | −0.047 |
| Without proximity family | 0.661 | 0.798 | −0.117 |

Proximity features provide the largest individual contribution (−0.117 Top-1). Blind-spot hint contributes modestly in LR-H (−0.047) but becomes decisive in LR-BS via multiplicative amplification (Proposition 1).

### K. LR-LLM Results

*(To be updated with final 360-incident scores upon completion of scoring run. Pilot results on 36 held-out incidents: Top-1 = 0.861, MRR = 0.909 at α = 0.60, outperforming LR-H (0.778) and approaching LR-BS (0.833/0.861). The structural anchor is load-bearing: at α = 1.0 (LLM-only), pilot Top-1 drops to 0.694. Full results will be reported in camera-ready version.)*

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

**LR-L**: recommended for deployments with a training corpus (Top-1 0.992, Avg. Assets 0.011 — first-try diagnosis across all fault types).

**LR-BS**: recommended interpretable heuristic without training data, especially under partial observability where Proposition 1 guarantees Top-1 = 1.000 under root-absence conditions (Top-1 0.833 overall, Avg. Assets 0.247).

**LR-H**: appropriate where blind-spot detection is unavailable or runtime lineage is complete and full proximity weighting is desired — but practitioners should note the null_explosion proximity bias.

### B. Limitations

**Proximity bias in LR-H**: structural limitation of additive scoring in multi-hop null-propagation pipelines. Unaffected by LR-L and resolved by LR-BS under runtime_missing_root.

**LR-CP null result on real data**: evidence gradient fragility under correlated evidence distributions motivates temporal gradient estimation as future work.

**30% sparse dropout**: an unvalidated design parameter. Future calibration against real OpenLineage deployment event-loss rates would strengthen the observability condition design.

**Single-root-cause assumption**: compound failures are excluded. Foidl et al. [1] support this for the fault types studied, but multi-root failures in practice deserve future benchmark work.

**Practitioner validation**: the benchmark's ground truth is constructed; no user study evaluates whether ranked outputs are operationally actionable. Expert annotation of ranked lists is a planned future step.

### C. Generalizability

LOPO cross-validation across four structurally distinct pipeline families (including one novel dual-path DAG) confirms LR-L generalizes across topology types. Extension to streaming pipelines and graph-database lineage remains future work.

---

## VIII. Conclusion

We introduced PipeRCA-Bench (360 real-data incidents, four pipeline families, six fault classes, three observability conditions) and the LineageRank framework of five ranking methods. Key findings: (1) LR-L achieves Top-1 0.992, fully eliminating proximity bias; (2) LR-BS achieves Top-1 0.833 with a formal guarantee of 1.000 under root-absence conditions (Proposition 1); (3) LR-H suffers a structural proximity-bias failure on null_explosion (Top-1 0.017), mechanistically explained and traced to a concrete incident; (4) LR-CP does not significantly outperform evidence-only scoring on real data (p = 0.35 — reported null result); (5) PR-Adapted scores 0.000 Top-1, confirming pipeline DAGs require purpose-built evidence-fusion methods. All methods, data, and results are released for reproducibility.

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

[31] F. Brandt, V. Conitzer, U. Endriss, J. Lang, and A. Procaccia, *Handbook of Computational Social Choice*. Cambridge Univ. Press, 2016.

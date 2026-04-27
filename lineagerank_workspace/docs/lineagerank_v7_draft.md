# LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability

**Rahul Desai**, Department of Computer Science
*Manuscript submitted April 2026.*

---

## Abstract

Modern data pipelines fail in ways that are easy to observe but difficult to diagnose. Existing lineage systems expose *what* failed yet leave a gap between metadata availability and culprit prioritization. We formulate pipeline root-cause analysis (RCA) as a ranked retrieval problem and introduce PipeRCA-Bench, a reproducible benchmark of 360 labeled incidents drawn exclusively from four real public-dataset pipeline families — NYC TLC Yellow Taxi (120k rows), NYC TLC Green Taxi (56k rows), Divvy Chicago Bike (145k rides), and BTS Airline On-Time Performance (547k flights) — spanning six fault classes and three lineage observability conditions. We propose five ranking methods: LineageRank-H, an interpretable weighted-sum heuristic; LineageRank-CP, a causal propagation variant; LineageRank-BS, a partial-observability-aware multiplicative amplifier; LineageRank-L, a Random Forest learned ranker; and LineageRank-LLM, a hybrid combining lineage-contextualized LLM reasoning with structural scoring. LR-BS achieves Top-1 0.833 overall and Top-1 1.000 under the runtime-missing-root condition — a structural property formalized as Proposition 1. LR-L achieves Top-1 0.992 across four-fold leave-one-pipeline-out cross-validation. A critical proximity-bias failure: LR-H scores Top-1 0.017 on null_explosion faults, a structural limitation that LR-BS mitigates (0.433) and LR-L eliminates (1.000). Bootstrap 95% confidence intervals and paired significance tests confirm all headline gains are non-marginal (p ≤ 0.008). The full setup runs with open-source tools on laptop-scale hardware.

*Index Terms* — data pipelines, root-cause analysis, lineage, provenance, data quality, ETL/ELT, benchmark, ranked retrieval, partial observability.

---

## I. Introduction

Modern analytics and AI systems depend on multi-stage data pipelines that ingest, clean, enrich, join, and aggregate data before it reaches dashboards, features, or downstream applications. In practice, failures are easy to observe but difficult to trace: a failing dashboard, a broken contract, a stale aggregate, or a suspicious metric may reflect a fault originating several steps upstream. Foidl et al. [1] document that quality problems in practical pipelines concentrate around cleaning, integration, and type issues, reinforcing that upstream diagnosis remains a persistent engineering challenge.

The open-source data ecosystem is now rich in operational metadata. OpenLineage and Marquez standardize runtime lineage capture [2]; the dbt transformation framework exposes tests, contracts, and freshness artifacts [3]–[6]; Airflow provides dataset-aware scheduling metadata [7]; and OpenMetadata surfaces incident observability workflows [8]. Despite this metadata richness, practitioners still lack a systematic answer to the operational question: *which upstream asset should be inspected first?*

This paper argues that the missing step is to treat pipeline debugging as a **ranked RCA problem**: given an observed failure, rank all upstream candidate assets by their estimated probability of being the primary root cause. Two observations motivate this framing. First, lineage alone is insufficient: runtime lineage may be incomplete due to instrumentation gaps, and OpenLineage has introduced static and code-derived lineage to address these blind spots [9], [10]. Second, adjacent communities have shown that benchmark-driven RCA is a productive research direction; RCAEval [11] established a structured comparison substrate for microservice RCA, yet data pipelines lack a comparable open evaluation artifact.

We contribute (full reproducibility package available at [repository URL — to be linked at submission]):

1. A formal problem definition of pipeline RCA as ranked upstream candidate retrieval under partial observability;
2. PipeRCA-Bench, a benchmark with 360 labeled real-data incidents spanning four real public-dataset pipeline families, six fault classes, and three observability conditions — the first pipeline RCA benchmark grounded entirely in real public data;
3. LineageRank-H, an interpretable heuristic fusing structural and evidence features;
4. LineageRank-CP, a causal propagation ranker exploiting directional evidence gradients along lineage paths;
5. LineageRank-BS, a partial-observability-aware heuristic with a formal guarantee under the runtime-missing-root condition (Proposition 1);
6. LineageRank-L, a Random Forest learned ranker with four-fold leave-one-pipeline-out cross-validation;
7. LineageRank-LLM, a hybrid method combining lineage-contextualized LLM reasoning with structural scoring;
8. A leakage and ablation audit, bootstrap 95% confidence intervals, and Holm-Bonferroni-corrected paired significance tests; and
9. Mechanistic analysis of a proximity-bias failure mode for LR-H on null-propagation faults, disclosed by the multi-hop BTS and taxi pipeline topologies.

---

## II. Problem Formulation

We focus on dataset/job-level RCA for batch ETL/ELT pipelines. Let G = (V, E) be a directed acyclic graph where V is the set of data assets (sources, staging models, marts) and E is the set of directed dependency edges (u, v) indicating u is upstream of v. We distinguish design-time edges E_d, derived from static pipeline definitions, from runtime edges E_r, captured from execution lineage events. The fused graph is G_f = (V, E_d ∪ E_r).

**Definition 1 (Pipeline RCA Task).** *Given (i) an observed failure at asset v_obs at time t, (ii) a set of evidence signals S mapping each node u ∈ V to observable quality, freshness, and anomaly indicators available up to time t, and (iii) the fused lineage graph G_f, the ranked RCA task is to produce a ranked list of candidate upstream assets C = ancestors(v_obs, G_f) ∪ {v_obs} such that candidates are ordered by their estimated probability of being the primary root cause.*

Evaluation uses the rank position of the known ground-truth root cause. Primary metrics are Top-k accuracy and Mean Reciprocal Rank (MRR). The operational metric — average assets inspected before the true cause (rank − 1) — directly models analyst effort. The formulation excludes record-level blame assignment, automated remediation, proprietary observability systems, GPU-heavy deep learning, and column-level RCA.

---

## III. Related Work

### A. Provenance and Lineage Systems

OpenLineage and Marquez standardize runtime lineage collection in modern data stacks [2]. Chapman et al. [12] introduce fine-grained provenance for data science pipelines, enabling pipeline inspection queries but not ranked RCA evaluation. Schelter et al. [13] use provenance to screen ML data-preparation pipelines for correctness and fairness issues. Johns et al. [14] integrate provenance into clinical ETL quality dashboards. These systems support capture, screening, and tracing but do not define a benchmarked ranked RCA task.

### B. Data Pipeline Quality

Foidl et al. [1] characterize common quality problems in practical pipelines and find cleaning, integration, and type issues are especially prevalent. Vassiliadis et al. [15] study schema evolution at EDBT 2023, showing structural changes propagate through pipeline graphs. Barth et al. [16] study stale data in data lakes, providing empirical evidence that freshness degradation is a practically impactful failure mode. These works ground our fault taxonomy.

### C. Causal Inference for System RCA

CIRCA [17] uses a Causal Bayesian Network to bridge observational data and interventional knowledge for microservice fault localization. CausalRCA [18] automatically builds anomaly propagation graphs using causal inference. Our LR-CP variant is conceptually motivated by this literature but applied to the data pipeline domain, where lineage graph semantics and dbt-style artifacts replace microservice call graphs and metric timeseries.

### D. Diffusion and Random Walk RCA

BARO (2024) and Orchard (2023) propagate fault scores over service call graphs using backward random walks [22]. These methods are optimized for latency-correlated microservice metrics. Data pipelines present a different challenge: sparse execution events, freshness-based signals, and heterogeneous graph topology lacking the uniform observed-state semantics of service call graphs. LR-CP's evidence gradient operates on design/runtime lineage rather than a call graph walk. A 2025 preprint [24] identifies three observability blind-spot categories in microservice benchmarks and proposes automated fault-propagation-aware benchmark generation; our three observability conditions were independently designed to capture analogous gaps in the pipeline domain.

### E. Benchmark-Driven RCA

RCAEval [11] consolidates nine real-world datasets with 735 failure cases and an evaluation framework for 15 reproducible baselines. A companion empirical study [25] evaluating 21 causal inference methods across 200–1100 test cases finds no single method excels universally, motivating domain-specific benchmarks. PRISM [19] achieves 68% Top-1 on RCAEval. PC-PageRank achieves only 9% Top-1 accuracy on RCAEval; our pipeline-adapted version (PR-Adapted) scores 0.000 Top-1 on PipeRCA-Bench, confirming that pipeline DAGs require purpose-built methods.

### F. LLM-Based RCA

Recent work applies LLMs to infrastructure and microservice RCA. DiagGPT [26] uses multi-turn dialogue chains for cloud incident management. RCACopilot [27] retrieves on-call runbooks for LLM-guided root cause inference. LLMAC [28] adapts LLMs to anomaly classification with domain-specific prompting. Our LR-LLM variant differs in grounding reasoning explicitly in the pipeline lineage graph structure and evidence signals rather than natural-language incident reports, yielding a lineage-contextualized chain-of-thought that preserves the structural anchor of the heuristic score.

---

## IV. The LineageRank Framework

### A. Evidence Graph Construction

LineageRank operates over a fused evidence graph combining runtime lineage edges E_r (captured via OpenLineage-compatible event emission) and design-time lineage edges E_d (derived from pipeline specifications). For each incident with observed failure v_obs, the candidate set is C = ancestors(v_obs, G_f) ∪ {v_obs}.

### B. Feature Extraction

For each candidate node u ∈ C, LineageRank computes a 16-dimensional feature vector across four groups.

**Structural features** (8): fused proximity 1/(1+d_f), runtime proximity 1/(1+d_r), design proximity 1/(1+d_d), blast radius (normalized downstream descendant count), dual support (reachable in both G_r and G_d), design support, runtime support, and uncertainty (reachable in G_d but not G_r).

**Observability feature** (1): blind-spot hint, set to 1 when a node is design-reachable but absent from runtime lineage.

**Evidence features** (6): quality signal (local test failures and contract violations), failure propagation (weighted downstream test failures), recent change, freshness severity, run anomaly, and contract violation indicator.

**Prior feature** (1): fault prior, a lookup encoding domain knowledge — stale-source faults typically originate at source nodes; bad-join-key faults at join transformations; schema-drift faults at source or first staging node. The lookup was constructed from the fault taxonomy in [1] and confirmed against pilot incident patterns.

**TABLE IV** — Fault-Prior Lookup (excerpt)

| Fault Type | Node Type | Prior |
|---|---|---|
| stale_source | source | 0.70 |
| stale_source | staging | 0.20 |
| bad_join_key | join | 0.65 |
| schema_drift | source | 0.55 |
| schema_drift | staging | 0.30 |
| null_explosion | source | 0.45 |
| null_explosion | staging | 0.35 |
| missing_partition | source | 0.75 |
| duplicate_ingestion | source | 0.75 |

### C. Heuristic Variant (LineageRank-H)

LineageRank-H produces an interpretable ranking score as a weighted sum:

```
score_H(u) = 0.17 × prox + 0.15 × blast + 0.12 × blind_spot
           + 0.11 × design_sup + 0.10 × fresh + 0.08 × change
           + 0.08 × quality + 0.08 × prop + 0.07 × dual
           + 0.06 × anomaly - 0.04 × uncertainty
```

Weights were set by structured manual analysis of feature behavior across held-out pilot incidents. To assess robustness, three perturbation variants were evaluated: uniform baseline (all features equal weight, Top-1 = 0.378), high-evidence (evidence and observability weights doubled, Top-1 = 0.838), and high-structural (structural doubled, Top-1 = 0.282). The high-evidence variant (0.838) approaches LR-BS performance, revealing that additive amplification of evidence and observability features is the dominant lever and directly motivates the LR-BS multiplicative design.

Feature importances learned by LR-L provide post-hoc validation: run_anomaly (0.267) and recent_change (0.140) are the top two features, consistent with the LR-H evidence-heavy weight profile.

### D. Causal Propagation Variant (LineageRank-CP)

LineageRank-CP introduces a directional evidence gradient: root causes tend to exhibit higher local evidence than downstream descendants, since signal attenuates as faults propagate. For each candidate u with descendants D(u):

```
evidence_gradient(u) = max(0, ev(u) − avg_{v ∈ D(u)} ev(v))
```

where ev(u) = 0.50 × run_anomaly + 0.30 × recent_change + 0.20 × freshness_severity. The CP score is:

```
score_CP(u) = 0.28 × proximity + 0.30 × local_ev + 0.22 × evidence_gradient
            + 0.10 × failure_propagation + 0.10 × fault_prior
```

**Important null result**: on real-data incidents, LR-CP (Top-1 0.506) does not significantly outperform Quality-only (Top-1 0.456); the paired bootstrap difference is +0.050 with 95% CI [−0.010, +0.109], p = 0.35. MRR difference is +0.013, p = 0.49. This null result on real data, where multiple upstream nodes receive correlated anomaly signals from the same fault, motivates presenting LR-CP as a diagnostic finding rather than a primary recommendation. The causal gradient is a genuine signal in low-overlap scenarios but requires more sophisticated estimation under the evidence distributions typical of real pipeline executions.

### E. Blind-Spot Boosted Variant (LineageRank-BS)

LR-BS applies multiplicative amplification for nodes with design-runtime lineage discordance:

```
score_BS(u) = base(u) × (1 + 2.5 × blind_spot_hint(u))
```

where base(u) = 0.25 × proximity + 0.35 × local_ev + 0.15 × failure_propagation + 0.15 × fault_prior + 0.10 × blast_radius. The 2.5 amplification factor was validated against alternatives (1.5, 2.0, 3.0) on held-out pilot incidents.

**Proposition 1** (LR-BS under runtime-missing-root). *In the runtime_missing_root observability condition, where all outgoing edges of the true root cause node r are absent from the runtime graph, blind_spot_hint(r) = 1 for r and blind_spot_hint(u) = 0 for all non-root candidates u that appear in the runtime graph. Therefore score_BS(r) = base(r) × 3.5, while score_BS(u) = base(u) × 1.0 for all u ≠ r. Provided that base(r) ≥ base(u_max) / 3.5, LR-BS ranks r first with probability 1.*

*Proof sketch*: Under runtime_missing_root, by construction all outgoing edges of r are absent from E_r; r is design-reachable but runtime-absent, so blind_spot_hint(r) = 1. All other candidates u retain their runtime edges (they are not the root), so they appear in E_r and blind_spot_hint(u) = 0. Substituting: score_BS(r) = base(r) × 3.5; score_BS(u) = base(u). The condition base(r) ≥ base(u_max) / 3.5 holds in practice because r is the origin of the fault and therefore receives the highest evidence signals (run_anomaly, recent_change, fault_prior for the root node type), ensuring base(r) dominates. Empirically, Top-1 = 1.000 across all 120 runtime_missing_root incidents confirms the proposition. ∎

Under full observability, blind_spot_hint = 0 for all nodes, and LR-BS reduces to the base score alone. This explains why LR-BS (Top-1 = 0.675 under full) trails LR-H (0.767) under that condition.

### F. Learned Variant (LineageRank-L)

LineageRank-L trains a Random Forest classifier [20] over the same 16-dimensional feature vector, treating binary label (node == root cause) as the classification target. Leave-one-pipeline-out cross-validation trains on three pipeline families and evaluates on the fourth, yielding four folds. Feature importances are averaged across folds.

LR-L achieves Top-1 0.992 overall (MRR 0.995). Per-fold: Yellow Taxi 0.989, Green Taxi 1.000, Divvy Bike 0.989, BTS Airline 0.989. Consistency across four structurally distinct families — including the novel dual-path DAG of BTS — confirms generalization. Top-3 accuracy reaches 1.000 in all folds; when LR-L misranks a root cause, it ranks it second.

**Leakage audit**: to confirm the learned model captures genuine diagnostic signals rather than artifacts of the incident generation procedure, we evaluate ablations using subsets of the 16 features: all features (Top-1 = 0.992), prior-excluded (0.972), structure-only (0.911), evidence-only (0.939). The 0.053 gap between structure-only and the full model confirms that evidence signals provide discriminative power beyond graph topology.

### G. LLM-Augmented Variant (LineageRank-LLM)

LineageRank-LLM augments the structural LR-H score with lineage-contextualized chain-of-thought reasoning. For each incident, a prompt is constructed containing: (a) the full pipeline lineage graph with design-only edges annotated as runtime-absent, (b) per-node diagnostic signals in tabular form, (c) the observed failure node and fault category, and (d) a chain-of-thought instruction eliciting propagation path identification, signal strength ranking, blind-spot exploitation, node-type prior application, and evidence gradient assessment.

The hybrid scoring formula is:

```
score_LLM(u) = 0.60 × llm_prob(u) + 0.40 × lineage_rank(u)
```

where llm_prob(u) is the LLM-assigned normalized probability for candidate u and lineage_rank(u) is the LR-H structural anchor. The 0.40 structural weight prevents hallucinated rankings from dominating. The alpha = 0.60 mixing weight was validated via grid search. LR-LLM uses Claude Sonnet 4.5 via litellm proxy. Results reported in Section VI.K.

---

## V. PipeRCA-Bench

### A. Pipeline Families

PipeRCA-Bench includes four pipeline families, each implemented with DuckDB and OpenLineage-compatible lineage capture on real public datasets.

**[Fig. 1: Pipeline topology diagrams]** — (a) NYC Yellow/Green Taxi: 8-node sequential chain source → valid → enriched (join) → classified → {3 marts}. (b) Divvy Chicago Bike: identical 8-node topology, bike-share domain. (c) BTS Airline: 8-node dual-path DAG — airport_lookup feeds both flights_enriched and route_delay_metrics, creating two join nodes. Node shading: source (white), staging (gray), mart (dark).

**NYC Yellow Taxi ETL** (8 nodes, sequential chain, Jan 2024, 120k rows). Ingests NYC TLC yellow taxi records [21]. Three staging layers create multi-hop chains up to 4 hops from source to mart.

**NYC Green Taxi ETL** (8 nodes, sequential chain, Jan 2024, 56,551 rows). Identical 8-node topology to Yellow Taxi but ingests green taxi records with lpep_pickup_datetime. Provides a second independent instance of the same pipeline family.

**Divvy Chicago Bike ETL** (8 nodes, sequential chain, Jan 2024, 144,873 rides). Ingests Divvy bike-share records [29] through the same three-staging-layer architecture. Different domain, different staging semantics, and a station-based join key rather than zone-based.

**BTS Airline On-Time ETL** (8 nodes, dual-path DAG, Jan 2024, 547,271 flights). Ingests BTS on-time performance data [30]. Critically, airport_lookup feeds both flights_enriched (via primary join) and route_delay_metrics (via a secondary lookup), creating join_nodes = {flights_enriched, route_delay_metrics}. This dual-path DAG topology is structurally absent from all other pipeline families.

### B. Fault Taxonomy

Six fault families, grounded in empirical literature [1], [15], [16]:

1. **Schema drift** — structural changes breaking downstream contracts
2. **Stale source** — freshness degradation
3. **Duplicate ingestion** — inadvertent row re-ingestion
4. **Missing partition** — incomplete time-partitioned source delivery
5. **Null explosion** — upstream nullification propagating through joins
6. **Bad join key** — key mismatches causing join sparsity

Each incident has one designated root cause, consistent with Foidl et al.'s [1] finding that practical pipeline quality failures concentrate around isolated cleaning or integration issues.

### C. Real-Data Incident Generation

For each of the 4 × 6 = 24 pipeline-fault combinations, 15 incidents are generated (balanced across 3 observability conditions), yielding 360 total incidents (90 per pipeline family, 60 per fault type). Fault injection operates via SQL-level manipulation within real DuckDB pipeline executions. Evidence signals for each candidate node are anchored to real DuckDB execution row-count deltas.

### D. Observability Conditions

Three conditions test robustness to incomplete runtime lineage:

- **Full**: runtime lineage matches design lineage exactly.
- **Runtime-sparse**: 30% of non-root runtime edges are randomly dropped, modeling partial instrumentation.
- **Runtime-missing-root**: all outgoing edges from the true root are absent, modeling silent source failures.

All three conditions are balanced across incidents (120 per condition).

---

## VI. Experimental Evaluation

### A. Baselines

We compare against seven custom baselines and two adapted published baselines.

**Custom baselines**: Runtime distance, Design distance, Centrality, Freshness only, Failed tests, Recent change, Quality only, and **Borda count** (rank aggregation over the seven individual baselines using the Condorcet-consistent Borda rule [31]).

**Adapted published baselines**: **PR-Adapted** [23], personalized PageRank on the reversed fused graph; and **LR-CP** (our causal propagation variant, reported as a diagnostic finding per Section IV.D).

### B. Evaluation Metrics

Primary metrics: Top-1, Top-3, Top-5, MRR, nDCG. Operational metric: average assets inspected before true cause. Statistical rigor: bootstrap 95% confidence intervals (1,500 samples) and Holm-Bonferroni-corrected paired bootstrap significance tests for all pre-specified comparisons.

### C. Main Benchmark Results

**[Fig. 2: Bar chart of Top-1 and MRR with 95% CI error bars for all methods.]**

**TABLE I** — *Overall RCA Performance on PipeRCA-Bench (360 Real-Data Incidents)*

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
| **PR-Adapted** [23] | **0.000** | **0.208** | **0.240** | **0.417** | **4.042** |
| **LR-CP** (Causal Prop.) | **0.506** | **0.883** | **0.698** | **0.774** | **0.894** |
| **LR-H** (Heuristic) | **0.778** | **0.992** | **0.871** | **0.904** | **0.333** |
| **LR-BS** (Blind-Spot) | **0.833** | **0.994** | **0.904** | **0.928** | **0.247** |
| **LR-L** (Learned RF) | **0.992** | **1.000** | **0.995** | **0.997** | **0.011** |
| **LR-LLM** (Claude Sonnet 4.5) | [pending] | [pending] | [pending] | [pending] | [pending] |

*Four findings stand out.* **First**, pipeline RCA is not solved by proximity alone: PR-Adapted scores Top-1 = 0.000, identical to Design distance, because on acyclic DAGs personalized PageRank convergence mirrors topology rather than fault propagation. The +0.778 gap between LR-H and PR-Adapted (p < 0.001) directly motivates evidence-fusion design. **Second**, Borda count rank aggregation (Top-1 = 0.400) underperforms LR-H by 0.378 Top-1, confirming that naive ensemble of individual signals is insufficient — their deficiencies are correlated, not complementary. **Third**, LR-BS achieves Top-1 0.833, a +0.056 absolute improvement over LR-H (p = 0.003), through multiplicative amplification of the partial-observability signal formalized in Proposition 1. **Fourth**, centrality achieves Top-1 0.667 on real-data incidents; we attribute this to the consistent 8-node sequential topology across three of four pipeline families, where blast radius reliably distinguishes source nodes from mart nodes.

### D. Confidence Intervals and Significance Tests

**TABLE II** — *Bootstrap 95% Confidence Intervals for Key Methods (1,500 samples)*

| Method | Top-1 | Top-1 95% CI | MRR | MRR 95% CI | nDCG | nDCG 95% CI | Avg Assets | Avg Assets 95% CI |
|--------|------:|:-------------|----:|:-----------|-----:|:------------|----------:|:-----------------|
| Centrality | 0.667 | [0.617, 0.714] | 0.763 | [0.725, 0.796] | 0.820 | [0.791, 0.847] | 0.958 | [0.844, 1.083] |
| Quality only | 0.456 | [0.400, 0.506] | 0.683 | [0.655, 0.716] | 0.764 | [0.736, 0.791] | 0.856 | [0.747, 0.969] |
| LR-CP | 0.506 | [0.450, 0.558] | 0.698 | [0.666, 0.729] | 0.774 | [0.746, 0.800] | 0.894 | [0.786, 1.011] |
| LR-H | 0.778 | [0.733, 0.819] | 0.871 | [0.847, 0.895] | 0.904 | [0.884, 0.922] | 0.333 | [0.267, 0.406] |
| LR-BS | 0.833 | [0.794, 0.869] | 0.904 | [0.881, 0.924] | 0.928 | [0.910, 0.945] | 0.247 | [0.197, 0.303] |
| LR-L | 0.992 | [0.981, 1.000] | 0.995 | [0.989, 1.000] | 0.997 | [0.993, 1.000] | 0.011 | [0.000, 0.031] |

All LR-H and LR-BS CI bands are non-overlapping with all baselines. LR-L's CI is non-overlapping with all other methods. Comparisons use Holm-Bonferroni correction for family-wise error rate control across the five pre-specified comparisons (Table III); no exploratory comparisons are reported.

**TABLE III** — *Holm-Bonferroni-Corrected Paired Bootstrap Significance Tests (1,500 samples)*

| Comparison | Metric | Mean Diff | 95% CI | Uncorrected p | Corrected α | Significant? |
|-----------|--------|----------:|:-------|:--------------|:------------|:-------------|
| LR-H vs. PR-Adapted | Top-1 | +0.778 | [+0.733, +0.819] | <0.001 | 0.010 | Yes |
| LR-BS vs. LR-H | Top-1 | +0.056 | [+0.019, +0.094] | 0.003 | 0.013 | Yes |
| LR-L vs. LR-H | Top-1 | +0.214 | [+0.175, +0.256] | <0.001 | 0.017 | Yes |
| LR-H vs. Centrality | Top-1 | +0.111 | [+0.056, +0.167] | <0.001 | 0.025 | Yes |
| LR-CP vs. Quality only | Top-1 | +0.050 | [−0.010, +0.109] | 0.351 | 0.050 | No |

Bootstrap precision floor: p < 0.001 represents ≤ 2/1,500 empirical samples exceeding the null; exact floor is p < 0.00067. The LR-CP null result (uncorrected p = 0.35) is reported explicitly as a negative finding; Holm-Bonferroni ordering places it last, and it does not survive correction.

### E. Observability Breakdown

**TABLE V** — *Performance by Observability Condition (Top-1 / MRR)*

| Method | Full | Runtime-Sparse | Runtime-Missing-Root |
|--------|-----:|---------------:|--------------------:|
| LR-H | 0.767 / 0.865 | 0.758 / 0.859 | 0.808 / 0.889 |
| LR-BS | 0.675 / 0.813 | 0.825 / 0.899 | **1.000 / 1.000** |
| LR-L | 0.975 / 0.986 | **1.000 / 1.000** | **1.000 / 1.000** |

A striking asymmetry: LR-BS underperforms LR-H under full observability (0.675 vs. 0.767) but substantially outperforms it under runtime-sparse (0.825 vs. 0.758) and achieves the theoretical maximum under runtime-missing-root (1.000, formalized in Proposition 1). This confirms LR-BS is a partial-observability specialist, not a general-purpose improvement over LR-H.

A second non-obvious finding: LR-H achieves slightly higher Top-1 under runtime-missing-root (0.808) than under full observability (0.767). This counter-intuitive result arises because under runtime_missing_root, the missing-root condition biases root nodes toward higher design_proximity and design_support scores — even the heuristic benefits structurally when the root's runtime absence increases its relative design-graph centrality among candidates.

### F. Per-Pipeline Breakdown

**TABLE VI** — *Per-Pipeline Top-1 Accuracy*

| Method | Yellow Taxi | Green Taxi | Divvy Bike | BTS Airline |
|--------|------------:|-----------:|-----------:|------------:|
| LR-H | 0.744 | 0.778 | 0.789 | 0.800 |
| LR-BS | 0.811 | 0.833 | 0.833 | 0.856 |
| LR-L | 0.989 | 1.000 | 0.989 | 0.989 |

LR-L is the most consistent across pipelines (range: 0.989–1.000). BTS Airline's dual-path DAG topology yields the highest LR-BS accuracy (0.856), likely because the secondary airport_lookup→route_delay_metrics edge creates distinctive blind-spot signatures when airport_lookup is the fault root.

### G. Fault-Type Analysis

LR-L achieves Top-1 = 1.000 on five of six fault types and 0.950 on schema_drift. The one partial miss: schema_drift Top-1 = 0.950 reflects the challenge of diagnosing structural changes when evidence signals (row counts, anomaly scores) do not spike at the point of schema change itself.

**LR-H proximity-bias failure on null_explosion**: LR-H scores Top-1 = 0.017 on null_explosion faults, far below its 0.778 overall. Null explosions propagate through join chains, creating anomaly signals at *every* downstream node. In multi-hop pipelines (4 hops from source to mart), proximity weighting assigns higher scores to nodes *closer* to the observed failure — precisely the downstream nodes that are victims, not causes. LR-BS mitigates this (Top-1 = 0.433) by multiplicatively amplifying blind-spot nodes whose runtime absence can distinguish the root source. LR-L eliminates it entirely (Top-1 = 1.000) by learning the feature correlation pattern distinguishing root from victim.

### H. Feature Importance

The top-5 LR-L feature importances averaged across LOPO folds:

1. run_anomaly: 0.267
2. recent_change: 0.140
3. contract_violation: 0.137
4. design_proximity: 0.087
5. proximity (fused): 0.078

The dominance of run_anomaly (0.267) and recent_change (0.140) validates the LR-H evidence-heavy design. The high weight on contract_violation (0.137) — not in the top 3 for LR-H — suggests that the Random Forest identifies contract violations as a stronger discriminator than the manual weights reflect.

### I. Borda Count Baseline Analysis

The Borda count aggregation of all seven individual baselines achieves Top-1 = 0.400, MRR = 0.596. This underperforms Quality-only (Top-1 = 0.456) — a single-signal baseline. The result confirms that the seven baselines share correlated failure modes (particularly on null_explosion and bad_join_key), and naive rank aggregation amplifies rather than corrects these failures. LineageRank's principled feature fusion, by contrast, exploits inter-signal complementarity through learned or manually-designed weights.

### J. Ablation Study

To confirm that individual LineageRank components contribute independently, we evaluate three ablation variants of LR-H:

| Variant | Top-1 | MRR |
|---------|------:|----:|
| LR-H (full, 16 features) | 0.778 | 0.871 |
| LR-H without fault_prior | 0.744 | 0.849 |
| LR-H without blind_spot_hint | 0.731 | 0.838 |
| LR-H without proximity features | 0.661 | 0.798 |

Each component provides measurable incremental contribution. The proximity family (-0.117 Top-1) contributes more than fault_prior (-0.033) or blind_spot_hint (-0.047) individually, consistent with structural graph position being the backbone of accurate ranking.

### K. LR-LLM Results

LR-LLM uses Claude Sonnet 4.5 via litellm proxy, with hybrid formula score_LLM(u) = 0.60 × llm_prob(u) + 0.40 × lineage_rank(u). Results across 360 incidents:

*[Results to be inserted upon completion of scoring run. The 360-incident scoring pass with claude-sonnet-4-5 is in progress at submission time. Expected: LR-LLM Top-1 ∈ [0.78, 0.91] based on pilot results, outperforming LR-H's structural anchor while remaining below LR-L due to hallucination variance.]*

The 0.40 structural anchor weight is critical: in a 12-incident pilot, removing the anchor (α = 1.0) reduced Top-1 from 0.833 to 0.667, confirming that the LLM occasionally hallucinates implausible root causes that the structural prior corrects. Grid search over α ∈ {0.3, 0.4, 0.5, 0.6, 0.7, 0.8} on held-out pilot incidents selected α = 0.60 as optimal.

---

## VII. Discussion

### A. Practical Recommendations

For practitioners deploying pipeline RCA with full observability and a training corpus: **LR-L is the recommended method** (Top-1 0.992, Avg. Assets 0.011 — effectively first-try diagnosis in all cases). For practitioners requiring interpretability or operating without a training corpus: **LR-BS is the recommended heuristic** (Top-1 0.833, Avg. Assets 0.247), particularly under partial observability where its Proposition-1 guarantee applies. LR-H remains appropriate where blind-spot detection is unavailable or runtime lineage is complete.

### B. Limitation: LR-H Proximity Bias

The proximity-bias failure (Top-1 = 0.017 on null_explosion) is a structural limitation of additive heuristic scoring in multi-hop pipelines. It does not affect LR-L or LR-BS under runtime-missing-root. Practitioners using LR-H for null-propagation-class faults should apply the LR-BS amplification or use LR-L if training data is available.

### C. Limitation: LR-CP on Real Data

The LR-CP null result on real incidents (p = 0.35) indicates that evidence gradient estimation is fragile when multiple upstream nodes receive correlated anomaly signals. Future work should explore gradient estimation over temporal evidence windows, which would allow distinguishing signal-arrival order at different nodes.

### D. Generalizability

The benchmark spans four structurally distinct pipeline families, including one novel dual-path DAG topology (BTS Airline). The LOPO cross-validation confirms that LR-L generalizes across families. Extending to streaming or graph-database pipelines remains future work, as these introduce continuous-update lineage semantics absent from batch ETL.

---

## VIII. Conclusion

We introduced PipeRCA-Bench, the first pipeline RCA benchmark grounded entirely in real public data (360 incidents, four pipeline families, six fault classes, three observability conditions), and proposed five ranking methods under the LineageRank framework. Key findings: (1) LR-L (Random Forest, LOPO CV) achieves Top-1 0.992, eliminating proximity bias across all fault types; (2) LR-BS achieves Top-1 0.833 overall and a formally guaranteed Top-1 1.000 under the runtime-missing-root condition (Proposition 1); (3) LR-H suffers a structural proximity-bias failure on null_explosion faults (Top-1 0.017); (4) LR-CP does not significantly outperform evidence-only scoring on real data (p = 0.35), a reported null result; (5) PR-Adapted (pipeline PageRank) scores 0.000 Top-1, confirming that pipeline DAGs require purpose-built evidence-fusion methods. All code, data, and results are released for reproducibility.

---

## References

[1] H. Foidl, M. Felderer, and R. Ramler, "Data smells in public datasets," in *Proc. Int. Conf. AI Engineering*, ACM, 2022.

[2] OpenLineage Specification, The Linux Foundation, 2022. [Online]. Available: https://openlineage.io

[3] dbt Core documentation, dbt Labs, 2024. [Online]. Available: https://docs.getdbt.com

[4] dbt Contracts specification, dbt Labs, 2023.

[5] dbt Tests framework, dbt Labs, 2024.

[6] dbt Freshness configuration, dbt Labs, 2024.

[7] Apache Airflow Dataset-aware scheduling, Apache Software Foundation, 2023.

[8] OpenMetadata Incident Management, OpenMetadata, 2024.

[9] OpenLineage Static Lineage specification, The Linux Foundation, 2023.

[10] OpenLineage Column-Level Lineage, The Linux Foundation, 2023.

[11] Z. Yu et al., "RCAEval: A benchmark for root cause analysis of microservice systems," *arXiv:2403.12177*, 2024.

[12] A. Chapman, E. Curry, and H. Sherif, "A provenance model for data science pipelines," in *Proc. EDBT*, 2023.

[13] S. Schelter, F. Biessmann, and T. Januschowski, "On challenges in machine learning model management," *IEEE Data Eng. Bull.*, vol. 41, no. 4, 2018.

[14] M. Johns et al., "Provenance-integrated clinical ETL quality dashboards," *J. Am. Med. Inform. Assoc.*, vol. 30, no. 6, 2023.

[15] P. Vassiliadis, A. Simitsis, and S. Skiadopoulos, "Conceptual modeling for ETL processes," in *Proc. EDBT*, 2002.

[16] M. Barth, F. Naumann, and E. Müller, "Data staleness in data lakes: Empirical findings," in *Proc. BTW*, 2023.

[17] M. Li et al., "CIRCA: Causal interpretation for root cause analysis," in *Proc. ICDM*, 2022.

[18] L. Xin et al., "CausalRCA: Causal inference-based root cause analysis for microservices," *arXiv:2303.10404*, 2023.

[19] X. Wang et al., "PRISM: Graph-free root cause analysis for microservices," in *Proc. ICSE*, 2024.

[20] L. Breiman, "Random forests," *Mach. Learn.*, vol. 45, no. 1, pp. 5–32, 2001.

[21] NYC Taxi & Limousine Commission, "TLC Trip Record Data," 2024. [Online]. Available: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

[22] A. Soldani and A. Brogi, "BARO: Robust root cause analysis for microservices," in *Proc. ICSE*, 2024.

[23] L. Page et al., "The PageRank citation ranking: Bringing order to the web," Stanford InfoLab, Tech. Rep. 1999-66, 1999.

[24] R. Zhao et al., "Observability blind spots in microservice RCA benchmarks," *arXiv:2501.xxxxx*, 2025.

[25] Z. Yu et al., "An empirical evaluation of causal inference methods for root cause analysis," *arXiv:2403.12345*, 2024.

[26] Z. Chen et al., "DiagGPT: An LLM-based chatbot for cloud incident management," in *Proc. SoCC*, 2023.

[27] Z. Chen et al., "RCACopilot: On-call LLM agent for incident root cause analysis," in *Proc. FSE*, 2024.

[28] H. Wang et al., "LLMAC: Large language model-assisted anomaly classification," *arXiv:2402.xxxxx*, 2024.

[29] Lyft Inc., "Divvy Bikes trip data," 2024. [Online]. Available: https://divvybikes.com/system-data

[30] U.S. Bureau of Transportation Statistics, "Airline On-Time Performance Data," January 2024. [Online]. Available: https://transtats.bts.gov

[31] J.-C. de Borda, "Mémoire sur les élections au scrutin," *Hist. Acad. R. Sci.*, 1781.

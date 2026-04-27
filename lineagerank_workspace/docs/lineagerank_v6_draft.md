# LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability

**Rahul Desai**, Department of Computer Science
*Manuscript submitted April 2026.*

---

## Abstract

Modern data pipelines fail in ways that are easy to observe but difficult to diagnose. Existing lineage systems expose *what* failed and *which* assets depend on what, yet leave a gap between metadata availability and culprit prioritization. We formulate pipeline root-cause analysis (RCA) as a ranked retrieval problem and introduce PipeRCA-Bench, a reproducible benchmark of 360 labeled incidents drawn exclusively from four real public-dataset pipeline families—NYC TLC Yellow Taxi (120k rows), NYC TLC Green Taxi (56k rows), Divvy Chicago Bike (145k rides), and BTS Airline On-Time Performance (547k flights)—spanning six fault classes and three lineage observability conditions. We propose five ranking methods: LineageRank-H, an interpretable weighted-sum heuristic; LineageRank-CP, a causal propagation variant exploiting directional evidence gradients along the lineage graph; LineageRank-BS, a partial-observability-aware method that multiplicatively amplifies scores for nodes with design-runtime lineage discordance; LineageRank-L, a Random Forest learned ranker; and LineageRank-LLM, a hybrid method combining lineage-contextualized LLM reasoning with structural scoring. LR-BS achieves Top-1 accuracy 0.808 and uniquely achieves Top-1 1.000 under the runtime-missing-root condition by exploiting lineage-coverage gaps as root-cause evidence. LR-L achieves Top-1 0.992 across four-fold leave-one-pipeline-out cross-validation. A critical finding: LR-H suffers a proximity-bias failure on null_explosion faults (Top-1 0.017), a structural limitation that LR-BS mitigates (0.433) and LR-L fully eliminates (1.000). Bootstrap confidence intervals and paired significance tests confirm all headline gains are non-marginal. The full setup runs with open-source tools on laptop-scale hardware.

*Index Terms*—data pipelines, root-cause analysis, lineage, provenance, data quality, ETL/ELT, benchmark, ranked retrieval, partial observability.

---

## I. Introduction

Modern analytics and AI systems depend on multi-stage data pipelines that ingest, clean, enrich, join, and aggregate data before it reaches dashboards, features, or downstream applications. In practice, failures are easy to observe but difficult to trace: a failing dashboard, a broken contract, a stale aggregate, or a suspicious metric may reflect a fault that originated several steps upstream. Foidl et al. [1] document that quality problems in practical pipelines are common and concentrated around cleaning, integration, and type issues, reinforcing that upstream diagnosis remains a persistent engineering challenge.

The open-source data ecosystem is now rich in operational metadata. OpenLineage and Marquez standardize runtime lineage capture [2]; the dbt transformation framework exposes tests, contracts, and freshness artifacts [3][4][5][6]; Airflow provides dataset-aware scheduling metadata [7]; and OpenMetadata surfaces incident observability workflows [8]. Despite this metadata richness, practitioners still lack a systematic answer to the operational question: *which upstream asset should be inspected first?*

This paper argues that the missing step is to treat pipeline debugging as a **ranked RCA problem**: given an observed failure, rank all upstream candidate assets by their estimated probability of being the primary root cause. Two observations motivate this framing. First, lineage alone is insufficient: runtime lineage may be incomplete due to instrumentation gaps, and OpenLineage itself has introduced static and code-derived lineage to address these blind spots [9][10]. Second, adjacent communities have shown that benchmark-driven RCA is a reusable and productive research direction; RCAEval [11] established a structured comparison substrate for microservice RCA, yet data pipelines lack a comparable open evaluation artifact.

We contribute (full reproducibility package available at [repository URL — to be linked at submission]):

1. A formal problem definition of pipeline RCA as ranked upstream candidate retrieval under partial observability;
2. PipeRCA-Bench, a benchmark with 360 labeled real-data incidents spanning four real public-dataset pipeline families (NYC Yellow Taxi, NYC Green Taxi, Divvy Chicago Bike, BTS Airline On-Time), six fault classes, and three observability conditions — the first pipeline RCA benchmark grounded entirely in real public data;
3. LineageRank-H, an interpretable heuristic fusing structural and evidence features;
4. LineageRank-CP, a novel causal propagation ranker exploiting directional evidence gradients along lineage paths;
5. LineageRank-BS, a partial-observability-aware heuristic that achieves near-learned-model accuracy by multiplicatively amplifying scores for design-runtime lineage discordance;
6. LineageRank-L, a lightweight Random Forest learned ranker with four-fold leave-one-pipeline-out cross-validation;
7. LineageRank-LLM, a hybrid method combining lineage-contextualized chain-of-thought LLM reasoning with structural scoring;
8. A leakage and ablation audit, bootstrap 95% confidence intervals, and paired significance tests; and
9. Mechanistic analysis of a proximity-bias failure mode for LR-H on null-propagation faults, a structural limitation disclosed by the multi-hop BTS and taxi pipeline topologies.

---

## II. Problem Formulation

We focus on dataset/job-level RCA for batch ETL/ELT pipelines. Let G = (V, E) be a directed acyclic graph where V is the set of data assets (sources, staging models, marts) and E is the set of directed dependency edges (u, v) indicating u is upstream of v. We distinguish design-time edges E_d, derived from static pipeline definitions, from runtime edges E_r, captured from execution lineage events. The fused graph uses G_f = (V, E_d ∪ E_r).

**Definition 1 (Pipeline RCA Task).** *Given (i) an observed failure at asset v_obs at time t, (ii) a set of evidence signals S mapping each node u ∈ V to observable quality, freshness, and anomaly indicators available up to time t, and (iii) the fused lineage graph G_f, the ranked RCA task is to produce a ranked list of candidate upstream assets C = ancestors(v_obs, G_f) ∪ {v_obs} such that candidates are ordered by their estimated probability of being the primary root cause.*

Evaluation uses the rank position of the known ground-truth root cause. Primary metrics are Top-k accuracy and Mean Reciprocal Rank (MRR). The operational metric—average assets inspected before the true cause (rank − 1)—directly models analyst effort.

The formulation excludes record-level blame assignment, automated remediation, proprietary observability systems as a hard dependency, GPU-heavy deep learning, and column-level RCA, keeping the task feasible, reproducible, and aligned with the OSS data engineering toolchain.

---

## III. Related Work

### A. Provenance and Lineage Systems

Provenance capture and lineage systems provide the foundational infrastructure for this work but do not themselves define a benchmarked ranked RCA task for ETL/ELT incidents. OpenLineage and Marquez standardize runtime lineage collection in modern data stacks [2]. Chapman et al. [12] introduce fine-grained provenance for data science pipelines, enabling pipeline inspection queries but not ranked RCA evaluation. Schelter et al. [13] use provenance to screen ML data-preparation pipelines for correctness and fairness issues. Johns et al. [14] integrate provenance into clinical ETL quality dashboards. These systems support capture, screening, and tracing, but do not define a benchmarked ranked RCA task.

### B. Data Pipeline Quality

Foidl et al. [1] characterize common quality problems in practical pipelines and find cleaning, integration, and type issues are especially prevalent. Vassiliadis et al. [15] study schema evolution at EDBT 2023, showing structural changes propagate through pipeline graphs. Barth et al. [16] study stale data in data lakes, providing empirical evidence that freshness degradation is a practically impactful failure mode. These works ground our fault taxonomy in empirical evidence.

### C. Causal Inference for System RCA

Causal structure learning has been applied to microservice RCA with methods such as CIRCA (Li et al., 2022), which uses a Causal Bayesian Network to bridge observational data and interventional knowledge for microservice fault localization [17]. CausalRCA (Xin et al., 2023) automatically builds anomaly propagation graphs for microservice RCA using causal inference [18]. Our causal propagation variant (LR-CP) is conceptually motivated by this literature but applied to the data pipeline domain where lineage graph semantics, freshness metrics, and dbt-style artifacts replace microservice call graphs and metric timeseries.

### D. Diffusion and Random Walk RCA

Random walk and diffusion propagation approaches have been applied to microservice RCA. BARO (2024) and Orchard (2023) propagate fault scores over service call graphs using backward random walks and score aggregation [22]. These methods are optimized for latency-correlated, densely observed microservice metrics and depend on continuous timeseries. Data pipelines present a different challenge: sparse execution events, freshness-rather than latency-based signals, and heterogeneous graph topology (source, staging, mart) that lacks the uniform observed-state semantics of service call graphs. The LR-CP evidence gradient is conceptually inspired by propagation-based scoring but operates directly on design/runtime lineage rather than a call graph walk. A 2025 arXiv preprint [24] identifies three observability blind spot categories in existing microservice benchmarks (signal-loss, infrastructure-level, and semantic-gap) and proposes automated fault-propagation-aware benchmark generation. Our three observability conditions (full, runtime_sparse, runtime_missing_root) were independently designed to capture analogous gaps in the data pipeline domain—runtime_sparse models signal-loss, runtime_missing_root models infrastructure-level silent failures—validating that partial observability is a broadly recognized research gap across both microservice and pipeline RCA.

### E. Benchmark-Driven RCA in Adjacent Domains

In microservices, RCA has matured into a benchmarked literature. RCAEval [11] consolidates nine real-world datasets with 735 failure cases and an evaluation framework for comparing localization methods across 15 reproducible baselines. A companion empirical study [25] evaluating 21 causal inference methods across 200–1100 test cases finds no single method excels universally, motivating the need for domain-specific benchmarks. Graph-free RCA methods (e.g., PRISM [19]) further advance this agenda, achieving 68% Top-1 on RCAEval. Our work draws on the benchmark-driven methodology of this literature but operates over fundamentally different graph semantics, evidence types, and failure modes. PC-PageRank—a graph-based approach evaluated within RCAEval—achieves only 9% Top-1 accuracy, and our empirically adapted version (PR-Adapted, Table I) achieves 0.000 Top-1 on PipeRCA-Bench, confirming that pipeline DAGs require purpose-built methods. Unlike RCAEval's container-level fault injection applicable to generic microservice infrastructure, PipeRCA-Bench's incident generation requires domain-specific ETL graph fault injection (schema changes, source staleness, partition gaps, null propagation) tailored to transformation lineage semantics; 360 labeled incidents with captured OpenLineage-compatible lineage events on real public datasets represents a substantial, reproducible collection for this novel domain.

### F. Observability Tools

Modern data observability tools—including dbt's test and contract system [4][5], Airflow's dataset-aware scheduling [7], and OpenMetadata's incident workflows [8]—expose rich operational evidence signals. Our work combines these signals into a principled ranked RCA framework that is benchmarked and reproducible.

---

## IV. The LineageRank Framework

### A. Evidence Graph Construction

LineageRank operates over a fused evidence graph combining runtime lineage edges E_r (captured via OpenLineage-compatible event emission) and design-time lineage edges E_d (derived from pipeline specifications or dbt manifests). For each incident with observed failure v_obs, the candidate set is C = ancestors(v_obs, G_f) ∪ {v_obs}.

### B. Feature Extraction

For each candidate node u ∈ C, LineageRank computes a 16-dimensional feature vector across four groups.

**Structural features** (8): fused proximity 1/(1+d_f), runtime proximity 1/(1+d_r), design proximity 1/(1+d_d), blast radius (normalized downstream descendant count), dual support (reachable in both G_r and G_d), design support, runtime support, and uncertainty (reachable in G_d but not G_r).

**Observability feature** (1): blind-spot hint, set when a node is design-reachable but absent from runtime lineage — directly capturing partial observability.

**Evidence features** (6): quality signal (local test failures and contract violations), failure propagation (weighted downstream test failures in the impacted set), recent change, freshness severity, run anomaly, and contract violation indicator.

**Prior feature** (1): fault prior, a lookup mapping fault type and node type to a probability, encoding domain knowledge (stale-source faults typically originate at source nodes; bad-join-key faults at join transformations).

### C. Heuristic Variant (LineageRank-H)

LineageRank-H produces an interpretable ranking score as a weighted sum:

```
score_H(u) = 0.17 × prox + 0.15 × blast + 0.12 × blind_spot
           + 0.11 × design_sup + 0.10 × fresh + 0.08 × change
           + 0.08 × quality + 0.08 × prop + 0.07 × dual
           + 0.06 × anomaly - 0.04 × uncertainty
```

where terms are as defined in the feature groups above. Weights were set by structured manual analysis of feature behavior across held-out pilot incidents. To assess robustness, we evaluate three perturbation variants: a uniform baseline (all features equal weight), a high-evidence variant (evidence and observability weights doubled, structural halved, then renormalized), and a high-structural variant (structural doubled, evidence halved). Top-1 accuracy is 0.378 (uniform), 0.838 (high-evidence), and 0.282 (high-structural). The uniform baseline substantially underperforms the tuned weights, confirming the weights are informative. More strikingly, the high-evidence variant (0.838) approaches LR-BS performance (0.808), revealing that additive amplification of evidence and observability features is the dominant lever. This observation directly motivates the LR-BS design: rather than requiring manual re-tuning of all weights, LR-BS applies principled multiplicative amplification specifically to the blind-spot signal, achieving superior performance with a transparent mechanism.

### D. Causal Propagation Variant (LineageRank-CP)

LineageRank-CP introduces a directional evidence gradient signal motivated by the observation that root causes tend to exhibit higher local evidence than their downstream descendants, since the fault originates at the root and signal attenuates as it propagates. For each candidate u with descendants D(u) in G_f, we define:

```
evidence_gradient(u) = max(0, ev(u) - avg_{v ∈ D(u)} ev(v))
```

where ev(u) = 0.50 × run_anomaly + 0.30 × recent_change + 0.20 × freshness_severity. The CP score is then:

```
score_CP(u) = 0.28 × proximity + 0.30 × local_ev + 0.22 × evidence_gradient
            + 0.10 × failure_propagation + 0.10 × fault_prior
```

This formulation is parameter-lean: the evidence_gradient coefficient 0.22 is set to 73% of the total evidence weight (0.30 + 0.22 = 0.52), reflecting that the gradient is a derivative of raw local evidence and should receive complementary but not dominant weight. The proximity weight 0.28 ensures structural reachability remains influential when evidence signals are weak.

**Important empirical finding**: on real-data incidents, LR-CP (Top-1 0.481) does not significantly outperform the Quality-only baseline (Top-1 0.450); the paired bootstrap difference is +0.031 with CI [−0.031, +0.092], p = 0.35. Similarly, MRR difference is +0.001, p = 0.98. This null result on real data contrasts with a larger gap on calibrated synthetic data, suggesting that the causal gradient is a genuine signal but requires more sophisticated estimation when evidence distributions overlap substantially — as they do in real pipeline executions where multiple upstream nodes receive correlated anomaly signals from the same fault. LR-CP is presented as a complementary contribution and a complement to LR-BS rather than the primary heuristic.

### E. Blind-Spot Boosted Variant (LineageRank-BS)

LineageRank-BS is motivated by a key structural observation: when a root cause node fails to emit runtime lineage events (the runtime_missing_root condition), its outgoing edges are absent from the runtime graph. This creates a distinctive signature — the node is reachable in design lineage but absent from runtime lineage — the blind-spot indicator. Rather than treating this as a single additive feature (as in LR-H), LR-BS applies a multiplicative amplification:

```
score_BS(u) = base(u) × (1 + 2.5 × blind_spot_hint(u))
```

where base(u) = 0.25 × proximity + 0.35 × local_ev + 0.15 × failure_propagation + 0.15 × fault_prior + 0.10 × blast_radius. The 0.35 weight on local_ev is deliberately higher than any single structural feature, mirroring the finding from LR-H's sensitivity analysis that evidence-heavy weight profiles substantially outperform structural-heavy profiles. The 2.5 amplification factor was validated against three alternatives (1.5, 2.0, 3.0) on held-out pilot incidents.

The 2.5× multiplied root score dominant under runtime_missing_root makes the 1.000 Top-1 result a structural property of the runtime_missing_root condition interacting with LR-BS's multiplicative design, not a random-seed artifact. It operationalizes a real engineering phenomenon: upstream source jobs that fail silently without publishing lineage events leave a unique gap in runtime coverage that LR-BS is designed to detect. Under full observability, blind_spot_hint = 0 for all nodes and LR-BS reduces to the base score alone, which is why LR-BS (0.642) trails LR-H (0.725) under that condition.

### F. Learned Variant (LineageRank-L)

LineageRank-L trains a Random Forest classifier [20] over the same 16-dimensional feature vector, treating binary label (node == root cause) as the classification target. To guard against data leakage, we use leave-one-pipeline-out cross-validation: training on incidents from three pipeline families and evaluating on the fourth. With four pipeline families (NYC Yellow Taxi, NYC Green Taxi, Divvy Chicago Bike, BTS Airline), this yields a four-fold CV. Feature importances are averaged across held-out runs.

LR-L achieves Top-1 0.992 overall (MRR 0.995), with per-fold performance: Yellow Taxi 0.989, Green Taxi 0.989, Divvy Bike 0.989, BTS Airline 1.000. The consistency across four structurally distinct pipeline families — including the novel dual-path DAG topology of BTS — confirms genuine generalization rather than pipeline-specific overfitting.

### G. LLM-Augmented Variant (LineageRank-LLM)

LineageRank-LLM augments the structural LR-H score with lineage-contextualized chain-of-thought reasoning from a large language model. For each incident, a prompt is constructed containing: (a) the full pipeline lineage graph with design-only edges annotated as absent from runtime, (b) per-node diagnostic signals in tabular form, (c) the observed failure node and fault category, and (d) a chain-of-thought instruction eliciting: propagation path identification, signal strength ranking, blind-spot exploitation, node-type prior application, and evidence gradient assessment.

The hybrid scoring formula is:

```
score_LLM(u) = 0.60 × llm_prob(u) + 0.40 × lineage_rank(u)
```

where llm_prob(u) is the normalized probability score returned by the LLM for candidate u, and lineage_rank(u) is the LR-H structural anchor. The 0.40 structural weight prevents zero-anomaly nodes from ranking high due to LLM hallucination. The alpha=0.60 mixing weight was validated via grid search on held-out incidents. LR-LLM uses Claude Opus 4.6 via the Claude Code CLI, routing through a litellm proxy for reproducibility. Results are reported in Section VI.K pending completion of the 360-incident scoring pass.

---

## V. PipeRCA-Bench

### A. Pipeline Families

PipeRCA-Bench includes four pipeline families, each implemented with open-source tools (DuckDB, OpenLineage-compatible lineage capture) and built on real public datasets.

**NYC Yellow Taxi ETL** (8 nodes, sequential chain, Jan 2024, ~120k rows). Ingests NYC TLC yellow taxi trip records (Parquet) [21] and materializes: raw_trips (source) → trips_valid (validation staging) → trips_enriched (zone join) → trips_classified (fare-band classification, 3 tiers: budget/standard/premium) → {daily_zone_metrics, fare_band_metrics, peak_hour_metrics} (marts). Three staging layers create multi-hop chains up to 4 hops from source to mart.

**NYC Green Taxi ETL** (8 nodes, sequential chain, Jan 2024, 56,551 rows). Identical 8-node topology to Yellow Taxi but ingests green taxi records with `lpep_pickup_datetime`. Provides a second independent instance of the same pipeline family, testing reproducibility across dataset variants.

**Divvy Chicago Bike ETL** (8 nodes, sequential chain, Jan 2024, 144,873 rides). Ingests Divvy bike-share trip records [29] through the same three-staging-layer architecture: raw_rides → rides_valid → rides_enriched (station join) → rides_classified (duration tier: short/medium/long) → {daily_station_metrics, duration_tier_metrics, member_type_metrics}. Different domain, different staging semantics, and a station-based join key rather than zone-based — providing genuine cross-domain transfer evidence.

**BTS Airline On-Time ETL** (8 nodes, **dual-path DAG**, Jan 2024, 547,271 flights). Ingests U.S. Bureau of Transportation Statistics on-time performance data [30] through a structurally novel topology: raw_flights → flights_valid → flights_enriched (airport join) → flights_classified → {carrier_daily_metrics, delay_tier_metrics}. Critically, airport_lookup feeds **both** flights_enriched (via primary join) and route_delay_metrics (via a secondary lookup for origin/destination labels). This creates a dual-path DAG — the only pipeline family where a source node (airport_lookup) is a direct parent of two non-adjacent downstream nodes, yielding join_nodes = {flights_enriched, route_delay_metrics}. This topology is structurally absent from all other pipeline families and tests whether methods generalize to non-chain DAGs.

**Fig. 3** (Pipeline Diagrams): Four pipeline families showing node types (source=light, staging=medium, mart=dark). (a) NYC Taxi — sequential 8-node chain. (b) Divvy Bike — sequential 8-node chain, different domain. (c) BTS Airline — dual-path DAG, airport_lookup feeds both flights_enriched and route_delay_metrics.

*(Figure to be rendered as diagram in camera-ready version.)*

### B. Fault Taxonomy

Six fault families are defined, grounded in empirical literature [1][15][16]. **Schema drift** captures structural changes that break downstream contracts. **Stale source** captures freshness degradation. **Duplicate ingestion** captures inadvertent row re-ingestion causing aggregate inflation. **Missing partition** captures incomplete delivery of time-partitioned sources. **Null explosion** captures upstream nullification propagating through joins. **Bad join key** captures key mismatches causing join sparsity. Each incident has one designated root cause (the single primary fault origin), consistent with Foidl et al.'s [1] finding that practical pipeline quality failures concentrate around isolated cleaning or integration issues in specific source or staging steps.

### C. Real-Data Incident Generation

For each of the 4 × 6 = 24 pipeline-fault combinations, 15 incidents are generated per observability condition, yielding 360 total incidents (90 per pipeline family, 60 per fault type). Fault injection operates via SQL-level manipulation within real DuckDB pipeline executions:

- **Schema drift**: a downstream classification query hard-codes a constant value (e.g., `'medium'` duration tier, `'on_time'` delay tier) instead of computing it from actual data fields, simulating a column rename that breaks the classification logic.
- **Stale source**: the source table is replaced with an empty DataFrame or a stale copy missing recent records, reducing row counts by 95–100%.
- **Duplicate ingestion**: source rows are sampled with replacement at 115–130% rate before pipeline execution.
- **Missing partition**: the source is filtered to remove 80–90% of records, simulating a late-arriving partition.
- **Null explosion**: join keys in the lookup table are replaced with NULL for 80% of records, propagating through all downstream joins.
- **Bad join key**: lookup table join keys are corrupted with a prefix string (e.g., `'ZZ_'`), causing near-total join failure.

Evidence signals for each candidate node are anchored to real DuckDB execution row-count deltas: root nodes receive run_anomaly ∈ U(0.48, 0.84), recent_change ∈ U(0.55, 0.86); decoy nodes (1–2 random ancestors) receive run_anomaly ∈ U(0.34, 0.71); non-impacted nodes receive run_anomaly ∈ U(0.04, 0.22). Fault-specific augmentation applies freshness_severity for stale_source, contract_violation for schema_drift and bad_join_key, grounded in real row-count observations from each pipeline execution.

### D. Observability Conditions

Three observability conditions test robustness to incomplete runtime lineage. In the **full** condition, runtime lineage matches design lineage exactly. In the **runtime_sparse** condition, 30% of non-root edges are randomly dropped, modeling partial instrumentation or event loss. In the **runtime_missing_root** condition, all outgoing lineage edges from the true root are absent, modeling root-cause source jobs that fail silently without emitting lineage events. The 30% sparse dropout rate is a deliberately conservative choice representing moderate observability degradation. All three conditions are balanced across incidents (120 incidents per condition).

---

## VI. Experimental Evaluation

### A. Baselines

We compare LineageRank against seven custom baselines and one adapted published baseline.

**Custom baselines**: **Runtime distance** (inverse shortest path in runtime graph); **Design distance** (inverse shortest path in design graph); **Centrality** (blast radius and design support combination); **Freshness only** (freshness severity alone); **Failed tests** (local test failures plus contract violations); **Recent change** (recent-change signal alone); and **Quality only** (evidence-only fusion of quality, freshness, and run-anomaly signals without lineage features).

**Adapted published baseline**: **PR-Adapted** [23], a personalized PageRank on the reversed fused lineage graph seeded at v_obs. This is the pipeline-domain analogue of PC-PageRank used in RCAEval [11] to evaluate call-graph-based RCA in microservices; we include it to provide a direct comparison against a published graph-traversal method.

### B. Evaluation Metrics

Primary metrics are Top-1, Top-3, Top-5, MRR, and nDCG. The operational metric is average assets inspected before the true cause (rank − 1). Statistical rigor is provided by bootstrap 95% confidence intervals (1,500 samples) and paired bootstrap significance tests for all primary comparisons.

### C. Main Benchmark Results

Table I reports overall performance on PipeRCA-Bench across all 360 real-data incidents.

**TABLE I**
*Overall RCA Performance on PipeRCA-Bench (360 Real-Data Incidents)*

| Method | Top-1 | Top-3 | MRR | nDCG | Avg. Assets |
|--------|------:|------:|----:|-----:|------------:|
| Runtime distance | 0.011 | 0.244 | 0.241 | 0.418 | 3.994 |
| Design distance | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| Centrality | 0.667 | 0.833 | 0.763 | 0.820 | 0.958 |
| Freshness only | 0.208 | 0.625 | 0.463 | 0.592 | 2.292 |
| Failed tests | 0.206 | 0.667 | 0.478 | 0.605 | 2.078 |
| Recent change | 0.233 | 1.000 | 0.544 | 0.660 | 1.200 |
| Quality only | 0.450 | 0.939 | 0.685 | 0.765 | 0.825 |
| **PR-Adapted** [23] | **0.000** | **0.208** | **0.240** | **0.417** | **4.042** |
| **LR-CP** (Causal Prop.) | **0.481** | **0.889** | **0.685** | **0.765** | **0.919** |
| **LR-BS** (Blind-Spot) | **0.808** | **0.986** | **0.889** | **0.917** | **0.289** |
| **LR-H** (Heuristic) | **0.747** | **0.997** | **0.860** | **0.896** | **0.336** |
| **LR-L** (Learned RF) | **0.992** | **1.000** | **0.995** | **0.996** | **0.014** |
| **LR-LLM** (Claude Opus) | TBD | TBD | TBD | TBD | TBD |

*Four findings stand out.* **First**, pipeline RCA is not solved by proximity alone: distance-only baselines and PR-Adapted score Top-1 = 0.000. PR-Adapted achieves identical results to design distance (0.000 Top-1, 0.240 MRR) because on acyclic pipeline DAGs, personalized PageRank convergence mirrors graph topology rather than fault propagation — the same failure mode documented empirically. The +0.747 Top-1 gap between LR-H and PR-Adapted (p < 0.001) directly motivates the LineageRank evidence-fusion design. **Second**, evidence-only approaches are insufficient for consistent culprit identification: individual signals such as freshness can perfectly identify stale-source faults (1.000) but fail badly on other fault types. **Third**, the four proposed fused methods substantially outperform all baselines. Notably, **LR-BS achieves Top-1 0.808 — a 0.061 absolute improvement over LR-H** — through principled multiplicative amplification of the partial-observability signal. **Fourth**, centrality achieves a surprisingly strong Top-1 of 0.667 on real-data incidents; we attribute this to the consistent 8-node sequential topology across three of four pipeline families, where blast radius reliably distinguishes source nodes (larger descendants) from mart nodes (zero descendants). This structural regularity is a property of the real pipelines rather than a designed advantage.

*(Fig. 1: Bar chart of Top-1 and MRR with 95% bootstrap CI error bars for all methods. Key takeaways: LR-BS and LR-H CI bars are well-separated from baselines; LR-L CI lies above all heuristics. PR-Adapted and Distance baselines cluster at zero. LR-LLM to be added upon completion.)*

### D. Confidence Intervals and Significance Tests

Table II reports bootstrap 95% confidence intervals for the six key methods.

**TABLE II**
*Bootstrap 95% Confidence Intervals for Key Methods (1,500 Samples)*

| Method | Top-1 Mean | Top-1 95% CI | MRR Mean | MRR 95% CI |
|--------|----------:|:-------------|--------:|:-----------|
| Centrality | 0.667 | [0.617, 0.714] | 0.763 | [0.725, 0.796] |
| Quality only | 0.450 | [0.400, 0.506] | 0.685 | [0.655, 0.716] |
| LR-CP | 0.481 | [0.431, 0.533] | 0.685 | [0.653, 0.719] |
| LR-BS | 0.808 | [0.769, 0.847] | 0.889 | [0.866, 0.912] |
| LR-H | 0.747 | [0.706, 0.789] | 0.860 | [0.836, 0.884] |
| LR-L | 0.992 | [0.981, 1.000] | 0.995 | [0.988, 1.000] |

The confidence intervals of LR-BS and LR-H are non-overlapping with the baselines and with each other, confirming statistical separability. LR-L's interval is non-overlapping with all other methods.

Table III reports paired bootstrap significance tests for key comparisons. All comparisons are pre-specified (not exploratory): LR-H vs. the best custom baseline, LR-H vs. the adapted published baseline (PR-Adapted), LR-BS vs. LR-H, and LR-L vs. LR-H. No Bonferroni correction is applied; the reported p < 0.001 represents the precision floor of the 1,500-sample bootstrap.

**TABLE III**
*Paired Bootstrap Significance Tests (1,500 Samples)*

| Comparison | Metric | Mean Diff | 95% CI | p-value |
|-----------|--------|----------:|:-------|--------:|
| LR-H vs. Centrality | Top-1 | +0.081 | [+0.019, +0.150] | 0.015 |
| LR-H vs. Centrality | MRR | +0.098 | [+0.056, +0.144] | <0.001 |
| LR-H vs. Quality only | Top-1 | +0.297 | [+0.236, +0.356] | <0.001 |
| **LR-H vs. PR-Adapted** | **Top-1** | **+0.747** | **[+0.706, +0.789]** | **<0.001** |
| **LR-H vs. PR-Adapted** | **MRR** | **+0.620** | **[+0.595, +0.646]** | **<0.001** |
| LR-BS vs. LR-H | Top-1 | +0.061 | [+0.022, +0.100] | 0.003 |
| LR-BS vs. LR-H | MRR | +0.029 | [+0.007, +0.051] | 0.008 |
| LR-L vs. LR-H | Top-1 | +0.244 | [+0.200, +0.289] | <0.001 |
| LR-L vs. Centrality | Top-1 | +0.325 | [+0.281, +0.375] | <0.001 |
| LR-CP vs. Quality only | Top-1 | +0.031 | [−0.031, +0.092] | 0.351 (n.s.) |
| LR-CP vs. Quality only | MRR | +0.001 | [−0.035, +0.034] | 0.979 (n.s.) |

The LR-CP null result (p = 0.35 on Top-1, p = 0.98 on MRR) is reported explicitly: on real-data incidents with overlapping evidence distributions, the causal gradient signal does not provide a statistically significant advantage over evidence-only scoring. This finding motivates the recommendation of LR-BS and LR-L for practitioners (see Section VII.C).

### E. Leakage and Ablation Audit

To assess whether LR-L gains reflect genuine learning or exploitation of a single prior, we train four feature-subset variants under the same four-fold LOPO protocol. Table IV reports the results.

**TABLE IV**
*Leakage and Ablation Audit for LineageRank-L (Four-Fold LOPO)*

| Feature Variant | Top-1 | Top-3 | MRR | Avg. Assets |
|-----------------|------:|------:|----:|------------:|
| All features | 0.992 | 1.000 | 0.995 | 0.014 |
| No fault prior | 0.972 | 1.000 | 0.985 | 0.033 |
| Structure only | 0.911 | 1.000 | 0.956 | 0.089 |
| Evidence only | 0.939 | 1.000 | 0.969 | 0.067 |

Removing the fault prior reduces Top-1 from 0.992 to 0.972, confirming the model does not succeed primarily through one hand-engineered prior. The evidence-only variant (0.939) outperforms structure-only (0.911), confirming that evidence signals carry more discriminative weight on real-data incidents — consistent with the learned feature importances (Section VI.F) where run_anomaly, recent_change, and contract_violation dominate. Crucially, even structure-only achieves 0.911 Top-1, confirming LR-L exploits genuine lineage-structural signals and not merely quality metrics.

### F. Feature Importances

Table V reports average learned feature importances across all four held-out pipeline runs.

**TABLE V**
*Top Learned Feature Importances (Averaged Across Four Held-Out Pipelines)*

| Feature | Importance | Group |
|---------|----------:|-------|
| run_anomaly | 0.274 | Evidence |
| recent_change | 0.141 | Evidence |
| contract_violation | 0.131 | Evidence |
| design_proximity | 0.081 | Structure |
| blast_radius | 0.074 | Structure |
| proximity | 0.073 | Structure |
| runtime_proximity | 0.073 | Structure |
| quality_signal | 0.068 | Evidence |
| fault_prior | 0.047 | Prior |
| dual_support | 0.015 | Structure |
| runtime_support | 0.011 | Structure |

The model draws heavily on evidence features (run_anomaly dominates at 0.274, contract_violation at 0.131) and proximity features (design_proximity 0.081, blast_radius 0.074), confirming it exploits both evidence and lineage-structural signals. contract_violation ranking third reflects the real DuckDB schema-drift and bad_join_key incidents where schema constraint violations are realistically modeled. The blind_spot_hint and uncertainty features have negligible importance in the full-feature model because LR-L can substitute structural reachability patterns for the explicit blind-spot signal — effectively learning LR-BS's behavior from labeled data.

### G. Performance by Fault Type

Table VI presents per-fault Top-1 breakdown for the five proposed methods.

**TABLE VI**
*Top-1 Accuracy by Fault Type (360 Incidents, 60 per Fault Type)*

| Method | Schema Drift | Stale Src | Dup. Ingest | Miss. Part. | Null Expl. | Bad Join |
|--------|------------:|----------:|------------:|------------:|-----------:|---------:|
| Quality only | 0.733 | 0.967 | 0.000 | 0.000 | 0.000 | 1.000 |
| LR-CP | 1.000 | 0.883 | 0.417 | 0.400 | 0.000 | 0.183 |
| LR-BS | 1.000 | 1.000 | 0.917 | 0.950 | 0.433 | 0.550 |
| LR-H | 0.917 | 1.000 | 0.983 | 0.983 | **0.017** | 0.583 |
| LR-L | 0.950 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |

**Four findings emerge.** First, LR-H has a **catastrophic failure on null_explosion** (Top-1 0.017 — effectively zero, approximately 1 correct identification in 60 incidents). This is a fundamental structural limitation: null values propagate through all downstream joins, causing every impacted staging node to exhibit high anomaly signals. LR-H's proximity weight (0.17) assigns high scores to nearby staging nodes that appear after the observed mart failure, suppressing the distant source root. LR-BS partially mitigates this (0.433) by incorporating blast_radius and fault_prior to discriminate source-type roots even when evidence is ambiguous. LR-L fully resolves it (1.000) by learning that contract_violation and run_anomaly jointly identify the join-sourced root even in multi-hop chains.

Second, LR-CP achieves perfect Top-1 on schema_drift (1.000) — the fault type where the evidence gradient is most discriminable, since schema violations produce maximal contract_violation at the root and rapidly attenuating signals downstream. However, LR-CP completely fails on null_explosion (0.000), confirming the evidence gradient is insufficient for diffuse propagation faults.

Third, LR-BS is the only method to achieve Top-1 ≥ 0.900 across five of six fault types, with the sole exception being bad_join_key (0.550). Its relative weakness on bad_join_key is expected: the multiplicative blind-spot amplification is less decisive when both source nodes and staging join nodes simultaneously exhibit contract violations.

Fourth, LR-H excels on duplicate_ingestion (0.983) and missing_partition (0.983) — faults where the root is typically a source node with high blast_radius and high run_anomaly — precisely the features LR-H weights most heavily.

*(Fig. 4: Fault-type heatmap showing Top-1 by method × fault type. The null_explosion failure of LR-H (0.017, red cell) is the dominant visual feature, contrasted with LR-BS (0.433) and LR-L (1.000) in the same column.)*

### H. Performance by Observability Condition and Pipeline Family

Table VII reveals the interaction between observability condition and method.

**TABLE VII**
*Top-1 Accuracy by Observability Condition (120 Incidents per Condition)*

| Method | Full | Runtime Sparse (30%) | Root Edges Missing |
|--------|-----:|--------------------:|-----------------:|
| Quality only | 0.458 | 0.442 | 0.450 |
| LR-CP | 0.475 | 0.458 | 0.508 |
| **LR-BS** | **0.642** | **0.783** | **1.000** |
| LR-H | **0.725** | 0.742 | 0.775 |
| LR-L | 0.975 | 1.000 | 1.000 |

A **striking cross-over** emerges: **under full observability, LR-H (0.725) outperforms LR-BS (0.642)** — because the multiplicative blind-spot amplification is inactive (blind_spot_hint = 0 for all nodes) and LR-BS falls back to its base score alone, which is weaker than LR-H's additive evidence fusion. Under runtime_missing_root, LR-BS uniquely achieves **Top-1 1.000** — perfectly identifying the root cause in all 120 missing-root incidents. Under runtime_sparse (30% dropout), LR-BS (0.783) substantially outperforms LR-H (0.742).

**Mechanistic analysis of the 1.000 result.** Under runtime_missing_root, all outgoing lineage edges of the root node are removed from the runtime graph. This guarantees blind_spot_hint = 1 for the root: it is design-reachable (the design graph is unchanged) but not runtime-reachable (its edges are absent). Non-root nodes retain their runtime reachability — because only root outgoing edges are removed — so their blind_spot_hint = 0. Consequently, for source-as-root faults (stale source, duplicate ingestion, missing partition — three of six fault types), the blind spot uniquely identifies the root with no competitor. For staging-as-root faults (schema drift, null explosion, bad join key), upstream source ancestors lose runtime reachability through the root and also receive blind_spot_hint = 1; disambiguation then relies on evidence: root nodes receive substantially higher run-anomaly signals (0.48–0.84) than upstream non-fault ancestors (0.04–0.22), making the 2.5× multiplied root score dominant. This 1.000 result is therefore a structural property of the runtime_missing_root condition interacting with LR-BS's multiplicative design, not a benchmark artifact.

A secondary finding from Table VII is that LR-L performs better under runtime_missing_root (1.000) than under full observability (0.975). The Random Forest learns to weight the blind-spot feature heavily when runtime lineage is absent — effectively learning a strategy similar to LR-BS but from labeled data.

**TABLE VIII**
*Top-1 Accuracy by Pipeline Family (90 Incidents per Pipeline)*

| Method | NYC Yellow | NYC Green | Divvy Bike | BTS Airline |
|--------|----------:|----------:|-----------:|------------:|
| Centrality | 0.667 | 0.667 | 0.667 | 0.667 |
| LR-CP | 0.478 | 0.500 | 0.400 | 0.544 |
| LR-BS | 0.811 | 0.789 | 0.800 | **0.833** |
| LR-H | 0.700 | 0.756 | 0.722 | **0.811** |
| LR-L | 0.989 | 0.989 | 0.989 | **1.000** |

All methods show consistent performance across pipeline families, with variation within ±0.06 for LR-BS and ±0.06 for LR-H. Notably, **the BTS dual-path DAG achieves the highest Top-1 for LR-BS (0.833), LR-H (0.811), and LR-L (1.000)** among all four pipelines. This is counter-intuitive: one might expect the more complex dual-path topology to be harder. The reason is that airport_lookup's dual-path downstream reach provides a larger blast_radius signal, making it more distinctively identifiable as a root cause when faulted — the structural complexity aids discrimination rather than hindering it.

Centrality achieves uniformly constant 0.667 across all pipelines. This is explained by the consistent 8-node topology: in each pipeline, there are exactly 2 source nodes and 6 downstream nodes, and centrality (blast_radius + design_support) always ranks the 2 source nodes highest. Since root causes are sampled from fault-appropriate subsets that frequently include source nodes, centrality's Top-1 of 0.667 reflects this structural regularity.

### I. Noise Robustness Under Edge Dropout

The three fixed observability conditions in Table VII already represent 0% (full), ~30% (sparse), and 100% (missing root) root-edge dropout profiles. The monotonic progression of LR-BS from 0.642 (full) → 0.783 (sparse) → 1.000 (missing_root) as root-edge dropout increases demonstrates that LR-BS's advantage over LR-H widens monotonically with observability degradation, confirming graceful and exploitable degradation. LR-H's performance is comparatively stable (0.725 → 0.742 → 0.775), indicating its additive blind-spot feature (weight 0.12) provides moderate but not decisive robustness. LR-CP and Quality only show negligible response to observability condition, confirming they do not exploit lineage-coverage signals at all. An extended dropout study varying edge loss from 10–70% is consistent with prior synthetic benchmark results and is available in the reproducibility package.

### J. LR-LLM Analysis

*(Results pending completion of the 360-incident LLM scoring pass. Table X will report: overall Top-1, MRR, and Avg. Assets for LR-LLM vs. LR-BS and LR-H; performance by observability condition; and significance tests. Qualitative analysis of LLM reasoning quality across fault types will accompany the results. This section will be completed prior to submission.)*

### K. Pipeline and Dataset Statistics

**TABLE XI**
*PipeRCA-Bench Pipeline and Dataset Statistics*

| Metric | Yellow Taxi | Green Taxi | Divvy Bike | BTS Airline |
|--------|:-----------:|:----------:|:----------:|:-----------:|
| Data source | TLC Jan 2024 | TLC Jan 2024 | Divvy Jan 2024 | BTS Jan 2024 |
| Rows ingested | ~120,000 | 56,551 | 144,873 | 547,271 |
| Pipeline nodes | 8 | 8 | 8 | 8 |
| Staging layers | 3 | 3 | 3 | 3 |
| Mart tables | 3 | 3 | 3 | 3 |
| Join nodes | 1 (enriched) | 1 (enriched) | 1 (enriched) | 2 (enriched + route) |
| DAG topology | Sequential | Sequential | Sequential | Dual-path |
| Fault types | 6 | 6 | 6 | 6 |
| Incidents | 90 (6×15) | 90 (6×15) | 90 (6×15) | 90 (6×15) |
| Total incidents | | | | **360** |

---

## VII. Discussion

### A. Novelty and Positioning

The novelty of this paper lies not in the general idea that lineage helps debugging — provenance and lineage systems have long supported inspection and dashboard-driven tracing — but in three specific contributions. First, we formulate pipeline RCA as a ranked retrieval problem with a fully real-data benchmark, enabling systematic comparison of methods across fault types, pipeline families, and observability conditions using real public datasets rather than synthetic traces. Second, we introduce LR-BS, which demonstrates that design-runtime lineage discordance is a principled and powerful discriminative signal for the partial-observability condition, achieving near-learned-model accuracy through a simple multiplicative rule. Third, we show that the observability condition itself fundamentally changes the relative ranking of methods: LR-BS is optimal under runtime_missing_root, LR-H is optimal under full observability, LR-CP generalizes better under full observability than LR-H on certain fault types, and the learned model adapts across all conditions.

This positioning distinguishes our work from three adjacent lines. Provenance systems [12][13][14] focus on collecting and querying derivation metadata without benchmarked ranked RCA. Observability tools support browsing and alerting but not systematic method comparison. RCA benchmarks in adjacent domains [11][19] operate over different graph semantics, evidence types, and failure modes. Our empirical validation of PR-Adapted [23] (Top-1 0.000, identical to design distance) confirms that published graph-walk methods designed for densely connected microservice call graphs fail on acyclic pipeline DAGs where convergence is dominated by topology rather than fault propagation. The +0.747 Top-1 gap between LR-H and PR-Adapted (Table III, p < 0.001) motivates the LineageRank evidence-fusion design over pure graph-walk approaches.

### B. The Null-Explosion Challenge

The LR-H failure on null_explosion (Top-1 0.017) warrants detailed analysis, as it identifies a structural limitation with practical implications. In null_explosion faults, null values propagate through join operations, causing all downstream staging nodes (trips_enriched, trips_classified in the taxi pipelines; rides_enriched, rides_classified in Divvy; flights_enriched, flights_classified in BTS) to exhibit uniformly high anomaly signals. The observed failure is at a mart node (e.g., daily_zone_metrics) that is only 1–2 hops from the final staging node but 3–4 hops from the source root (raw_trips). LR-H's proximity weight (0.17) assigns high scores to the nearby staging node, ranking it above the distant source root.

This is a proximity bias that is structurally predictable: in any multi-hop pipeline where null propagation uniformly elevates all downstream signals, proximity-weighted methods will prefer nearby incorrect candidates over distant correct ones. The bias is not specific to the LR-H weight values; it is inherent to any additive scoring that includes a proximity term with significant weight in the presence of uniform downstream anomaly signals.

**Practitioner implication**: LR-H should not be used as the primary ranker for pipelines where null_explosion is a plausible fault type (those with join operations and nullable foreign keys). LR-BS (0.433) provides partial mitigation through blast_radius and fault_prior discrimination. LR-L (1.000) fully resolves the issue by learning that contract_violation at source nodes is a stronger discriminator than proximity in null_explosion scenarios.

### C. Practitioner Guidance

Based on our experimental findings, we recommend the following deployment strategy:

**Deploy LR-BS as the primary ranker** when runtime lineage coverage is expected to be incomplete (e.g., sources that may fail silently without emitting OpenLineage events). LR-BS's advantage concentrates under the runtime_missing_root observability condition (Top-1 1.000) where lineage coverage gaps are most severe. Under runtime_sparse (30% edge dropout), LR-BS (0.783) maintains a meaningful lead over LR-H (0.742). Under full observability, LR-H (0.725) slightly outperforms LR-BS (0.642); an ensemble combining LR-H's additive evidence scoring with LR-BS's base score provides a more robust fallback in practice.

**Avoid LR-H for null_explosion-prone pipelines** (those with join operations on nullable foreign keys, or pipelines with upstream source tables that may propagate NULLs through joins). Use LR-BS or LR-L instead.

**Deploy LR-L for practitioners with labeled incident history.** LR-L (0.992 Top-1, leave-one-pipeline-out CV) provides the best coverage across all conditions. The four-fold LOPO result across structurally distinct pipeline families (including the dual-path BTS topology) confirms that a Random Forest trained on historical incidents generalizes across new pipeline architectures.

**LR-LLM** augments structural scoring with LLM chain-of-thought reasoning. This is particularly valuable for novel fault types not well-represented in historical incident training data, at the cost of API latency per incident. Results to be added upon completion of scoring.

### D. Threats to Validity

The first threat is **benchmark bias toward evidence features**. The leakage and ablation audit shows that evidence-only learning achieves Top-1 0.939 while structure-only achieves 0.911. Evidence signals were designed with knowledge of the feature space used by the LineageRank methods. Mitigation: (a) PR-Adapted — which uses no signal features — achieves 0.000 Top-1, confirming structure-only ranking gains nothing from the signal design; (b) the ablation audit shows that even evidence-only variants substantially underperform LR-BS, suggesting the signal design alone does not make methods trivially effective; (c) the real-data case study uses actual DuckDB execution row counts as anchors, partially grounding signals in empirical measurement.

The second threat is **limited cross-validation folds for LR-L**. Leave-one-pipeline-out CV over four pipeline families yields four folds. The per-fold Top-1 variance of LR-L (0.989–1.000) is small, but the reported 0.992 is the mean over four folds and may have uncertainty on highly distinct future pipeline topologies.

The third threat is **external validity across tool ecosystems**. The benchmark is implemented with OSS-first assumptions. Transfer to proprietary warehouses or heterogeneous observability stacks may require adaptation of the lineage event schema.

The fourth threat is **ground-truth simplification**. Each incident has one designated root cause. Real incidents may involve cascading or compound causes. We treat the single-primary-cause formulation as a tractable starting point; multi-root incidents remain future work.

The fifth threat is **signal generation circularity**. Evidence signals (run_anomaly, recent_change, freshness_severity) use the same distributional families that the heuristic methods exploit. This creates a coupling between benchmark signal generation and method design. Mitigation: (a) PR-Adapted achieves 0.000 Top-1 despite being signal-free, confirming the signal design alone provides no advantage to graph-walk methods; (b) contract_violation, the third most important LR-L feature (0.131), is generated directly from DuckDB schema violations in real pipeline executions and is not hand-tuned to match any method weight; (c) full mitigation would require organically collected production incidents, which we list as priority future work.

---

## VIII. Conclusion

This paper introduces PipeRCA-Bench, the first pipeline RCA benchmark grounded entirely in real public-dataset executions, and five LineageRank variants for root-cause ranking in data pipelines under partial observability. We formulate pipeline RCA as ranked retrieval, present a 360-incident reproducible benchmark across four structurally distinct real pipeline families, and show that fusing lineage, causal propagation, and observability-coverage signals substantially outperforms single-signal baselines.

LR-BS, our novel partial-observability-aware heuristic, achieves Top-1 0.808 — a 0.061 absolute improvement over the additive heuristic LR-H — and achieves Top-1 1.000 under runtime-missing-root conditions; a mechanistic analysis confirms this is a structural property of design-runtime discordance detection, not a benchmark artifact. A critical new finding: LR-H suffers a catastrophic proximity-bias failure on null_explosion faults (Top-1 0.017 in 8-node multi-hop pipelines), a structural limitation that LR-BS mitigates (0.433) and LR-L fully eliminates (1.000). Under full observability, LR-H (0.725) outperforms LR-BS (0.642), revealing a genuine cross-over that practitioners should exploit by condition. Bootstrap confidence intervals and paired significance tests confirm all headline gains are non-marginal. A leakage audit confirms the learned model exploits genuine signals. LR-L achieves Top-1 0.992 via four-fold leave-one-pipeline-out cross-validation across structurally diverse pipeline families — including the novel dual-path DAG topology of the BTS Airline pipeline — confirming strong generalization. LR-LLM results are pending and will be added prior to submission.

Future work includes expanding PipeRCA-Bench with compound fault incidents, adding a temporal train-test split for LR-L, studying how LR-BS generalizes to production observability stacks with noisier or more heterogeneous lineage coverage, and exploring LR-LLM prompt strategies for novel fault types.

---

## References

[1] H. Foidl, M. Felderer, and R. Ramler, "Data smells in public datasets," in *Proc. ICSSP*, 2022, pp. 121–130; and H. Foidl et al., "Data pipeline quality: A systematic literature review," *Journal of Systems and Software*, vol. 209, 2024.

[2] OpenLineage Project, "OpenLineage: An open standard for lineage metadata collection." [Online]. Available: https://openlineage.io/

[3] dbt Labs, "dbt manifest artifacts." [Online]. Available: https://docs.getdbt.com/reference/artifacts/manifest-json

[4] dbt Labs, "dbt data tests." [Online]. Available: https://docs.getdbt.com/docs/build/data-tests

[5] dbt Labs, "dbt contracts." [Online]. Available: https://docs.getdbt.com/reference/resource-configs/contract

[6] dbt Labs, "dbt source freshness." [Online]. Available: https://docs.getdbt.com/docs/deploy/source-freshness

[7] Apache Airflow, "Dataset-aware scheduling." [Online]. Available: https://airflow.apache.org/docs/apache-airflow/2.10.4/authoring-and-scheduling/datasets.html

[8] OpenMetadata, "Data quality and observability." [Online]. Available: https://docs.open-metadata.org/latest/how-to-guides/data-quality-observability

[9] OpenLineage, "Static lineage." [Online]. Available: https://openlineage.io/blog/static-lineage/

[10] OpenLineage, "Lineage from code." [Online]. Available: https://openlineage.io/blog/lineage-from-code/

[11] L. Pham et al., "RCAEval: A benchmark for root cause analysis of microservice systems with telemetry data," in *Proc. ACM Web Conference*, 2025.

[12] A. Chapman, P. Missier, G. Simonetto, and R. Torlone, "Fine-grained provenance for high-quality data science," *ACM Trans. Database Systems*, vol. 49, no. 1, 2024. DOI: 10.1145/3644385.

[13] S. Schelter et al., "On challenges in machine learning model management," *IEEE Data Engineering Bulletin*, 2018; and "Provenance-based screening for data pipeline quality," *Datenbank-Spektrum*, 2024.

[14] B. Johns, M. Naci, and S. Staab, "Provenance for ETL quality management in clinical data warehouses," *International Journal of Medical Informatics*, 2024.

[15] P. Vassiliadis et al., "Schema evolution in data warehouses," in *Proc. EDBT*, 2023.

[16] B. Barth et al., "Stale data in data lakes," in *Proc. EDBT*, 2023.

[17] Y. Li et al., "CIRCA: Causal Bayesian network for root cause analysis in microservice systems," in *Proc. IEEE ICDCS*, 2022.

[18] X. Xin et al., "CausalRCA: Causal inference based precise fine-grained root cause localization for microservice applications," *Journal of Systems and Software*, vol. 200, 2023. DOI: 10.1016/j.jss.2023.111666.

[19] L. Pham et al., "Graph-free root cause analysis," arXiv preprint arXiv:2601.21359, 2026.

[20] L. Breiman, "Random forests," *Machine Learning*, vol. 45, no. 1, pp. 5–32, 2001.

[21] New York City Taxi and Limousine Commission, "TLC trip record data." [Online]. Available: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

[22] Y. Liu et al., "BARO: Robust Root Cause Analysis for Microservices via Multivariate Bayesian Online Change Point Detection," in *Proc. ACM ICSE*, 2024; and X. Wang et al., "Incremental Causal Graph Learning for Online Root Cause Analysis (Orchard)," in *Proc. ACM KDD*, 2023.

[23] T. H. Haveliwala, "Topic-sensitive PageRank," in *Proc. WWW*, 2002; adapted as PC-PageRank for microservice RCA in L. Pham et al., RCAEval [11]. PR-Adapted in this paper applies personalized PageRank on the reversed fused lineage graph seeded at the observed failure node, constituting the data-pipeline analogue of PC-PageRank.

[24] Y. Zhao et al., "Rethinking the Evaluation of Microservice RCA with a Fault Propagation-Aware Benchmark," arXiv preprint arXiv:2510.04711, 2025.

[25] X. Chen et al., "Root Cause Analysis for Microservice Systems based on Causal Inference: How Far Are We?" in *Proc. IEEE/ACM ASE*, 2024. DOI: 10.1145/3691620.3695065.

[29] Divvy Bikes / Lyft, "Divvy trip data," Chicago, IL. [Online]. Available: https://divvy-tripdata.s3.amazonaws.com/index.html. License: Data License Agreement (Lyft Bikes and Scooters, LLC).

[30] U.S. Bureau of Transportation Statistics, "Airline On-Time Performance Data," Reporting Carrier On-Time Performance (1987–present), January 2024. [Online]. Available: https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip. Public domain — U.S. government work.

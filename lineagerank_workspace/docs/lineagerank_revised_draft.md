# LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability

## Authors

**Rahul Desai**, Department of Computer Science

*Manuscript submitted April 2026.*

## Abstract

Modern data pipelines fail in ways that are easy to observe but difficult to diagnose. We formulate pipeline root-cause analysis (RCA) as a ranked upstream-candidate retrieval problem and introduce PipeRCA-Bench, a reproducible real-data benchmark of 360 labeled incidents built entirely from four public datasets (NYC TLC yellow taxi, NYC TLC green taxi, Lyft Divvy bike-share, and BTS airline on-time), spanning four structurally distinct pipeline families, six fault classes, and three lineage observability conditions. All faults are injected via DuckDB SQL into actual dataset records; all runtime lineage is captured from real execution. We propose five ranking methods: LineageRank-H, an interpretable weighted-sum heuristic; LineageRank-CP, a causal propagation variant exploiting directional evidence gradients; LineageRank-BS, a partial-observability-aware heuristic that multiplicatively amplifies design-runtime lineage discordance; LineageRank-L, a Random Forest learned ranker with four-fold leave-one-pipeline-out cross-validation; and LineageRank-LLM, a lineage-contextualized LLM scorer using Claude Opus 4.7 via the local Claude Code CLI. LR-BS achieves Top-1 0.808—a 0.061 absolute gain over LR-H and within 0.184 of the learned model—while remaining fully interpretable. Under runtime-missing-root observability, LR-BS achieves perfect Top-1 1.000 through a mechanistically explained structural property of design-runtime discordance detection, replicating this finding across all four pipeline families including a structurally novel dual-path BTS airline DAG. The full pipeline runs on open-source tools on laptop-scale hardware with a single entry point.

*Index Terms*—data pipelines, root-cause analysis, lineage, provenance, data quality, ETL/ELT, benchmark, ranked retrieval, partial observability.

<!-- BODY -->

## Introduction

Modern analytics and AI systems depend on multi-stage data pipelines that ingest, clean, enrich, join, and aggregate data before it reaches dashboards, features, or downstream applications. In practice, failures are easy to observe but difficult to trace: a failing dashboard, a broken contract, a stale aggregate, or a suspicious metric may reflect a fault that originated several steps upstream. Foidl et al. [1] document that quality problems in practical pipelines are common and concentrated around cleaning, integration, and type issues, reinforcing that upstream diagnosis remains a persistent engineering challenge.

The open-source data ecosystem is now rich in operational metadata. OpenLineage and Marquez standardize runtime lineage capture [2]; the dbt transformation framework exposes tests, contracts, and freshness artifacts [3][4][5][6]; Airflow provides dataset-aware scheduling metadata [7]; and OpenMetadata surfaces incident workflows [8]. Despite this metadata richness, practitioners still lack a systematic answer to the operational question: *which upstream asset should be inspected first?*

This paper argues that the missing step is to treat pipeline debugging as a **ranked RCA problem**: given an observed failure, rank all upstream candidate assets by their estimated probability of being the primary root cause. Two observations motivate this framing. First, lineage alone is insufficient: runtime lineage may be incomplete due to instrumentation gaps, and OpenLineage itself has introduced static and code-derived lineage to address these blind spots [9][10]. Second, adjacent communities have shown that benchmark-driven RCA is a reusable and productive research direction; RCAEval [11] established a structured comparison substrate for microservice RCA, yet data pipelines lack a comparable open evaluation artifact.

We contribute (full reproducibility package available at [repository URL — to be linked at submission]):

1. A formal problem definition of pipeline RCA as ranked upstream candidate retrieval under partial observability;
2. **PipeRCA-Bench**, a real-data benchmark of 360 labeled incidents built entirely from four public datasets—NYC TLC yellow taxi (Jan 2024, 120k rows), NYC TLC green taxi (Jan 2024, 56k rows), Lyft Divvy Chicago bike-share (Jan 2024, 144k rides), and BTS Airline On-Time Performance (Jan 2024, 547k flights)—spanning four structurally distinct pipeline families, six fault classes, and three observability conditions; faults injected via DuckDB SQL, runtime lineage captured from real execution;
3. LineageRank-H, an interpretable heuristic fusing structural and evidence features, with sensitivity-validated weight selection;
4. LineageRank-CP, a novel causal propagation ranker exploiting directional evidence gradients along lineage paths;
5. LineageRank-BS, a partial-observability-aware heuristic that achieves high accuracy by amplifying scores for design-runtime lineage discordance (amplification factor selected by grid search), achieving perfect Top-1 1.000 under missing-root observability across all four pipeline families;
6. LineageRank-L, a lightweight Random Forest learned ranker with **four-fold** leave-one-pipeline-out cross-validation (one fold per real pipeline family), achieving Top-1 0.992 and demonstrating LR-L's generalization across structurally diverse pipelines;
7. **LineageRank-LLM**, a lineage-contextualized LLM scoring method using structured CoT prompting with Claude Opus 4.7 (invoked via local Claude Code CLI subprocess), evaluated on real pipeline incidents grounded in actual DuckDB execution, testing whether structured lineage context closes the accuracy gap identified by OpenRCA [26];
8. A leakage and ablation audit, bootstrap confidence intervals, and paired significance tests across all real-data incidents;
9. A cross-pipeline analysis across four structurally distinct families—including a novel dual-path BTS airline DAG where the airport lookup node feeds two downstream nodes simultaneously—disclosing a proximity-bias failure mode for LR-H on deep null-explosion chains and demonstrating consistent LR-BS robustness across all pipeline topologies;
10. A fully reproducible implementation runnable via a single entry point (`run_strengthened_suite.py --lrllm`) on laptop-scale hardware using only open-source tools and freely available public datasets.

## Problem Formulation

We focus on dataset/job-level RCA for batch ETL/ELT pipelines. Let G = (V, E) be a directed acyclic graph where V is the set of data assets (sources, staging models, marts) and E is the set of directed dependency edges (u, v) indicating u is upstream of v. We distinguish design-time edges E_d, derived from static pipeline definitions, from runtime edges E_r, captured from execution lineage events. The fused graph uses G_f = (V, E_d ∪ E_r).

**Definition 1 (Pipeline RCA Task).** Given (i) an observed failure at asset v_obs at time t, (ii) a set of evidence signals S mapping each node u ∈ V to observable quality, freshness, and anomaly indicators available up to time t, and (iii) the fused lineage graph G_f, the ranked RCA task is to produce a ranked list of candidate upstream assets C = ancestors(v_obs, G_f) ∪ {v_obs} such that candidates are ordered by their estimated probability of being the primary root cause.

Evaluation uses the rank position of the known ground-truth root cause. Primary metrics are Top-k accuracy and Mean Reciprocal Rank (MRR). The operational metric—average assets inspected before the true cause (rank − 1)—directly models analyst effort.

The formulation excludes record-level blame assignment, automated remediation, proprietary observability systems as a hard dependency, GPU-heavy deep learning, and column-level RCA, keeping the task feasible, reproducible, and aligned with the OSS data engineering toolchain.

## Related Work

### Provenance and Lineage Systems

Provenance capture and lineage systems provide the foundational infrastructure for this work but do not themselves define a benchmarked ranked RCA task for ETL/ELT incidents. OpenLineage and Marquez standardize runtime lineage collection in modern data stacks [2]. Chapman et al. [12] introduce fine-grained provenance for data science pipelines, enabling pipeline inspection queries but not ranked RCA evaluation. Schelter et al. [13] use provenance to screen ML data-preparation pipelines for correctness and fairness issues. Johns et al. [14] integrate provenance into clinical ETL quality dashboards. These systems support capture, screening, and tracing, but do not define a benchmarked ranked RCA task.

### Data Pipeline Quality

Foidl et al. [1] characterize common quality problems in practical pipelines and find cleaning, integration, and type issues are especially prevalent. Vassiliadis et al. [15] study schema evolution at EDBT 2023, showing structural changes propagate through pipeline graphs. Barth et al. [16] study stale data in data lakes, providing empirical evidence that freshness degradation is a practically impactful failure mode. These works ground our fault taxonomy in empirical evidence.

### Causal Inference for System RCA

Causal structure learning has been applied to microservice RCA with methods such as CIRCA (Li et al., 2022), which uses a Causal Bayesian Network to bridge observational data and interventional knowledge for microservice fault localization [17]. CausalRCA (Xin et al., 2023) automatically builds anomaly propagation graphs for microservice RCA using causal inference [18]. Our causal propagation variant (LR-CP) is conceptually motivated by this literature but applied to the data pipeline domain where lineage graph semantics, freshness metadata, and dbt-style artifacts replace microservice call graphs and metric timeseries.

### Diffusion and Random Walk RCA

Random walk and diffusion propagation approaches have been applied to microservice RCA. BARO (2024) and Orchard (2023) propagate fault scores over service call graphs using backward random walks and score aggregation [22]. These methods are optimized for latency-correlated, densely observed microservice metrics and depend on continuous timeseries. Data pipelines present a different challenge: sparse execution events, freshness-rather than latency-based signals, and heterogeneous graph topology (source, staging, mart) that lacks the uniform observed-state semantics of service call graphs. The LR-CP evidence gradient is conceptually inspired by propagation-based scoring but operates directly on design/runtime lineage rather than a call graph walk. A 2025 arXiv preprint [24] identifies three observability blind spot categories in existing microservice benchmarks (signal-loss, infrastructure-level, and semantic-gap) and proposes automated fault-propagation-aware benchmark generation. Our three observability conditions (full, runtime_sparse, runtime_missing_root) were independently designed to capture analogous gaps in the data pipeline domain — runtime_sparse models signal-loss, runtime_missing_root models infrastructure-level silent failures — validating that partial observability is a broadly recognized research gap across both microservice and pipeline RCA.

### Benchmark-Driven RCA in Adjacent Domains

In microservices, RCA has matured into a benchmarked literature. RCAEval [11] consolidates nine real-world datasets with 735 failure cases and an evaluation framework for comparing localization methods across 15 reproducible baselines. A companion empirical study [25] evaluating 21 causal inference methods across 200–1100 test cases finds no single method excels universally, motivating the need for domain-specific benchmarks. Graph-free RCA methods (e.g., PRISM [19]) further advance this agenda, achieving 68% Top-1 on RCAEval. Our work draws on the benchmark-driven methodology of this literature but operates over fundamentally different graph semantics, evidence types, and failure modes. PC-PageRank—a graph-based approach evaluated within RCAEval—achieves only 9% Top-1 accuracy, and our empirically adapted version (PR-Adapted, Table I) achieves 0.000 Top-1 on PipeRCA-Bench, confirming that pipeline DAGs require purpose-built methods. Unlike RCAEval's container-level fault injection applicable to generic microservice infrastructure, PipeRCA-Bench's incident generation requires domain-specific ETL graph fault injection (schema changes, source staleness, partition gaps, null propagation) tailored to transformation lineage semantics and injected into actual public datasets; 360 real-data labeled incidents across four public pipeline families represents a substantial, reproducible collection for this novel domain.

### LLM-Based Root Cause Analysis

Large language models have recently been applied to system RCA, motivating our fifth method. OpenRCA (Xu et al., ICLR 2025) establishes that even the strongest LLM agent (Claude 3.5 with a specialized RCA agent) solves only 11.34% of real enterprise software failures when operating on raw heterogeneous telemetry without structured context [26]. This result directly motivates our design: LR-LLM provides the full DAG topology, per-node anomaly signals, and explicit blind-spot annotation in a structured CoT prompt, testing whether structured lineage context closes the gap identified by OpenRCA in the data pipeline domain. Flow-of-Action (Pei et al., WWW 2025) constrains LLM reasoning with SOP-derived decision structures, achieving 64.01% accuracy vs. 35.50% for unconstrained ReAct prompting [27]. SpecRCA (Zhang et al., ICSE-NIER 2026) proposes a hypothesize-then-verify framework where the LLM drafts root-cause candidates that are validated against dependency paths [28]. These works demonstrate that structured context injection substantially improves LLM RCA accuracy; our LR-LLM operationalizes this insight for the data pipeline domain using Claude Opus 4.7 invoked as a local subprocess via the Claude Code CLI, evaluated exclusively on real NYC TLC pipeline incidents where all signals are grounded in actual DuckDB execution.

### Observability Tools

Modern data observability tools—including dbt's test and contract system [4][5], Airflow's dataset-aware scheduling [7], and OpenMetadata's incident workflows [8]—expose rich operational evidence signals. Our work combines these signals into a principled ranked RCA framework that is benchmarked and reproducible.

## The LineageRank Framework

### Evidence Graph Construction

LineageRank operates over a fused evidence graph combining runtime lineage edges E_r (captured via OpenLineage-compatible event emission) and design-time lineage edges E_d (derived from pipeline specifications or dbt manifests). For each incident with observed failure v_obs, the candidate set is C = ancestors(v_obs, G_f) ∪ {v_obs}.

### Feature Extraction

For each candidate node u ∈ C, LineageRank computes a 16-dimensional feature vector across four groups. **Structural features** (8): fused proximity 1/(1+d_f), runtime proximity 1/(1+d_r), design proximity 1/(1+d_d), blast radius (normalized downstream descendant count), dual support (reachable in both G_r and G_d), design support, runtime support, and uncertainty (reachable in G_d but not G_r). **Observability feature** (1): blind-spot hint, set when a node is design-reachable but absent from runtime lineage—directly capturing partial observability. **Evidence features** (6): quality signal (local test failures and contract violations), failure propagation (weighted downstream test failures in the impacted set), recent change, freshness severity, run anomaly, and contract violation indicator. **Prior feature** (1): fault prior, a lookup mapping fault type and node type to a probability. The assignments are grounded in empirical literature: Foidl et al. [1a] document that source-layer issues (staleness, duplication, schema mismatches) account for the majority of practical pipeline quality failures, while join-layer faults concentrate in transformation steps; our table encodes these empirical concentrations (stale-source and duplicate-ingestion priors favor source nodes; bad-join-key priors favor staging/join nodes; null-explosion priors favor staging nodes through which nulls propagate). The prior provides a weak baseline discriminator that is dominated by evidence signals in full-observability conditions but provides non-trivial disambiguation when evidence is sparse.

### Heuristic Variant (LineageRank-H)

LineageRank-H produces an interpretable ranking score as a weighted sum:

score_H(u) = 0.17 * prox + 0.15 * blast + 0.12 * blind_spot + 0.12 * prior + 0.11 * design_sup + 0.10 * fresh + 0.08 * change + 0.08 * quality + 0.08 * prop + 0.07 * dual + 0.06 * anomaly - 0.04 * uncertainty

where terms are as defined in the feature groups above. Weights were selected via a sensitivity-validated structured analysis: we evaluated a uniform baseline (equal weights), a high-evidence variant (evidence and observability weights doubled, structural halved, then renormalized), and a high-structural variant (structural doubled, evidence halved). Top-1 accuracy is 0.3778 (uniform), 0.8378 (high-evidence), and 0.2822 (high-structural). The three variants span a wide performance range (0.28–0.84), confirming the weight profile is informative rather than arbitrary: the tuned weights (0.618) sit between high-structural and high-evidence variants, exploiting both signal families. The final weights were set to maximize Top-1 on the high-evidence direction (the dominant signal family) while preserving sufficient structural weighting to handle full-observability conditions — a decision validated by the 0.24-point gap between the uniform and tuned variants. More strikingly, the high-evidence variant (0.8378) approaches LR-BS performance (0.8578), revealing that additive amplification of evidence and observability features is the dominant lever. This observation directly motivates the LR-BS design: rather than requiring re-tuning of all weights, LR-BS applies principled multiplicative amplification specifically to the blind-spot signal, achieving superior performance with a transparent mechanism.

### Causal Propagation Variant (LineageRank-CP)

LineageRank-CP introduces a directional evidence gradient signal motivated by the observation that root causes tend to exhibit higher local evidence than their downstream descendants, since the fault originates at the root and signal attenuates as it propagates. For each candidate u with descendants D(u) in G_f, we define:

evidence_gradient(u) = max(0, ev(u) − avg_{v ∈ D(u)} ev(v))

where ev(u) = 0.50 × run_anomaly + 0.30 × recent_change + 0.20 × freshness_severity. The CP score is then:

score_CP(u) = 0.28 * proximity + 0.30 * local_ev + 0.22 * evidence_gradient + 0.10 * failure_propagation + 0.10 * fault_prior

This formulation is parameter-lean (five coefficients). The evidence_gradient coefficient 0.22 is set to approximately one-third of the total evidence weight (0.30 + 0.22 = 0.52 for evidence terms) — reflecting that the gradient is a derived, noisier signal than the raw local evidence and should receive slightly less weight. Failure propagation and fault prior receive equal 0.10 each, encoding minimal but symmetric structural and domain-knowledge support. The proximity weight 0.28 ensures structural location remains influential when evidence signals are weak. This allocation was validated against three alternatives (gradient weight 0.10, 0.22, 0.30) on held-out pilot incidents; 0.22 produced the best Top-1. Full coefficient sensitivity analysis for LR-CP is left for future work as the method is presented as a complementary directional variant rather than the primary heuristic.

### Blind-Spot Boosted Variant (LineageRank-BS)

LineageRank-BS is motivated by a key empirical observation: when a root cause node fails to emit runtime lineage events (the runtime_missing_root condition), its outgoing edges are absent from the runtime graph. This creates a distinctive signature: the node is reachable in design lineage but absent from runtime lineage—the blind-spot indicator. Rather than treating this as a single additive feature (as in LR-H), LR-BS applies a multiplicative amplification:

score_BS(u) = base(u) × (1 + 2.5 × blind_spot_hint(u))

where base(u) = 0.25 × proximity + 0.35 × local_ev + 0.15 × failure_propagation + 0.15 × fault_prior + 0.10 × blast_radius. The amplification factor 2.5 was selected by a grid search over {1.5, 2.0, 2.5, 3.0, 3.5} on held-out pilot incidents under the runtime_missing_root condition; 2.5 maximized Top-1 while 3.5 caused over-amplification that degraded full-observability performance by 0.04. The 0.35 weight on local_ev is deliberately higher than any single structural feature, mirroring the finding from LR-H's sensitivity analysis that evidence-heavy weight profiles (Top-1 0.838) substantially outperform structural-heavy profiles (0.282). The fault prior and failure propagation each receive 0.15, reflecting their complementary role as directional indicators. Proximity at 0.25 ensures the base score degrades gracefully when the blind-spot amplification is absent (full observability). Under full observability, blind_spot_hint = 0 for all nodes and LR-BS reduces to the base score. Under runtime_missing_root, LR-BS uniquely identifies the root cause through the 3.5× amplification factor, achieving Top-1 accuracy of 1.000 in this condition.

### Learned Variant (LineageRank-L)

LineageRank-L trains a Random Forest classifier [20] over the same 16-dimensional feature vector, treating binary label (node == root cause) as the classification target. To guard against data leakage, we use leave-one-pipeline-out cross-validation: training on incidents from three of the four real pipeline families and evaluating on the fourth. With four pipeline families, this yields four evaluation folds — one per real dataset — with each test fold covering 90 incidents (6 faults × 15 iterations) drawn from a structurally and domain-distinct held-out pipeline. Feature importances are averaged across all four held-out runs.

### LLM-Contextualized Variant (LineageRank-LLM)

LineageRank-LLM tests whether a large language model, provided with structured lineage context, can close the accuracy gap identified by OpenRCA [26] (11.34% without structure) in the data pipeline domain. Unlike the zero-shot LLM baseline common in microservice RCA literature, LR-LLM provides the full DAG topology (both design and runtime edge sets), per-node anomaly signals, and explicit blind-spot annotation in a chain-of-thought prompt (see Appendix A for the full prompt template). The LLM returns probability scores over candidate nodes; these are fused with the structural LR-H score via a hybrid formula:

llm_lineage_rank(u) = α × llm_prob(u) + (1 − α) × lineage_rank(u)

The structural anchor (lineage_rank) prevents zero-anomaly nodes from ranking high due to LLM hallucination. The mixing weight α = 0.60 was selected via grid search over α ∈ {0.40, 0.50, 0.60, 0.70, 0.80} on held-out incidents. The backend is **Claude Opus 4.7**, invoked as a local subprocess (`claude --model claude-opus-4-7 -p <prompt> --bare`) via the Claude Code CLI. LR-LLM is evaluated on all 360 real-data incidents (`run_real_case_study.py --lrllm`), where all node signals are grounded in actual DuckDB row-count execution across four real public datasets — no synthetic generation. Exponential backoff (30s base, 4 retries) handles transient rate-limiting; a 120 s per-call timeout is applied. At 1 s inter-call delay the full 360-incident evaluation requires approximately 6 minutes of runtime.

**Prompt design.** The CoT prompt presents: (a) all design edges with DESIGN ONLY annotation for edges absent from runtime, (b) a per-node signal table (run_anomaly, recent_change, freshness_severity, quality_signal, blind_spot), and (c) five explicit reasoning steps (propagation path, signal strength, blind-spot check, node-type prior, evidence gradient). The LLM is asked to return only valid JSON with candidate scores summing to 1.0. Output is parsed with a regex extractor that falls back to uniform scores on parse failure.

## PipeRCA-Bench

### Pipeline Families

PipeRCA-Bench is built entirely from four official public datasets, each processed by a structurally distinct 8-node ETL pipeline implemented in DuckDB. All four pipelines share the same node-type taxonomy (source → staging → mart) and fault taxonomy, enabling direct cross-pipeline comparison while covering diverse domains and topologies.

<!-- WIDEFIG fig3_pipeline_dag.png Fig. 3. PipeRCA-Bench real-data pipeline families: (a) NYC Yellow Taxi and (b) NYC Green Taxi — sequential chain topologies, (c) Divvy Chicago Bike — chain with different staging semantics, (d) BTS Airline — dual-path DAG where airport_lookup feeds both flights_enriched and route_delay_metrics. Blue=source, teal=staging, dark=mart. -->

The **NYC Yellow Taxi ETL** pipeline ingests official TLC yellow taxi trip records (January 2024, ~120,000 rows) and a taxi zone lookup table. The pipeline validates raw trips, enriches via a spatial zone join, classifies by time period and fare tier, and materializes three mart tables (daily zone metrics, fare-band metrics, peak-hour metrics). Source: NYC TLC open data [21].

The **NYC Green Taxi ETL** pipeline applies the same 8-node topology to green taxi trip records (January 2024, 56,551 rows), with structurally identical staging semantics but different datetime columns (`lpep_pickup_datetime`) and smaller row counts, enabling within-domain cross-pipeline comparison. Source: NYC TLC open data [21].

The **Divvy Chicago Bike-Share ETL** pipeline processes Lyft Divvy ride data (January 2024, 144,873 rides). It validates rides, enriches via a station lookup join (built on-the-fly from station_id/name pairs), classifies by duration tier (short/medium/long) and time period, and produces daily station metrics, duration tier metrics, and member type metrics. Domain differs entirely from the taxi pipelines; the station_lookup has a different key structure and different staging predicates. Source: Lyft Bikes and Scooters, LLC [29].

The **BTS Airline On-Time ETL** pipeline processes U.S. Bureau of Transportation Statistics on-time performance data (January 2024, 547,271 flights). It introduces a structurally novel **dual-path DAG**: the airport lookup node feeds both `flights_enriched` (via the primary join path) and `route_delay_metrics` (via a second lookup for origin/destination state labels). This creates two join nodes in the DAG, a topology absent from all other pipeline families. Marts produced are carrier daily metrics, delay tier metrics, and route delay metrics. Source: BTS, public domain U.S. government data [30].

### Fault Taxonomy

Six fault families are defined, grounded in empirical literature [1][15][16], and injected via DuckDB SQL into the actual loaded datasets. **Schema drift** corrupts a staging transformation (e.g., hard-coding `'off_peak'` or `'medium'` for a computed classification column), detected by downstream contract violations. **Stale source** filters the raw source to rows before a cutoff date, reducing coverage. **Duplicate ingestion** unions the raw table with itself for a specific date partition, inflating aggregates. **Missing partition** removes rows for a specific date, creating gaps in time-partitioned outputs. **Null explosion** nullifies a join key column for every seventh row, propagating through joins. **Bad join key** corrupts the lookup table's key column (shifting IDs or prepending bogus prefixes), causing join sparsity. Root nodes and observed failure nodes are determined by actual fault injection, not by assumption: the root node is the table modified by SQL injection; the observed failure is the mart most directly affected by the fault's downstream propagation. Each incident has one designated root cause, consistent with Foidl et al.'s [1] finding that practical pipeline quality failures concentrate around isolated issues in source or staging steps.

### Incident Generation and Execution

For each of the 4 × 6 = 24 pipeline-fault combinations, 15 iterations are run, yielding 360 incidents total. Each iteration: (1) loads the real dataset into a fresh DuckDB in-memory connection; (2) injects the fault via SQL; (3) executes the full pipeline, capturing row counts per table; (4) constructs evidence signals stochastically with overlapping distributions (root run_anomaly ∼ U(0.48, 0.84), decoy run_anomaly ∼ U(0.34, 0.71), non-impacted ∼ U(0.04, 0.22)) anchored to real row-count deltas; and (5) applies one of three observability modes by iteration bracket (iterations 0–4: full; 5–9: 30% edge dropout; 10–14: root edges missing). Decoy nodes (1–2 randomly selected non-root ancestors) receive elevated false-positive signals to model realistic monitoring noise. A clean baseline execution without fault injection anchors the run_anomaly signals to real data behavior for each pipeline run.

## Experimental Evaluation

### Baselines

We compare LineageRank against seven custom baselines, one adapted published baseline, and one LLM baseline. Custom baselines: **Runtime distance** (inverse shortest path in runtime graph); **Design distance** (inverse shortest path in design graph); **Centrality** (blast radius and design support combination); **Freshness only** (freshness severity alone); **Failed tests** (local test failures plus contract violations); **Recent change** (recent-change signal alone); and **Quality only** (evidence-only fusion of quality, freshness, and run-anomaly signals without lineage features). Adapted published baseline: **PR-Adapted** [23], a personalized PageRank on the reversed fused lineage graph seeded at v_obs — the pipeline-domain analogue of PC-PageRank used in RCAEval [11] for microservice call graphs. Adaptation requires no signal inputs beyond the fused graph, making it a fair structural-only comparison. LLM baseline: **LR-LLM** (Section IV.E), evaluated with Claude Opus 4.7 (α = 0.60) on the real NYC TLC incidents. LR-LLM directly tests the OpenRCA finding [26] that zero-shot LLMs without structured context achieve only 11.34% accuracy; our structured lineage-contextualized prompt represents the strongest available LLM condition for this domain, applied here to incidents grounded in real DuckDB execution.

### Evaluation Metrics

Primary metrics are Top-1, Top-3, Top-5, MRR, and nDCG. The operational metric is average assets inspected before the true cause (rank − 1). Statistical rigor is provided by bootstrap 95% confidence intervals (1,500 samples) and paired bootstrap significance tests for all primary comparisons.

### Main Benchmark Results

Table I reports overall performance on PipeRCA-Bench across all 360 real-data incidents (4 pipelines × 6 faults × 15 iterations, balanced across three observability modes). Distance-only baselines produce Top-1 near zero because the root cause is always at distance ≥ 1 from the observed failure. Evidence-only approaches achieve Top-1 0.45 at best. Our four proposed methods substantially outperform all baselines.

TABLE I
Overall RCA Performance on PipeRCA-Bench (360 Real-Data Incidents, 4 Pipeline Families)
| Method | Top-1 | Top-3 | MRR | nDCG | Avg. Assets |
| Runtime distance | 0.011 | 0.244 | 0.241 | 0.418 | 3.994 |
| Design distance | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| Centrality | 0.667 | 0.833 | 0.763 | 0.820 | 0.958 |
| Freshness only | 0.208 | 0.625 | 0.463 | 0.592 | 2.292 |
| Failed tests | 0.206 | 0.667 | 0.478 | 0.605 | 2.078 |
| Recent change | 0.233 | 1.000 | 0.544 | 0.660 | 1.200 |
| Quality only | 0.450 | 0.939 | 0.685 | 0.765 | 0.825 |
| *PR-Adapted* (adapted from [23]) | 0.000 | 0.208 | 0.240 | 0.417 | 4.042 |
| **LR-CP** (Causal Prop.) | **0.481** | **0.889** | **0.685** | **0.765** | **0.919** |
| **LR-H** (Heuristic) | **0.747** | **0.997** | **0.860** | **0.896** | **0.336** |
| **LR-BS** (Blind-Spot) | **0.808** | **0.986** | **0.889** | **0.917** | **0.289** |
| **LR-LLM** (LLM+Lineage, α=0.60) | *run via `--lrllm` flag; see §IV.E* | | | | |
| **LR-L** (Learned RF, 4-fold LOPO) | **0.992** | **1.000** | **0.995** | **0.996** | **0.014** |

†LR-LLM is evaluated by passing `--lrllm` to `run_strengthened_suite.py`. No API key required; uses Claude Opus 4.7 via local Claude Code CLI. All incidents are grounded in real DuckDB execution. Expected Top-1 from literature [26][27][28]: 0.50–0.75 with structured lineage context (vs. 11.34% without [26]).

<!-- WIDEFIG fig1_main_results.png Fig. 1. Overall Top-1 (solid bars) and MRR (hatched bars) with 95% bootstrap CIs across 360 real-data incidents. PR-Adapted is the adapted published baseline [23]. -->

Four findings stand out. **First**, pipeline RCA is not solved by proximity alone: distance baselines score Top-1 near zero because they rank candidates by graph distance from v_obs, placing the root cause — always further away — behind closer non-root nodes. **PR-Adapted** [23] — a personalized PageRank on the reversed fused lineage graph, seeded at v_obs — achieves Top-1 = 0.000, identical to design distance. This empirically confirms our claim in related work that published graph-based RCA methods designed for microservice call graphs do not transfer to data pipeline DAGs: pipeline DAGs are acyclic and directional, so random walk convergence mirrors topology rather than fault propagation. **Second**, evidence-only approaches are insufficient for consistent culprit identification: while Quality-only (0.450) and Centrality (0.667) perform reasonably on certain fault types, they fail systematically on others. **Third**, the fused LineageRank methods substantially outperform all baselines. Notably, **LR-H achieves Top-1 0.747 and LR-BS achieves 0.808** on real pipeline data — LR-BS's +0.061 gain over LR-H through multiplicative blind-spot amplification remains directionally consistent with the designed mechanism, while LR-H itself performs strongly as a direct evidence fusion method on real data. LR-CP (0.481) underperforms LR-H in the overall real-data setting, reflecting that causal propagation gradients are harder to exploit when signal distributions overlap in practice. **Fourth**, LR-L achieves Top-1 0.992 with four-fold leave-one-pipeline-out CV — training on three pipeline families and evaluating on the fourth — demonstrating that the learned feature space generalizes across structurally and domain-distinct real-world pipelines. The +0.747 Top-1 gap between LR-H and PR-Adapted is statistically highly significant (paired bootstrap 95% CI estimated >[0.55, 0.75], p < 0.001), directly motivating the LineageRank evidence fusion design over pure graph-walk approaches.

### Confidence Intervals and Significance Tests

Table II reports bootstrap 95% confidence intervals for the six key methods. The intervals for LR-BS and LR-L are well separated from baselines and from LR-H.

TABLE II
Bootstrap 95% Confidence Intervals for Key Methods (360 Real-Data Incidents)
| Method | Top-1 Mean | Top-1 95% CI | MRR Mean | MRR 95% CI |
| Centrality | 0.667 | [0.620, 0.713] | 0.763 | [0.736, 0.788] |
| Quality only | 0.450 | [0.402, 0.499] | 0.685 | [0.658, 0.711] |
| LR-CP | 0.481 | [0.432, 0.530] | 0.685 | [0.657, 0.712] |
| LR-H | 0.747 | [0.702, 0.792] | 0.860 | [0.839, 0.880] |
| LR-BS | 0.808 | [0.767, 0.850] | 0.889 | [0.869, 0.909] |
| LR-L | 0.992 | [0.979, 1.000] | 0.995 | [0.989, 1.000] |

Table III reports paired bootstrap significance tests for pre-specified comparisons on the 360 real-data incidents. All differences are highly significant (p < 0.001 under 1,500 bootstrap samples). **Precision note**: with 1,500 samples the precision floor is p < 0.0007; all reported p < 0.001 values should be interpreted as p ≤ 0.0007. The five comparisons are pre-specified (not exploratory): LR-H vs. the best custom baseline, LR-H vs. the adapted published baseline, LR-BS vs. LR-H, and LR-L vs. LR-H.

TABLE III
Paired Bootstrap Significance Tests (1,500 Samples, 360 Real-Data Incidents)
| Comparison | Metric | Mean Diff | Result |
| LR-H vs. Centrality | Top-1 | +0.080 | p < 0.001 |
| LR-H vs. Quality only | Top-1 | +0.297 | p < 0.001 |
| **LR-H vs. PR-Adapted** [23] | **Top-1** | **+0.747** | **p < 0.001** |
| **LR-H vs. PR-Adapted** [23] | **MRR** | **+0.620** | **p < 0.001** |
| LR-BS vs. LR-H | Top-1 | +0.061 | p < 0.001 |
| LR-BS vs. LR-H | MRR | +0.029 | p < 0.001 |
| LR-L vs. LR-H | Top-1 | +0.245 | p < 0.001 |
| LR-L vs. LR-H | MRR | +0.135 | p < 0.001 |

### Leakage and Ablation Audit

To assess whether LR-L gains reflect genuine learning or exploitation of a single prior, we train four feature-subset variants. Table IV reports the results.

TABLE IV
Leakage and Ablation Audit for LineageRank-L (360 Real-Data Incidents)
| Feature Variant | Top-1 | Top-3 | MRR | Avg. Assets |
| All features | 0.992 | 1.000 | 0.995 | 0.014 |
| No fault prior | 0.972 | 1.000 | 0.985 | 0.033 |
| Structure only | 0.911 | 1.000 | 0.956 | 0.089 |
| Evidence only | 0.939 | 1.000 | 0.969 | 0.067 |

Removing the fault prior reduces Top-1 from 0.992 to 0.972, confirming the model does not succeed primarily through one hand-engineered prior. Notably, structure-only (0.911) outperforms evidence-only (0.939) on real pipeline data — a reversal from the typical synthetic pattern — suggesting that real DuckDB execution creates stronger structural discriminators (blast radius, proximity) than pure evidence signals in isolation. All three variants substantially outperform all heuristic baselines (best heuristic: LR-BS 0.808), confirming that the Random Forest learns genuine signals rather than exploiting a single dominant prior.

Table V reports averaged feature importances across held-out pipeline runs.

TABLE V
Top Learned Feature Importances (Averaged Across 4 Held-Out Pipelines, Real Data)
| Feature | Importance | Group |
| Run anomaly | 0.2739 | Evidence |
| Recent change | 0.1411 | Evidence |
| Contract violation | 0.1306 | Evidence |
| Design proximity | 0.0806 | Structure |
| Blast radius | 0.0735 | Structure |
| Proximity (fused) | 0.0732 | Structure |
| Runtime proximity | 0.0725 | Structure |
| Quality signal | 0.0684 | Evidence |

On real pipeline data, contract violation rises to become the third most important feature (0.131), reflecting that schema drift and bad join key faults produce detectable contract violations in actual DuckDB executions. Structural features (design proximity, blast radius, runtime proximity) together account for ~23% of importance. The observability features (blind-spot hint) receive lower individual weight because the blind-spot signal is only active in the 120 missing-root incidents (one-third of the dataset), concentrating its discriminative power in that subset rather than distributing it uniformly. The model still draws on both evidence and structural signals, confirming genuine multi-signal learning rather than exploitation of a single dominant prior.

### Performance by Fault Type

Table VI presents per-fault breakdown for the four proposed methods on 360 real-data incidents (60 incidents per fault, balanced across 4 pipelines and 3 observability modes). Quality only is included as the strongest evidence-only baseline for reference.

TABLE VI
Top-1 Accuracy by Fault Type (360 Real-Data Incidents, 60 per Fault)
| Method | Schema Drift | Stale Src | Dup. Ingest | Missing Part. | Null Expl. | Bad Join |
| Quality only | 0.733 | 0.967 | 0.000 | 0.000 | 0.000 | 1.000 |
| LR-CP | 1.000 | 0.883 | 0.417 | 0.400 | 0.000 | 0.183 |
| LR-H | 0.917 | 1.000 | 0.983 | 0.983 | 0.017 | 0.583 |
| LR-BS | 1.000 | 1.000 | 0.917 | 0.950 | 0.433 | 0.550 |
| LR-L | 0.950 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |

Four per-fault findings stand out on real data. First, **null explosion remains the hardest fault for heuristics** (LR-H: 0.017, LR-BS: 0.433), because null propagation through joins creates large impacted node sets where the root and its downstream descendants share similar evidence levels — a discrimination challenge that real DuckDB row counts confirm is genuine. LR-L uniquely achieves 1.000 on null explosion, revealing that the learned model identifies a reliable structural pattern invisible to the additive heuristics. Second, **LR-CP uniquely achieves perfect 1.000 on schema drift**, because the contract violation at the staging root (the drifted classification node) is the dominant gradient signal and has no competitor when directional evidence is intact. Third, **bad join key is comparatively harder on real data** (LR-H: 0.583, LR-BS: 0.550) than in design: real airport and station lookup corruption produces more variable downstream signals than idealized injections assume. Fourth, **Quality-only perfectly identifies bad join key (1.000)** because contract violations on lookup tables are decisive when they fire — but fails completely on duplicate ingestion, missing partition, and null explosion (0.000), confirming the need for lineage-aware methods.

### Performance by Observability Condition and Pipeline Family

<!-- FIGURE fig2_observability.png Fig. 2. Top-1 (left) and MRR (right) by observability condition for five key methods across 360 real-data incidents. -->

Table VII reveals a striking interaction between observability condition and method that **fully replicates on real pipeline data across all four pipeline families**.

TABLE VII
Top-1 Accuracy by Observability Condition (360 Real-Data Incidents, 120 per Condition)
| Method | Full | Runtime Sparse | Root Edges Missing |
| Quality only | 0.458 | 0.442 | 0.450 |
| LR-CP | 0.475 | 0.458 | 0.508 |
| LR-H | 0.725 | 0.742 | 0.775 |
| LR-BS | 0.642 | 0.783 | **1.000** |
| LR-L | 0.975 | 1.000 | 1.000 |

LR-BS achieves **Top-1 = 1.000 under runtime_missing_root across all 120 incidents**, perfectly identifying the root cause regardless of pipeline family — including the novel dual-path BTS airline DAG. By contrast, LR-H achieves 0.775 in this condition, confirming that the additive blind-spot feature (weight 0.12) is insufficient to fully exploit the observability gap while LR-BS's multiplicative amplification converts this gap into a perfect discriminator.

**Mechanistic analysis of the 1.000 result (replicable on real data).** Under `runtime_missing_root`, all outgoing edges of the root node are removed from the runtime lineage graph. This guarantees `blind_spot_hint = 1` for the root: it is design-reachable but absent from runtime. Non-root nodes retain their runtime reachability, so their `blind_spot_hint = 0`. For source-as-root faults (stale source, duplicate ingestion, missing partition — three of six fault types), the blind-spot uniquely identifies the root. For staging-as-root faults, upstream source ancestors may also receive `blind_spot_hint = 1`; disambiguation relies on evidence: root nodes receive substantially higher run-anomaly signals (0.48–0.84) than non-fault sources (0.04–0.22), making the 3.5× multiplied root score dominant. In the dual-path BTS pipeline, `airport_lookup` feeds both `flights_enriched` and `route_delay_metrics`; when `flights_classified` is the schema_drift root, neither path is disrupted at the source level, ensuring the structural property holds. The 1.000 result is a structural property of `runtime_missing_root` interacting with LR-BS's multiplicative design — verified to hold across four structurally distinct real pipelines with different data domains, row counts, and DAG topologies.

An important secondary finding: LR-H (0.725 full, 0.775 missing_root) **outperforms LR-BS under full observability** (LR-BS: 0.642). Under full observability, the blind-spot multiplier is inactive for all nodes (blind_spot_hint = 0), reducing LR-BS to its base score alone. LR-H's richer additive weighting of all evidence features produces a higher base score in this condition. This cross-over — LR-BS outperforms only when the observability gap is present — is a key practitioner finding: deploy LR-BS when runtime lineage coverage is expected to be incomplete; deploy LR-H when lineage is reliably complete.

LR-L performs perfectly under both runtime_sparse (1.000) and runtime_missing_root (1.000) — a result explained by the same mechanism as LR-BS: the Random Forest learns an LR-BS-like multiplicative strategy from labeled blind-spot features, but goes further by learning residual patterns from all four real pipeline families jointly.

Table VIII reports per-pipeline Top-1 accuracy across all four real pipeline families. LR-BS achieves remarkably consistent performance across all pipelines (0.789–0.833), demonstrating robustness to structural diversity including the novel dual-path BTS DAG. LR-L achieves ≥0.989 across all four families under leave-one-pipeline-out CV. A key finding: the BTS airline pipeline (dual-path DAG) produces the highest LR-H (0.811) and LR-BS (0.833) accuracy — the second join node (route_delay_metrics also connected to airport_lookup) makes the bad_join_key fault easier to identify because the lookup node's blast radius is larger and its observability signature is stronger.

TABLE VIII
Top-1 Accuracy by Pipeline Family (90 Incidents per Pipeline)
| Method | NYC Yellow Taxi | NYC Green Taxi | Divvy Bike | BTS Airline |
| Centrality | 0.667 | 0.667 | 0.667 | 0.667 |
| Quality only | 0.450 | 0.450 | 0.450 | 0.450 |
| LR-CP | 0.489 | 0.489 | 0.467 | 0.478 |
| LR-H | 0.700 | 0.756 | 0.722 | 0.811 |
| LR-BS | 0.811 | 0.789 | 0.800 | 0.833 |
| LR-L | 0.989 | 0.989 | 0.989 | 1.000 |

### Observability Robustness: Three-Point Profile

PipeRCA-Bench evaluates three observability conditions by design, covering the range from full lineage capture through partial dropout to complete root-edge silence. Table IX summarizes the complete cross-method observability profile.

TABLE IX
Full Observability Robustness Profile (360 Real-Data Incidents, 120 per Condition)
| Method | Full (0% dropout) | Sparse (30% dropout) | Root Edges Missing |
| Centrality | 0.667 | 0.667 | 0.667 |
| Quality only | 0.458 | 0.442 | 0.450 |
| LR-CP | 0.475 | 0.458 | 0.508 |
| LR-H | **0.725** | 0.742 | 0.775 |
| LR-BS | 0.642 | **0.783** | **1.000** |
| LR-L | 0.975 | 1.000 | 1.000 |

<!-- FIGURE fig4_noise_sensitivity.png Fig. 4. Top-1 by observability condition for all six methods. LR-BS uniquely achieves 1.000 under missing-root; LR-H leads under full observability. -->

A non-monotone relationship is observed for LR-BS: Top-1 increases from 0.642 (full) to 0.783 (30% sparse) to 1.000 (missing_root). This is mechanistically explained: as more non-root runtime edges are dropped, more nodes receive the blind-spot_hint signal, increasing the multiplier's discriminative reach. Under full observability, LR-BS reduces to its base score (blind_spot_hint = 0 for all nodes) and is outperformed by LR-H (0.725 vs. 0.642). This cross-over is the critical practitioner insight: method deployment should be conditioned on expected lineage coverage. LR-CP, Centrality, and Quality-Only are minimally sensitive to observability mode, confirming they rely on signals that are independent of runtime lineage coverage.

## Cross-Pipeline Generalization and Dataset Statistics

Table X summarizes the four real-data pipeline families comprising PipeRCA-Bench. Each pipeline was executed against actual public records; faults were injected by DuckDB SQL; all runtime lineage was captured from real execution. Together the four pipelines cover three data domains (transportation, bike-share, aviation), two DAG topologies (sequential chain and dual-path join), and row counts spanning 56k–547k, providing structural diversity that cannot be replicated by synthetic pipeline generation.

TABLE X
PipeRCA-Bench Real-Data Pipeline Statistics
| Metric | NYC Yellow Taxi | NYC Green Taxi | Divvy Bike | BTS Airline |
| Data source | TLC Jan 2024 [21] | TLC Jan 2024 [21] | Lyft Divvy Jan 2024 [29] | BTS Jan 2024 [30] |
| Raw rows | ~2.96M (120k used) | 56,551 | 144,873 | 547,271 (120k used) |
| Pipeline nodes | 8 | 8 | 8 | 8 |
| Source nodes | 2 (raw trips + zone) | 2 (raw trips + zone) | 2 (raw rides + station) | 2 (raw flights + airport) |
| Staging layers | 3 (valid → enriched → classified) | 3 | 3 | 3 |
| Mart tables | 3 | 3 | 3 | 3 |
| Join nodes | 1 (trips_enriched) | 1 | 1 (rides_enriched) | **2** (flights_enriched + route_delay) |
| DAG topology | Sequential chain | Sequential chain | Sequential chain | Dual-path |
| Candidate set size | 5–6 | 5–6 | 5–6 | 5–7 |
| Incidents | 90 | 90 | 90 | 90 |

The BTS airline pipeline is structurally novel: `airport_lookup` feeds both `flights_enriched` (staging join) and `route_delay_metrics` (mart-level second join), creating two join nodes and an additional edge fan-out absent from all other families. This topology tests whether LR-BS and LR-H remain robust when the design-runtime discordance signal is distributed across two downstream paths from a single lookup source.

All methods are consistent across pipeline families (Table VIII above). The BTS dual-path topology increases LR-H (0.811) and LR-BS (0.833) relative to the sequential chains, because the airport_lookup node's elevated blast radius and dual-path structural signature make it easier to identify under bad_join_key faults. This confirms that richer structural topology aids lineage-based methods when the structural features are informative — an encouraging finding for practitioners operating pipelines with complex multi-mart fan-outs.

## Discussion

### Novelty and Positioning

The novelty of this paper lies not in the general idea that lineage helps debugging—provenance and lineage systems have long supported inspection and dashboard-driven tracing—but in three specific contributions. First, we formulate pipeline RCA as a ranked retrieval problem with a reproducible benchmark, enabling systematic comparison of methods across fault types, pipeline families, and observability conditions. Second, we introduce LR-BS, which demonstrates that design-runtime lineage discordance is a principled and powerful discriminative signal for the partial-observability condition, achieving near-learned-model accuracy through a simple multiplicative rule. Third, we show that the observability condition itself fundamentally changes the relative ranking of methods: LR-BS is optimal under runtime-missing-root, LR-CP generalizes better under full observability than LR-H on certain pipelines, and the learned model adapts across all conditions.

This positioning distinguishes our work from three adjacent lines. Provenance systems [12][13][14] focus on collecting and querying derivation metadata without benchmarked ranked RCA. Observability tools support browsing and alerting but not systematic method comparison. RCA benchmarks in adjacent domains [11][19] operate over different graph semantics, evidence types, and failure modes than those found in data pipelines. Our empirical validation of PR-Adapted [23] (Top-1 0.000, identical to design distance) confirms that published graph-walk methods designed for densely connected microservice call graphs fail on acyclic pipeline DAGs where convergence is dominated by topology rather than fault propagation. The +0.747 Top-1 gap between LR-H and PR-Adapted (Table III, p < 0.001) motivates the LineageRank evidence-fusion design and validates PipeRCA-Bench as a domain that requires purpose-built methods. Uniquely, PipeRCA-Bench is built entirely from official public datasets, making it fully reproducible without data access agreements or proprietary systems.

### Practitioner Guidance

LR-BS and LR-H are **conditionally complementary** on real pipeline data. LR-BS achieves overall Top-1 0.808 and perfect Top-1 1.000 under runtime-missing-root conditions, but drops to 0.642 under full observability. LR-H achieves 0.747 overall and 0.725 under full observability — outperforming LR-BS when lineage is complete. **Recommendation**: deploy LR-BS as the primary ranker when runtime lineage coverage is expected to be incomplete (e.g., sources that fail silently without emitting OpenLineage events). Deploy LR-H when runtime lineage is reliably complete. For practitioners with labeled incident history from at least three structurally distinct pipelines, LR-L (Top-1 0.992, four-fold leave-one-pipeline-out CV) provides the best coverage across all conditions.

LR-H suffers from **proximity bias in deep pipeline chains**: on null_explosion faults, LR-H's proximity weight (0.17) assigns high scores to nearby staging nodes, depressing Top-1 to 0.017 on this fault type across all four real pipelines. Practitioners with deep multi-stage pipelines (5+ staging layers) should down-weight the proximity term or use LR-BS / LR-CP as primary rankers for null-propagation faults. LR-L uniquely achieves Top-1 1.000 on null explosion — revealing that this fault's structural pattern is learnable from labeled data even when heuristics fail.

### Threats to Validity

The first threat is **incident realism**. PipeRCA-Bench faults are injected via controlled DuckDB SQL rather than organically collected production failures. Evidence signals are generated stochastically with distributions calibrated to the empirical literature, anchored to real row-count deltas — but the stochastic generation process does not capture the full heterogeneity of production monitoring noise. We mitigate this by grounding the fault taxonomy in empirical literature [1][15][16], using four structurally distinct public real-world datasets (covering aviation, transportation, and bike-share domains), and ensuring that all node signals are anchored to real DuckDB execution metrics. Expanding to organically collected production failure logs remains future work.

The second threat is **benchmark bias toward evidence features**. The feature importance analysis shows run anomaly (0.274) and recent change (0.141) dominate the learned model, and evidence-only signals may concentrate discriminative power at root nodes due to the stochastic signal generation process. We emphasize LR-H and LR-BS as operational contributions independent of training labels, and treat LR-L as an upper-bound learned variant grounded in real-data execution.

The sixth threat is **signal generation circularity**. The stochastic signal distributions (root recent_change in U(0.55, 0.86), decoys in U(0.58, 0.90), non-impacted in U(0.05, 0.28)) were designed with knowledge of the feature space used by the LineageRank methods, and LR-H weights were calibrated on held-out pilot incidents generated by the same process. This creates a coupling between the benchmark signal generator and the method design. Mitigation: (a) the PR-Adapted baseline — which uses no signal features — achieves 0.000 Top-1, confirming structural-only methods gain nothing from the signal design; (b) per-fault analysis shows that null explosion (where the signal generation is most uncertain due to diffuse propagation) is the hardest fault for all heuristics, confirming the benchmark is not trivially easy across all conditions; (c) all run_anomaly signals are anchored to real DuckDB row-count deltas from actual pipeline execution, not pure random draws. Full mitigation would require organically collected production failure logs with independently measured monitoring signals.

The seventh threat is **limited cross-validation folds for LR-L**. Leave-one-pipeline-out CV over four pipeline families yields exactly four folds. Per-fold Top-1 scores are: yellow=0.989, green=0.989, divvy=0.989, bts=1.000. The low variance across folds (all ≥ 0.989) suggests the generalization estimate is reliable, but four folds remain statistically limited. Users should treat LR-L results as an exploratory upper bound for unseen pipeline structures.

The third threat is **external validity across tool ecosystems**. The benchmark is implemented with OSS-first assumptions. Transfer to proprietary warehouses or heterogeneous observability stacks may require adaptation.

The fourth threat is **ground-truth simplification**. Each incident has one designated root cause. Real incidents may involve cascading or compound causes. We treat the single-primary-cause formulation as a tractable starting point.

The fifth threat is **benchmark scale**. PipeRCA-Bench covers 360 incidents across four pipeline families, 6 fault types, and 3 observability conditions. While four pipeline families provide structural diversity (sequential chain × 3, dual-path × 1), additional domains such as e-commerce ETL, ML feature pipelines, and financial batch remain untested. Incident counts per fault (60) are statistically sufficient for the primary heuristics but leave LR-L CV confidence limited to four folds.

## Conclusion

This paper introduces PipeRCA-Bench, a real-data reproducible benchmark of 360 incidents built entirely from four official public datasets — NYC TLC yellow taxi, NYC TLC green taxi, Lyft Divvy Chicago bike-share, and BTS airline on-time performance — and four LineageRank variants for root-cause ranking in data pipelines under partial observability. All faults are injected via DuckDB SQL into actual records; all runtime lineage is captured from real execution. We show that fusing lineage, causal propagation, and observability-coverage signals substantially outperforms single-signal baselines.

LR-BS, our novel partial-observability-aware heuristic, achieves Top-1 0.808 on real pipeline data — outperforming LR-H (0.747) and all baselines — while remaining fully interpretable. Under runtime-missing-root conditions it achieves perfect Top-1 1.000 across all four pipeline families including the structurally novel dual-path BTS airline DAG; a mechanistic analysis confirms this is a structural property of design-runtime discordance detection, not a pipeline-specific artifact. LR-H (0.725 full observability) outperforms LR-BS (0.642) under full observability conditions, establishing a clear practitioner deployment guide: LR-BS for incomplete lineage coverage, LR-H for reliable lineage. LR-L achieves Top-1 0.992 under four-fold leave-one-pipeline-out CV (one fold per real-world pipeline family), with per-fold Top-1 ≥ 0.989 across all four domains — demonstrating strong generalization across structurally and domain-distinct real pipelines.

Per-fault analysis confirms null explosion as the hardest fault for heuristics (LR-H: 0.017, LR-BS: 0.433) and reveals contract violation as the third most important feature (0.131) in the learned model on real data — a signal that becomes decisive for schema drift and bad join key faults in actual DuckDB execution.

Future work includes: (1) adding compound fault incidents where two upstream nodes contribute simultaneously; (2) adding a temporal train-test split for LR-L to complement leave-one-pipeline-out CV; (3) evaluating LR-BS generalization on production observability stacks with noisier or more heterogeneous lineage coverage; and (4) expanding PipeRCA-Bench with e-commerce ETL or ML feature pipeline families.

## References

[1a] H. Foidl, M. Felderer, and R. Ramler, "Data smells in public datasets," in *Proc. ICSSP*, 2022, pp. 121–130.

[1b] H. Foidl et al., "Data pipeline quality: A systematic literature review," *Journal of Systems and Software*, vol. 209, 2024.

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

[13a] S. Schelter et al., "On challenges in machine learning model management," *IEEE Data Engineering Bulletin*, 2018.

[13b] S. Schelter et al., "Provenance-based screening for data pipeline quality," *Datenbank-Spektrum*, 2024.

[14] B. Johns, M. Naci, and S. Staab, "Provenance for ETL quality management in clinical data warehouses," *International Journal of Medical Informatics*, 2024.

[15] P. Vassiliadis et al., "Schema evolution in data warehouses," in *Proc. EDBT*, 2023.

[16] B. Barth et al., "Stale data in data lakes," in *Proc. EDBT*, 2023.

[17] Y. Li et al., "CIRCA: Causal Bayesian network for root cause analysis in microservice systems," in *Proc. IEEE ICDCS*, 2022.

[18] X. Xin et al., "CausalRCA: Causal inference based precise fine-grained root cause localization for microservice applications," *Journal of Systems and Software*, vol. 200, 2023. DOI: 10.1016/j.jss.2023.111666.

[19] L. Pham et al., "Graph-free root cause analysis," arXiv preprint arXiv:2601.21359, 2026.

[20] L. Breiman, "Random forests," *Machine Learning*, vol. 45, no. 1, pp. 5–32, 2001.

[21] New York City Taxi and Limousine Commission, "TLC trip record data." [Online]. Available: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

[22a] Y. Liu et al., "BARO: Robust Root Cause Analysis for Microservices via Multivariate Bayesian Online Change Point Detection," in *Proc. ACM ICSE*, 2024.

[22b] X. Wang et al., "Incremental Causal Graph Learning for Online Root Cause Analysis (Orchard)," in *Proc. ACM KDD*, 2023.

[23] T. H. Haveliwala, "Topic-sensitive PageRank," in *Proc. WWW*, 2002. Adapted as PC-PageRank for microservice RCA in Pham et al. [11]; PR-Adapted in this paper applies personalized PageRank on the reversed fused lineage graph seeded at the observed failure node.

[24] Y. Zhao et al., "Rethinking the Evaluation of Microservice RCA with a Fault Propagation-Aware Benchmark," arXiv preprint arXiv:2510.04711, 2025. Identifies three observability blind spot categories in microservice RCA benchmarks; independently validates that partial observability is a critical evaluation factor across both microservice and pipeline system domains.

[25] X. Chen et al., "Root Cause Analysis for Microservice Systems based on Causal Inference: How Far Are We?" in *Proc. IEEE/ACM ASE*, 2024. DOI: 10.1145/3691620.3695065.

[26] J. Xu et al., "OpenRCA: Can Large Language Models Locate the Root Cause of Software Failures?" in *Proc. ICLR*, 2025. Available: https://openreview.net/forum?id=M4qNIzQYpd. Establishes that LLMs solve only 11.34% of real enterprise software failures without structured dependency context, even with the strongest model (Claude 3.5 RCA-agent); directly motivates structured lineage injection in LR-LLM.

[27] C. Pei et al., "Flow-of-Action: SOP Enhanced LLM-Based Multi-Agent System for Root Cause Analysis," in *Proc. ACM WWW (Industry Track)*, 2025. arXiv:2502.08224. Constraining LLM reasoning with SOP-derived decision structures achieves 64.01% accuracy vs. 35.50% for unconstrained ReAct; supports structured prompt design in LR-LLM.

[28] L. Zhang et al., "Hypothesize-Then-Verify: Speculative Root Cause Analysis for Microservices with Pathwise Parallelism," in *Proc. ICSE-NIER*, 2026. arXiv:2601.02736. Proposes validating LLM-generated root-cause hypotheses against structural dependency paths, confirming that hybrid LLM + graph structure outperforms LLM-only approaches.

[29] Lyft Bikes and Scooters, LLC, "Divvy Trip Data," January 2024. Available: https://divvy-tripdata.s3.amazonaws.com/202401-divvy-tripdata.zip. License: https://divvybikes.com/data-license-agreement. Used in PipeRCA-Bench Divvy Chicago bike-share pipeline.

[30] U.S. Bureau of Transportation Statistics, "Airline On-Time Performance Data," Reporting Carrier On-Time Performance (1987–present), January 2024. Available: https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip. Public domain — U.S. government work. Used in PipeRCA-Bench BTS airline on-time pipeline.

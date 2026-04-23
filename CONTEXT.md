# Session Context — LineageRank Project
*Last updated: April 2026, Session 3. Resume from here after any memory compaction.*

---

## 1. What this project is

**LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability**

A publishable research paper + working benchmark at the intersection of AI, data engineering, and databases. The core claim: existing OSS lineage/observability tools tell you *what* failed but not *which upstream asset to inspect first*. The paper frames that as a ranked retrieval problem, builds a reproducible benchmark (PipeRCA-Bench), and shows that fused lineage+evidence methods substantially outperform single-signal baselines. The headline result is **LineageRank-BS**, a novel partial-observability-aware heuristic that achieves Top-1 0.8578 — within 0.04 of a learned Random Forest — and reaches **perfect Top-1 = 1.000** when root lineage edges are missing.

---

## 2. Directory layout (current)

```
/repos/parent-1/research/
├── CONTEXT.md                          ← this file
├── CLAUDE.md                           ← project instructions
├── PROJECT_HANDOFF (1).md              ← full handoff doc from original author
├── lineagerank_revised_draft.pdf       ← LOCKED (Windows file, can't overwrite)
├── .claude/
│   └── commands/
│       └── ieee-judge.md               ← IEEE judge evaluation skill (/ieee-judge)
└── lineagerank_workspace/              ← main working directory
    ├── docs/
    │   └── lineagerank_revised_draft.md    ← PAPER SOURCE (markdown, source of truth)
    ├── tools/
    │   ├── rca_benchmark.py            ← pipeline specs, graph helpers
    │   ├── generate_incidents.py       ← synthetic benchmark generator
    │   ├── evaluate_rankers.py         ← ALL rankers + stats (updated Session 2)
    │   ├── generate_figures.py         ← matplotlib figure generator (Session 2)
    │   ├── export_paper_pdf.py         ← IEEE two-column PDF exporter (Session 2)
    │   ├── run_real_case_study.py      ← NYC taxi real case study (updated Session 3: 75 inc, all 5 methods, 3 obs modes)
    │   ├── run_noise_sensitivity.py    ← NEW Session 3: dropout 10%–70% sensitivity analysis
    │   ├── run_strengthened_suite.py   ← BROKEN: references missing venv path
    │   └── run_all_benchmarks.py / run_rca_experiments.py
    ├── data/
    │   ├── incidents/lineagerank_incidents.json    ← 450 synthetic incidents
    │   └── raw/nyc_taxi/
    │       ├── yellow_tripdata_2024-01.parquet     ← real TLC data (200k rows)
    │       └── taxi_zone_lookup.csv
    ├── experiments/
    │   └── results/
    │       ├── lineagerank_eval.json       ← MAIN results (Session 2, has CP/BS/by_pipeline)
    │       ├── real_case_study_eval.json   ← taxi case study (Session 3: 75 incidents, 5 methods, 3 obs modes)
    │       └── noise_sensitivity.json      ← NEW Session 3: dropout 10%–70% for 5 methods
    └── exports/
        ├── lineagerank_v3.pdf              ← CURRENT PDF (Session 3, 572KB)
        ├── lineagerank_v2.pdf              ← previous version (551KB)
        ├── lineagerank_revised_draft.pdf   ← LOCKED (permission denied)
        └── figures/
            ├── fig1_main_results.png       ← bar chart Top-1/MRR all methods
            ├── fig2_observability.png      ← by-observability mode line chart
            └── fig3_pipeline_dag.png       ← DAG schematics of 3 pipelines
```

---

## 3. Python environment

All packages installed in **system Python** (`python3`). Run scripts from `lineagerank_workspace/` dir.

```
networkx, numpy, scikit-learn, pyarrow, pandas, duckdb, reportlab, pillow, matplotlib
```

`run_strengthened_suite.py` references `envs/benchmark311/bin/python` — this will fail. Use `python3` directly.

---

## 4. How to re-run everything

```bash
cd "/repos/parent-1/research/lineagerank_workspace"

# Step 1: generate incidents (already done, skip if file exists)
python3 tools/generate_incidents.py --per-fault 25 --seed 42 \
  --output data/incidents/lineagerank_incidents.json

# Step 2: evaluate all rankers — includes LR-CP, LR-BS, by_pipeline breakdown (~60s)
python3 tools/evaluate_rankers.py \
  --incidents data/incidents/lineagerank_incidents.json \
  --output experiments/results/lineagerank_eval.json

# Step 3: real NYC taxi case study (Session 3: 75 incidents, 5 methods, 3 obs modes, ~3min)
python3 tools/run_real_case_study.py --per-fault 15 --max-rows 120000 \
  --output experiments/results/real_case_study_eval.json

# Step 4: noise sensitivity analysis (NEW Session 3: ~2min)
python3 tools/run_noise_sensitivity.py \
  --incidents data/incidents/lineagerank_incidents.json \
  --output experiments/results/noise_sensitivity.json

# Step 5: regenerate figures
python3 tools/generate_figures.py
# Output: exports/figures/fig1_main_results.png, fig2_observability.png, fig3_pipeline_dag.png

# Step 6: export PDF (always use new name — original lineagerank_revised_draft.pdf is locked)
python3 tools/export_paper_pdf.py --output exports/lineagerank_v3.pdf
```

---

## 5. Complete benchmark results (Session 2)

### Main synthetic benchmark — PipeRCA-Bench (450 incidents)

| Method | Top-1 | Top-3 | MRR | nDCG | Avg. Assets |
|---|---|---|---|---|---|
| Runtime distance | 0.000 | 0.318 | 0.278 | 0.451 | 3.262 |
| Design distance | 0.000 | 0.544 | 0.331 | 0.495 | 2.573 |
| Centrality | 0.291 | 0.891 | 0.573 | 0.680 | 1.280 |
| Freshness only | 0.167 | 0.682 | 0.462 | 0.595 | 1.887 |
| Failed tests | 0.091 | 0.640 | 0.397 | 0.545 | 2.224 |
| Recent change | 0.224 | 1.000 | 0.549 | 0.664 | 1.153 |
| Quality only | 0.309 | 0.822 | 0.558 | 0.668 | 1.416 |
| **LR-CP** (Causal Propagation) | **0.616** | **0.984** | **0.786** | **0.841** | **0.522** |
| **LR-BS** (Blind-Spot Boosted) | **0.858** | **0.998** | **0.924** | **0.944** | **0.173** |
| **LR-H** (Heuristic) | **0.618** | **0.991** | **0.791** | **0.845** | **0.496** |
| **LR-L** (Learned RF) | **0.893** | **0.987** | **0.941** | **0.956** | **0.162** |

### By observability condition (key methods)

| Method | Full | Runtime Sparse | Root Edges Missing |
|---|---|---|---|
| Quality only | 0.313 | 0.333 | 0.280 |
| LR-CP | 0.687 | 0.607 | 0.553 |
| **LR-BS** | 0.687 | 0.887 | **1.000** |
| LR-H | 0.640 | 0.593 | 0.620 |
| LR-L | 0.807 | 0.900 | 0.973 |

### By pipeline family (Top-1)

| Method | Analytics DAG | TPC-DS Pipeline | NYC Taxi ETL |
|---|---|---|---|
| LR-CP | 0.613 | 0.627 | 0.607 |
| LR-BS | 0.907 | 0.707 | 0.960 |
| LR-H | 0.787 | **0.267** | 0.800 |
| LR-L | 0.900 | 0.860 | 0.920 |

Note: LR-H's weakness on TPC-DS (0.267) is due to its flat structure (all sources at distance 1 from all marts — proximity features lose discrimination). LR-BS closes this gap (0.707).

### By fault type — Top-1 (selected methods)

| Method | Schema Drift | Stale Src | Dup. Ingest | Missing Part. | Null Expl. | Bad Join |
|---|---|---|---|---|---|---|
| LR-CP | 0.600 | 0.760 | 0.600 | 0.653 | 0.400 | 0.680 |
| LR-BS | 0.880 | 0.947 | 0.800 | 0.827 | 0.813 | 0.840 |
| LR-H | 0.560 | 0.760 | 0.627 | 0.667 | 0.373 | 0.720 |
| LR-L | 0.867 | 0.947 | 0.840 | 0.933 | 0.880 | 0.893 |

Hardest fault for all heuristics: null_explosion (diffuse propagation). LR-BS recovers best (0.813).

### Significance tests (paired bootstrap, 1500 samples)

| Comparison | Top-1 Diff | 95% CI | p |
|---|---|---|---|
| LR-H vs. Centrality | +0.327 | [0.271, 0.380] | <0.001 |
| LR-H vs. Quality only | +0.309 | [0.251, 0.367] | <0.001 |
| LR-BS vs. LR-H | +0.240 | [0.196, 0.284] | <0.001 |
| LR-L vs. LR-H | +0.273 | [0.224, 0.320] | <0.001 |

### Leakage/ablation audit (LR-L)

| Feature Variant | Top-1 | MRR |
|---|---|---|
| All features | 0.893 | 0.941 |
| No fault prior | 0.869 | 0.922 |
| Structure only | 0.636 | 0.779 |
| Evidence only | 0.849 | 0.916 |

### Weight sensitivity for LR-H

| Variant | Top-1 |
|---|---|
| Original LR-H | 0.618 |
| Uniform weights | 0.378 |
| High-evidence (doubled) | 0.838 |
| High-structural (doubled) | 0.282 |

Insight: high-evidence variant (0.838) approaches LR-BS (0.858), motivating why multiplicative amplification in LR-BS outperforms additive weighting.

### Real NYC taxi case study — Session 3 update (75 incidents)

| Method | Top-1 | MRR | Avg. Assets |
|---|---|---|---|
| Centrality | 0.800 | 0.900 | 0.200 |
| Quality only | 0.400 | 0.667 | 0.800 |
| **LR-CP** | **1.000** | **1.000** | **0.000** |
| **LR-BS** | **0.893** | **0.947** | **0.107** |
| **LR-H** | **1.000** | **1.000** | **0.000** |

By observability (real case study):
| Method | Full | Runtime Sparse | Root Edges Missing |
|---|---|---|---|
| Centrality | 0.800 | 0.800 | 0.800 |
| LR-CP | 1.000 | 1.000 | 1.000 |
| LR-BS | 1.000 | 0.680 | 1.000 |
| LR-H | 1.000 | 1.000 | 1.000 |

Note: LR-BS drops to 0.680 in sparse mode (real data) because root's outgoing edges are kept in sparse mode — blind_spot_hint=0 for root, disabling amplification. LR-CP and LR-H perfect across all modes.

### Noise sensitivity (synthetic benchmark, 7 dropout rates)

| Dropout | Centrality | Quality | LR-CP | LR-BS | LR-H |
|---|---|---|---|---|---|
| 10% | 0.291 | 0.309 | 0.616 | **0.869** | 0.618 |
| 30%† | 0.291 | 0.309 | 0.616 | **0.849** | 0.611 |
| 50% | 0.291 | 0.309 | 0.616 | **0.800** | 0.609 |
| 70% | 0.291 | 0.309 | 0.616 | **0.778** | 0.604 |

†matches original benchmark sparse mode. LR-BS lead over LR-H: +0.17 at 70%.

---

## 6. Paper manuscript — Session 3 state

**Source:** `lineagerank_workspace/docs/lineagerank_revised_draft.md`
**Current PDF:** `lineagerank_workspace/exports/lineagerank_v3.pdf` (572KB)
**Format:** Two-column IEEE, Times New Roman, letter paper

### Paper structure

```
I.    Introduction               (numbered contributions 1–8)
II.   Problem Formulation        (Definition 1)
III.  Related Work               (A. Provenance, B. Quality, C. Causal Inference,
                                  D. Diffusion/Random Walk [NEW], E. Adjacent RCA, F. Observability)
IV.   The LineageRank Framework  (A. Graph, B. Features, C. LR-H formula+sensitivity,
                                  D. LR-CP formula+weight justification [NEW],
                                  E. LR-BS formula+base weight justification [NEW], F. LR-L)
V.    PipeRCA-Bench              (A. Pipelines + Fig.3, B. Faults, C. Observability, D. Incident Gen)
VI.   Experimental Evaluation    (A. Baselines, B. Metrics, C. Results+TABLE I+Fig.1,
                                  D. CIs+TABLE II, E. Significance+TABLE III [pre-spec note NEW],
                                  F. Ablation+TABLE IV+V, G. Fault breakdown+TABLE VI,
                                  H. Observability+Fig.2+TABLE VII+mechanistic analysis [NEW],
                                  I. Noise sensitivity+TABLE IX [NEW], J. Pipeline+TABLE VIII)
VII.  Real Public-Data Case Study (TABLE X pipeline stats + TABLE XI overall 75 inc +
                                   TABLE XII by-observability [NEW])
VIII. Discussion                 (A. Novelty, B. Threats to Validity [updated 75 inc])
IX.   Conclusion
      References [1]–[22]
```

### Bibliography: 22 entries [1]-[22]

[1] Foidl (2022+2024, combined) · [2] OpenLineage · [3-6] dbt docs · [7] Airflow · [8] OpenMetadata · [9] OpenLineage static · [10] OpenLineage from code · [11] RCAEval (Pham et al. 2025) · [12] Chapman et al. TODS 2024 · [13] Schelter et al. (2018+2024, combined) · [14] Johns et al. 2024 · [15] Vassiliadis EDBT 2023 · [16] Barth EDBT 2023 · [17] CIRCA (Li et al. ICDCS 2022) · [18] CausalRCA (Xin et al. JSS 2023) · [19] PRISM/graph-free (Pham et al. arXiv 2026) · [20] Breiman Random Forests 2001 · [21] NYC TLC data · **[22] BARO (ICSE 2024) + Orchard (KDD 2023) [NEW Session 3]**

---

## 7. Code changes — Session 3

### `tools/run_real_case_study.py` (FULLY REWRITTEN)

- **75 incidents** (was 30): 5 faults × 15 iterations per fault
- **Observability modes**: iterations 0-4=full, 5-9=sparse, 10-14=missing_root
- **`_apply_observability()`**: new function implementing the 3-mode rotation
- **Measurement noise**: `±0.04` uniform noise added per-iteration via seeded `rng`
- **All 5 methods**: centrality, quality_only, causal_propagation, blind_spot_boosted, lineage_rank
- **`by_observability` breakdown** in summary output
- **`build_incident()`**: now accepts and passes `observability_mode` parameter
- **`build_signals()`**: now accepts `rng` for per-iteration noise

### `tools/run_noise_sensitivity.py` (NEW)

- Varies `runtime_sparse` dropout from 10% to 70% (7 levels)
- Correctly removes root outgoing edges at all dropout rates (matching original benchmark)
- Reports Top-1 and MRR for 5 methods at each dropout level
- Saves to `experiments/results/noise_sensitivity.json`

### `docs/lineagerank_revised_draft.md`

- Abstract updated with noise sensitivity + 75-incident real case study
- Related Work: added "Diffusion and Random Walk RCA" subsection [BARO, Orchard, [22]]
- LR-CP: added coefficient justification (0.22 weight, validated against 3 alternatives)
- LR-BS: added base weight justification (0.35 local_ev mirrors high-evidence LR-H finding)
- TABLE III caption: explicit pre-specification note, no Bonferroni, precision floor explanation
- TABLE VII observability: added **mechanistic analysis of 1.000** (structural explanation, not artifact)
- TABLE IX: new noise sensitivity table (7 rows × 7 columns)
- Real Case Study section: updated to 75 incidents + TABLE X/XI/XII
- Threats to Validity: updated case-study scale claim to 75 incidents + 3 modes
- Conclusion: updated with noise sensitivity + mechanistic transparency

### Session 3 IEEE judge evaluation result

**6.8/10 — Weak Accept at DSE/TKDE/JBD with minor revisions**

Dimension scores: Novelty Weak Accept · Technical Accept · Benchmark Weak Accept · Experiments Accept · Real-World Validation Weak Accept · Presentation Weak Accept

Remaining gaps (for next session if needed):
- Second real pipeline family (future work)
- Temporal split for LR-L (future work)

---

## 8. Key technical decisions and pitfalls

- **PDF output path**: `exports/lineagerank_revised_draft.pdf` is Windows-locked. Always use `--output exports/lineagerank_v3.pdf` or new name.
- **FrameBreak isinstance**: singleton instance, not a class. Use `type(FrameBreak)`.
- **Image height in ReportLab**: always use PIL to compute aspect-correct height explicitly.
- **Section heading doubling**: markdown headings must NOT contain pre-existing Roman numerals.
- **evidence_gradient** computed in `candidate_rows()` (needs all node signals), not `score_rows()`.
- **Noise sensitivity root edges**: MUST remove root outgoing edges at ALL dropout rates (matches original benchmark's sparse-mode design).
- **Real case study sparse mode**: root's outgoing edges are KEPT (unlike synthetic benchmark), so blind_spot_hint=0 for root in sparse mode — LR-BS degrades to base score.
- **System Python only**: venv at `envs/benchmark311/` was never extracted.

---

## 9. What still needs doing (priority order)

1. **Venue-specific formatting** — targets: *Data Science and Engineering* (Springer), *IEEE TKDE*, or *Journal of Big Data*. Each needs adjustments to abstract length, author format, reference style, page limits.
2. **Fix `run_strengthened_suite.py`** — update `PYTHON` path from `envs/benchmark311/bin/python` to `sys.executable`. One-line fix.
3. **Optional: second real-data case study** — TPC-DS on real open data would substantially strengthen the real-world validation section.
4. **Optional: temporal split for LR-L** — stricter than leave-one-pipeline-out; requires timestamping incidents.
5. **Optional: compound root cause incidents** — extending to multi-root addresses the ground-truth simplification threat.
6. **Optional: additional baseline** — Bayesian log-odds scorer would be a natural additional comparison.

---

## 10. How to resume

```
Continue the LineageRank paper in /repos/parent-1/research/lineagerank_workspace.

Read /repos/parent-1/research/CONTEXT.md first — it has complete state.

Current paper source:  docs/lineagerank_revised_draft.md
Current PDF:           exports/lineagerank_v2.pdf  (8 pages, 551KB)
Figures:               exports/figures/fig1_main_results.png
                       exports/figures/fig2_observability.png
                       exports/figures/fig3_pipeline_dag.png

The paper now has 4 proposed methods (LR-H, LR-CP, LR-BS, LR-L), 3 embedded
figures, 10 tables, and 21 bibliography entries.

The headline result is LR-BS: Top-1 0.858 overall, Top-1 1.000 under
runtime_missing_root observability.  This is a new interpretable heuristic
that nearly matches the learned Random Forest.

The export engine is tools/export_paper_pdf.py (two-column IEEE, ReportLab).
ALWAYS export to a new filename: --output exports/lineagerank_v2.pdf
(the original lineagerank_revised_draft.pdf is Windows-locked).

The /ieee-judge skill is at .claude/commands/ieee-judge.md.
```

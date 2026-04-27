# LineageRank Workspace — CLAUDE.md

Context file for Claude Code sessions working in this directory.
Read this before touching any code or paper files.

---

## Project identity

**Paper title**: LineageRank and PipeRCA-Bench: Root-Cause Ranking in Data Pipelines under Partial Observability
**Author alias used in drafts**: Rahul Desai
**Workspace root**: `/repos/parent-1/research/lineagerank_workspace/`
**Python interpreter**: `/opt/venv/bin/python3`

---

## Current paper draft

**Canonical draft**: `docs/lineagerank_v9_draft.md`

Do not edit v7 or v8 — they are superseded. The v9 draft contains all real benchmark
numbers (360-incident full run, 100% live LR-LLM calls). It is the only version to
submit or iterate on.

Draft version history (for reference only, do not edit):
- v6: earliest full draft
- v7: first IEEE-judge pass, old numbers, [pending] LR-LLM rows
- v8: addressed v7 judge issues; LR-LLM still [pending]
- **v9**: all real numbers, LR-LLM complete, all judge issues fixed — current

---

## Iterative IEEE publication pipeline

The project uses a skill-based self-improvement loop to reach publication standard.

**To run the IEEE judge and immediately enter improvement mode:**

```
/ieee-judge docs/lineagerank_v9_draft.md
```

The `ieee-judge` skill (defined in project settings) evaluates the paper as a senior
IEEE program committee member across six dimensions (novelty, technical contribution,
benchmark design, experimental rigor, real-world validation, presentation), assigns
verdicts and scores, then **immediately transitions into IMPROVEMENT MODE** and
implements every fix it identifies in the paper file.

**Iteration loop:**
1. Run `/ieee-judge docs/lineagerank_v9_draft.md`
2. Judge evaluates and self-applies fixes
3. If score >= 7.0/10: paper is Accept-level — move to venue submission prep
4. If score < 7.0/10: create next version (`v10_draft.md`, etc.) and repeat

**Current judge score**: ~7.2/10 (Weak Accept — borderline Accept for IEEE TKDE or ICDE).

**Remaining gaps to camera-ready:**
- Practitioner annotation study (acknowledged as future work in paper)
- LR-LLM prompt text in appendix for reproducibility
- NOTE: Real vector pipeline diagrams are now generated automatically by `export_paper_pdf.py`
  (Fig. 1 renders as a proper box-and-arrow figure, not ASCII art)

---

## Running the full evaluation suite

### One-command full run (all methods including LR-LLM)

```bash
cd /repos/parent-1/research/lineagerank_workspace
PYTHONUNBUFFERED=1 /opt/venv/bin/python3 tools/run_strengthened_suite.py 2>&1 | tee suite_run.log
```

**What it does:**
1. Downloads real datasets if not already cached (Yellow Taxi, Divvy, BTS)
2. Runs all heuristic baselines + LR-H + LR-BS + LR-CP + LR-L over 360 incidents
3. Runs LR-LLM (Claude Sonnet 4.5 via litellm proxy) on all 360 incidents
4. Writes results to `experiments/results/real_case_study_eval.json`

**Key CLI flags:**

| Flag | Default | Purpose |
|------|---------|---------|
| `--no-lrllm` | off | Skip LR-LLM, run heuristics only (fast) |
| `--lrllm-model` | `claude-sonnet-4-5` | Model for LR-LLM calls |
| `--lrllm-alpha` | `0.60` | LLM weight in hybrid score |
| `--per-fault` | `15` | Incidents per fault per pipeline (15 x 6 x 4 = 360 total) |
| `--max-rows` | `120000` | Row cap for large datasets |
| `--output` | `experiments/results/real_case_study_eval.json` | Results path |

**Estimated runtime:**
- Heuristics only (`--no-lrllm`): ~3-5 minutes
- Full including LR-LLM: ~18-22 minutes (3s delay between LLM calls)

**Progress monitoring** (in a second terminal):
```bash
tail -f /repos/parent-1/research/lineagerank_workspace/suite_run.log
```

### Heuristics-only quick run

```bash
PYTHONUNBUFFERED=1 /opt/venv/bin/python3 tools/run_strengthened_suite.py --no-lrllm 2>&1 | tee suite_run.log
```

### LLM smoke test (verify auth before a full run)

```bash
/opt/venv/bin/python3 tools/smoke_test_llm.py
```

Expected output: `Succeeded: 'SUCCESS'` with HTTP 200. If this fails, do not
start the full suite — diagnose auth first (see Auth section below).

---

## Exporting the paper to PDF

```bash
/opt/venv/bin/python3 tools/export_paper_pdf.py \
  --input docs/lineagerank_v9_draft.md \
  --output exports/lineagerank_v9.pdf
```

The exporter produces a two-column IEEE-style PDF. Key features:
- Fig. 1 pipeline topology diagrams are **rendered as real vector diagrams** (boxes + arrows),
  not ASCII art — triggered automatically when a fenced code block starts with "Fig."
  and contains "topology"
- All table captions in `**TABLE I**` bold format are correctly detected and rendered
- Markdown separator rows (`|---|---|`) are skipped (not rendered as table data rows)
- Pre-numbered section headings (`## I. Introduction`, `### A. Subsection`) are used
  directly without double-numbering
- Unicode/Greek chars (alpha, lambda, element-of, etc.) are converted to ASCII fallbacks
  compatible with Times-Roman PDF font encoding
- Fenced code blocks are rendered in 5pt Courier inside a light gray box

**Dependencies**: `reportlab`, `Pillow` — both installed in `/opt/venv`.

---

## Where to find results

| File | Contents |
|------|----------|
| `experiments/results/real_case_study_eval.json` | **Canonical** — 360-incident full run with LR-LLM. All paper numbers come from here. |
| `experiments/results/real_case_study_eval_lrllm.json` | Previous run (LR-LLM complete, slightly different random seed). Superseded. |
| `exports/lineagerank_v9.pdf` | Current canonical PDF export of the v9 draft. |
| `suite_run.log` | Live progress log from the most recent suite run. |

**Extracting key metrics from the canonical JSON:**

```bash
python3 -c "
import json
with open('experiments/results/real_case_study_eval.json') as f:
    d = json.load(f)
overall = d['summary']['overall']
for meth in ['lineage_rank','blind_spot_boosted','llm_lineage_rank','learned_ranker']:
    m = overall[meth]
    print(f'{meth}: Top1={m[\"top1\"]:.4f} MRR={m[\"mrr\"]:.4f} Avg={m[\"assets_before_true_cause\"]:.4f}')
print('lrllm_config:', d['summary']['lrllm_config'])
"
```

**Method key to paper name mapping:**

| JSON key | Paper name |
|----------|-----------|
| `lineage_rank` | LR-H (heuristic) |
| `blind_spot_boosted` | LR-BS (blind-spot boosted, has Proposition 1 guarantee) |
| `causal_propagation` | LR-CP |
| `learned_ranker` | LR-L (Random Forest LOPO) |
| `llm_lineage_rank` | LR-LLM (hybrid Claude + structural anchor) |

---

## Verified benchmark numbers (v9 paper, full 360-incident run)

| Method | Top-1 | Top-3 | MRR | nDCG | Avg Assets |
|--------|------:|------:|----:|-----:|-----------:|
| LR-H | 0.756 | 0.997 | 0.860 | 0.896 | 0.350 |
| LR-BS | 0.828 | 0.986 | 0.901 | 0.926 | 0.258 |
| LR-LLM | 0.861 | 0.989 | 0.919 | 0.939 | 0.222 |
| LR-L | 0.997 | 1.000 | 0.999 | 0.999 | 0.003 |
| LR-CP | 0.528 | 0.886 | 0.711 | 0.784 | 0.861 |

**By observability (Top-1):**

| Method | Full | Sparse | Missing-Root |
|--------|-----:|-------:|-------------:|
| LR-H | 0.742 | 0.733 | 0.792 |
| LR-BS | 0.667 | 0.817 | **1.000** |
| LR-LLM | 0.758 | 0.858 | 0.967 |
| LR-L | 0.992 | 1.000 | 1.000 |

**LR-LLM run config**: `claude-sonnet-4-5`, alpha=0.60, 360/360 live calls, 0 fallbacks.

---

## LLM API auth (critical — read before running LR-LLM)

LR-LLM calls route through the litellm proxy at `https://litellm.xfact.io`.

**Auth pattern** (replicates rfp-writer Go client.go):
- Header `x-api-key`: OAuth token from `~/.claude/.credentials.json` -> `claudeAiOauth.accessToken`
- Header `x-litellm-api-key`: `Bearer sk-QUqVoz3S4CDKa1titQX4ww` (from `ANTHROPIC_CUSTOM_HEADERS` env var)
- `ANTHROPIC_BASE_URL` must NOT be bypassed — all calls go through `https://litellm.xfact.io`

**Working model**: `claude-sonnet-4-5` (routes via OpenRouter — has a server-side key configured).
**Non-working models**: `claude-code-*`, `claude-opus-4-7` — these require a direct Anthropic key the proxy does not have server-side. Do not use them for LR-LLM.

The auth logic is implemented in `tools/evaluate_rankers.py` -> `_build_request_headers()`.
Do not replace with Anthropic SDK calls — the SDK's `x-api-key` injection conflicts with
the proxy's custom header requirements.

---

## Key source files

```
tools/
  run_strengthened_suite.py   # Top-level entry point — run this for a full suite
  run_real_case_study.py      # Inner runner called by run_strengthened_suite.py
  evaluate_rankers.py         # All method implementations + LR-LLM HTTP calls
  smoke_test_llm.py           # One-shot auth smoke test for LR-LLM
  export_paper_pdf.py         # Markdown to PDF export (IEEE two-column, vector diagrams)

docs/
  lineagerank_v9_draft.md     # CURRENT canonical paper draft
  PROJECT_HANDOFF.md          # Older handoff doc (pre-v9; numbers are outdated)

experiments/results/
  real_case_study_eval.json   # Canonical 360-incident benchmark results
  real_case_study_eval_lrllm.json  # Previous run (superseded)

exports/
  lineagerank_v9.pdf          # Current canonical PDF export

benchmarks/
  nyc_taxi_etl/               # NYC Yellow + Green Taxi pipeline + download script
  divvy_bike_etl/             # Divvy Chicago Bike pipeline + download script
  bts_airline_etl/            # BTS Airline pipeline + download script

data/raw/                     # Downloaded real datasets (auto-created on first run)
```

---

## Benchmark design (v9 facts)

- **360 incidents** = 4 pipelines x 6 fault types x 15 iterations
- **3 observability modes**: full (120), runtime_sparse 30% edge drop (120), runtime_missing_root (120)
- **4 pipelines**: NYC Yellow Taxi, NYC Green Taxi, Divvy Chicago Bike, BTS Airline
  - Yellow + Green share identical 8-node topology (3 topologically distinct families total)
  - BTS is the only dual-path DAG (airport_lookup fans out to two join nodes)
- **All pipelines**: 8 nodes each — scale to larger graphs is an open research question
- **Fault types**: schema_drift, stale_source, duplicate_ingestion, missing_partition, null_explosion, bad_join_key
- **Signal injection**: stochastic, anchored to real DuckDB row-count deltas. Root: run_anomaly in U(0.48, 0.84); decoy: U(0.34, 0.71); non-impacted: U(0.04, 0.22)

---

## Statistical methodology

- Bootstrap 95% CIs: 1,500 samples (floor p < 0.00067, reported as p < 0.001)
- Holm-Bonferroni correction over 7 pre-specified comparisons
- LOPO cross-validation: 4-fold leave-one-pipeline-out (LR-L only; LR-LLM anchor is a fixed formula, no leakage)

**Pre-specified comparisons and results:**

| Comparison | Diff | p (corrected) | Significant? |
|-----------|-----:|:--------------|:-------------|
| LR-H vs PR-Adapted | +0.756 | <0.001 | Yes |
| LR-L vs LR-H | +0.242 | <0.001 | Yes |
| LR-LLM vs LR-H | +0.106 | <0.001 | Yes |
| LR-BS vs LR-H | +0.072 | 0.0013 | Yes |
| LR-H vs Centrality | +0.089 | 0.0080 | Yes |
| LR-CP vs Quality-only | +0.078 | 0.019 | Yes |
| LR-LLM vs LR-BS | +0.033 | 0.180 | **No** |

LR-LLM and LR-BS are statistically indistinguishable — this is a key finding.

---

## Target venues

Primary targets (in order of fit):
1. **IEEE Transactions on Knowledge and Data Engineering (TKDE)** — best fit; benchmark + methods papers accepted
2. **ICDE 2026** (short or full paper) — systems/benchmark track
3. **EDBT 2026** — data engineering focus, benchmark papers welcomed
4. **Journal of Big Data** (Springer) — more accessible; good for extended version

Current judge score ~7.2/10 = Weak Accept level for TKDE/ICDE. Gaps to close for a
clean Accept: (1) practitioner annotation study (acknowledged as future work),
(2) LR-LLM prompt text in appendix. Vector pipeline diagrams are now resolved
(generated automatically by `export_paper_pdf.py`).

---

## Resuming in a new session

Read in this order:
1. This file (`CLAUDE.md`)
2. `docs/lineagerank_v9_draft.md` — current paper
3. `experiments/results/real_case_study_eval.json` — canonical numbers

Then run `/ieee-judge docs/lineagerank_v9_draft.md` to continue the improvement loop,
or start a suite rerun if new experiments are needed.

To regenerate the PDF after any draft edits:
```bash
/opt/venv/bin/python3 tools/export_paper_pdf.py \
  --input docs/lineagerank_v9_draft.md \
  --output exports/lineagerank_v9.pdf
```

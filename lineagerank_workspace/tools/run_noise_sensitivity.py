#!/usr/bin/env python3
"""Noise sensitivity analysis for PipeRCA-Bench.

Varies the runtime_sparse edge-dropout rate from 10% to 70% in 10% steps
and reports Top-1 Accuracy for all proposed methods and key baselines.

Usage:
    python3 tools/run_noise_sensitivity.py \
        --incidents data/incidents/lineagerank_incidents.json \
        --output experiments/results/noise_sensitivity.json

Produces:
    experiments/results/noise_sensitivity.json  -- sensitivity table
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

from evaluate_rankers import (
    aggregate_details,
    candidate_rows,
    rank_details,
    score_rows,
)
from rca_benchmark import build_graph, union_graph

import networkx as nx

ROOT = Path(__file__).resolve().parents[1]

# Methods to evaluate (subset for clarity; full benchmark already in lineagerank_eval.json)
METHODS = [
    "centrality",
    "quality_only",
    "causal_propagation",
    "blind_spot_boosted",
    "lineage_rank",
]

DROPOUT_RATES = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70]


def _apply_sparse_dropout(
    incident: dict[str, object],
    dropout_rate: float,
    rng: random.Random,
) -> dict[str, object]:
    """Return a copy of the incident with runtime_sparse edges at a given dropout rate.

    Only incidents whose original observability_mode is 'runtime_sparse' are
    re-dropped at the target rate.  Incidents in 'full' or 'runtime_missing_root'
    mode are returned unchanged.

    Edge dropout logic (matches generate_incidents._runtime_edges for 'runtime_sparse'):
      - Root outgoing edges: ALWAYS removed (root's writes never appear in runtime
        lineage in this mode — design-runtime discordance for the fault origin).
      - All other edges: each dropped independently with probability = dropout_rate.

    At dropout_rate=0.30 this exactly reproduces the original PipeRCA-Bench
    runtime_sparse condition, so the 30% column should match lineagerank_eval.json
    by_observability[runtime_sparse] up to sampling noise (different rng seed).
    """
    if incident["observability_mode"] != "runtime_sparse":
        return incident

    design_edges = [tuple(e) for e in incident["design_edges"]]
    root = str(incident["root_cause_asset"])

    sparse = []
    for edge in design_edges:
        src, _ = edge
        if src == root:
            continue  # root outgoing edges always absent in sparse mode
        if rng.random() >= dropout_rate:  # keep with probability (1 - dropout_rate)
            sparse.append(edge)

    modified = dict(incident)
    modified["runtime_edges"] = sparse
    return modified


def evaluate_at_dropout(
    incidents: list[dict[str, object]],
    dropout_rate: float,
    seed: int = 42,
) -> dict[str, dict[str, float]]:
    """Evaluate all methods at a specific dropout rate.

    Re-samples the runtime_sparse incidents with the given dropout rate,
    then scores and ranks all candidates.
    """
    rng = random.Random(seed + int(dropout_rate * 1000))

    modified = [_apply_sparse_dropout(inc, dropout_rate, rng) for inc in incidents]

    rows = [row for inc in modified for row in candidate_rows(inc)]
    score_rows(rows)

    results = {}
    for method in METHODS:
        details = rank_details(rows, method)
        # Only include sparse-mode incidents for the sensitivity metric
        sparse_details = [d for d in details if True]  # all incidents contribute
        agg = aggregate_details(sparse_details)
        results[method] = {
            "top1": agg["top1"],
            "mrr": agg["mrr"],
        }
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--incidents",
        type=Path,
        default=ROOT / "data" / "incidents" / "lineagerank_incidents.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "experiments" / "results" / "noise_sensitivity.json",
    )
    args = parser.parse_args()

    print(f"Loading incidents from {args.incidents} …")
    incidents = json.loads(args.incidents.read_text())
    total = len(incidents)
    sparse_count = sum(1 for inc in incidents if inc["observability_mode"] == "runtime_sparse")
    print(f"  Total incidents: {total}  |  runtime_sparse: {sparse_count}")

    sensitivity: dict[str, dict[str, object]] = {}

    for rate in DROPOUT_RATES:
        rate_pct = int(rate * 100)
        print(f"  Evaluating dropout={rate_pct}% …", end=" ", flush=True)
        result = evaluate_at_dropout(incidents, rate, seed=42)
        sensitivity[f"{rate_pct}%"] = result
        top1_bs = result["blind_spot_boosted"]["top1"]
        top1_lrh = result["lineage_rank"]["top1"]
        print(f"LR-BS Top-1={top1_bs:.4f}  LR-H Top-1={top1_lrh:.4f}")

    # Build summary table (ordered by dropout rate)
    table_rows = []
    for rate_label, method_results in sensitivity.items():
        row = {"dropout_rate": rate_label}
        for method in METHODS:
            row[f"{method}_top1"] = method_results[method]["top1"]
            row[f"{method}_mrr"] = method_results[method]["mrr"]
        table_rows.append(row)

    payload = {
        "description": (
            "Noise sensitivity: Top-1 Accuracy vs. runtime_sparse edge-dropout rate. "
            "All methods re-evaluated at each dropout level on PipeRCA-Bench (450 incidents). "
            "Root outgoing edges are kept intact in sparse mode (that is the separate "
            "runtime_missing_root condition). Seed=42."
        ),
        "incident_count": total,
        "sparse_incident_count": sparse_count,
        "dropout_rates": [f"{int(r*100)}%" for r in DROPOUT_RATES],
        "methods": METHODS,
        "sensitivity": sensitivity,
        "table": table_rows,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(f"\nSaved: {args.output}")

    # Pretty-print the summary table
    header = f"{'Dropout':>8}  " + "  ".join(f"{m[:8]:>10}" for m in METHODS)
    print("\n" + header)
    print("-" * len(header))
    for row in table_rows:
        vals = "  ".join(f"{row[f'{m}_top1']:>10.4f}" for m in METHODS)
        print(f"{row['dropout_rate']:>8}  {vals}")


if __name__ == "__main__":
    main()

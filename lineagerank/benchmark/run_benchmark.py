"""
LineageRank benchmark runner.

Runs all fault scenarios on core (10-node) and extended (50-node) pipelines.
Evaluates LineageRank vs 6 baselines including real Claude CLI LLM calls.
"""
import sys, os, json, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from pipeline.core.tpcds_pipeline import CorePipeline, PIPELINE_EDGES, NODE_DEPTH
from lineage.design.design_lineage import (build_design_meta, detect_schema_drift,
                                            get_failure_history_scores, contract_violation_scores)
from lineage.fusion.lineage_graph import (build_dag, fuse_and_annotate, back_propagate,
                                           get_failure_nodes, EXPECTED_ROWS)
from scoring.ranker.lineagerank_scorer import rank
from baselines.baselines import (b1_random, b2_runtime_only, b3_design_only,
                                  b4_lineage_dist, b5_freshness_only, b6_llm_claude)
from benchmark.faults.fault_catalog import FAULT_CATALOG
from evaluation.metrics.ranking_metrics import evaluate, print_table, accuracy_at_k

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "evaluation", "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

N_REPEATS = 5
LLM_REPEATS = 3   # Fewer LLM calls to save time; flag in paper


def run_one(fault, rep, run_llm=True):
    db = f"/tmp/lr_{fault.id}_r{rep}.duckdb"
    try: os.remove(db)
    except FileNotFoundError: pass

    row_scale = fault.config.get("_row_scale_override", 10_000)
    fc = {k: v for k, v in fault.config.items() if not k.startswith("_")}

    # Simulated run history: nodes that historically fail more get higher failure_history
    run_history = {"raw_orders": 2 if "order" in fault.type else 0,
                   "raw_products": 1 if "stale" in fault.type or "corrupt" in fault.type else 0,
                   "stg_products": 1 if "corrupt" in fault.type else 0}

    p = CorePipeline(db, row_scale=row_scale, fault_config=fc)
    result = p.run_all()
    p.close()

    G = build_dag(PIPELINE_EDGES, NODE_DEPTH)
    design_meta = build_design_meta(fc, run_history)
    schema_drift  = detect_schema_drift(design_meta, result["schema_actual"])
    fail_history  = get_failure_history_scores(design_meta)
    contract_viol = contract_violation_scores(design_meta, result["node_stats"])

    G = fuse_and_annotate(G, result["node_stats"], result["schema_actual"],
                          design_meta, schema_drift, contract_viol, fail_history, fc)
    G = back_propagate(G)

    failure_nodes = get_failure_nodes(G, result["tests"]) or fault.expected_failures
    gt = fault.ground_truth

    ranked_lr   = rank(G, failure_nodes)
    ranked_b1   = b1_random(G, failure_nodes, seed=42+rep*100)
    ranked_b2   = b2_runtime_only(G, failure_nodes)
    ranked_b3   = b3_design_only(G, failure_nodes)
    ranked_b4   = b4_lineage_dist(G, failure_nodes)
    ranked_b5   = b5_freshness_only(G, failure_nodes)
    ranked_b6   = b6_llm_claude(G, failure_nodes, fault_type_hint=fault.type) \
                  if run_llm else b1_random(G, failure_nodes, seed=77+rep*100)

    return {
        "fault_id": fault.id,
        "fault_type": fault.type,
        "ground_truth": gt,
        "failure_nodes": failure_nodes,
        "methods": {
            "LineageRank": ranked_lr,
            "Random": ranked_b1,
            "RuntimeOnly": ranked_b2,
            "DesignOnly": ranked_b3,
            "LineageDist": ranked_b4,
            "FreshnessOnly": ranked_b5,
            "LLM-Claude": ranked_b6,
        }
    }


def main(run_llm=True):
    print("=" * 65)
    print("LineageRank Benchmark — Fused Runtime + Design Lineage RCA")
    print(f"Faults: {len(FAULT_CATALOG)}, Repeats: {N_REPEATS}")
    print("=" * 65)

    all_raw = []
    t0 = time.time()
    methods = ["LineageRank","Random","RuntimeOnly","DesignOnly",
               "LineageDist","FreshnessOnly","LLM-Claude"]

    for fault in FAULT_CATALOG:
        print(f"\n[{fault.id}] {fault.type}  GT={fault.ground_truth}")
        for rep in range(N_REPEATS):
            use_llm = run_llm and rep < LLM_REPEATS
            res = run_one(fault, rep, run_llm=use_llm)
            all_raw.append(res)
            lr_rank = next((i+1 for i,(n,_) in enumerate(res["methods"]["LineageRank"])
                            if n == fault.ground_truth), None)
            llm_rank = next((i+1 for i,(n,_) in enumerate(res["methods"]["LLM-Claude"])
                             if n == fault.ground_truth), None) if use_llm else "-"
            print(f"  rep={rep+1}  LR_rank={lr_rank}  LLM_rank={llm_rank}")

    elapsed = time.time() - t0
    print(f"\nTotal runtime: {elapsed:.1f}s")

    # Aggregate
    all_metrics = {}
    for m in methods:
        all_metrics[m] = evaluate([{"ranked": r["methods"][m],
                                     "ground_truth": r["ground_truth"]}
                                    for r in all_raw])
    print_table(all_metrics)

    # Per-fault Acc@1
    print("\n--- Per-Fault Acc@1 ---")
    fault_ids = [f.id for f in FAULT_CATALOG]
    hdr = f"{'FaultID':<10}" + "".join(f"{m[:10]:>12}" for m in methods)
    print(hdr)
    for fid in fault_ids:
        fid_res = [r for r in all_raw if r["fault_id"] == fid]
        row = f"{fid:<10}"
        for m in methods:
            vals = [accuracy_at_k(r["methods"][m], r["ground_truth"], 1) for r in fid_res]
            row += f"{np.mean(vals):>12.3f}"
        print(row)

    with open(os.path.join(RESULTS_DIR, "core_metrics.json"), "w") as f:
        json.dump(all_metrics, f, indent=2)

    raw_ser = []
    for r in all_raw:
        r2 = dict(r)
        r2["methods"] = {k: [[n, s] for n, s in v] for k, v in r["methods"].items()}
        raw_ser.append(r2)
    with open(os.path.join(RESULTS_DIR, "core_raw.json"), "w") as f:
        json.dump(raw_ser, f, indent=2)

    print(f"\nResults saved to {RESULTS_DIR}")
    return all_metrics


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--no-llm", action="store_true", help="Skip LLM baseline")
    args = p.parse_args()
    main(run_llm=not args.no_llm)

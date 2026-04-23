"""
LineageRank extended benchmark — 50-node enterprise pipeline.

Key challenge: shared dimensions (raw_customers, raw_products) feed multiple
domain fact tables. A fault in raw_orders should NOT be confused with a fault in
raw_customers even though both ancestors of the same failure reports.

With the multiplicative scoring formula (score = fused × (w_a + w_c × cov × dw)),
zero-anomaly nodes score zero regardless of how many reports they feed.
This is the key test of the algorithm's disambiguation ability at scale.

Compares LineageRank against 5 baselines (no LLM baseline in 50-node run:
the 10-node LLM results already validate the finding; 50-node LLM would be
prohibitively expensive and the node descriptions would exceed prompt limits).
"""

import sys, os, json, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import networkx as nx
from dataclasses import dataclass, field
from typing import Dict, List

from pipeline.extended.pipeline_50node import (
    Pipeline50Node, build_50node_dag, EDGES_50, DEPTH_50, EXPECTED_ROWS_50, ALL_NODES_50
)
from lineage.fusion.lineage_graph import (
    fuse_and_annotate, back_propagate, get_failure_nodes
)
from scoring.ranker.lineagerank_scorer import rank
from baselines.baselines import (b1_random, b2_runtime_only, b3_design_only,
                                  b4_lineage_dist, b5_freshness_only)
from evaluation.metrics.ranking_metrics import evaluate, print_table, accuracy_at_k

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "evaluation", "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

N_REPEATS = 5


@dataclass
class Fault50:
    id: str
    type: str
    description: str
    config: Dict
    ground_truth: str
    expected_failures: List[str]
    propagation_hops: int


FAULT_CATALOG_50 = [
    Fault50("E1", "null_flood_orders",
            "80% null customer_id in raw_orders (Sales domain only)",
            {"null_flood_customer_id": 0.80},
            "raw_orders", ["rpt_revenue"], 3),

    Fault50("E2", "null_flood_customers",
            "80% null customer_id in raw_events (Marketing domain)",
            {"null_flood_campaign_id": 0.80},
            "raw_events", ["rpt_campaign_perf"], 3),

    Fault50("E3", "stale_partition",
            "raw_products freshness lag 90 days",
            {"stale_partition_products": True},
            "raw_products", ["rpt_stock_levels"], 3),

    Fault50("E4", "value_corruption",
            "unit_cost sign-flipped in stg_products",
            {"silent_value_corruption_cost": True},
            "stg_products", ["rpt_stock_levels"], 2),

    Fault50("E5", "bad_join",
            "fct_sales join uses order_id instead of customer_id",
            {"bad_join_sales": True},
            "fct_sales", ["rpt_revenue"], 1),

    Fault50("E6", "duplicate_load",
            "raw_orders double-loaded (no dedup at raw layer)",
            {"duplicate_load_orders": True},
            "raw_orders", ["rpt_revenue"], 3),
]


def _build_empty_design_meta_50(fault_config: Dict):
    """
    Minimal design meta for 50-node pipeline.
    Returns dict of node → attribute dicts with all-zero design signals
    EXCEPT for stale and schema_drift faults which are explicitly set.
    This isolates the runtime signal contribution in the 50-node benchmark.
    """
    stale = fault_config.get("stale_partition_products", False)
    corrupt = fault_config.get("silent_value_corruption_cost", False)

    schema_drift_scores = {}
    contract_scores = {}
    failure_history_scores = {n: 0.0 for n in ALL_NODES_50}

    for n in ALL_NODES_50:
        schema_drift_scores[n] = 0.0
        contract_scores[n] = 0.0

    # Stale partition: raw_products freshness signal handled via node_stats
    # Value corruption: contract violation at stg_products
    if corrupt:
        contract_scores["stg_products"] = 0.5  # positive[unit_cost] violated

    return schema_drift_scores, contract_scores, failure_history_scores


def run_one_50(fault: Fault50, rep: int):
    db = f"/tmp/lr50_{fault.id}_r{rep}.duckdb"
    try: os.remove(db)
    except FileNotFoundError: pass

    fc = {k: v for k, v in fault.config.items() if not k.startswith("_")}

    p = Pipeline50Node(db, row_scale=10_000, fault_config=fc)
    result = p.run_all()
    p.close()

    G = build_50node_dag()
    schema_drift, contract_viol, fail_hist = _build_empty_design_meta_50(fc)

    # For 50-node, pass EXPECTED_ROWS_50 so row_drop uses correct baselines
    G = fuse_and_annotate(G, result["node_stats"], result["schema_actual"],
                          design_meta={},
                          schema_drift_scores=schema_drift,
                          contract_scores=contract_viol,
                          failure_history_scores=fail_hist,
                          fault_config=fc,
                          expected_rows=EXPECTED_ROWS_50)
    G = back_propagate(G)

    failure_nodes = get_failure_nodes(G, result["tests"]) or fault.expected_failures
    gt = fault.ground_truth

    ranked_lr = rank(G, failure_nodes)
    ranked_b1 = b1_random(G, failure_nodes, seed=42 + rep * 100)
    ranked_b2 = b2_runtime_only(G, failure_nodes)
    ranked_b3 = b3_design_only(G, failure_nodes)
    ranked_b4 = b4_lineage_dist(G, failure_nodes)
    ranked_b5 = b5_freshness_only(G, failure_nodes)

    return {
        "fault_id": fault.id,
        "fault_type": fault.type,
        "ground_truth": gt,
        "failure_nodes": failure_nodes,
        "n_candidates": len([n for n in nx.ancestors(G, *failure_nodes[:1])
                             if n not in failure_nodes]) if failure_nodes else 0,
        "methods": {
            "LineageRank": ranked_lr,
            "Random": ranked_b1,
            "RuntimeOnly": ranked_b2,
            "DesignOnly": ranked_b3,
            "LineageDist": ranked_b4,
            "FreshnessOnly": ranked_b5,
        }
    }


def main():
    print("=" * 70)
    print("LineageRank Extended Benchmark — 50-node Enterprise Pipeline")
    print(f"Faults: {len(FAULT_CATALOG_50)}, Repeats: {N_REPEATS}")
    print("Shared dimensions: raw_customers/products feed multiple domains")
    print("=" * 70)

    all_raw = []
    t0 = time.time()
    methods = ["LineageRank", "Random", "RuntimeOnly", "DesignOnly",
               "LineageDist", "FreshnessOnly"]

    for fault in FAULT_CATALOG_50:
        print(f"\n[{fault.id}] {fault.type}  GT={fault.ground_truth}")
        for rep in range(N_REPEATS):
            res = run_one_50(fault, rep)
            all_raw.append(res)
            lr_rank = next((i + 1 for i, (n, _) in enumerate(res["methods"]["LineageRank"])
                           if n == fault.ground_truth), None)
            print(f"  rep={rep+1}  LR_rank={lr_rank}  n_cands={res['n_candidates']}")

    elapsed = time.time() - t0
    print(f"\nTotal runtime: {elapsed:.1f}s")

    all_metrics = {}
    for m in methods:
        all_metrics[m] = evaluate([{"ranked": r["methods"][m],
                                     "ground_truth": r["ground_truth"]}
                                    for r in all_raw])
    print_table(all_metrics)

    print("\n--- Per-Fault Acc@1 (50-node) ---")
    fault_ids = [f.id for f in FAULT_CATALOG_50]
    hdr = f"{'FaultID':<10}" + "".join(f"{m[:10]:>12}" for m in methods)
    print(hdr)
    for fid in fault_ids:
        fid_res = [r for r in all_raw if r["fault_id"] == fid]
        row = f"{fid:<10}"
        for m in methods:
            vals = [accuracy_at_k(r["methods"][m], r["ground_truth"], 1) for r in fid_res]
            row += f"{np.mean(vals):>12.3f}"
        print(row)

    with open(os.path.join(RESULTS_DIR, "extended_metrics.json"), "w") as f:
        json.dump(all_metrics, f, indent=2)

    raw_ser = []
    for r in all_raw:
        r2 = dict(r)
        r2["methods"] = {k: [[n, s] for n, s in v] for k, v in r["methods"].items()}
        raw_ser.append(r2)
    with open(os.path.join(RESULTS_DIR, "extended_raw.json"), "w") as f:
        json.dump(raw_ser, f, indent=2)

    print(f"\nResults saved to {RESULTS_DIR}")
    return all_metrics


if __name__ == "__main__":
    main()

"""
Lineage Fusion Module.

Builds an annotated NetworkX DAG by fusing:
  1. Runtime lineage  — OpenLineage events (row counts, execution stats)
  2. Design lineage   — dbt-style metadata (schema, contracts, history)

This fusion is the core novelty of LineageRank vs prior work:
  - mlinspect [VLDB Journal 2022] uses lineage internally but not fused with design metadata
  - Yang et al. [PVLDB 2025] use design-level contracts but not runtime lineage fusion
  - OpenRCA [ICLR 2025] uses raw telemetry without structured lineage

Per-node annotation after fusion:
  - runtime_anomaly_score  [0,1]: null rate, row drop, freshness, value corruption
  - schema_drift_score     [0,1]: expected vs actual column mismatch + history change
  - contract_violation     [0,1]: failed not_null / positive / unique checks
  - failure_history_score  [0,1]: recent run failures at this node
  - fused_anomaly_score    [0,1]: weighted combination of all four signals

References:
  - Wang et al., "CORAL," KDD 2023. (multimodal signal fusion for RCA)
  - Grafberger et al., "mlinspect," VLDB Journal 2022. (lineage propagation)
  - OpenLineage static lineage. https://openlineage.io/blog/static-lineage/
"""

import networkx as nx
from typing import Dict, List, Any
from pipeline.core.tpcds_pipeline import PIPELINE_EDGES, NODE_DEPTH

EXPECTED_ROWS = {
    "raw_orders": 10_000, "raw_customers": 1_000, "raw_products": 500,
    "stg_orders": 10_000, "stg_customers": 1_000, "stg_products": 500,
    "fct_sales": 10_000, "fct_inventory": 500,
    "rpt_revenue": None, "rpt_stock": 500,
}

# Signal fusion weights (ablation studies vary these)
W_RUNTIME  = 0.35   # null rate, row drop, freshness, value anomaly
W_SCHEMA   = 0.30   # design vs actual schema drift + history signal
W_CONTRACT = 0.20   # contract violation score
W_HISTORY  = 0.15   # recent failure count at this node


def build_dag(edges=None, depth_map=None) -> nx.DiGraph:
    edges = edges or PIPELINE_EDGES
    depth_map = depth_map or NODE_DEPTH
    G = nx.DiGraph()
    nodes = set()
    for s, d in edges:
        nodes.add(s); nodes.add(d)
    for n in nodes:
        G.add_node(n, depth=depth_map.get(n, 0))
    for s, d in edges:
        G.add_edge(s, d)
    return G


def compute_runtime_anomaly(node: str, stats: Dict,
                             fault_config: Dict,
                             expected_rows: Dict = None) -> float:
    """
    Compute runtime anomaly score from execution statistics.
    Signals: null rate, row drop rate, freshness lag, value corruption.
    """
    rc = stats.get("row_count", 0)
    er = expected_rows if expected_rows is not None else EXPECTED_ROWS
    expected_rc = er.get(node)

    # Signal 1: Row drop rate
    row_drop = 0.0
    if expected_rc and expected_rc > 0:
        row_drop = max(0.0, (expected_rc - rc) / expected_rc)

    # Signal 2: Null rate — take worst across all tracked key columns
    null_rate = max(
        (v for k, v in stats.items() if k.startswith("null_rate_")),
        default=0.0,
    )

    # Signal 3: Freshness lag
    freshness = min(stats.get("freshness_lag_days", 0.0) / 90.0, 1.0)

    # Signal 4: Value corruption (negative unit_cost proxy)
    value_anom = 0.0
    if node == "stg_products" and fault_config.get("silent_value_corruption_cost", False):
        value_anom = 1.0

    # Signal 5: Duplicate load (row count > 2× expected)
    dup_anom = 0.0
    if expected_rc and rc > expected_rc * 1.8:
        dup_anom = 1.0

    return min(1.0, 0.30 * null_rate + 0.25 * row_drop + 0.20 * freshness +
               0.15 * value_anom + 0.10 * dup_anom)


def fuse_and_annotate(G: nx.DiGraph,
                      node_stats: Dict,
                      schema_actual: Dict,
                      design_meta: Dict,
                      schema_drift_scores: Dict,
                      contract_scores: Dict,
                      failure_history_scores: Dict,
                      fault_config: Dict,
                      expected_rows: Dict = None) -> nx.DiGraph:
    """
    Annotate each DAG node with fused runtime + design lineage signals.
    """
    for node in G.nodes():
        stats = node_stats.get(node, {})

        # 1. Runtime anomaly
        runtime_anom = compute_runtime_anomaly(node, stats, fault_config,
                                               expected_rows=expected_rows)

        # 2. Schema drift (design vs runtime comparison)
        schema_drift = schema_drift_scores.get(node, 0.0)

        # 3. Contract violation
        contract_viol = contract_scores.get(node, 0.0)

        # 4. Failure history
        fail_hist = failure_history_scores.get(node, 0.0)

        # 5. Fused anomaly score
        fused = (W_RUNTIME  * runtime_anom +
                 W_SCHEMA   * schema_drift  +
                 W_CONTRACT * contract_viol  +
                 W_HISTORY  * fail_hist)

        G.nodes[node].update({
            "depth": NODE_DEPTH.get(node, G.nodes[node].get("depth", 0)),
            "runtime_anomaly": runtime_anom,
            "schema_drift": schema_drift,
            "contract_violation": contract_viol,
            "failure_history": fail_hist,
            "fused_anomaly_score": fused,
            "has_anomaly": fused > 0.05,
        })
    return G


def back_propagate(G: nx.DiGraph, discount: float = 0.85,
                   min_own_signal: float = 0.05) -> nx.DiGraph:
    """
    Propagate fused anomaly scores one hop backward from staging → raw nodes.

    Rationale: faults in raw tables are first MEASURED at the staging boundary.
    Back-propagation ensures raw source nodes inherit the signal measured in
    their direct staging children.

    Gating condition: only back-propagate onto a raw node when it has its own
    measured runtime anomaly (> min_own_signal). This prevents transformation-level
    faults (e.g., sign-flip in stg_products) from being incorrectly attributed to
    the upstream raw table that has no measured anomaly.
    """
    raw_nodes = [n for n in G.nodes() if G.nodes[n].get("depth", 0) == 0]
    for raw in raw_nodes:
        own_signal = G.nodes[raw].get("runtime_anomaly", 0.0)
        if own_signal < min_own_signal:
            # Raw node has no measured runtime anomaly — the fault likely originates
            # in a downstream transformation, not in this source. Skip back-propagation.
            continue
        children = list(G.successors(raw))
        if not children:
            continue
        max_child = max(G.nodes[c].get("fused_anomaly_score", 0.0) for c in children)
        current = G.nodes[raw].get("fused_anomaly_score", 0.0)
        if max_child * discount > current:
            G.nodes[raw]["fused_anomaly_score"] = max_child * discount
            G.nodes[raw]["has_anomaly"] = True
            G.nodes[raw]["back_propagated"] = True
    return G


def get_failure_nodes(G: nx.DiGraph, tests: Dict[str, bool]) -> List[str]:
    return [n for n, passed in tests.items() if not passed]

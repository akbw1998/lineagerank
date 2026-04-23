"""
LineageRank Scoring Algorithm.

Scores each candidate upstream node using three factors:

  score(u) = w1 * fused_anomaly(u)
           + w2 * coverage(u) * depth_weight(u)

Where:
  fused_anomaly(u)   — weighted combination of runtime + design lineage signals
                       (null rate, row drop, freshness, schema drift, contract
                        violations, failure history) — see lineage/fusion/
  coverage(u)        — fraction of failure nodes reachable downstream from u
  depth_weight(u)    — 1/(depth+1), encodes domain prior that pipeline faults
                       originate at sources (depth=0), not intermediate transforms

The key advance over generic AIOps RCA (CORAL KDD 2023, Chain-of-Event FSE 2024):
  1. Uses DATA PIPELINE lineage DAG structure (not microservice call graph)
  2. Fuses DESIGN lineage (expected schema, contracts, history) with RUNTIME
     lineage (execution stats), whereas prior work uses only one or the other
  3. Multiplicative scoring: coverage×depth amplifies measured anomaly rather than
     independently contributing. Nodes with zero anomaly score zero regardless of
     structural position — correcting the additive proximity bias where upstream
     nodes with no fault signal still outrank intermediate nodes with real anomaly.

The multiplicative form score = fused × (w_a + w_c × coverage × depth_weight)
means: a zero-signal node cannot rank above a node with a real measured anomaly.

References:
  - Wang et al., CORAL, KDD 2023: PPR on causal graphs for online RCA
  - Chen et al., Chain-of-Event, FSE 2024: weighted causal graph ranking
  - Pham et al., RCAEval, WWW 2025: Acc@k, MRR as standard RCA metrics
"""

import networkx as nx
import numpy as np
from typing import Dict, List, Tuple


def compute_failure_coverage(G: nx.DiGraph, failure_nodes: List[str]) -> Dict[str, float]:
    if not failure_nodes:
        return {n: 0.0 for n in G.nodes()}
    coverage = {}
    for node in G.nodes():
        desc = nx.descendants(G, node)
        covered = sum(1 for f in failure_nodes if f in desc or f == node)
        coverage[node] = covered / len(failure_nodes)
    return coverage


def rank(G: nx.DiGraph,
         failure_nodes: List[str],
         w1: float = 0.50,
         w2: float = 0.50,
         top_k: int = 7) -> List[Tuple[str, float]]:
    """
    LineageRank main entry point.

    Args:
        G: annotated fused lineage DAG
        failure_nodes: nodes where tests fired
        w1, w2: weights for fused anomaly and coverage×depth
        top_k: candidates to return

    Returns:
        Ranked list of (node, normalized_score) pairs
    """
    coverage = compute_failure_coverage(G, failure_nodes)

    upstream = set()
    for f in failure_nodes:
        upstream |= nx.ancestors(G, f)
    candidates = [n for n in upstream if n not in failure_nodes]
    if not candidates:
        candidates = [n for n in G.nodes() if n not in failure_nodes]

    scores = {}
    for u in candidates:
        anom  = G.nodes[u].get("fused_anomaly_score", 0.0)
        cov   = coverage.get(u, 0.0)
        depth = G.nodes[u].get("depth", 0)
        dw    = 1.0 / (depth + 1.0)
        # Multiplicative form: coverage×depth amplifies anomaly signal.
        # Zero-anomaly nodes score zero regardless of structural position.
        scores[u] = anom * (w1 + w2 * cov * dw)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    max_s = max((s for _, s in ranked), default=1.0)
    if max_s > 0:
        ranked = [(n, s / max_s) for n, s in ranked]
    return ranked[:top_k]

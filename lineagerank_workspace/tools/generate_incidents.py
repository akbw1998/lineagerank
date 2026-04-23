from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import networkx as nx

from rca_benchmark import (
    FAULT_TYPES,
    OBSERVABILITY_MODES,
    build_graph,
    PipelineSpec,
    eligible_roots,
    get_pipeline_specs,
    impacted_assets,
    leaf_descendants,
)


ROOT = Path(__file__).resolve().parents[1]


def _runtime_edges(spec: PipelineSpec, root: str, mode: str, rng: random.Random) -> list[tuple[str, str]]:
    edges = list(spec.edges)
    if mode == "full":
        return edges
    if mode == "runtime_missing_root":
        return [edge for edge in edges if edge[0] != root]
    if mode == "runtime_sparse":
        sparse = []
        for edge in edges:
            if edge[0] == root:
                continue
            if rng.random() > 0.3:
                sparse.append(edge)
        return sparse
    return edges


def _node_signals(
    spec: PipelineSpec,
    root: str,
    observed: str,
    fault_type: str,
    impacted: list[str],
    rng: random.Random,
) -> dict[str, dict[str, float | int]]:
    graph = build_graph(spec.edges)
    ancestors_of_observed = sorted(nx.ancestors(graph, observed)) if observed in graph else []
    immediate_parents = list(graph.predecessors(observed)) if observed in graph else []
    decoy_candidates = [node for node in ancestors_of_observed if node != root]
    decoys = rng.sample(decoy_candidates, k=min(len(decoy_candidates), 2)) if decoy_candidates else []

    signals: dict[str, dict[str, float | int]] = {}
    impacted_set = set(impacted)
    for node in spec.nodes:
        is_root = node == root
        is_impacted = node in impacted_set
        signals[node] = {
            "recent_change": round(rng.uniform(0.05, 0.28), 3),
            "freshness_severity": 0.0,
            "run_anomaly": round(rng.uniform(0.04, 0.22), 3),
            "contract_violation": 0,
            "local_test_failures": 0,
        }

        if is_impacted:
            signals[node]["run_anomaly"] = round(rng.uniform(0.12, 0.38), 3)

        if is_root:
            signals[node]["recent_change"] = round(rng.uniform(0.55, 0.86), 3)
            signals[node]["run_anomaly"] = round(rng.uniform(0.48, 0.84), 3)

        if node in decoys:
            signals[node]["recent_change"] = round(rng.uniform(0.58, 0.9), 3)
            signals[node]["run_anomaly"] = round(rng.uniform(0.34, 0.71), 3)

        if fault_type == "stale_source" and is_root:
            signals[node]["freshness_severity"] = round(rng.uniform(0.82, 0.98), 3)
        elif fault_type == "stale_source" and (is_impacted or node in immediate_parents):
            signals[node]["freshness_severity"] = round(rng.uniform(0.32, 0.74), 3)

        if fault_type == "schema_drift" and is_root:
            signals[node]["contract_violation"] = 1 if rng.random() > 0.25 else 0
        if fault_type == "schema_drift" and node in immediate_parents:
            signals[node]["contract_violation"] = max(int(signals[node]["contract_violation"]), 1 if rng.random() > 0.4 else 0)

        if node == observed:
            signals[node]["local_test_failures"] = rng.randint(3, 6)
        elif node in immediate_parents:
            signals[node]["local_test_failures"] = rng.randint(1, 4)
        elif is_root and fault_type in {"schema_drift", "stale_source"}:
            signals[node]["local_test_failures"] = rng.randint(0, 2)
        elif is_impacted and spec.nodes[node] == "mart":
            signals[node]["local_test_failures"] = rng.randint(0, 2)

    return signals


def generate_incidents(per_fault: int, seed: int) -> list[dict[str, object]]:
    rng = random.Random(seed)
    specs = get_pipeline_specs()
    incidents: list[dict[str, object]] = []
    incident_num = 1

    for pipeline_name, spec in specs.items():
        for fault_type in FAULT_TYPES:
            roots = eligible_roots(spec, fault_type)
            for i in range(per_fault):
                root = roots[(i + seed) % len(roots)]
                impacted = impacted_assets(spec, root)
                if not impacted:
                    continue
                leaves = leaf_descendants(spec, root)
                observed = rng.choice(leaves or impacted)
                mode = OBSERVABILITY_MODES[(i + len(pipeline_name)) % len(OBSERVABILITY_MODES)]
                signals = _node_signals(spec, root, observed, fault_type, impacted, rng)
                incident = {
                    "incident_id": f"inc_{incident_num:04d}",
                    "pipeline": pipeline_name,
                    "fault_type": fault_type,
                    "observability_mode": mode,
                    "root_cause_asset": root,
                    "observed_failure_asset": observed,
                    "impacted_assets": impacted,
                    "design_edges": list(spec.edges),
                    "runtime_edges": _runtime_edges(spec, root, mode, rng),
                    "leaf_test_weights": spec.leaf_test_weights,
                    "node_types": spec.nodes,
                    "node_signals": signals,
                }
                incidents.append(incident)
                incident_num += 1

    return incidents


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-fault", type=int, default=25)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "incidents" / "lineagerank_incidents.json")
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    incidents = generate_incidents(args.per_fault, args.seed)
    args.output.write_text(json.dumps(incidents, indent=2))
    print(json.dumps({"incident_count": len(incidents), "output": str(args.output)}, indent=2))


if __name__ == "__main__":
    main()

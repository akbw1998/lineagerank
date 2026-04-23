from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path

import networkx as nx
import numpy as np
from sklearn.ensemble import RandomForestClassifier

from rca_benchmark import build_graph, union_graph


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BOOTSTRAP_SAMPLES = 1500

FEATURE_SETS = {
    "all": [
        "proximity",
        "runtime_proximity",
        "design_proximity",
        "blast_radius",
        "quality_signal",
        "failure_propagation",
        "recent_change",
        "freshness_severity",
        "run_anomaly",
        "contract_violation",
        "dual_support",
        "design_support",
        "runtime_support",
        "uncertainty",
        "blind_spot_hint",
        "fault_prior",
    ],
    "no_fault_prior": [
        "proximity",
        "runtime_proximity",
        "design_proximity",
        "blast_radius",
        "quality_signal",
        "failure_propagation",
        "recent_change",
        "freshness_severity",
        "run_anomaly",
        "contract_violation",
        "dual_support",
        "design_support",
        "runtime_support",
        "uncertainty",
        "blind_spot_hint",
    ],
    "structure_only": [
        "proximity",
        "runtime_proximity",
        "design_proximity",
        "blast_radius",
        "dual_support",
        "design_support",
        "runtime_support",
        "uncertainty",
        "blind_spot_hint",
        "fault_prior",
    ],
    "evidence_only": [
        "quality_signal",
        "failure_propagation",
        "recent_change",
        "freshness_severity",
        "run_anomaly",
        "contract_violation",
        "fault_prior",
    ],
}


def normalize(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return max(0.0, min(1.0, value / max_value))


def fault_prior(fault_type: str, node_type: str) -> float:
    if fault_type in {"stale_source", "duplicate_ingestion", "missing_partition"}:
        return {"source": 1.0, "staging": 0.45, "mart": 0.15}.get(node_type, 0.15)
    if fault_type == "schema_drift":
        return {"source": 0.8, "staging": 1.0, "mart": 0.35}.get(node_type, 0.2)
    if fault_type == "null_explosion":
        return {"source": 0.55, "staging": 1.0, "mart": 0.3}.get(node_type, 0.2)
    if fault_type == "bad_join_key":
        return {"source": 0.45, "staging": 1.0, "mart": 0.85}.get(node_type, 0.25)
    return 0.2


def candidate_rows(incident: dict[str, object]) -> list[dict[str, object]]:
    design_edges = [tuple(edge) for edge in incident["design_edges"]]
    runtime_edges = [tuple(edge) for edge in incident["runtime_edges"]]
    design_graph = build_graph(design_edges)
    runtime_graph = build_graph(runtime_edges)
    fused_graph = union_graph(design_edges, runtime_edges)

    observed = str(incident["observed_failure_asset"])
    root = str(incident["root_cause_asset"])
    impacted = set(incident["impacted_assets"])
    node_signals: dict[str, dict[str, float | int]] = incident["node_signals"]  # type: ignore[assignment]
    leaf_test_weights: dict[str, int] = incident["leaf_test_weights"]  # type: ignore[assignment]
    node_types: dict[str, str] = incident["node_types"]  # type: ignore[assignment]

    candidate_nodes = sorted(nx.ancestors(fused_graph, observed))
    if observed in fused_graph.nodes:
        candidate_nodes.append(observed)
    candidate_nodes = sorted(set(candidate_nodes))
    max_blast = max(1, max((len(nx.descendants(fused_graph, node)) for node in candidate_nodes), default=1))

    # Personalized PageRank adapted to the pipeline lineage graph.
    # We reverse the fused graph (data flows A→B, but fault propagates B←A),
    # then run PageRank personalized at v_obs.  The stationary score of each
    # ancestor node represents its estimated probability of being the root cause.
    # This is the data-pipeline analogue of PC-PageRank used in RCAEval for
    # microservice call graphs (Haveliwala 2002; Liu et al., IEEE TSE 2014).
    rev_fused = fused_graph.reverse(copy=True)
    personalization = {n: 0.0 for n in rev_fused.nodes()}
    if observed in rev_fused.nodes():
        personalization[observed] = 1.0
    try:
        pr_scores = nx.pagerank(rev_fused, alpha=0.85, personalization=personalization,
                                max_iter=200, tol=1e-6)
    except nx.PowerIterationFailedConvergence:
        pr_scores = {n: 1.0 / max(1, len(rev_fused.nodes())) for n in rev_fused.nodes()}

    rows: list[dict[str, object]] = []
    for node in candidate_nodes:
        design_reachable = nx.has_path(design_graph, node, observed) if node in design_graph and observed in design_graph else False
        runtime_reachable = nx.has_path(runtime_graph, node, observed) if node in runtime_graph and observed in runtime_graph else False
        design_distance = nx.shortest_path_length(design_graph, node, observed) if design_reachable else 99
        runtime_distance = nx.shortest_path_length(runtime_graph, node, observed) if runtime_reachable else 99
        fused_distance = min(design_distance, runtime_distance)
        descendants = set(nx.descendants(fused_graph, node))
        local_fails = int(node_signals[node]["local_test_failures"])
        local_failure_support = local_fails + int(node_signals[node]["contract_violation"]) * 2
        quality_signal = normalize(local_failure_support, 8)
        # Evidence gradient: root causes tend to have higher local evidence than their
        # downstream descendants (fault signal originates at root, then attenuates).
        # This provides a directional discriminator beyond simple proximity.
        local_ev_raw = (
            0.50 * float(node_signals[node]["run_anomaly"])
            + 0.30 * float(node_signals[node]["recent_change"])
            + 0.20 * float(node_signals[node]["freshness_severity"])
        )
        if descendants:
            avg_desc_ev = sum(
                0.50 * float(node_signals.get(d, {}).get("run_anomaly", 0.0))
                + 0.30 * float(node_signals.get(d, {}).get("recent_change", 0.0))
                + 0.20 * float(node_signals.get(d, {}).get("freshness_severity", 0.0))
                for d in descendants
            ) / len(descendants)
        else:
            avg_desc_ev = 0.0
        evidence_gradient = max(0.0, local_ev_raw - avg_desc_ev)

        row = {
            "incident_id": incident["incident_id"],
            "pipeline": incident["pipeline"],
            "fault_type": incident["fault_type"],
            "observability_mode": incident["observability_mode"],
            "candidate": node,
            "node_type": node_types[node],
            "label": int(node == root),
            "design_distance": float(design_distance),
            "runtime_distance": float(runtime_distance),
            "fused_distance": float(fused_distance),
            "proximity": 1.0 / (1.0 + fused_distance) if fused_distance < 99 else 0.0,
            "runtime_proximity": 1.0 / (1.0 + runtime_distance) if runtime_distance < 99 else 0.0,
            "design_proximity": 1.0 / (1.0 + design_distance) if design_distance < 99 else 0.0,
            "blast_radius": normalize(len(descendants), max_blast),
            "quality_signal": quality_signal,
            "failure_propagation": normalize(sum(leaf_test_weights.get(asset, 0) for asset in descendants.intersection(impacted)), max(1, sum(leaf_test_weights.values()))),
            "recent_change": float(node_signals[node]["recent_change"]),
            "freshness_severity": float(node_signals[node]["freshness_severity"]),
            "run_anomaly": float(node_signals[node]["run_anomaly"]),
            "contract_violation": float(node_signals[node]["contract_violation"]),
            "dual_support": 1.0 if design_reachable and runtime_reachable else 0.0,
            "design_support": 1.0 if design_reachable else 0.0,
            "runtime_support": 1.0 if runtime_reachable else 0.0,
            "uncertainty": 1.0 if design_reachable and not runtime_reachable else 0.0,
            "blind_spot_hint": 1.0 if design_reachable and not runtime_reachable else 0.0,
            "fault_prior": fault_prior(str(incident["fault_type"]), node_types[node]),
            "evidence_gradient": evidence_gradient,
            "pagerank_adapted": pr_scores.get(node, 0.0),
        }
        rows.append(row)
    return rows


def score_rows(rows: list[dict[str, object]]) -> None:
    for row in rows:
        quality = float(row["quality_signal"]) + 0.5 * float(row["contract_violation"])

        # Local evidence composite: shared by CP and BS
        local_ev = (
            0.45 * float(row["run_anomaly"])
            + 0.30 * float(row["recent_change"])
            + 0.15 * float(row["freshness_severity"])
            + 0.10 * quality
        )

        # LineageRank-CP (Causal Propagation): integrates a directional evidence-gradient
        # signal. Nodes whose local evidence substantially exceeds their downstream
        # descendants' average evidence are more likely to be root causes.
        ev_grad = float(row.get("evidence_gradient", 0.0))
        causal_propagation = (
            0.28 * float(row["proximity"])
            + 0.30 * local_ev
            + 0.22 * ev_grad
            + 0.10 * float(row["failure_propagation"])
            + 0.10 * float(row["fault_prior"])
        )

        # LineageRank-BS (Blind-Spot Boosted): multiplicatively amplifies the evidence
        # score for nodes present in design lineage but absent from runtime lineage.
        # Motivated by the empirical finding that runtime_missing_root mode
        # disproportionately benefits from design-runtime discordance detection.
        base_bs = (
            0.25 * float(row["proximity"])
            + 0.35 * local_ev
            + 0.15 * float(row["failure_propagation"])
            + 0.15 * float(row["fault_prior"])
            + 0.10 * float(row["blast_radius"])
        )
        blind_spot = float(row["blind_spot_hint"])
        blind_spot_boosted = base_bs * (1.0 + 2.5 * blind_spot)

        row.update(
            {
                "runtime_distance": float(row["runtime_proximity"]),
                "design_distance": float(row["design_proximity"]),
                "centrality": 0.75 * float(row["blast_radius"]) + 0.25 * float(row["design_support"]),
                "freshness_only": float(row["freshness_severity"]),
                "failed_tests": quality,
                "recent_change": float(row["recent_change"]),
                "quality_only": 0.45 * quality + 0.35 * float(row["freshness_severity"]) + 0.20 * float(row["run_anomaly"]),
                "lineage_rank": (
                    0.17 * float(row["proximity"])
                    + 0.15 * float(row["blast_radius"])
                    + 0.08 * float(row["recent_change"])
                    + 0.08 * quality
                    + 0.08 * float(row["failure_propagation"])
                    + 0.10 * float(row["freshness_severity"])
                    + 0.06 * float(row["run_anomaly"])
                    + 0.07 * float(row["dual_support"])
                    + 0.11 * float(row["design_support"])
                    + 0.12 * float(row["blind_spot_hint"])
                    + 0.12 * float(row["fault_prior"])
                    - 0.04 * float(row["uncertainty"])
                ),
                "causal_propagation": causal_propagation,
                "blind_spot_boosted": blind_spot_boosted,
                "pagerank_adapted": float(row["pagerank_adapted"]),
            }
        )


def rank_details(rows: list[dict[str, object]], score_key: str) -> list[dict[str, object]]:
    per_incident: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        per_incident.setdefault(str(row["incident_id"]), []).append(row)

    details: list[dict[str, object]] = []
    for incident_id, items in per_incident.items():
        ranked = sorted(items, key=lambda item: (-float(item[score_key]), str(item["candidate"])))
        rank = next((idx + 1 for idx, item in enumerate(ranked) if int(item["label"]) == 1), len(ranked))
        details.append(
            {
                "incident_id": incident_id,
                "pipeline": str(ranked[0]["pipeline"]),
                "fault_type": str(ranked[0]["fault_type"]),
                "observability_mode": str(ranked[0]["observability_mode"]),
                "rank": rank,
                "top1": 1.0 if rank <= 1 else 0.0,
                "top3": 1.0 if rank <= 3 else 0.0,
                "top5": 1.0 if rank <= 5 else 0.0,
                "mrr": 1.0 / rank,
                "ndcg": 1.0 / math.log2(rank + 1),
                "assets_before_true_cause": float(rank - 1),
            }
        )
    return details


def aggregate_details(details: list[dict[str, object]]) -> dict[str, float]:
    if not details:
        return {
            "incident_count": 0,
            "top1": 0.0,
            "top3": 0.0,
            "top5": 0.0,
            "mrr": 0.0,
            "ndcg": 0.0,
            "assets_before_true_cause": 0.0,
        }

    return {
        "incident_count": len(details),
        "top1": round(sum(float(item["top1"]) for item in details) / len(details), 4),
        "top3": round(sum(float(item["top3"]) for item in details) / len(details), 4),
        "top5": round(sum(float(item["top5"]) for item in details) / len(details), 4),
        "mrr": round(sum(float(item["mrr"]) for item in details) / len(details), 4),
        "ndcg": round(sum(float(item["ndcg"]) for item in details) / len(details), 4),
        "assets_before_true_cause": round(sum(float(item["assets_before_true_cause"]) for item in details) / len(details), 4),
    }


def rank_metrics(rows: list[dict[str, object]], score_key: str) -> dict[str, float]:
    return aggregate_details(rank_details(rows, score_key))


def bootstrap_ci(details: list[dict[str, object]], metric: str, *, samples: int = DEFAULT_BOOTSTRAP_SAMPLES, seed: int = 13) -> dict[str, float]:
    rng = random.Random(seed)
    values = [float(item[metric]) for item in details]
    if not values:
        return {"mean": 0.0, "ci_low": 0.0, "ci_high": 0.0}
    means = []
    for _ in range(samples):
        draw = [values[rng.randrange(len(values))] for _ in range(len(values))]
        means.append(sum(draw) / len(draw))
    means.sort()
    low_idx = int(0.025 * (samples - 1))
    high_idx = int(0.975 * (samples - 1))
    return {
        "mean": round(sum(values) / len(values), 4),
        "ci_low": round(means[low_idx], 4),
        "ci_high": round(means[high_idx], 4),
    }


def paired_significance(
    details_a: list[dict[str, object]],
    details_b: list[dict[str, object]],
    metric: str,
    *,
    samples: int = DEFAULT_BOOTSTRAP_SAMPLES,
    seed: int = 13,
) -> dict[str, float]:
    by_incident_a = {str(item["incident_id"]): float(item[metric]) for item in details_a}
    by_incident_b = {str(item["incident_id"]): float(item[metric]) for item in details_b}
    common_ids = sorted(set(by_incident_a).intersection(by_incident_b))
    diffs = [by_incident_a[incident_id] - by_incident_b[incident_id] for incident_id in common_ids]
    if not diffs:
        return {"mean_diff": 0.0, "ci_low": 0.0, "ci_high": 0.0, "p_value": 1.0}

    rng = random.Random(seed)
    boot = []
    for _ in range(samples):
        draw = [diffs[rng.randrange(len(diffs))] for _ in range(len(diffs))]
        boot.append(sum(draw) / len(draw))
    boot.sort()
    low_idx = int(0.025 * (samples - 1))
    high_idx = int(0.975 * (samples - 1))
    non_positive = sum(1 for value in boot if value <= 0) / len(boot)
    non_negative = sum(1 for value in boot if value >= 0) / len(boot)
    return {
        "mean_diff": round(sum(diffs) / len(diffs), 4),
        "ci_low": round(boot[low_idx], 4),
        "ci_high": round(boot[high_idx], 4),
        "p_value": round(min(1.0, 2 * min(non_positive, non_negative)), 4),
    }


def learned_ranker_predictions(rows: list[dict[str, object]], feature_keys: list[str], *, random_state: int = 13) -> tuple[dict[str, float], list[dict[str, object]], dict[str, float]]:
    pipelines = sorted({str(row["pipeline"]) for row in rows})
    predictions: list[dict[str, object]] = []
    importances = np.zeros(len(feature_keys))

    for held_out in pipelines:
        train = [row for row in rows if row["pipeline"] != held_out]
        test = [row for row in rows if row["pipeline"] == held_out]
        x_train = [[float(row[key]) for key in feature_keys] for row in train]
        y_train = [int(row["label"]) for row in train]
        x_test = [[float(row[key]) for key in feature_keys] for row in test]
        model = RandomForestClassifier(n_estimators=200, random_state=random_state, class_weight="balanced")
        model.fit(x_train, y_train)
        importances += np.array(model.feature_importances_)
        probs = model.predict_proba(x_test)[:, 1]
        for row, prob in zip(test, probs):
            enriched = dict(row)
            enriched["learned_ranker"] = float(prob)
            predictions.append(enriched)

    importance_payload = {
        feature: round(float(score), 4)
        for feature, score in sorted(zip(feature_keys, importances / max(1, len(pipelines))), key=lambda item: -item[1])
    }
    return aggregate_details(rank_details(predictions, "learned_ranker")), predictions, importance_payload


def by_group_from_details(details: list[dict[str, object]], group_key: str) -> dict[str, dict[str, float]]:
    values: dict[str, list[dict[str, object]]] = {}
    for item in details:
        values.setdefault(str(item[group_key]), []).append(item)
    return {group: aggregate_details(group_rows) for group, group_rows in values.items()}


def confidence_table(all_details: dict[str, list[dict[str, object]]]) -> dict[str, dict[str, dict[str, float]]]:
    metrics = ["top1", "mrr", "ndcg", "assets_before_true_cause"]
    payload: dict[str, dict[str, dict[str, float]]] = {}
    for method, details in all_details.items():
        payload[method] = {metric: bootstrap_ci(details, metric) for metric in metrics}
    return payload


def significance_table(all_details: dict[str, list[dict[str, object]]]) -> dict[str, dict[str, dict[str, float]]]:
    comparisons = [
        ("lineage_rank", "centrality"),
        ("lineage_rank", "quality_only"),
        ("lineage_rank", "pagerank_adapted"),
        ("causal_propagation", "quality_only"),
        ("blind_spot_boosted", "lineage_rank"),
        ("learned_ranker", "lineage_rank"),
        ("learned_ranker", "centrality"),
    ]
    metrics = ["top1", "mrr", "assets_before_true_cause"]
    payload: dict[str, dict[str, dict[str, float]]] = {}
    for winner, loser in comparisons:
        label = f"{winner}_vs_{loser}"
        payload[label] = {metric: paired_significance(all_details[winner], all_details[loser], metric) for metric in metrics}
    return payload


def leakage_audit(rows: list[dict[str, object]]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for name, feature_keys in FEATURE_SETS.items():
        metrics, predictions, importances = learned_ranker_predictions(rows, feature_keys)
        payload[name] = {
            "metrics": metrics,
            "feature_importances": importances,
            "by_fault_type": by_group_from_details(rank_details(predictions, "learned_ranker"), "fault_type"),
        }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--incidents", type=Path, default=ROOT / "data" / "incidents" / "lineagerank_incidents.json")
    parser.add_argument("--output", type=Path, default=ROOT / "experiments" / "results" / "lineagerank_eval.json")
    args = parser.parse_args()

    incidents = json.loads(args.incidents.read_text())
    rows = [row for incident in incidents for row in candidate_rows(incident)]
    score_rows(rows)

    methods = [
        "runtime_distance",
        "design_distance",
        "centrality",
        "freshness_only",
        "failed_tests",
        "recent_change",
        "quality_only",
        "pagerank_adapted",
        "causal_propagation",
        "blind_spot_boosted",
        "lineage_rank",
    ]

    all_details = {method: rank_details(rows, method) for method in methods}

    summary = {
        "incident_count": len(incidents),
        "candidate_row_count": len(rows),
        "overall": {method: aggregate_details(all_details[method]) for method in methods},
        "by_fault_type": {method: by_group_from_details(all_details[method], "fault_type") for method in methods},
        "by_observability": {method: by_group_from_details(all_details[method], "observability_mode") for method in methods},
        "by_pipeline": {method: by_group_from_details(all_details[method], "pipeline") for method in methods},
        "confidence_intervals": confidence_table(all_details),
    }

    learned_metrics, learned_rows, feature_importances = learned_ranker_predictions(rows, FEATURE_SETS["all"])
    learned_details = rank_details(learned_rows, "learned_ranker")
    all_details["learned_ranker"] = learned_details
    summary["overall"]["learned_ranker"] = learned_metrics
    summary["by_fault_type"]["learned_ranker"] = by_group_from_details(learned_details, "fault_type")
    summary["by_observability"]["learned_ranker"] = by_group_from_details(learned_details, "observability_mode")
    summary["by_pipeline"]["learned_ranker"] = by_group_from_details(learned_details, "pipeline")
    summary["confidence_intervals"]["learned_ranker"] = {
        metric: bootstrap_ci(learned_details, metric) for metric in ["top1", "mrr", "ndcg", "assets_before_true_cause"]
    }
    summary["significance_tests"] = significance_table(all_details)
    summary["learned_feature_importances"] = feature_importances
    summary["leakage_audit"] = leakage_audit(rows)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["overall"], indent=2))


if __name__ == "__main__":
    main()

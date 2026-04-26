from __future__ import annotations

import argparse
import json
import math
import random
import re
import subprocess
import time
from pathlib import Path

import networkx as nx
import numpy as np
from sklearn.ensemble import RandomForestClassifier

from rca_benchmark import build_graph, union_graph


# ─── LR-LLM: lineage-contextualized LLM scoring ──────────────────────────────
# Positioning: unlike the zero-shot LLM baseline (b6_llm_claude in the lineagerank/
# package), LR-LLM provides the full DAG topology (both design and runtime edge
# sets), per-node anomaly signals, and explicit blind-spot annotation. This tests
# whether structured lineage context closes the gap identified in OpenRCA [ICLR 2025]
# (11.34% accuracy without structured context) in the data pipeline domain.
#
# Free LLM backends for Google Colab:
#   gemini     — Google Gemini 1.5 Flash, free tier 15 RPM / 1M TPD
#                Get key: https://aistudio.google.com/app/apikey
#   huggingface — HuggingFace Inference API, free tier
#                Get token: https://huggingface.co/settings/tokens
#   claude_cli — `claude -p` subprocess (requires Claude Code CLI)


_NODE_TYPE_EMOJI = {"source": "📥 source", "staging": "🔄 staging", "mart": "📊 mart"}


def _build_llm_prompt(incident: dict, rows_for_incident: list[dict]) -> str:
    """
    Build a lineage-contextualized CoT prompt for one incident.

    Uses the incident's actual design_edges and runtime_edges (separate lists)
    to show graph topology and blind-spot status — richer than the zero-shot
    b6 baseline which provides only node names.
    """
    design_edges = [tuple(e) for e in incident["design_edges"]]
    runtime_edges = set(map(tuple, incident["runtime_edges"]))
    design_only = [(s, d) for s, d in design_edges if (s, d) not in runtime_edges]

    node_types: dict[str, str] = incident["node_types"]
    observed = str(incident["observed_failure_asset"])
    fault_type = str(incident["fault_type"])
    candidates = [str(row["candidate"]) for row in rows_for_incident]

    # Graph section
    edge_lines = []
    for s, d in design_edges:
        suffix = "  ← DESIGN ONLY (absent from runtime)" if (s, d) in set(map(tuple, design_only)) else ""
        edge_lines.append(f"  {s} ({node_types.get(s, '?')})  →  {d} ({node_types.get(d, '?')}){suffix}")

    # Signal table
    signal_rows = ["Node                  | run_anom | recent_chg | freshness | quality | blind_spot",
                   "-" * 85]
    row_by_cand = {str(r["candidate"]): r for r in rows_for_incident}
    for c in candidates:
        r = row_by_cand[c]
        signal_rows.append(
            f"  {c:<20} | {float(r['run_anomaly']):>8.3f} | {float(r['recent_change']):>10.3f}"
            f" | {float(r['freshness_severity']):>9.3f} | {float(r['quality_signal']):>7.3f}"
            f" | {int(float(r['blind_spot_hint'])):>10}"
        )

    return f"""You are a senior data engineer performing root cause analysis on a failing data pipeline.

## Pipeline Lineage Graph
Edges show data flow (A → B = A is upstream of B). "DESIGN ONLY" edges exist in the
pipeline specification but are ABSENT from runtime execution records — their source
node ran silently without emitting lineage events (the blind-spot condition).

{chr(10).join(edge_lines)}

## Observed Failure
Node: {observed}  |  Fault category: {fault_type}

## Per-Node Diagnostic Signals  (0=normal, 1=severe anomaly)
blind_spot=1: node is in design lineage but ABSENT from runtime lineage.
This is the strongest signal for the runtime_missing_root observability condition.

{chr(10).join(signal_rows)}

## Candidate Nodes to Score
{', '.join(candidates)}

## Reasoning Steps (think before answering)
1. Propagation: which candidates have a directed design-graph path to {observed}?
2. Signal strength: which have the highest run_anomaly or quality signal?
3. Blind spots: any candidate with blind_spot=1 is highly suspicious — it failed
   silently without emitting runtime lineage events.
4. Node type prior: source nodes are more common root causes than staging or mart nodes.
5. Evidence gradient: root causes show higher local anomaly than their downstream children.

## Output
Return ONLY valid JSON — candidate names as keys, probability scores [0.0–1.0] as values.
Scores should sum to approximately 1.0. No explanation outside the JSON block.

Example: {{"customers": 0.72, "orders": 0.20, "products": 0.08}}

JSON:"""


def _parse_llm_json(raw: str | None, candidates: list[str]) -> dict[str, float]:
    """Extract and normalise probability scores from LLM output."""
    uniform = {c: 1.0 / len(candidates) for c in candidates}
    if not raw:
        return uniform
    text = re.sub(r"```(?:json)?", "", raw).strip()
    m = re.search(r"\{[^{}]+\}", text, re.DOTALL)
    if not m:
        return uniform
    try:
        parsed = json.loads(m.group())
        scores = {c: float(parsed.get(c, 0.0)) for c in candidates}
        total = sum(scores.values())
        return {c: s / total for c, s in scores.items()} if total > 0 else uniform
    except Exception:
        return uniform


def _call_gemini(prompt: str, api_key: str, model: str = "gemini-1.5-flash") -> str | None:
    """Call Google Gemini API. Free tier: 15 RPM, 1M tokens/day."""
    try:
        import google.generativeai as genai  # pip install google-generativeai
        genai.configure(api_key=api_key)
        resp = genai.GenerativeModel(model).generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.1, max_output_tokens=512),
        )
        return resp.text
    except Exception as exc:
        print(f"    [LR-LLM Gemini] {exc}")
        return None


def _call_huggingface(prompt: str, api_key: str,
                      model: str = "microsoft/Phi-3-mini-4k-instruct") -> str | None:
    """Call HuggingFace Inference API. Free with HF account."""
    try:
        from huggingface_hub import InferenceClient  # pip install huggingface_hub
        client = InferenceClient(token=api_key)
        return client.text_generation(prompt, model=model, max_new_tokens=512, temperature=0.1)
    except Exception as exc:
        print(f"    [LR-LLM HuggingFace] {exc}")
        return None


def _call_claude_cli(prompt: str, timeout: int = 60) -> str | None:
    """Call Claude via `claude -p` subprocess (requires Claude Code CLI)."""
    try:
        result = subprocess.run(["claude", "-p", prompt], capture_output=True,
                                text=True, timeout=timeout)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as exc:
        print(f"    [LR-LLM claude-cli] {exc}")
        return None


def _llm_call(prompt: str, backend: str, api_key: str | None,
              model: str | None) -> str | None:
    if backend == "gemini":
        return _call_gemini(prompt, api_key or "", model or "gemini-1.5-flash")
    if backend == "huggingface":
        return _call_huggingface(prompt, api_key or "", model or "microsoft/Phi-3-mini-4k-instruct")
    if backend == "claude_cli":
        return _call_claude_cli(prompt)
    return None


def llm_score_incidents(
    incidents: list[dict],
    all_rows: list[dict],
    backend: str = "gemini",
    api_key: str | None = None,
    model: str | None = None,
    alpha: float = 0.60,
    rate_limit_delay: float = 4.0,
) -> None:
    """
    Score all incidents with LR-LLM and write 'llm_lineage_rank' into each row in-place.

    Hybrid formula:
        llm_lineage_rank(u) = alpha * llm_prob(u) + (1 - alpha) * lineage_rank(u)

    The structural anchor (lineage_rank) prevents zero-anomaly nodes from ranking
    high due to LLM hallucination. alpha=0.60 validated via grid search.

    Rate limiting: adds a short delay between incidents to stay within free-tier RPM.
    Gemini 1.5 Flash free tier allows 15 RPM; 4s delay gives ~15 req/min.
    """
    # Index rows by incident_id
    by_incident: dict[str, list[dict]] = {}
    for row in all_rows:
        by_incident.setdefault(str(row["incident_id"]), []).append(row)

    # Index incidents by id
    incident_by_id = {str(inc["incident_id"]): inc for inc in incidents}

    total = len(by_incident)
    for i, (incident_id, rows) in enumerate(by_incident.items(), 1):
        incident = incident_by_id.get(incident_id)
        candidates = [str(r["candidate"]) for r in rows]

        llm_probs: dict[str, float] = {}
        if incident is not None:
            prompt = _build_llm_prompt(incident, rows)
            raw = _llm_call(prompt, backend, api_key, model)
            llm_probs = _parse_llm_json(raw, candidates)
            if i < total:
                time.sleep(rate_limit_delay)   # stay within free-tier RPM

        for row in rows:
            c = str(row["candidate"])
            llm_p = llm_probs.get(c, 1.0 / max(len(candidates), 1))
            struct_s = float(row.get("lineage_rank", 0.0))
            row["llm_lineage_rank"] = alpha * llm_p + (1.0 - alpha) * struct_s

        if i % 10 == 0 or i == total:
            print(f"    LR-LLM scored {i}/{total} incidents")


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
    base_comparisons = [
        ("lineage_rank",      "centrality"),
        ("lineage_rank",      "quality_only"),
        ("lineage_rank",      "pagerank_adapted"),
        ("causal_propagation","quality_only"),
        ("blind_spot_boosted","lineage_rank"),
        ("learned_ranker",    "lineage_rank"),
        ("learned_ranker",    "centrality"),
    ]
    # LR-LLM comparisons added when LR-LLM results are present
    llm_comparisons = [
        ("llm_lineage_rank",  "lineage_rank"),        # LR-LLM vs LR-H
        ("llm_lineage_rank",  "blind_spot_boosted"),  # LR-LLM vs LR-BS
        ("llm_lineage_rank",  "pagerank_adapted"),    # LR-LLM vs PR-Adapted
    ]
    comparisons = base_comparisons + [
        (w, l) for w, l in llm_comparisons if w in all_details and l in all_details
    ]
    metrics = ["top1", "mrr", "assets_before_true_cause"]
    payload: dict[str, dict[str, dict[str, float]]] = {}
    for winner, loser in comparisons:
        if winner not in all_details or loser not in all_details:
            continue
        label = f"{winner}_vs_{loser}"
        payload[label] = {
            metric: paired_significance(all_details[winner], all_details[loser], metric)
            for metric in metrics
        }
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


def _add_method_to_summary(
    summary: dict,
    all_details: dict,
    method: str,
    details: list[dict],
) -> None:
    """Add a method's results to all summary sections in-place."""
    summary["overall"][method] = aggregate_details(details)
    summary["by_fault_type"][method] = by_group_from_details(details, "fault_type")
    summary["by_observability"][method] = by_group_from_details(details, "observability_mode")
    summary["by_pipeline"][method] = by_group_from_details(details, "pipeline")
    summary["confidence_intervals"][method] = {
        metric: bootstrap_ci(details, metric)
        for metric in ["top1", "mrr", "ndcg", "assets_before_true_cause"]
    }
    all_details[method] = details


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate all LineageRank methods on PipeRCA-Bench incidents."
    )
    parser.add_argument(
        "--incidents", type=Path,
        default=ROOT / "data" / "incidents" / "lineagerank_incidents.json",
    )
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "experiments" / "results" / "lineagerank_eval.json",
    )
    # LR-LLM arguments
    parser.add_argument(
        "--lrllm-backend", default=None,
        choices=["gemini", "huggingface", "claude_cli"],
        help=(
            "Enable LR-LLM scoring with the specified backend. "
            "gemini: free Gemini 1.5 Flash (15 RPM); "
            "huggingface: HF Inference API; "
            "claude_cli: `claude -p` subprocess. "
            "Omit to skip LR-LLM (runs all other methods regardless)."
        ),
    )
    parser.add_argument(
        "--lrllm-key", default=None,
        help="API key for gemini or huggingface backends.",
    )
    parser.add_argument(
        "--lrllm-model", default=None,
        help="Model override (default: gemini-1.5-flash / Phi-3-mini-4k-instruct).",
    )
    parser.add_argument(
        "--lrllm-alpha", type=float, default=0.60,
        help="LLM weight in hybrid score: score = alpha*llm + (1-alpha)*lineage_rank. Default 0.60.",
    )
    parser.add_argument(
        "--lrllm-delay", type=float, default=4.0,
        help="Seconds between LLM calls (rate-limit guard). Default 4.0 for Gemini free tier.",
    )
    args = parser.parse_args()

    incidents = json.loads(args.incidents.read_text())
    rows = [row for incident in incidents for row in candidate_rows(incident)]
    score_rows(rows)

    heuristic_methods = [
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

    all_details = {method: rank_details(rows, method) for method in heuristic_methods}

    summary: dict = {
        "incident_count": len(incidents),
        "candidate_row_count": len(rows),
        "overall":              {m: aggregate_details(all_details[m]) for m in heuristic_methods},
        "by_fault_type":        {m: by_group_from_details(all_details[m], "fault_type") for m in heuristic_methods},
        "by_observability":     {m: by_group_from_details(all_details[m], "observability_mode") for m in heuristic_methods},
        "by_pipeline":          {m: by_group_from_details(all_details[m], "pipeline") for m in heuristic_methods},
        "confidence_intervals": confidence_table(all_details),
    }

    # ── LR-L: learned Random Forest (leave-one-pipeline-out) ─────────────────
    learned_metrics, learned_rows, feature_importances = learned_ranker_predictions(
        rows, FEATURE_SETS["all"]
    )
    learned_details = rank_details(learned_rows, "learned_ranker")
    _add_method_to_summary(summary, all_details, "learned_ranker", learned_details)
    summary["learned_feature_importances"] = feature_importances
    summary["leakage_audit"] = leakage_audit(rows)

    # ── LR-LLM: lineage-contextualized LLM scoring ───────────────────────────
    if args.lrllm_backend:
        print(f"\nRunning LR-LLM ({args.lrllm_backend}, alpha={args.lrllm_alpha})...")
        print(f"  {len(incidents)} incidents — estimated time: "
              f"~{len(incidents) * args.lrllm_delay / 60:.1f} min at {args.lrllm_delay}s/call")
        llm_score_incidents(
            incidents=incidents,
            all_rows=rows,
            backend=args.lrllm_backend,
            api_key=args.lrllm_key,
            model=args.lrllm_model,
            alpha=args.lrllm_alpha,
            rate_limit_delay=args.lrllm_delay,
        )
        # Rows that got scored will have 'llm_lineage_rank'; rows without a call
        # (if any failed) fall back to lineage_rank via the hybrid formula already.
        llm_details = rank_details(rows, "llm_lineage_rank")
        _add_method_to_summary(summary, all_details, "llm_lineage_rank", llm_details)
        summary["lrllm_config"] = {
            "backend": args.lrllm_backend,
            "model":   args.lrllm_model,
            "alpha":   args.lrllm_alpha,
        }
        print(f"  LR-LLM overall: {summary['overall']['llm_lineage_rank']}")

    # ── Significance tests (includes LR-LLM comparisons when present) ────────
    summary["significance_tests"] = significance_table(all_details)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary["overall"], indent=2))


if __name__ == "__main__":
    main()

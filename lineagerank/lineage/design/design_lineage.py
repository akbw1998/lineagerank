"""
Design Lineage Module.

Simulates dbt manifest.json design-time metadata:
  - Static DAG (which models depend on which)
  - Expected schema per node (column definitions)
  - Data contracts (not_null, positive, unique invariants)
  - Schema change history (simulated version history)
  - Run failure history (recent failure count per node)

This is the KEY addition over PipeRCA v1 — prior work used only
runtime lineage (row counts, null rates from execution). Design lineage
gives us the *expected* state to compare against *actual* runtime state,
enabling richer fault attribution signals.

References:
  - dbt Labs. dbt manifest.json specification (2024).
    https://docs.getdbt.com/reference/artifacts/manifest-json
  - Yang et al., "Unlocking CI/CD for Data Pipelines," PVLDB 2025.
    (uses schema history for drift detection; we extend to RCA)
  - OpenLineage static lineage proposal (2024).
    https://openlineage.io/blog/static-lineage/
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipeline.core.tpcds_pipeline import DESIGN_SCHEMA, DESIGN_CONTRACTS, NODE_DEPTH


@dataclass
class SchemaVersion:
    version: int
    columns: List[str]
    changed_from_prev: bool = False
    change_type: Optional[str] = None   # "added", "removed", "renamed"


@dataclass
class NodeDesignMeta:
    name: str
    depth: int
    expected_schema: List[str]
    contracts: Dict
    schema_history: List[SchemaVersion] = field(default_factory=list)
    recent_failure_count: int = 0       # failures in last N runs
    sql_complexity: int = 1             # 1=simple select, 2=join, 3=agg+join


# Default SQL complexity per node
SQL_COMPLEXITY = {
    "raw_orders": 1, "raw_customers": 1, "raw_products": 1,
    "stg_orders": 1, "stg_customers": 1, "stg_products": 1,
    "fct_sales": 2, "fct_inventory": 3,
    "rpt_revenue": 3, "rpt_stock": 3,
}


def build_design_meta(fault_config: Dict = None,
                      run_history: Dict = None) -> Dict[str, NodeDesignMeta]:
    """
    Build design lineage metadata for all pipeline nodes.

    Args:
        fault_config: active fault configuration (used to simulate schema drift history)
        run_history: dict of node → recent_failure_count from historical runs
    """
    fault_config = fault_config or {}
    run_history = run_history or {}

    meta = {}
    for node in DESIGN_SCHEMA:
        # Schema history: 3 versions, last one may have changed if schema_drift active
        history = [
            SchemaVersion(1, DESIGN_SCHEMA[node], False),
            SchemaVersion(2, DESIGN_SCHEMA[node], False),
        ]

        if fault_config.get("schema_drift_customers", False) and node == "raw_customers":
            # Schema drift: email column was removed in the latest version
            drifted = [c for c in DESIGN_SCHEMA[node] if c != "email"]
            history.append(SchemaVersion(3, drifted, changed_from_prev=True,
                                         change_type="removed"))
        else:
            history.append(SchemaVersion(3, DESIGN_SCHEMA[node], False))

        meta[node] = NodeDesignMeta(
            name=node,
            depth=NODE_DEPTH[node],
            expected_schema=DESIGN_SCHEMA[node],
            contracts=DESIGN_CONTRACTS.get(node, {}),
            schema_history=history,
            recent_failure_count=run_history.get(node, 0),
            sql_complexity=SQL_COMPLEXITY.get(node, 1),
        )
    return meta


def detect_schema_drift(design_meta: Dict[str, NodeDesignMeta],
                        schema_actual: Dict[str, List[str]]) -> Dict[str, float]:
    """
    Compare expected schema (design lineage) vs actual schema (runtime).
    Returns drift score per node in [0, 1].

    A node with missing expected columns gets drift_score = len(missing)/len(expected).
    A node with a schema_history change in the latest version gets an additional signal.
    """
    drift_scores = {}
    for node, actual_cols in schema_actual.items():
        dm = design_meta.get(node)
        if not dm:
            drift_scores[node] = 0.0
            continue

        expected = set(dm.expected_schema)
        actual = set(actual_cols)
        missing = expected - actual
        extra = actual - expected

        # Structural drift: fraction of expected columns missing
        structural_drift = len(missing) / max(len(expected), 1)

        # History drift: did the latest schema version change?
        history_drift = 0.0
        if dm.schema_history and dm.schema_history[-1].changed_from_prev:
            history_drift = 0.5   # recent schema change is a strong fault signal

        drift_scores[node] = min(1.0, structural_drift + history_drift)

    return drift_scores


def get_failure_history_scores(design_meta: Dict[str, NodeDesignMeta]) -> Dict[str, float]:
    """
    Normalize recent failure counts to [0, 1] scores.
    Nodes with more recent failures are more suspicious as root causes.
    """
    max_failures = max((dm.recent_failure_count for dm in design_meta.values()), default=1)
    if max_failures == 0:
        return {n: 0.0 for n in design_meta}
    return {n: dm.recent_failure_count / max_failures
            for n, dm in design_meta.items()}


def contract_violation_scores(design_meta: Dict[str, NodeDesignMeta],
                               node_stats: Dict) -> Dict[str, float]:
    """
    Check data contracts (not_null, positive invariants) against runtime stats.
    Returns violation score per node in [0, 1].
    """
    scores = {}
    for node, dm in design_meta.items():
        stats = node_stats.get(node, {})
        violations = 0
        total_checks = 0

        contracts = dm.contracts
        if "not_null" in contracts:
            for col in contracts["not_null"]:
                total_checks += 1
                null_key = f"null_rate_{col}"
                null_rate = stats.get(null_key, 0.0)
                if null_rate > 0.05:
                    violations += 1

        if "positive" in contracts:
            for col in contracts["positive"]:
                total_checks += 1
                # Check avg value of column — negative avg signals sign-flip corruption
                avg_key = f"avg_{col}"
                avg_val = stats.get(avg_key)
                if avg_val is not None and avg_val < 0:
                    violations += 1

        scores[node] = violations / max(total_checks, 1)

    return scores

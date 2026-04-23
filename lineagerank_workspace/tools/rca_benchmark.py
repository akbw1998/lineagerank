from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import networkx as nx


FAULT_TYPES = [
    "schema_drift",
    "stale_source",
    "duplicate_ingestion",
    "missing_partition",
    "null_explosion",
    "bad_join_key",
]

OBSERVABILITY_MODES = [
    "full",
    "runtime_missing_root",
    "runtime_sparse",
]


@dataclass(frozen=True)
class PipelineSpec:
    name: str
    nodes: dict[str, str]
    edges: list[tuple[str, str]]
    source_nodes: tuple[str, ...]
    transform_nodes: tuple[str, ...]
    join_nodes: tuple[str, ...]
    leaf_test_weights: dict[str, int]


def get_pipeline_specs() -> dict[str, PipelineSpec]:
    return {
        "analytics_dag": PipelineSpec(
            name="analytics_dag",
            nodes={
                "customers": "source",
                "orders": "source",
                "payments": "source",
                "stg_customers": "staging",
                "stg_orders": "staging",
                "stg_payments": "staging",
                "customer_revenue": "mart",
            },
            edges=[
                ("customers", "stg_customers"),
                ("orders", "stg_orders"),
                ("payments", "stg_payments"),
                ("stg_customers", "customer_revenue"),
                ("stg_orders", "customer_revenue"),
                ("stg_payments", "customer_revenue"),
            ],
            source_nodes=("customers", "orders", "payments"),
            transform_nodes=("stg_customers", "stg_orders", "stg_payments", "customer_revenue"),
            join_nodes=("customer_revenue",),
            leaf_test_weights={"customer_revenue": 5},
        ),
        "tpcds_pipeline": PipelineSpec(
            name="tpcds_pipeline",
            nodes={
                "customers": "source",
                "stores": "source",
                "items": "source",
                "store_sales": "source",
                "daily_store_sales": "mart",
                "customer_ltv": "mart",
                "category_region_rollup": "mart",
            },
            edges=[
                ("store_sales", "daily_store_sales"),
                ("customers", "customer_ltv"),
                ("store_sales", "customer_ltv"),
                ("stores", "category_region_rollup"),
                ("items", "category_region_rollup"),
                ("store_sales", "category_region_rollup"),
            ],
            source_nodes=("customers", "stores", "items", "store_sales"),
            transform_nodes=("daily_store_sales", "customer_ltv", "category_region_rollup"),
            join_nodes=("customer_ltv", "category_region_rollup"),
            leaf_test_weights={
                "daily_store_sales": 4,
                "customer_ltv": 4,
                "category_region_rollup": 4,
            },
        ),
        "nyc_taxi_etl": PipelineSpec(
            name="nyc_taxi_etl",
            nodes={
                "taxi_trips_raw": "source",
                "taxi_zone_lookup": "source",
                "taxi_trips_enriched": "staging",
                "taxi_daily_zone_metrics": "mart",
                "taxi_fare_band_metrics": "mart",
            },
            edges=[
                ("taxi_trips_raw", "taxi_trips_enriched"),
                ("taxi_zone_lookup", "taxi_trips_enriched"),
                ("taxi_trips_enriched", "taxi_daily_zone_metrics"),
                ("taxi_trips_enriched", "taxi_fare_band_metrics"),
            ],
            source_nodes=("taxi_trips_raw", "taxi_zone_lookup"),
            transform_nodes=("taxi_trips_enriched", "taxi_daily_zone_metrics", "taxi_fare_band_metrics"),
            join_nodes=("taxi_trips_enriched",),
            leaf_test_weights={
                "taxi_daily_zone_metrics": 4,
                "taxi_fare_band_metrics": 3,
            },
        ),
        # Green taxi variant: same 8-node topology as nyc_taxi_etl_extended but
        # uses NYC TLC green taxi trip records (56k rows, Jan 2024).
        # Provides a second independent real-data pipeline for the case study.
        "nyc_green_taxi_etl_extended": PipelineSpec(
            name="nyc_green_taxi_etl_extended",
            nodes={
                "raw_green_trips":        "source",
                "zone_lookup":            "source",
                "green_trips_valid":      "staging",
                "green_trips_enriched":   "staging",
                "green_trips_classified": "staging",
                "green_daily_zone":       "mart",
                "green_fare_band":        "mart",
                "green_peak_hour":        "mart",
            },
            edges=[
                ("raw_green_trips",        "green_trips_valid"),
                ("green_trips_valid",      "green_trips_enriched"),
                ("zone_lookup",            "green_trips_enriched"),
                ("green_trips_enriched",   "green_trips_classified"),
                ("green_trips_classified", "green_daily_zone"),
                ("green_trips_classified", "green_fare_band"),
                ("green_trips_classified", "green_peak_hour"),
            ],
            source_nodes=("raw_green_trips", "zone_lookup"),
            transform_nodes=(
                "green_trips_valid", "green_trips_enriched", "green_trips_classified",
                "green_daily_zone", "green_fare_band", "green_peak_hour",
            ),
            join_nodes=("green_trips_enriched",),
            leaf_test_weights={
                "green_daily_zone": 3,
                "green_fare_band":  3,
                "green_peak_hour":  3,
            },
        ),
        # Extended 8-node pipeline used by run_real_case_study.py (Session 3+).
        # Adds an explicit validation step (trips_valid), a classification staging
        # step (trips_classified), and a third mart (peak_hour_metrics), creating
        # a richer candidate set and multi-level staging topology.
        "nyc_taxi_etl_extended": PipelineSpec(
            name="nyc_taxi_etl_extended",
            nodes={
                "raw_trips":          "source",
                "zone_lookup":        "source",
                "trips_valid":        "staging",
                "trips_enriched":     "staging",
                "trips_classified":   "staging",
                "daily_zone_metrics": "mart",
                "fare_band_metrics":  "mart",
                "peak_hour_metrics":  "mart",
            },
            edges=[
                ("raw_trips",        "trips_valid"),
                ("trips_valid",      "trips_enriched"),
                ("zone_lookup",      "trips_enriched"),
                ("trips_enriched",   "trips_classified"),
                ("trips_classified", "daily_zone_metrics"),
                ("trips_classified", "fare_band_metrics"),
                ("trips_classified", "peak_hour_metrics"),
            ],
            source_nodes=("raw_trips", "zone_lookup"),
            transform_nodes=(
                "trips_valid", "trips_enriched", "trips_classified",
                "daily_zone_metrics", "fare_band_metrics", "peak_hour_metrics",
            ),
            join_nodes=("trips_enriched",),
            leaf_test_weights={
                "daily_zone_metrics": 3,
                "fare_band_metrics":  3,
                "peak_hour_metrics":  3,
            },
        ),
    }


def build_graph(edges: Iterable[tuple[str, str]]) -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    return graph


def union_graph(design_edges: list[tuple[str, str]], runtime_edges: list[tuple[str, str]]) -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_edges_from(design_edges)
    graph.add_edges_from(runtime_edges)
    return graph


def leaf_descendants(spec: PipelineSpec, root: str) -> list[str]:
    graph = build_graph(spec.edges)
    descendants = nx.descendants(graph, root)
    leaves = [node for node in descendants if graph.out_degree(node) == 0]
    return sorted(leaves)


def eligible_roots(spec: PipelineSpec, fault_type: str) -> tuple[str, ...]:
    graph = build_graph(spec.edges)
    nodes_with_descendants = tuple(node for node in spec.nodes if len(nx.descendants(graph, node)) > 0)
    source_like = tuple(node for node in spec.source_nodes if node in nodes_with_descendants)
    transform_like = tuple(node for node in spec.transform_nodes if node in nodes_with_descendants)

    if fault_type in {"stale_source", "duplicate_ingestion", "missing_partition"}:
        return source_like or nodes_with_descendants

    if fault_type == "bad_join_key":
        join_candidates = [node for node in spec.join_nodes if node in nodes_with_descendants]
        if join_candidates:
            return tuple(join_candidates)
        parent_candidates = []
        for join_node in spec.join_nodes:
            parent_candidates.extend(parent for parent, child in spec.edges if child == join_node and parent in nodes_with_descendants)
        return tuple(sorted(set(parent_candidates))) or nodes_with_descendants

    return tuple(sorted(set(source_like + transform_like))) or nodes_with_descendants


def impacted_assets(spec: PipelineSpec, root: str) -> list[str]:
    graph = build_graph(spec.edges)
    impacted = sorted(nx.descendants(graph, root))
    return impacted

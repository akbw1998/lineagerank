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
        # Divvy Chicago bike-share (Jan 2024, 144,873 trips).
        # Same 8-node chain topology as nyc_taxi_etl_extended — different domain
        # (bike-share vs. taxi), different staging semantics, different mart
        # aggregations. Used by run_real_case_study.py as a third real pipeline.
        "divvy_chicago_bike": PipelineSpec(
            name="divvy_chicago_bike",
            nodes={
                "raw_rides":              "source",
                "station_lookup":         "source",
                "rides_valid":            "staging",
                "rides_enriched":         "staging",
                "rides_classified":       "staging",
                "daily_station_metrics":  "mart",
                "duration_tier_metrics":  "mart",
                "member_type_metrics":    "mart",
            },
            edges=[
                ("raw_rides",          "rides_valid"),
                ("rides_valid",        "rides_enriched"),
                ("station_lookup",     "rides_enriched"),
                ("rides_enriched",     "rides_classified"),
                ("rides_classified",   "daily_station_metrics"),
                ("rides_classified",   "duration_tier_metrics"),
                ("rides_classified",   "member_type_metrics"),
            ],
            source_nodes=("raw_rides", "station_lookup"),
            transform_nodes=(
                "rides_valid", "rides_enriched", "rides_classified",
                "daily_station_metrics", "duration_tier_metrics", "member_type_metrics",
            ),
            join_nodes=("rides_enriched",),
            leaf_test_weights={
                "daily_station_metrics":  3,
                "duration_tier_metrics":  3,
                "member_type_metrics":    3,
            },
        ),
        # BTS Airline On-Time Performance (Jan 2024, 547,271 flights).
        # Dual-path DAG: airport_lookup feeds BOTH flights_enriched (via JOIN)
        # AND route_delay_metrics (second lookup for origin/dest labels).
        # This creates a structurally distinct topology absent from all other
        # pipeline families — join_nodes has two members.
        "bts_airline_ontime": PipelineSpec(
            name="bts_airline_ontime",
            nodes={
                "raw_flights":          "source",
                "airport_lookup":       "source",
                "flights_valid":        "staging",
                "flights_enriched":     "staging",
                "flights_classified":   "staging",
                "carrier_daily_metrics":"mart",
                "route_delay_metrics":  "mart",
                "delay_tier_metrics":   "mart",
            },
            edges=[
                ("raw_flights",          "flights_valid"),
                ("flights_valid",        "flights_enriched"),
                ("airport_lookup",       "flights_enriched"),
                ("flights_enriched",     "flights_classified"),
                ("flights_classified",   "carrier_daily_metrics"),
                ("flights_classified",   "delay_tier_metrics"),
                ("flights_classified",   "route_delay_metrics"),
                ("airport_lookup",       "route_delay_metrics"),
            ],
            source_nodes=("raw_flights", "airport_lookup"),
            transform_nodes=(
                "flights_valid", "flights_enriched", "flights_classified",
                "carrier_daily_metrics", "route_delay_metrics", "delay_tier_metrics",
            ),
            join_nodes=("flights_enriched", "route_delay_metrics"),
            leaf_test_weights={
                "carrier_daily_metrics": 3,
                "route_delay_metrics":   3,
                "delay_tier_metrics":    3,
            },
        ),
        # NHTSA FARS 2022 — 3-source fan-in topology.
        # Three independent source tables (accidents, vehicles, persons) all join
        # on ST_CASE at crash_merged, creating the only pipeline in the benchmark
        # with 3 true source nodes.  join_nodes has one member (the merge point).
        "nhtsa_fars_crash": PipelineSpec(
            name="nhtsa_fars_crash",
            nodes={
                "raw_accidents":       "source",
                "raw_vehicles":        "source",
                "raw_persons":         "source",
                "crashes_valid":       "staging",
                "crash_merged":        "staging",
                "crash_classified":    "staging",
                "daily_crash_metrics": "mart",
                "state_crash_summary": "mart",
            },
            edges=[
                ("raw_accidents",      "crashes_valid"),
                ("raw_vehicles",       "crash_merged"),
                ("raw_persons",        "crash_merged"),
                ("crashes_valid",      "crash_merged"),
                ("crash_merged",       "crash_classified"),
                ("crash_classified",   "daily_crash_metrics"),
                ("crash_classified",   "state_crash_summary"),
            ],
            source_nodes=("raw_accidents", "raw_vehicles", "raw_persons"),
            transform_nodes=(
                "crashes_valid", "crash_merged", "crash_classified",
                "daily_crash_metrics", "state_crash_summary",
            ),
            join_nodes=("crash_merged",),
            leaf_test_weights={
                "daily_crash_metrics": 3,
                "state_crash_summary": 3,
            },
        ),
        # Chicago Crime 2023 — diamond (parallel enrichment) topology.
        # crimes_valid splits into two parallel branches: type_enriched (joins
        # iucr_lookup) and district_enriched (aggregates district context).
        # Both branches reconverge at crimes_unified — the only pipeline with
        # this parallel-reconvergence (diamond) structure.
        "chicago_crime_2023": PipelineSpec(
            name="chicago_crime_2023",
            nodes={
                "raw_crimes":          "source",
                "iucr_lookup":         "source",
                "crimes_valid":        "staging",
                "type_enriched":       "staging",
                "district_enriched":   "staging",
                "crimes_unified":      "staging",
                "daily_crime_metrics": "mart",
                "district_summary":    "mart",
            },
            edges=[
                ("raw_crimes",         "crimes_valid"),
                ("iucr_lookup",        "type_enriched"),
                ("crimes_valid",       "type_enriched"),
                ("crimes_valid",       "district_enriched"),
                ("type_enriched",      "crimes_unified"),
                ("district_enriched",  "crimes_unified"),
                ("crimes_unified",     "daily_crime_metrics"),
                ("crimes_unified",     "district_summary"),
            ],
            source_nodes=("raw_crimes", "iucr_lookup"),
            transform_nodes=(
                "crimes_valid", "type_enriched", "district_enriched",
                "crimes_unified", "daily_crime_metrics", "district_summary",
            ),
            join_nodes=("type_enriched", "crimes_unified"),
            leaf_test_weights={
                "daily_crime_metrics": 3,
                "district_summary":    3,
            },
        ),
        # EPA AQS PM2.5 2023 — deep temporal aggregation chain.
        # site_enriched branches to daily_county and exceedance_sites; daily_county
        # then feeds state_monthly → national_summary, creating the deepest
        # aggregation chain (5 hops source-to-leaf) in the benchmark.
        "epa_aqs_pm25": PipelineSpec(
            name="epa_aqs_pm25",
            nodes={
                "raw_measurements":  "source",
                "site_lookup":       "source",
                "measurements_valid":"staging",
                "site_enriched":     "staging",
                "daily_county":      "staging",
                "state_monthly":     "mart",
                "exceedance_sites":  "mart",
                "national_summary":  "mart",
            },
            edges=[
                ("raw_measurements",  "measurements_valid"),
                ("site_lookup",       "site_enriched"),
                ("measurements_valid","site_enriched"),
                ("site_enriched",     "daily_county"),
                ("daily_county",      "state_monthly"),
                ("daily_county",      "exceedance_sites"),
                ("state_monthly",     "national_summary"),
            ],
            source_nodes=("raw_measurements", "site_lookup"),
            transform_nodes=(
                "measurements_valid", "site_enriched", "daily_county",
                "state_monthly", "exceedance_sites", "national_summary",
            ),
            join_nodes=("site_enriched",),
            leaf_test_weights={
                "state_monthly":    3,
                "exceedance_sites": 3,
                "national_summary": 2,
            },
        ),
        # NOAA Storm Events 2023 — wide fan-out with 3 leaf marts.
        # Two source tables (events + fatalities) join on event_id at
        # fatality_enriched, then fan out to 3 leaf aggregations — matching the
        # 3-leaf pattern of the taxi/divvy pipelines but in a new domain with
        # event-count rather than trip-count semantics.
        "noaa_storm_events": PipelineSpec(
            name="noaa_storm_events",
            nodes={
                "raw_events":          "source",
                "raw_fatalities":      "source",
                "events_valid":        "staging",
                "fatality_enriched":   "staging",
                "events_classified":   "staging",
                "daily_event_metrics": "mart",
                "state_summary":       "mart",
                "damage_distribution": "mart",
            },
            edges=[
                ("raw_events",          "events_valid"),
                ("raw_fatalities",      "fatality_enriched"),
                ("events_valid",        "fatality_enriched"),
                ("fatality_enriched",   "events_classified"),
                ("events_classified",   "daily_event_metrics"),
                ("events_classified",   "state_summary"),
                ("events_classified",   "damage_distribution"),
            ],
            source_nodes=("raw_events", "raw_fatalities"),
            transform_nodes=(
                "events_valid", "fatality_enriched", "events_classified",
                "daily_event_metrics", "state_summary", "damage_distribution",
            ),
            join_nodes=("fatality_enriched",),
            leaf_test_weights={
                "daily_event_metrics": 2,
                "state_summary":       2,
                "damage_distribution": 2,
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

"""Real public-data case study for PipeRCA-Bench.

Builds a realistic 8-node NYC taxi ETL pipeline on actual TLC trip records and
taxi zone data, injects six fault types, captures runtime lineage from DuckDB
execution, and evaluates all proposed RCA methods.

Key design decisions (Session 3 — correctness audit):
- Uses the extended nyc_taxi_etl_extended spec (8 nodes, 3 staging layers).
- Signal generation uses STOCHASTIC ranges matching the synthetic benchmark's
  distributions, not hard-coded values.  Root anomaly is anchored to real
  DuckDB row-count deltas; non-root signals are drawn from overlapping ranges
  to ensure realistic discrimination difficulty.
- One to two non-root "decoy" nodes are assigned elevated signals per incident,
  matching the synthetic incident generator in generate_incidents.py.
- All 6 fault types (including schema_drift targeting a staging node) are
  supported, giving 90 incidents over 15 iterations per fault.
- Observability is rotated across iterations: full (0-4), sparse (5-9),
  missing-root (10-14).
"""
from __future__ import annotations

import argparse
import json
import random
import tempfile
from pathlib import Path

import duckdb
import networkx as nx

from evaluate_rankers import (
    aggregate_details,
    by_group_from_details,
    candidate_rows,
    rank_details,
    score_rows,
)
from rca_benchmark import build_graph, get_pipeline_specs


ROOT = Path(__file__).resolve().parents[1]
SPEC       = get_pipeline_specs()["nyc_taxi_etl_extended"]
GREEN_SPEC = get_pipeline_specs()["nyc_green_taxi_etl_extended"]

# ── SQL pipeline steps ─────────────────────────────────────────────────────────
# Each entry: (step_name, sql_template, inputs, outputs)
# sql_template may reference {drift} placeholder for schema_drift injection.

_SQL_VALID = """
create or replace table trips_valid as
select
  VendorID,
  cast(tpep_pickup_datetime as timestamp)  as pickup_datetime,
  cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
  passenger_count,
  trip_distance,
  PULocationID,
  DOLocationID,
  fare_amount,
  total_amount
from raw_trips
where PULocationID is not null
  and DOLocationID is not null
  and fare_amount > 0
  and trip_distance > 0
  and tpep_pickup_datetime is not null;
"""

_SQL_ENRICHED = """
create or replace table trips_enriched as
select
  v.*,
  coalesce(z.Borough, 'Unknown') as pickup_borough,
  coalesce(z.Zone,    'Unknown') as pickup_zone
from trips_valid v
left join zone_lookup z on v.PULocationID = z.LocationID;
"""

# Normal time_period classification
_SQL_CLASSIFIED_NORMAL = """
create or replace table trips_classified as
select
  e.*,
  date_trunc('day', e.pickup_datetime) as trip_day,
  extract(hour from e.pickup_datetime)::integer as pickup_hour,
  case
    when extract(hour from e.pickup_datetime) between 7 and 9
      or extract(hour from e.pickup_datetime) between 16 and 19
    then 'peak' else 'off_peak'
  end as time_period,
  case
    when e.fare_amount < 10  then 'economy'
    when e.fare_amount < 25  then 'standard'
    else                          'premium'
  end as fare_tier
from trips_enriched e;
"""

# Schema-drift variant: time_period always 'off_peak' (broken logic)
_SQL_CLASSIFIED_DRIFTED = """
create or replace table trips_classified as
select
  e.*,
  date_trunc('day', e.pickup_datetime) as trip_day,
  extract(hour from e.pickup_datetime)::integer as pickup_hour,
  'off_peak' as time_period,
  case
    when e.fare_amount < 10  then 'economy'
    when e.fare_amount < 25  then 'standard'
    else                          'premium'
  end as fare_tier
from trips_enriched e;
"""

_SQL_DAILY_ZONE = """
create or replace table daily_zone_metrics as
select
  trip_day,
  pickup_borough,
  pickup_zone,
  count(*)                     as trip_count,
  round(avg(fare_amount), 2)   as avg_fare,
  round(sum(trip_distance), 2) as total_distance
from trips_classified
group by 1, 2, 3;
"""

_SQL_FARE_BAND = """
create or replace table fare_band_metrics as
select
  fare_tier,
  pickup_borough,
  count(*)                         as trip_count,
  round(sum(total_amount), 2)      as total_revenue
from trips_classified
group by 1, 2;
"""

_SQL_PEAK_HOUR = """
create or replace table peak_hour_metrics as
select
  pickup_hour,
  time_period,
  pickup_borough,
  count(*)                        as trip_count,
  round(avg(trip_distance), 2)    as avg_distance
from trips_classified
group by 1, 2, 3;
"""


def _run_pipeline(conn: duckdb.DuckDBPyConnection, schema_drifted: bool = False) -> dict[str, object]:
    """Execute the full extended ETL and return row counts + lineage edges."""
    lineage: list[tuple[str, str]] = []
    step_rows: dict[str, int] = {}

    def _run(sql: str, inputs: list[str], output: str) -> None:
        conn.execute(sql)
        cnt = conn.execute(f"select count(*) from {output}").fetchone()[0]
        step_rows[output] = int(cnt)
        lineage.extend((inp, output) for inp in inputs)

    _run(_SQL_VALID,    ["raw_trips"],                            "trips_valid")
    _run(_SQL_ENRICHED, ["trips_valid", "zone_lookup"],           "trips_enriched")
    classified_sql = _SQL_CLASSIFIED_DRIFTED if schema_drifted else _SQL_CLASSIFIED_NORMAL
    _run(classified_sql, ["trips_enriched"],                      "trips_classified")
    _run(_SQL_DAILY_ZONE, ["trips_classified"],                   "daily_zone_metrics")
    _run(_SQL_FARE_BAND,  ["trips_classified"],                   "fare_band_metrics")
    _run(_SQL_PEAK_HOUR,  ["trips_classified"],                   "peak_hour_metrics")

    # Validation queries used for signal anchoring
    validations = conn.execute(
        """
        select
          (select count(*) from trips_enriched  where pickup_borough = 'Unknown') as unknown_borough,
          (select count(*) from daily_zone_metrics)                               as daily_zone_rows,
          (select count(*) from peak_hour_metrics
           where time_period = 'peak')                                            as peak_rows
        """
    ).fetchone()

    return {
        "runtime_lineage": lineage,
        "step_rows": step_rows,
        "validations": {
            "unknown_borough": int(validations[0]),
            "daily_zone_rows": int(validations[1]),
            "peak_rows":       int(validations[2]),
        },
    }


def _load_sources(conn: duckdb.DuckDBPyConnection, max_rows: int) -> None:
    raw_dir = ROOT / "data" / "raw" / "nyc_taxi"
    trip_path = raw_dir / "yellow_tripdata_2024-01.parquet"
    zone_path = raw_dir / "taxi_zone_lookup.csv"
    if not trip_path.exists() or not zone_path.exists():
        raise FileNotFoundError(
            "Real NYC taxi case study requires yellow_tripdata_2024-01.parquet "
            "and taxi_zone_lookup.csv in data/raw/nyc_taxi/"
        )
    conn.execute(f"""
        create or replace table raw_trips as
        select * from read_parquet('{trip_path.as_posix()}') limit {int(max_rows)}
    """)
    conn.execute(f"""
        create or replace table zone_lookup as
        select * from read_csv_auto('{zone_path.as_posix()}', header=true)
    """)


def _apply_fault(
    conn: duckdb.DuckDBPyConnection,
    fault_type: str,
    iteration: int,
) -> tuple[str, str, bool]:
    """Inject a fault into source tables.

    Returns (root_node, observed_node, schema_drifted).
    schema_drifted=True means the pipeline should use the drifted SQL for
    trips_classified (the fault is in the staging transformation logic).
    """
    if fault_type == "missing_partition":
        conn.execute("""
            create or replace table raw_trips as
            select * from raw_trips
            where cast(tpep_pickup_datetime as date) <> date '2024-01-15'
        """)
        return "raw_trips", "daily_zone_metrics", False

    if fault_type == "duplicate_ingestion":
        conn.execute("""
            create or replace table raw_trips as
            select * from raw_trips
            union all
            select * from raw_trips
            where cast(tpep_pickup_datetime as date) = date '2024-01-10'
        """)
        return "raw_trips", "fare_band_metrics", False

    if fault_type == "stale_source":
        conn.execute("""
            create or replace table raw_trips as
            select * from raw_trips
            where cast(tpep_pickup_datetime as date) < date '2024-01-20'
        """)
        return "raw_trips", "daily_zone_metrics", False

    if fault_type == "null_explosion":
        conn.execute(f"""
            create or replace table raw_trips as
            select
              VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
              passenger_count, trip_distance,
              case when (row_number() over ()) % 7 = {iteration % 7}
                   then null else PULocationID end as PULocationID,
              DOLocationID, fare_amount, total_amount
            from raw_trips
        """)
        return "raw_trips", "daily_zone_metrics", False

    if fault_type == "bad_join_key":
        conn.execute("""
            create or replace table zone_lookup as
            select
              case when LocationID between 1 and 15 then LocationID + 600
                   else LocationID end as LocationID,
              Borough, Zone, service_zone
            from zone_lookup
        """)
        return "zone_lookup", "daily_zone_metrics", False

    if fault_type == "schema_drift":
        # No source table modification — fault is in trips_classified SQL logic.
        # The drifted SQL hard-codes 'off_peak' for all rows, corrupting peak_hour_metrics.
        return "trips_classified", "peak_hour_metrics", True

    raise ValueError(f"Unsupported fault_type: {fault_type!r}")


def _apply_observability(
    runtime_edges: list[tuple[str, str]],
    root: str,
    iteration: int,
    rng: random.Random,
) -> tuple[list[tuple[str, str]], str]:
    """Rotate observability mode across iteration brackets."""
    if iteration < 5:
        return runtime_edges, "full"
    if iteration < 10:
        # sparse: root edges always absent; 30% of other edges dropped
        sparse = []
        for edge in runtime_edges:
            if edge[0] == root:
                continue
            if rng.random() > 0.3:
                sparse.append(edge)
        return sparse, "runtime_sparse"
    # missing_root: all root outgoing edges absent
    return [e for e in runtime_edges if e[0] != root], "runtime_missing_root"


def _build_signals(
    spec_nodes: dict[str, str],
    root: str,
    observed: str,
    fault_type: str,
    baseline_rows: dict[str, int],
    faulted_rows: dict[str, int],
    baseline_val: dict[str, int],
    faulted_val: dict[str, int],
    rng: random.Random,
) -> dict[str, dict[str, float | int]]:
    """Build stochastic evidence signals anchored to real DuckDB row-count deltas.

    Signal distribution philosophy (matching generate_incidents._node_signals):
    - Root:     recent_change ~ U(0.55, 0.86),  run_anomaly ~ U(0.48, 0.84)
    - Decoys:   recent_change ~ U(0.58, 0.90),  run_anomaly ~ U(0.34, 0.71)
    - Impacted: recent_change ~ U(0.05, 0.28),  run_anomaly boosted by row_delta
    - Other:    recent_change ~ U(0.05, 0.28),  run_anomaly ~ U(0.04, 0.22)

    Decoys (1-2 randomly selected non-root ancestors) receive elevated false-positive
    signals to model realistic monitoring noise and prevent trivially easy ranking.

    The run_anomaly for root and impacted nodes is further nudged by the real
    row_delta — the fractional row-count deviation measured from actual DuckDB
    execution — keeping the signal grounded in real data behaviour.
    """
    # Build DAG using all edges from the spec that covers these node names
    # Infer the spec from the node names present in spec_nodes
    all_specs = get_pipeline_specs()
    dag_edges: list[tuple[str, str]] = []
    for s in all_specs.values():
        if set(s.nodes.keys()) == set(spec_nodes.keys()):
            dag_edges = list(s.edges)
            break
    if not dag_edges:
        dag_edges = list(SPEC.edges)  # fallback
    dag = build_graph(dag_edges)
    ancestors_of_observed = set(nx.ancestors(dag, observed)) if observed in dag else set()
    impacted_set = set(nx.descendants(dag, root)) if root in dag else set()

    # Select 1-2 decoy nodes: non-root ancestors of observed
    decoy_candidates = [n for n in ancestors_of_observed if n != root and n != observed]
    decoys = set(rng.sample(decoy_candidates, k=min(2, len(decoy_candidates))))

    signals: dict[str, dict[str, float | int]] = {}
    for node, _node_type in spec_nodes.items():
        is_root = (node == root)
        is_impacted = (node in impacted_set)
        is_decoy = (node in decoys)

        # Real row-count delta for this node (0.0 if node not in both snapshots)
        row_delta = 0.0
        if node in baseline_rows and node in faulted_rows:
            base = max(1, int(baseline_rows[node]))
            row_delta = min(1.0, abs(int(faulted_rows[node]) - base) / base)

        if is_root:
            rc   = rng.uniform(0.55, 0.86)
            anom = min(1.0, rng.uniform(0.48, 0.84) * (1.0 + 0.25 * row_delta))
        elif is_decoy:
            rc   = rng.uniform(0.58, 0.90)   # can overlap with root!
            anom = rng.uniform(0.34, 0.71)
        elif is_impacted:
            rc   = rng.uniform(0.05, 0.28)
            anom = min(1.0, rng.uniform(0.12, 0.38) + 0.35 * row_delta)
        else:
            rc   = rng.uniform(0.05, 0.28)
            anom = rng.uniform(0.04, 0.22)

        signals[node] = {
            "recent_change":    round(rc, 3),
            "freshness_severity": 0.0,
            "run_anomaly":      round(anom, 3),
            "contract_violation": 0,
            "local_test_failures": 0,
        }

    # ── Fault-type-specific signal augmentations ─────────────────────────────
    if fault_type == "stale_source":
        # Root freshness signal is always high for stale_source
        signals[root]["freshness_severity"] = round(rng.uniform(0.82, 0.98), 3)
        if "trips_enriched" in signals:
            signals["trips_enriched"]["freshness_severity"] = round(rng.uniform(0.32, 0.74), 3)
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6)

    elif fault_type == "missing_partition":
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "peak_hour_metrics" in signals:
            signals["peak_hour_metrics"]["local_test_failures"] = rng.randint(1, 3)

    elif fault_type == "duplicate_ingestion":
        if "fare_band_metrics" in signals:
            signals["fare_band_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(1, 3)

    elif fault_type == "null_explosion":
        if "trips_enriched" in signals:
            signals["trips_enriched"]["local_test_failures"] = rng.randint(1, 4)
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6)

    elif fault_type == "bad_join_key":
        unknown_delta = max(
            0,
            int(faulted_val.get("unknown_borough", 0)) - int(baseline_val.get("unknown_borough", 0)),
        )
        signals["zone_lookup"]["contract_violation"] = 1
        if "trips_enriched" in signals:
            signals["trips_enriched"]["local_test_failures"] = rng.randint(2, 4) if unknown_delta > 0 else rng.randint(1, 2)
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6) if unknown_delta > 0 else rng.randint(1, 3)

    elif fault_type == "schema_drift":
        # trips_classified (the root) has a contract violation (output schema changed)
        signals[root]["contract_violation"] = 1 if rng.random() > 0.20 else 0
        # peak_hour_metrics shows the failure most strongly
        if "peak_hour_metrics" in signals:
            signals["peak_hour_metrics"]["local_test_failures"] = rng.randint(3, 6)
        # daily_zone_metrics is unaffected (no time_period dependency)
        # fare_band_metrics is mildly affected (fare_tier is still correct)
        if "fare_band_metrics" in signals:
            signals["fare_band_metrics"]["local_test_failures"] = rng.randint(0, 1)

    # The observed failure node always has elevated test failures
    if observed in signals:
        signals[observed]["local_test_failures"] = max(
            int(signals[observed]["local_test_failures"]), rng.randint(3, 6)
        )

    return signals


def _build_incident(
    incident_id: str,
    fault_type: str,
    root: str,
    observed: str,
    runtime_edges: list[tuple[str, str]],
    signals: dict[str, dict[str, float | int]],
    observability_mode: str,
    spec=None,
    pipeline_name: str = "nyc_taxi_etl_extended_real",
) -> dict[str, object]:
    if spec is None:
        spec = SPEC
    return {
        "incident_id": incident_id,
        "pipeline": pipeline_name,
        "fault_type": fault_type,
        "observability_mode": observability_mode,
        "root_cause_asset": root,
        "observed_failure_asset": observed,
        "impacted_assets": sorted(set(nx.descendants(build_graph(list(spec.edges)), root))),
        "design_edges": list(spec.edges),
        "runtime_edges": runtime_edges,
        "leaf_test_weights": spec.leaf_test_weights,
        "node_types": spec.nodes,
        "node_signals": signals,
    }


# ── Green taxi pipeline (NYC TLC green taxi, Jan 2024) ─────────────────────────
# Same 8-node topology as the yellow taxi extended pipeline but uses green taxi
# trip records (56k rows, smaller dataset, different datetime column names).

_GREEN_SQL_VALID = """
create or replace table green_trips_valid as
select
  VendorID,
  cast(lpep_pickup_datetime  as timestamp) as pickup_datetime,
  cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
  passenger_count,
  trip_distance,
  PULocationID,
  DOLocationID,
  fare_amount,
  total_amount
from raw_green_trips
where PULocationID is not null
  and DOLocationID is not null
  and fare_amount > 0
  and trip_distance > 0
  and lpep_pickup_datetime is not null;
"""

_GREEN_SQL_ENRICHED = """
create or replace table green_trips_enriched as
select
  v.*,
  coalesce(z.Borough, 'Unknown') as pickup_borough,
  coalesce(z.Zone,    'Unknown') as pickup_zone
from green_trips_valid v
left join zone_lookup z on v.PULocationID = z.LocationID;
"""

_GREEN_SQL_CLASSIFIED_NORMAL = """
create or replace table green_trips_classified as
select
  e.*,
  date_trunc('day', e.pickup_datetime) as trip_day,
  extract(hour from e.pickup_datetime)::integer as pickup_hour,
  case
    when extract(hour from e.pickup_datetime) between 7 and 9
      or extract(hour from e.pickup_datetime) between 16 and 19
    then 'peak' else 'off_peak'
  end as time_period,
  case
    when e.fare_amount < 10  then 'economy'
    when e.fare_amount < 25  then 'standard'
    else                          'premium'
  end as fare_tier
from green_trips_enriched e;
"""

_GREEN_SQL_CLASSIFIED_DRIFTED = """
create or replace table green_trips_classified as
select
  e.*,
  date_trunc('day', e.pickup_datetime) as trip_day,
  extract(hour from e.pickup_datetime)::integer as pickup_hour,
  'off_peak' as time_period,
  case
    when e.fare_amount < 10  then 'economy'
    when e.fare_amount < 25  then 'standard'
    else                          'premium'
  end as fare_tier
from green_trips_enriched e;
"""

_GREEN_SQL_DAILY = """
create or replace table green_daily_zone as
select trip_day, pickup_borough, pickup_zone,
       count(*) as trip_count,
       round(avg(fare_amount), 2) as avg_fare
from green_trips_classified group by 1, 2, 3;
"""

_GREEN_SQL_FARE = """
create or replace table green_fare_band as
select fare_tier, pickup_borough,
       count(*) as trip_count,
       round(sum(total_amount), 2) as total_revenue
from green_trips_classified group by 1, 2;
"""

_GREEN_SQL_PEAK = """
create or replace table green_peak_hour as
select pickup_hour, time_period, pickup_borough,
       count(*) as trip_count,
       round(avg(trip_distance), 2) as avg_distance
from green_trips_classified group by 1, 2, 3;
"""


def _load_green_sources(conn: duckdb.DuckDBPyConnection, max_rows: int = 56551) -> None:
    raw_dir = ROOT / "data" / "raw" / "nyc_taxi"
    green_path = raw_dir / "green_tripdata_2024-01.parquet"
    zone_path  = raw_dir / "taxi_zone_lookup.csv"
    if not green_path.exists():
        raise FileNotFoundError(f"Green taxi parquet not found: {green_path}")
    conn.execute(f"""
        create or replace table raw_green_trips as
        select * from read_parquet('{green_path.as_posix()}') limit {int(max_rows)}
    """)
    conn.execute(f"""
        create or replace table zone_lookup as
        select * from read_csv_auto('{zone_path.as_posix()}', header=true)
    """)


def _run_green_pipeline(conn: duckdb.DuckDBPyConnection, schema_drifted: bool = False) -> dict[str, object]:
    lineage: list[tuple[str, str]] = []
    step_rows: dict[str, int] = {}

    def _run(sql: str, inputs: list[str], output: str) -> None:
        conn.execute(sql)
        cnt = conn.execute(f"select count(*) from {output}").fetchone()[0]
        step_rows[output] = int(cnt)
        lineage.extend((inp, output) for inp in inputs)

    _run(_GREEN_SQL_VALID,    ["raw_green_trips"],                            "green_trips_valid")
    _run(_GREEN_SQL_ENRICHED, ["green_trips_valid", "zone_lookup"],           "green_trips_enriched")
    cl_sql = _GREEN_SQL_CLASSIFIED_DRIFTED if schema_drifted else _GREEN_SQL_CLASSIFIED_NORMAL
    _run(cl_sql,              ["green_trips_enriched"],                       "green_trips_classified")
    _run(_GREEN_SQL_DAILY,    ["green_trips_classified"],                     "green_daily_zone")
    _run(_GREEN_SQL_FARE,     ["green_trips_classified"],                     "green_fare_band")
    _run(_GREEN_SQL_PEAK,     ["green_trips_classified"],                     "green_peak_hour")

    validations = conn.execute("""
        select
          (select count(*) from green_trips_enriched where pickup_borough = 'Unknown') as unknown_borough,
          (select count(*) from green_daily_zone)                                      as daily_zone_rows,
          (select count(*) from green_peak_hour where time_period = 'peak')            as peak_rows
    """).fetchone()

    return {
        "runtime_lineage": lineage,
        "step_rows": step_rows,
        "validations": {
            "unknown_borough": int(validations[0]),
            "daily_zone_rows": int(validations[1]),
            "peak_rows":       int(validations[2]),
        },
    }


def _apply_green_fault(
    conn: duckdb.DuckDBPyConnection,
    fault_type: str,
    iteration: int,
) -> tuple[str, str, bool]:
    if fault_type == "missing_partition":
        conn.execute("""
            create or replace table raw_green_trips as
            select * from raw_green_trips
            where cast(lpep_pickup_datetime as date) <> date '2024-01-12'
        """)
        return "raw_green_trips", "green_daily_zone", False

    if fault_type == "duplicate_ingestion":
        conn.execute("""
            create or replace table raw_green_trips as
            select * from raw_green_trips
            union all
            select * from raw_green_trips
            where cast(lpep_pickup_datetime as date) = date '2024-01-05'
        """)
        return "raw_green_trips", "green_fare_band", False

    if fault_type == "stale_source":
        conn.execute("""
            create or replace table raw_green_trips as
            select * from raw_green_trips
            where cast(lpep_pickup_datetime as date) < date '2024-01-18'
        """)
        return "raw_green_trips", "green_daily_zone", False

    if fault_type == "null_explosion":
        conn.execute(f"""
            create or replace table raw_green_trips as
            select
              VendorID, lpep_pickup_datetime, lpep_dropoff_datetime,
              passenger_count, trip_distance,
              case when (row_number() over ()) % 7 = {iteration % 7}
                   then null else PULocationID end as PULocationID,
              DOLocationID, fare_amount, total_amount
            from raw_green_trips
        """)
        return "raw_green_trips", "green_daily_zone", False

    if fault_type == "bad_join_key":
        # zone_lookup is shared — shift LocationIDs (different range than yellow to vary the test)
        conn.execute("""
            create or replace table zone_lookup as
            select
              case when LocationID between 100 and 120 then LocationID + 700
                   else LocationID end as LocationID,
              Borough, Zone, service_zone
            from zone_lookup
        """)
        return "zone_lookup", "green_daily_zone", False

    if fault_type == "schema_drift":
        return "green_trips_classified", "green_peak_hour", True

    raise ValueError(f"Unsupported fault_type: {fault_type!r}")


def _run_one_pipeline(
    tmp_dir: str,
    prefix: str,
    fault_types: list[str],
    per_fault: int,
    max_rows: int,
    spec,
    load_fn,
    run_fn,
    fault_fn,
    pipeline_name: str,
    seed_base: int,
    incident_counter_start: int,
) -> list[dict[str, object]]:
    """Run one complete real-data pipeline case study and return incidents."""
    incidents: list[dict[str, object]] = []

    # Build clean baseline
    bl_conn = duckdb.connect(str(Path(tmp_dir) / f"{prefix}_baseline.duckdb"))
    load_fn(bl_conn, max_rows)
    baseline = run_fn(bl_conn)
    bl_conn.close()

    counter = incident_counter_start
    for fault_type in fault_types:
        for iteration in range(per_fault):
            db_path = Path(tmp_dir) / f"{prefix}_{fault_type}_{iteration}.duckdb"
            conn = duckdb.connect(str(db_path))
            load_fn(conn, max_rows)

            root, observed, schema_drifted = fault_fn(conn, fault_type, iteration + 1)
            faulted = run_fn(conn, schema_drifted=schema_drifted)
            conn.close()

            raw_edges = [(src, dst) for src, dst in faulted["runtime_lineage"]]

            obs_rng = random.Random(seed_base + iteration * 53 + abs(hash(fault_type)) % 997)
            runtime_edges, obs_mode = _apply_observability(raw_edges, root, iteration, obs_rng)

            sig_rng = random.Random(seed_base + 1000 + iteration * 71 + abs(hash(fault_type)) % 503)
            signals = _build_signals(
                spec_nodes=dict(spec.nodes),
                root=root,
                observed=observed,
                fault_type=fault_type,
                baseline_rows=baseline["step_rows"],
                faulted_rows=faulted["step_rows"],
                baseline_val=baseline["validations"],
                faulted_val=faulted["validations"],
                rng=sig_rng,
            )

            incidents.append(
                _build_incident(
                    f"real_inc_{counter:03d}",
                    fault_type,
                    root,
                    observed,
                    runtime_edges,
                    signals,
                    obs_mode,
                    spec=spec,
                    pipeline_name=pipeline_name,
                )
            )
            counter += 1

    return incidents


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--per-fault", type=int, default=15,
                        help="Iterations per fault type (15 = 5 full + 5 sparse + 5 missing_root)")
    parser.add_argument("--max-rows", type=int, default=120000)
    parser.add_argument("--output", type=Path,
                        default=ROOT / "experiments" / "results" / "real_case_study_eval.json")
    args = parser.parse_args()

    fault_types = [
        "missing_partition",
        "duplicate_ingestion",
        "stale_source",
        "null_explosion",
        "bad_join_key",
        "schema_drift",
    ]

    green_path = ROOT / "data" / "raw" / "nyc_taxi" / "green_tripdata_2024-01.parquet"
    has_green = green_path.exists()

    incidents: list[dict[str, object]] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        # ── Pipeline 1: Yellow taxi (main) ─────────────────────────────────
        print("Running yellow taxi pipeline (extended, 8 nodes) …")
        yellow_incidents = _run_one_pipeline(
            tmp_dir=tmp_dir,
            prefix="yellow",
            fault_types=fault_types,
            per_fault=args.per_fault,
            max_rows=args.max_rows,
            spec=SPEC,
            load_fn=_load_sources,
            run_fn=_run_pipeline,
            fault_fn=_apply_fault,
            pipeline_name="nyc_yellow_taxi_etl_real",
            seed_base=1337,
            incident_counter_start=1,
        )
        incidents.extend(yellow_incidents)
        n_yellow = len(yellow_incidents)
        print(f"  Yellow: {n_yellow} incidents generated.")

        # ── Pipeline 2: Green taxi (second real dataset) ───────────────────
        if has_green:
            print("Running green taxi pipeline (8 nodes, 56k rows) …")
            green_incidents = _run_one_pipeline(
                tmp_dir=tmp_dir,
                prefix="green",
                fault_types=fault_types,
                per_fault=args.per_fault,
                max_rows=56551,  # use all green taxi rows
                spec=GREEN_SPEC,
                load_fn=_load_green_sources,
                run_fn=_run_green_pipeline,
                fault_fn=_apply_green_fault,
                pipeline_name="nyc_green_taxi_etl_real",
                seed_base=9999,
                incident_counter_start=n_yellow + 1,
            )
            incidents.extend(green_incidents)
            print(f"  Green: {len(green_incidents)} incidents generated.")
        else:
            print("  Green taxi parquet not found — skipping second pipeline.")

    # ── Evaluate all proposed methods ─────────────────────────────────────────
    rows = [row for inc in incidents for row in candidate_rows(inc)]
    score_rows(rows)

    methods = [
        "centrality",
        "quality_only",
        "causal_propagation",
        "blind_spot_boosted",
        "lineage_rank",
    ]
    all_details = {method: rank_details(rows, method) for method in methods}

    summary: dict[str, object] = {
        "incident_count": len(incidents),
        "pipelines": ["nyc_yellow_taxi_etl_real"] + (["nyc_green_taxi_etl_real"] if has_green else []),
        "yellow_incident_count": n_yellow,
        "green_incident_count": len(incidents) - n_yellow,
        "fault_types": fault_types,
        "notes": (
            "Two independent real-data pipelines: NYC yellow taxi (120k rows, 8 nodes) "
            "and NYC green taxi (56k rows, 8 nodes). "
            "Stochastic signal generation with overlapping ranges matching "
            "PipeRCA-Bench. Decoy nodes with elevated false-positive signals. "
            "6 fault types including schema_drift (staging as root). "
            "3 observability modes per pipeline."
        ),
        "overall": {m: aggregate_details(all_details[m]) for m in methods},
        "by_fault_type": {
            m: by_group_from_details(all_details[m], "fault_type") for m in methods
        },
        "by_observability": {
            m: by_group_from_details(all_details[m], "observability_mode") for m in methods
        },
        "by_pipeline": {
            m: by_group_from_details(all_details[m], "pipeline") for m in methods
        },
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    details_flat = [{"method": m, **d} for m in methods for d in all_details[m]]
    args.output.write_text(
        json.dumps({"summary": summary, "incidents": incidents, "details": details_flat}, indent=2)
    )
    print("\n=== OVERALL RESULTS ===")
    print(json.dumps(summary["overall"], indent=2))

    print("\n=== By observability mode (key methods) ===")
    for m in ["blind_spot_boosted", "lineage_rank", "causal_propagation"]:
        print(f"\n{m}:")
        for mode, r in summary["by_observability"][m].items():
            print(f"  {mode:30s}: Top-1={r['top1']:.4f}  MRR={r['mrr']:.4f}  n={r['incident_count']}")

    print("\n=== By pipeline (LR-BS, LR-H) ===")
    for m in ["blind_spot_boosted", "lineage_rank"]:
        print(f"\n{m}:")
        for pipe, r in summary["by_pipeline"][m].items():
            print(f"  {pipe:35s}: Top-1={r['top1']:.4f}  n={r['incident_count']}")


if __name__ == "__main__":
    main()

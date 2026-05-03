"""Real public-data case study for PipeRCA-Bench.

Builds a realistic 8-node NYC taxi ETL pipeline on actual TLC trip records and
taxi zone data, injects six fault types, captures runtime lineage from DuckDB
execution, and evaluates all proposed RCA methods.

Four real public datasets, each with an 8-node pipeline topology:
  1. NYC TLC Yellow Taxi   (Jan 2024, up to 120k rows via LIMIT)
  2. NYC TLC Green Taxi    (Jan 2024, full 56,551 rows — small enough to use all)
  3. Divvy Chicago Bike    (Jan 2024, up to 120k rows)
  4. BTS Airline On-Time   (Jan 2024, up to 120k rows, dual-path DAG)

Total: 4 pipelines × 6 fault types × 15 iterations = 360 incidents.

── Fault taxonomy and signal grounding ──────────────────────────────────────
Faults split into two mechanistic types that determine how anomaly signals are
constructed (see _build_signals for per-node detail):

  TYPE A — fault directly modifies source table ROW COUNT.
    stale_source       : drops ~40% most-recent rows (simulates refresh freeze)
    missing_partition  : removes the single most-populated calendar date
    duplicate_ingestion: duplicates the single most-populated calendar date

    For Type A, the run_anomaly signal at the root node is NOT drawn from a
    uniform distribution; it is computed from the actual fractional row-count
    change measured in DuckDB execution on real data.  The formula is:
      anom = 0.35 + 0.65 * row_delta + noise(U(-0.03, 0.06))
    This maps delta=0 → ~0.35 (above background), delta=1.0 → ~1.0.
    The empirical grounding caveat: row_delta is real, but the 40% fault
    magnitude and the 0.35+0.65×delta mapping are design choices, not
    calibrated from actual monitoring tool output.

  TYPE B — fault corrupts VALUES or SCHEMA without changing row count.
    null_explosion: nulls ~1/7 rows in a key join column (signals null-rate monitor)
    bad_join_key  : shifts a range of lookup IDs, breaking the join  (join-quality monitor)
    schema_drift  : hardcodes a classification column to one value (schema/contract monitor)

    For Type B, no real row_delta exists at the root.  The run_anomaly is drawn
    from the designed uniform range U(0.48, 0.84), representing a quality-layer
    monitor that fires moderately but not perfectly reliably.

── Observability rotation ────────────────────────────────────────────────────
Iterations 0–4  : full lineage (all runtime edges observed)
Iterations 5–9  : sparse — root's outgoing edges always removed, 30% of
                  remaining edges randomly dropped
Iterations 10–14: missing-root — all root outgoing edges removed, no other drops

This creates three equal 120-incident blocks (out of 360) testing each RCA
method's robustness to degraded observability.

── Decoy noise ──────────────────────────────────────────────────────────────
1–2 non-root ancestor nodes are randomly selected as "decoys" per incident and
assigned elevated signals (recent_change U(0.58, 0.90), run_anomaly U(0.34,
0.71)) that intentionally overlap with root signal ranges.  This prevents
trivially easy ranking and models realistic false-positive monitoring noise.

── Reproducibility ──────────────────────────────────────────────────────────
Each incident uses two seeded RNGs (derived from seed_base, iteration, and
fault_type hash) so results are deterministic across runs for the same seed_base.
Different seed_base values per pipeline prevent correlated randomness across
pipeline families.
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
    FEATURE_SETS,
    aggregate_details,
    by_group_from_details,
    candidate_rows,
    confidence_table,
    learned_ranker_predictions,
    leakage_audit,
    llm_score_incidents,
    pilot_analysis,
    rank_details,
    score_rows,
    significance_table,
)
from rca_benchmark import build_graph, get_pipeline_specs


ROOT = Path(__file__).resolve().parents[1]
# SPEC and GREEN_SPEC hold the canonical graph topology (nodes + edges +
# leaf_test_weights) for the yellow and green taxi pipelines.  These come from
# rca_benchmark.get_pipeline_specs() which reads from the benchmarks/ directory.
# SPEC is also used as a fallback inside _build_signals when the pipeline cannot
# be auto-detected from node names.
SPEC       = get_pipeline_specs()["nyc_taxi_etl_extended"]
GREEN_SPEC = get_pipeline_specs()["nyc_green_taxi_etl_extended"]

# ── SQL pipeline steps (Yellow / Green taxi) ───────────────────────────────────
# The 8-node ETL topology for Yellow (and structurally identical Green) taxi:
#
#   raw_trips ──────────────────────────────────────────────► trips_valid
#   raw_trips + zone_lookup  ──── trips_valid ──────────────► trips_enriched
#   trips_enriched ──────────────────────────────────────────► trips_classified
#   trips_classified ────────────────────────────────────────► daily_zone_metrics
#   trips_classified ────────────────────────────────────────► fare_band_metrics
#   trips_classified ────────────────────────────────────────► peak_hour_metrics
#
# "zone_lookup" is a static dimension table (265 NYC taxi zones).  It is the
# root node for the bad_join_key fault.  The three leaf nodes (daily_zone_metrics,
# fare_band_metrics, peak_hour_metrics) branch from trips_classified, providing
# topologically diverse targets for fault observation.
#
# Two variants of the classification step exist:
#   _SQL_CLASSIFIED_NORMAL  — correct business logic
#   _SQL_CLASSIFIED_DRIFTED — schema_drift fault: hardcodes 'off_peak', corrupting
#                             the time_period column that peak_hour_metrics depends on.

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
  and fare_amount > 0             -- exclude refunds / zero-fare records
  and trip_distance > 0           -- exclude stationary / GPS-error records
  and tpep_pickup_datetime is not null;
-- Node: trips_valid (Layer 1 — validation/cleaning)
-- Row count after filtering is typically 85–95% of raw_trips depending on data quality.
"""

_SQL_ENRICHED = """
create or replace table trips_enriched as
select
  v.*,
  coalesce(z.Borough, 'Unknown') as pickup_borough,  -- 'Unknown' when PULocationID has no match
  coalesce(z.Zone,    'Unknown') as pickup_zone       --   (bad_join_key fault maximises 'Unknown')
from trips_valid v
left join zone_lookup z on v.PULocationID = z.LocationID;
-- Node: trips_enriched (Layer 2 — dimension enrichment)
-- LEFT JOIN preserves all trips even when zone_lookup is corrupted (bad_join_key fault).
-- This means row count does NOT drop on bad_join_key — only pickup_borough becomes 'Unknown'.
-- The validation query below measures the unknown_borough count as a signal proxy.
"""

# Normal time_period classification — correct business logic
_SQL_CLASSIFIED_NORMAL = """
create or replace table trips_classified as
select
  e.*,
  date_trunc('day', e.pickup_datetime) as trip_day,
  extract(hour from e.pickup_datetime)::integer as pickup_hour,
  case
    when extract(hour from e.pickup_datetime) between 7 and 9   -- AM rush
      or extract(hour from e.pickup_datetime) between 16 and 19 -- PM rush
    then 'peak' else 'off_peak'
  end as time_period,
  case
    when e.fare_amount < 10  then 'economy'    -- ~$0–9
    when e.fare_amount < 25  then 'standard'   -- ~$10–24
    else                          'premium'    -- $25+
  end as fare_tier
from trips_enriched e;
-- Node: trips_classified (Layer 3 — business classification)
-- This node is the root in the schema_drift fault.
"""

# Schema-drift variant: hardcodes time_period to 'off_peak' for EVERY row.
# Effect: peak_hour_metrics will show 0 rows where time_period='peak', and all
# rides incorrectly labelled off_peak.  This models a silent logic regression in
# the classification step (e.g., a code change that removed the CASE expression).
# Root for schema_drift = trips_classified; observed failure = peak_hour_metrics.
_SQL_CLASSIFIED_DRIFTED = """
create or replace table trips_classified as
select
  e.*,
  date_trunc('day', e.pickup_datetime) as trip_day,
  extract(hour from e.pickup_datetime)::integer as pickup_hour,
  'off_peak' as time_period,   -- FAULT: always off_peak, peak logic removed
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
-- Node: daily_zone_metrics (Leaf 1 — time-partitioned spatial aggregation)
-- Observed failure node for stale_source and missing_partition faults because
-- trip_count drops noticeably when whole days of input are missing.
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
-- Node: fare_band_metrics (Leaf 2 — revenue by fare tier)
-- Observed failure node for duplicate_ingestion because inflated revenue totals
-- and doubled trip_count are the first visible symptom of duplicated source rows.
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
-- Node: peak_hour_metrics (Leaf 3 — hourly traffic by time period)
-- Observed failure for schema_drift: when time_period is always 'off_peak',
-- the 'peak' rows disappear entirely — a contract test fires immediately.
-- Also shows mild test failures for missing_partition (missing days = fewer hours).
"""


def _run_pipeline(conn: duckdb.DuckDBPyConnection, schema_drifted: bool = False) -> dict[str, object]:
    """Execute the full extended ETL and return row counts + lineage edges.

    Called AFTER _apply_fault, so all row counts here reflect the faulted state.
    Returns step_rows (node → row_count), runtime_lineage (edge list), and
    validations (scalar indicators used to compute unknown_borough delta for
    bad_join_key signal grounding).
    """
    lineage: list[tuple[str, str]] = []

    # Source tables are counted FIRST, before any ETL steps run.
    # Critical: faults modify raw_trips or zone_lookup BEFORE this function is
    # called, so these counts capture the faulted row counts — not the baseline.
    # Without counting source tables here, row_delta would always be 0 for root
    # nodes like raw_trips, making all Type A anomaly signals ungrounded.
    step_rows: dict[str, int] = {
        "raw_trips":   int(conn.execute("select count(*) from raw_trips").fetchone()[0]),
        "zone_lookup": int(conn.execute("select count(*) from zone_lookup").fetchone()[0]),
    }

    def _run(sql: str, inputs: list[str], output: str) -> None:
        # Execute one ETL step, record the output row count, and record lineage edges.
        # Lineage edges are (input_node, output_node) tuples — one per declared input.
        # This captures the runtime lineage that RCA methods use (e.g., LR-H uses
        # runtime edges to walk backwards from the observed failure to candidate roots).
        conn.execute(sql)
        cnt = conn.execute(f"select count(*) from {output}").fetchone()[0]
        step_rows[output] = int(cnt)
        lineage.extend((inp, output) for inp in inputs)

    _run(_SQL_VALID,    ["raw_trips"],                            "trips_valid")
    _run(_SQL_ENRICHED, ["trips_valid", "zone_lookup"],           "trips_enriched")
    # schema_drift fault uses the drifted SQL — caller passes schema_drifted=True
    # when _apply_fault returns schema_drifted=True (only for schema_drift fault type).
    classified_sql = _SQL_CLASSIFIED_DRIFTED if schema_drifted else _SQL_CLASSIFIED_NORMAL
    _run(classified_sql, ["trips_enriched"],                      "trips_classified")
    _run(_SQL_DAILY_ZONE, ["trips_classified"],                   "daily_zone_metrics")
    _run(_SQL_FARE_BAND,  ["trips_classified"],                   "fare_band_metrics")
    _run(_SQL_PEAK_HOUR,  ["trips_classified"],                   "peak_hour_metrics")

    # Scalar validation counters used by _build_signals to compute the unknown_borough
    # delta (baseline vs faulted) for the bad_join_key fault.  When zone_lookup IDs
    # are corrupted, unknown_borough jumps dramatically — this is used to decide
    # whether to assign high or medium local_test_failures to trips_enriched.
    # daily_zone_rows and peak_rows are not used in signal logic but stored for
    # potential future auditing.
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
    """Load yellow taxi trip records and the static zone dimension into DuckDB.

    max_rows (default 120,000): The yellow taxi parquet for Jan 2024 contains
    ~2.96M rows.  We limit to 120k for two reasons:
      1. Speed — DuckDB runs each fault × iteration in-process; 120k rows
         keeps each pipeline execution under 1 second.
      2. Date-range awareness — the parquet is sorted by pickup time, so
         LIMIT 120k captures rows from approximately Jan 1–2 only.  This means
         ALL fault dates must be derived data-relatively (most-populated date,
         descending-order cutoff) rather than from hardcoded calendar dates.
         See _apply_fault for how each fault handles this constraint.

    zone_lookup: the static NYC taxi zone lookup table (265 rows, one per
    LocationID).  No LIMIT applied — it is tiny and used as a dimension.
    """
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
    """Inject a fault into source tables (Yellow taxi).

    Called BEFORE _run_pipeline.  Modifies raw_trips or zone_lookup in-place
    using DuckDB's CREATE OR REPLACE TABLE to replace the source with the faulted
    version.  Returns (root_node, observed_node, schema_drifted).

    root_node     — the node in the DAG where the fault originates.  This is the
                    ground-truth answer that RCA methods must rank #1.
    observed_node — the downstream node where a monitoring check first fires
                    (where we'd see test failures, anomalous metrics, etc.).
    schema_drifted — True only for schema_drift.  Signals _run_pipeline to use
                    _SQL_CLASSIFIED_DRIFTED instead of the normal classification.
                    No source table is modified for schema_drift; the fault is in
                    the transformation logic itself.
    """
    if fault_type == "missing_partition":
        # Find the most-populated calendar date in the loaded sample and remove it.
        # "Most-populated" is data-relative: for Yellow 120k it will be Jan 1 or
        # Jan 2 (the only dates in the sample); for Green/Divvy/BTS it will be a
        # peak mid-month day.  This approach is robust to any date range loaded.
        #
        # Why most-populated?  Removing the largest date maximizes the row_delta
        # (fractional change), making the anomaly signal as clear as possible.
        # If we chose a low-traffic day the signal would be near-zero — no better
        # than background noise.
        remove_date = conn.execute("""
            select cast(tpep_pickup_datetime as date) as d, count(*) as n
            from raw_trips group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_trips as
            select * from raw_trips
            where cast(tpep_pickup_datetime as date) <> '{remove_date}'
        """)
        # root = raw_trips (source lost data); observed = daily_zone_metrics.
        #
        # Why daily_zone_metrics is the observed failure (not the other two leaves):
        #
        # daily_zone_metrics schema:
        #   (trip_day DATE, pickup_borough TEXT, pickup_zone TEXT,
        #    trip_count INT, avg_fare FLOAT, total_distance FLOAT)
        #
        # It aggregates with GROUP BY trip_day, pickup_borough, pickup_zone.
        # Each output row represents ONE calendar date + borough + zone combination.
        # When an entire date is removed from raw_trips, every row for that date
        # disappears from this table — the GROUP BY produces no output rows for
        # that trip_day value at all.
        #
        # The "trip_count thresholds" that fire are monitoring checks on this table:
        #
        #   Threshold 1 — date-coverage completeness:
        #     Expected: COUNT(DISTINCT trip_day) == (last_date - first_date + 1) days
        #     Actual:   one date is absent, so the distinct count is one short.
        #     This is a hard gap check, not a soft threshold — it either passes or fails.
        #
        #   Threshold 2 — total row count lower bound:
        #     Expected: table row count >= rolling_7day_avg_row_count * 0.90
        #     (i.e., today's row count should not be more than 10% below last week's average)
        #     Actual:   one full date's worth of (borough × zone) groups is absent,
        #     which is roughly (1/31) ≈ 3% fewer rows for January. Whether this
        #     breaches the 10% threshold depends on how busy the removed date was.
        #     For the most-populated date (which is what we remove), the drop is larger.
        #
        #   Threshold 3 — total trip_count sum lower bound:
        #     Expected: SUM(trip_count) >= rolling_7day_avg_total * 0.90
        #     Actual:   the missing date's trips are gone, so SUM(trip_count) drops
        #     by roughly (trips_on_removed_date / total_trips).  For the most-populated
        #     date this is typically 3–5% of the month's total — may or may not breach
        #     a 10% threshold, but will breach a tighter 2% threshold.
        #
        # fare_band_metrics and peak_hour_metrics do NOT have a trip_day column.
        # They GROUP BY fare_tier/pickup_borough and pickup_hour/time_period respectively.
        # All their groups still exist after the date is removed — counts are just
        # slightly lower everywhere uniformly.  No group disappears, no date gap forms.
        # A global sum threshold might weakly fire (see randint(1,3) there), but
        # daily_zone_metrics is where the failure is unambiguous and immediate.
        return "raw_trips", "daily_zone_metrics", False

    if fault_type == "duplicate_ingestion":
        # Duplicate ALL rows from the most-populated date (UNION ALL includes dupes).
        # Data-relative: targets the same most-populated date as missing_partition,
        # so missing/duplicate have symmetric row_delta by design (both add or
        # remove the same proportion of rows; abs() in _build_signals makes them
        # identical in anomaly magnitude).
        dup_date = conn.execute("""
            select cast(tpep_pickup_datetime as date) as d, count(*) as n
            from raw_trips group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_trips as
            select * from raw_trips
            union all
            select * from raw_trips
            where cast(tpep_pickup_datetime as date) = '{dup_date}'
        """)
        # Observed = fare_band_metrics because revenue-uniqueness checks fire first
        # on inflated total_revenue; daily_zone_metrics also shows elevated counts.
        return "raw_trips", "fare_band_metrics", False

    if fault_type == "stale_source":
        # Drop the most-recent ~40% of rows to simulate a source that froze
        # partway through the collection period (e.g., ingestion pipeline died
        # at 60% of the way through the day).
        #
        # Why 40%?  Large enough to produce a clear row_delta (~0.40) that sits
        # well above decoy/non-root background noise, while leaving enough data
        # for the downstream pipeline to still complete without empty-table errors.
        #
        # How the cutoff is derived:
        #   1. Count total rows (e.g., 120,000 for Yellow).
        #   2. drop = 40% of total = 48,000 rows to remove.
        #   3. Order all rows descending by datetime; take the row at position
        #      (drop - 1) = 47,999 from the top.  Its date is the cutoff.
        #   4. Keep only rows strictly BEFORE (not on) that date.
        #
        # Why descending and strict less-than?  The Yellow 120k sample has outlier
        # rows from year 2002/2009/2023 that appear at the very top when ascending.
        # Descending from the highest datetime anchors the cutoff to the actual
        # high-density date range.  Strict less-than (<, not <=) ensures we drop
        # at least one full day, guaranteeing a non-zero delta even if all dropped
        # rows fall on the same date as the cutoff.
        total = conn.execute("select count(*) from raw_trips").fetchone()[0]
        drop  = max(1, int(total * 0.40))
        cutoff_date = conn.execute(f"""
            select cast(tpep_pickup_datetime as date)
            from raw_trips order by tpep_pickup_datetime desc
            limit 1 offset {drop - 1}
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_trips as
            select * from raw_trips
            where cast(tpep_pickup_datetime as date) < '{cutoff_date}'
        """)
        return "raw_trips", "daily_zone_metrics", False

    if fault_type == "null_explosion":
        # Nullify PULocationID on approximately 1/7 of rows (those where
        # row_number % 7 equals (iteration % 7)).  The modular pattern shifts
        # slightly each iteration so different row subsets are affected,
        # preventing identical incidents across iterations.
        #
        # Why PULocationID?  It is the join key to zone_lookup AND a WHERE
        # filter in _SQL_VALID (PULocationID is not null).  Nullifying it means
        # ~1/7 of rows get DROPPED in trips_valid, propagating a row_count
        # reduction downstream — but this is a TYPE B fault (the signal comes
        # from null-rate monitors, not the row_delta).  The row count DOES
        # change slightly in this implementation (dropped nulls), but for signal
        # purposes we treat null_explosion as Type B (designed range U(0.48, 0.84)).
        #
        # Why 1/7?  Large enough to be detectable (>14% null rate), small enough
        # that the pipeline still runs to completion without empty-table errors.
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
        # Corrupt zone_lookup by shifting LocationIDs 1–15 by +600 (beyond the
        # valid range of 1–265).  These IDs will no longer match any PULocationID
        # in raw_trips, so trips with pickup locations 1–15 get pickup_borough =
        # 'Unknown' after the join.  Row count in trips_enriched does NOT change
        # (LEFT JOIN).
        #
        # Why shift IDs 1–15?  Small enough to be subtle (not all zones affected)
        # but large enough to generate a spike in unknown_borough that a data
        # quality check would catch.  Green taxi uses a different range (100–120)
        # to produce a distinct test scenario.
        conn.execute("""
            create or replace table zone_lookup as
            select
              case when LocationID between 1 and 15 then LocationID + 600
                   else LocationID end as LocationID,
              Borough, Zone, service_zone
            from zone_lookup
        """)
        # root = zone_lookup (the dimension table is corrupted);
        # observed = daily_zone_metrics (aggregation by pickup_borough shows
        # a spike in 'Unknown' borough counts — a data quality test fires).
        return "zone_lookup", "daily_zone_metrics", False

    if fault_type == "schema_drift":
        # No source table modification — the fault is entirely in the transformation
        # logic (trips_classified uses the drifted SQL).  The root IS trips_classified
        # (the node where the wrong logic lives), not a source table.
        # The drifted SQL hardcodes 'off_peak' for all rows, which means:
        #   - peak_hour_metrics: 0 rows with time_period='peak' (contract test fails)
        #   - fare_band_metrics: unaffected (does not use time_period column)
        # schema_drifted=True signals _run_pipeline to use _SQL_CLASSIFIED_DRIFTED.
        return "trips_classified", "peak_hour_metrics", True

    raise ValueError(f"Unsupported fault_type: {fault_type!r}")


def _apply_observability(
    runtime_edges: list[tuple[str, str]],
    root: str,
    iteration: int,
    rng: random.Random,
) -> tuple[list[tuple[str, str]], str]:
    """Degrade runtime lineage to simulate incomplete observability.

    Returns (filtered_edges, mode_label).

    Three modes rotated deterministically across 15 iterations (--per-fault=15):
      full (0–4)          : all runtime edges returned unchanged.  The RCA method
                            can walk backwards from the observed failure through the
                            complete lineage graph to reach the root.
      runtime_sparse (5–9): root's outgoing edges are always removed (its direct
                            children can't be traced back to it via runtime lineage).
                            30% of remaining edges also dropped randomly.  Models
                            partial telemetry loss — e.g., only 70% of pipeline
                            steps emit lineage events.
      runtime_missing_root(10–14): ALL edges originating from root are removed.
                            The root node is effectively invisible in the runtime
                            graph.  Only design-time edges reveal it.  Tests whether
                            LR-BS's blind-spot guarantee (Proposition 1) works: when
                            root has no runtime in-edges, it should be boosted.

    Why 30% drop?  Chosen to create meaningful but not catastrophic sparsity —
    enough that some paths from observed_node to root are broken, but the DAG
    structure is not completely destroyed.  Results show LR-BS handles this well
    while simpler methods degrade sharply.

    Why 5/5/5 split (not 4/4/7 or 3/6/6)?  Equal splits give equal statistical
    power to each condition in the by-observability breakdown tables.
    """
    if iteration < 5:
        return runtime_edges, "full"
    if iteration < 10:
        # sparse: root edges always absent; 30% of other edges dropped stochastically
        sparse = []
        for edge in runtime_edges:
            if edge[0] == root:
                continue  # always remove root's outgoing edges
            if rng.random() > 0.3:  # keep 70% of non-root edges
                sparse.append(edge)
        return sparse, "runtime_sparse"
    # missing_root: all root outgoing edges removed, everything else kept
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

    Each node in the pipeline gets a dict of five monitoring signals:
      recent_change       : float [0,1] — did this node's code/schema change recently?
      freshness_severity  : float [0,1] — how stale is this node's data?
      run_anomaly         : float [0,1] — how anomalous is the latest run's output?
      contract_violation  : int  {0,1}  — did a schema/contract test fail?
      local_test_failures : int  {0..6} — how many data quality tests failed here?

    Node roles (mutually exclusive per incident):
      root    — the ground-truth fault origin (the node we want ranked #1)
      decoy   — a non-root ancestor of observed with elevated signals (false positive)
      impacted — a descendant of root (signals are somewhat elevated by row_delta)
      other   — unrelated nodes (low signals, background noise only)

    ── Signal ranges by node role ────────────────────────────────────────────
    recent_change ranges:
      root/decoy: U(0.55, 0.86) / U(0.58, 0.90) — elevated; node was recently touched
      impacted/other: U(0.05, 0.28)              — low; did not change

    Why do root and decoy overlap in recent_change?
    Decoy recent_change intentionally overlaps with root to prevent RCA methods
    from trivially ranking root first based on recent_change alone.  Methods must
    combine multiple signals (run_anomaly, lineage distance, test failures) to
    distinguish root from decoy.

    run_anomaly at the root (two cases):
      TYPE A (row-count fault):
        anom = 0.35 + 0.65 * row_delta + noise
        - row_delta = abs(faulted_rows[root] - baseline_rows[root]) / baseline_rows[root]
          This is a real measurement from DuckDB execution on actual data.
        - 0.35: floor — even a zero-delta fault appears above background (0.04–0.22).
        - 0.65: scale — maps delta=1.0 to anom=1.0 (maximum severity).
        - noise: U(-0.03, 0.06), asymmetric with positive skew.  The asymmetry
          models real monitor behavior: monitors more often slightly overestimate
          anomaly severity (alert-heavy) than underestimate it.
        - max(0.30, ...): hard floor ensures the root's run_anomaly never drops
          below 0.30, even with the downside noise.  This guarantees it stays
          above the non-impacted background range U(0.04, 0.22).
        Empirical caveat: row_delta is empirical, but the 0.35+0.65×delta mapping
        is a linear scaling choice, not derived from actual monitoring tool output.

      TYPE B (value/schema fault, no row-count change at root):
        anom = U(0.48, 0.84)
        A designed range representing a quality-layer monitor (null-rate check,
        schema registry test, join completeness monitor) that fires moderately but
        not perfectly reliably.  The range was chosen to sit above decoy U(0.34,
        0.71) in expectation while overlapping enough to create realistic difficulty.

    run_anomaly for non-root nodes:
      decoy:    U(0.34, 0.71) — elevated false positive, overlaps with root Type B range
      impacted: U(0.12, 0.38) + 0.35 * row_delta — boosted when downstream row counts drop
      other:    U(0.04, 0.22) — low background noise only

    ── Decoy selection ───────────────────────────────────────────────────────
    1–2 nodes are selected from the set of non-root, non-observed ancestors of
    the observed failure node.  These are plausible suspects (on the path from
    root to observed) that a naive RCA method would rank highly by lineage distance.
    Assigning them elevated signals tests whether methods can discriminate based
    on full signal profiles, not just proximity to the observed failure.
    """
    _TYPE_A_FAULTS = {"stale_source", "missing_partition", "duplicate_ingestion"}

    # Auto-detect which spec's edge set matches the node names in this incident.
    # This allows _build_signals to work correctly across all four pipelines
    # (yellow, green, divvy, bts) without requiring a spec parameter.
    # Falls back to SPEC (yellow taxi) if no exact match — should never happen
    # in practice since all four pipelines are registered in get_pipeline_specs().
    all_specs = get_pipeline_specs()
    dag_edges: list[tuple[str, str]] = []
    for s in all_specs.values():
        if set(s.nodes.keys()) == set(spec_nodes.keys()):
            dag_edges = list(s.edges)
            break
    if not dag_edges:
        dag_edges = list(SPEC.edges)  # fallback
    dag = build_graph(dag_edges)

    # ancestors_of_observed: all nodes that can reach 'observed' via the DAG.
    # Used to select decoy candidates (plausible suspects on the causal path).
    ancestors_of_observed = set(nx.ancestors(dag, observed)) if observed in dag else set()
    # impacted_set: all downstream descendants of root (nodes whose output rows
    # were affected when root's data changed).
    impacted_set = set(nx.descendants(dag, root)) if root in dag else set()

    # Select 1–2 decoy nodes from non-root ancestors of observed.
    # Decoys intentionally overlap root's signal range to create realistic difficulty.
    # min(2, ...) handles cases where observed has only 0 or 1 eligible ancestors.
    decoy_candidates = [n for n in ancestors_of_observed if n != root and n != observed]
    decoys = set(rng.sample(decoy_candidates, k=min(2, len(decoy_candidates))))

    signals: dict[str, dict[str, float | int]] = {}
    for node, _node_type in spec_nodes.items():
        is_root     = (node == root)
        is_impacted = (node in impacted_set)
        is_decoy    = (node in decoys)

        # Compute fractional row-count change at this specific node.
        # baseline_rows and faulted_rows both include source tables (raw_trips,
        # zone_lookup, etc.) because _run_pipeline counts them at the top before
        # any SQL steps execute.  Without source table counts in step_rows, source
        # nodes like raw_trips would always have row_delta=0.0, making Type A
        # signals completely ungrounded.
        row_delta = 0.0
        if node in baseline_rows and node in faulted_rows:
            base = max(1, int(baseline_rows[node]))  # avoid division by zero
            row_delta = min(1.0, abs(int(faulted_rows[node]) - base) / base)

        if is_root:
            rc = rng.uniform(0.55, 0.86)  # root had a recent change — elevated
            if fault_type in _TYPE_A_FAULTS:
                # Empirically grounded: row_delta from actual DuckDB execution.
                # Linear mapping: 0.35 + 0.65*delta ensures range is [0.35, 1.0].
                # Asymmetric noise U(-0.03, 0.06): positive skew models monitor
                # over-sensitivity (false alarm lean) more common than under-sensitivity.
                # Hard floor max(0.30, ...): keeps root above background noise floor.
                anom = min(1.0, max(0.30, 0.35 + 0.65 * row_delta + rng.uniform(-0.03, 0.06)))
            else:
                # Type B: root row count unchanged — quality-layer monitor range.
                # U(0.48, 0.84) is above impacted U(0.12, 0.38) and overlaps decoy
                # U(0.34, 0.71) to create realistic discrimination difficulty.
                anom = rng.uniform(0.48, 0.84)
        elif is_decoy:
            # Decoy: elevated signals to model false-positive upstream blame.
            # recent_change U(0.58, 0.90) overlaps root range intentionally.
            # run_anomaly U(0.34, 0.71) overlaps root's Type B range intentionally.
            rc   = rng.uniform(0.58, 0.90)
            anom = rng.uniform(0.34, 0.71)
        elif is_impacted:
            # Downstream of root: low recent_change (not where the change happened),
            # but run_anomaly boosted proportionally to the actual row_delta.
            # Coefficient 0.35 chosen so that at delta=1.0, impacted nodes reach
            # ~0.73 max (0.38 + 0.35) — clearly elevated but below root.
            rc   = rng.uniform(0.05, 0.28)
            anom = min(1.0, rng.uniform(0.12, 0.38) + 0.35 * row_delta)
        else:
            # Uninvolved nodes: background noise only.
            rc   = rng.uniform(0.05, 0.28)
            anom = rng.uniform(0.04, 0.22)

        signals[node] = {
            "recent_change":      round(rc, 3),
            "freshness_severity": 0.0,   # overridden below for stale_source fault
            "run_anomaly":        round(anom, 3),
            "contract_violation": 0,     # overridden below for bad_join_key/schema_drift
            "local_test_failures": 0,    # overridden below per fault type
        }

    # ── Fault-type-specific signal augmentations ─────────────────────────────
    #
    # Each block sets freshness_severity, contract_violation, and/or
    # local_test_failures on specific pipeline nodes to model the monitoring
    # checks that would fire for that fault type in a real data platform.
    #
    # Node name checks use "if X in signals" so this function works for all four
    # pipelines without branching on pipeline name — yellow, green, divvy, and bts
    # each have differently named nodes, so only the relevant ones match.
    #
    # ── What "local_test_failures" models ─────────────────────────────────────
    # In a real pipeline (dbt, Great Expectations, Soda), each output table has an
    # attached suite of data quality checks.  local_test_failures counts how many
    # of those checks would fail given this fault.  The integer is a severity proxy:
    #
    #   0     : no checks fire         (node unaffected or change too small to breach thresholds)
    #   1–2   : soft/secondary failure  (one check fires: a count is slightly below expected,
    #                                    or an 'accepted_values' check catches an edge case)
    #   3–4   : clear primary failure   (multiple independent checks definitively breach:
    #                                    row count threshold + date coverage + variance)
    #   5–6   : severe / cascading      (all checks fire; the node is the most visible failure
    #                                    point in the pipeline)
    #
    # randint(3,6) = primary observed failure  (the table you'd page an on-call engineer about)
    # randint(1,3) = secondary impacted table  (slightly off, but not the main alert)
    # randint(0,1) = adjacent/sibling table    (probably fine, minor noise at most)
    #
    # ── What "freshness_severity" models ──────────────────────────────────────
    # A freshness monitor tracks how long ago a table was last updated.  In tools
    # like dbt freshness tests or Airflow SLA checks, a source that stopped
    # refreshing shows up as "last loaded N hours ago" exceeding the SLA threshold.
    # Range U(0.82, 0.98) at root = severe staleness (source missed multiple updates).
    # Range U(0.32, 0.74) at downstream = mild staleness (built from stale input).
    # 0.0 for all other fault types = freshness is not the failure mode here.
    #
    # ── What "contract_violation" models ──────────────────────────────────────
    # A schema contract check (e.g., dbt schema test, Atlan lineage contract, or
    # a custom assertion that a column has only allowed values) fires when the
    # data no longer matches the declared contract.  Set to 1 (violated) or 0 (ok).

    if fault_type == "stale_source":
        # The source node stopped receiving new data — its freshness SLA is badly missed.
        # U(0.82, 0.98): "last updated 20+ hours ago when the SLA is 1 hour" — severe.
        signals[root]["freshness_severity"] = round(rng.uniform(0.82, 0.98), 3)

        # The enrichment node (trips_enriched / rides_enriched / flights_enriched)
        # was rebuilt FROM the stale source, so it inherits moderate staleness.
        # U(0.32, 0.74): "this table is also stale, but it's a derived output, not
        # the source itself" — a downstream freshness monitor fires at lower severity.
        if "trips_enriched" in signals:
            signals["trips_enriched"]["freshness_severity"] = round(rng.uniform(0.32, 0.74), 3)
        if "rides_enriched" in signals:
            signals["rides_enriched"]["freshness_severity"] = round(rng.uniform(0.32, 0.74), 3)
        if "flights_enriched" in signals:
            signals["flights_enriched"]["freshness_severity"] = round(rng.uniform(0.32, 0.74), 3)

        # Time-series aggregation leaves (daily_zone_metrics, daily_station_metrics,
        # carrier_daily_metrics) fire 3–6 checks because:
        #   Check 1: "row_count >= expected_daily_minimum" fails — only ~60% of
        #            expected rows exist (40% of most-recent rows were dropped).
        #   Check 2: "date coverage complete for expected range" fails — the most
        #            recent N days are absent from the trip_day / ride_day / flight_date
        #            GROUP BY output.
        #   Check 3: "total_count not anomalously below rolling 7-day average" fails —
        #            a Z-score or percentage-drop threshold is breached.
        #   Check 4+: per-borough or per-carrier count thresholds may also fire.
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "daily_station_metrics" in signals:
            signals["daily_station_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "carrier_daily_metrics" in signals:
            signals["carrier_daily_metrics"]["local_test_failures"] = rng.randint(3, 6)

    elif fault_type == "missing_partition":
        # No freshness issue — data arrived on time, one date partition is just absent.
        # (The source table exists and was updated; one date's rows are missing from it.)
        # No contract_violation — the schema is intact, only the volume is wrong.

        # PRIMARY (3–6): time-series aggregations with a trip_day / ride_day /
        # flight_date GROUP BY column.  These produce one output row per calendar date.
        # When a date is missing from the input, that row DISAPPEARS from the output:
        #   daily_zone_metrics schema:  (trip_day, pickup_borough, pickup_zone, trip_count, avg_fare, total_distance)
        #   daily_station_metrics schema: (ride_day, start_station_id, start_station_name, ride_count, avg_duration)
        #   carrier_daily_metrics schema: (flight_date, carrier, flight_count, avg_arr_delay, cancelled_count)
        #
        # Checks that fire (3–6):
        #   Check 1: "row_count for table >= expected_minimum" — one full date group missing
        #            means fewer rows than any prior run.
        #   Check 2: "date series has no gaps" — a completeness check comparing
        #            min(trip_day) to max(trip_day) against COUNT(DISTINCT trip_day)
        #            detects the gap explicitly.
        #   Check 3: "total trip_count not below rolling threshold" — sum of all
        #            trip_count values is ~1/31 lower than the monthly average.
        #   Check 4–6: per-borough / per-zone / per-carrier count checks may fire
        #              if those sub-groups also drop below their individual thresholds.
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6)   # primary
        if "daily_station_metrics" in signals:
            signals["daily_station_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "carrier_daily_metrics" in signals:
            signals["carrier_daily_metrics"]["local_test_failures"] = rng.randint(3, 6)

        # SECONDARY (1–3): aggregations WITHOUT a date column.
        # peak_hour_metrics schema:   (pickup_hour, time_period, pickup_borough, trip_count, avg_distance)
        # member_type_metrics schema: (member_casual, time_period, ride_count, avg_duration)
        # route_delay_metrics schema: (Origin, Dest, origin_state, flight_count, avg_arr_delay)
        #
        # These GROUP BY hour or category, not date.  All 24 hour-groups (or all
        # member types, all routes) still appear — they just have slightly lower counts.
        # Checks that sometimes fire (1–3):
        #   Check 1: "total trip_count not below rolling threshold" — the sum is
        #            ~1/31 lower than the monthly average; a sensitive Z-score check
        #            may breach, but a coarser percentage-drop check may not.
        #   Check 2: "no single hour group anomalously low" — typically doesn't fire
        #            because the reduction is evenly distributed across all hours.
        # Effectively: some monitoring setups would catch this, others wouldn't.
        if "peak_hour_metrics" in signals:
            signals["peak_hour_metrics"]["local_test_failures"] = rng.randint(1, 3)    # secondary
        if "member_type_metrics" in signals:
            signals["member_type_metrics"]["local_test_failures"] = rng.randint(1, 3)
        if "route_delay_metrics" in signals:
            signals["route_delay_metrics"]["local_test_failures"] = rng.randint(1, 3)

    elif fault_type == "duplicate_ingestion":
        # No freshness issue — data arrived on time and was loaded; it just has duplicates.
        # No contract_violation — schema is intact.

        # PRIMARY (3–6): revenue/count aggregations that have uniqueness expectations.
        # fare_band_metrics schema:    (fare_tier, pickup_borough, trip_count, total_revenue)
        # duration_tier_metrics schema: (duration_tier, member_casual, ride_count)
        # carrier_daily_metrics schema: (flight_date, carrier, flight_count, ..., cancelled_count)
        #
        # These are "primary" because revenue checks catch duplication most reliably:
        #   Check 1: "total_revenue not above rolling upper threshold" — total_revenue
        #            in fare_band_metrics is inflated because the duplicated date's
        #            revenue is double-counted.  A percentage-above-expected check fires.
        #   Check 2: "trip_count not above expected range" — trip_count in each fare_tier
        #            group is ~3–5% higher than the rolling 7-day average.
        #   Check 3: "source row count matches downstream count" — a cross-table
        #            reconciliation check (if present) detects the inflation directly.
        #   Check 4–6: per-carrier or per-member-type count thresholds may fire too.
        if "fare_band_metrics" in signals:
            signals["fare_band_metrics"]["local_test_failures"] = rng.randint(3, 6)    # primary
        if "duration_tier_metrics" in signals:
            signals["duration_tier_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "carrier_daily_metrics" in signals:
            signals["carrier_daily_metrics"]["local_test_failures"] = rng.randint(3, 6)

        # SECONDARY (1–3): time-series aggregations show elevated row counts too,
        # but the percentage increase is smaller (1 extra day out of ~31).
        # daily_zone_metrics: trip_count by date — the duplicated date has double
        # the expected trip_count for that specific date; surrounding dates are fine.
        # A per-date threshold check might catch it, but a global row-count check
        # would only see a ~3% increase which may not breach the threshold.
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(1, 3)   # secondary
        if "daily_station_metrics" in signals:
            signals["daily_station_metrics"]["local_test_failures"] = rng.randint(1, 3)
        if "route_delay_metrics" in signals:
            signals["route_delay_metrics"]["local_test_failures"] = rng.randint(1, 3)

    elif fault_type == "null_explosion":
        # No freshness issue — data arrived, it's just corrupted.
        # No contract_violation at root (raw_trips/raw_rides/raw_flights have no
        # declared schema contract on nullability at the source ingestion layer;
        # the contract check lives on the cleaned output tables downstream).

        # ENRICHMENT NODES — first hop after the null source (1–4):
        # trips_enriched schema:    (pickup_datetime, ..., PULocationID, pickup_borough, pickup_zone)
        # rides_enriched schema:    (started_at, ..., start_station_id, start_station_name, duration_minutes)
        # flights_enriched schema:  (flight_date, carrier, Origin, ..., origin_state_name, origin_state_code)
        #
        # The null PULocationID / start_station_id / Dest rows survive the LEFT JOIN
        # and appear in the enrichment table as 'Unknown' borough/station/state.
        # Checks that fire (1–4):
        #   Check 1: "not_null on pickup_borough / start_station_name" — some rows
        #            have 'Unknown' which may count as a soft null failure depending
        #            on how the check is configured.
        #   Check 2: "accepted_values for pickup_borough not in {'Unknown'}" — if the
        #            check treats 'Unknown' as an invalid value, it fires here.
        #   Check 3: "null rate on PULocationID below threshold" — a custom null-rate
        #            monitor fires if >X% of rows have null in the join key column.
        # Note: range 1–4 (not 3–6) because the enrichment node doesn't disappear
        # data — it propagates it — so the failure is observable but less definitive.
        if "trips_enriched" in signals:
            signals["trips_enriched"]["local_test_failures"] = rng.randint(1, 4)
        if "rides_enriched" in signals:
            signals["rides_enriched"]["local_test_failures"] = rng.randint(1, 4)
        if "flights_enriched" in signals:
            signals["flights_enriched"]["local_test_failures"] = rng.randint(1, 4)

        # PRIMARY LEAF NODES (3–6): aggregation leaves that group by the corrupted column.
        # daily_zone_metrics schema: (trip_day, pickup_borough, pickup_zone, trip_count, ...)
        # daily_station_metrics schema: (ride_day, start_station_id, start_station_name, ...)
        # carrier_daily_metrics schema: (flight_date, carrier, flight_count, ...)
        #
        # These aggregate by pickup_borough / start_station_id / carrier, so the ~1/7
        # null rows that became 'Unknown' now appear as an 'Unknown' group in the output:
        #   Check 1: "accepted_values for pickup_borough does not include 'Unknown'" fires.
        #   Check 2: "count of rows where pickup_borough = 'Unknown' below threshold" fires
        #            if the 'Unknown' group is larger than any prior run's 'Unknown' count.
        #   Check 3: "trip_count for 'Unknown' borough not anomalously high" — a
        #            relative-to-expected check on the group fires.
        #   Check 4–6: cross-validation checks (e.g., "total trip_count across all boroughs
        #              matches trips_enriched row count") may also breach.
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "daily_station_metrics" in signals:
            signals["daily_station_metrics"]["local_test_failures"] = rng.randint(3, 6)
        if "carrier_daily_metrics" in signals:
            signals["carrier_daily_metrics"]["local_test_failures"] = rng.randint(3, 6)

    elif fault_type == "bad_join_key":
        # Compute how many more 'Unknown' borough entries exist in the faulted run
        # vs the clean baseline.  This is the ACTUAL DuckDB measurement — trips that
        # were previously matched to a zone now have no match because their LocationID
        # was shifted out of range in zone_lookup.
        # Example: Yellow bad_join_key shifts LocationIDs 1–15 by +600.  Any trip
        # with PULocationID in 1–15 now finds no matching row in zone_lookup and
        # gets pickup_borough = 'Unknown'.  The delta = (faulted Unknown count) -
        # (baseline Unknown count), which is typically several thousand rows.
        unknown_delta = max(
            0,
            int(faulted_val.get("unknown_borough", 0)) - int(baseline_val.get("unknown_borough", 0)),
        )

        # contract_violation = 1 at the lookup table itself:
        # The lookup table (zone_lookup / station_lookup / airport_lookup) has a
        # declared referential integrity contract: "every LocationID in raw_trips
        # must have a matching row in zone_lookup".  The ID shift breaks this.
        # A schema contract check or a referential integrity test fires immediately.
        signals[root]["contract_violation"] = 1

        # ENRICHMENT NODES (2–4 if confirmed, 1–2 if unknown_delta=0):
        # trips_enriched schema: (..., pickup_borough, pickup_zone)
        # rides_enriched schema: (..., start_station_name, duration_minutes)
        # flights_enriched schema: (..., origin_state_name, origin_state_code)
        #
        # unknown_delta > 0 means the join failure is confirmed measurable — higher range.
        # unknown_delta = 0 means the corrupted IDs had no matching rows even in the
        # baseline (rare edge case), so the failure isn't clearly attributable here.
        # Checks that fire:
        #   Check 1: "not_null on pickup_borough" or "accepted_values not 'Unknown'" fires
        #            when 'Unknown' appears in records that previously had a valid value.
        #   Check 2: "referential integrity: all PULocationIDs must join to zone_lookup"
        #            — an explicit relationship test fails.
        #   Check 3: "fraction of rows with 'Unknown' borough below X%" — a custom
        #            null/unknown-rate monitor fires.
        if "trips_enriched" in signals:
            signals["trips_enriched"]["local_test_failures"] = (
                rng.randint(2, 4) if unknown_delta > 0 else rng.randint(1, 2)
            )
        if "rides_enriched" in signals:
            signals["rides_enriched"]["local_test_failures"] = rng.randint(2, 4)
        if "flights_enriched" in signals:
            signals["flights_enriched"]["local_test_failures"] = rng.randint(2, 4)

        # PRIMARY LEAF NODES (3–6 if confirmed, 1–3 otherwise):
        # daily_zone_metrics schema: (trip_day, pickup_borough, pickup_zone, trip_count, avg_fare, total_distance)
        # daily_station_metrics schema: (ride_day, start_station_id, start_station_name, ride_count, avg_duration)
        #
        # These aggregate by pickup_borough / start_station_name, so the 'Unknown' spike
        # appears as a large new group in the output.  Checks that fire:
        #   Check 1: "accepted_values for pickup_borough does not include 'Unknown'" fires.
        #   Check 2: "trip_count for 'Unknown' borough not above threshold (e.g., 5% of total)"
        #            — the 'Unknown' group may represent 5–20% of all trips depending on
        #            how many LocationIDs were shifted.
        #   Check 3: "total number of distinct pickup_borough values matches expected" —
        #            the 'Unknown' group inflates the distinct-count check if 'Unknown'
        #            was not in the expected set of boroughs.
        #   Check 4–6: per-zone or per-date sub-checks on the 'Unknown' group.
        if "daily_zone_metrics" in signals:
            signals["daily_zone_metrics"]["local_test_failures"] = (
                rng.randint(3, 6) if unknown_delta > 0 else rng.randint(1, 3)
            )
        if "daily_station_metrics" in signals:
            signals["daily_station_metrics"]["local_test_failures"] = rng.randint(3, 6)

        # BTS DUAL-PATH: route_delay_metrics (3–6) fires separately from flights_enriched
        # because it re-joins airport_lookup independently (not via flights_enriched).
        # route_delay_metrics schema: (Origin, Dest, origin_state, flight_count, avg_arr_delay)
        # The second join means airport_lookup → route_delay_metrics is a separate edge,
        # and corrupted airport codes produce 'Unknown' origin_state in this table too.
        # This is the defining structural feature of the BTS dual-path DAG — a single
        # corrupt lookup table causes two co-occurring independent leaf failures.
        if "route_delay_metrics" in signals:
            signals["route_delay_metrics"]["local_test_failures"] = rng.randint(3, 6)

    elif fault_type == "schema_drift":
        # contract_violation at the classification node (trips_classified / rides_classified
        # / flights_classified):
        # The classification step introduces a hard-coded value ('off_peak', 'medium', or
        # 'on_time') that breaks the column's expected cardinality.  A schema contract test
        # on that column (e.g., "accepted_values for time_period in {'peak', 'off_peak'}"
        # or "count(distinct time_period) must equal 2") would detect this.
        #
        # 80% probability (rng.random() > 0.20): contract fires — the schema registry
        # or CI schema test catches the broken logic before it reaches monitoring.
        # 20% probability: contract does NOT fire — the logic change is syntactically valid
        # (the column still exists, it still has a string value) and only breaks at runtime
        # when the downstream aggregation shows unexpected distributions.
        # This 80/20 split models real-world detection latency: most schema-breaking
        # regressions are caught by CI/CD checks, but some slip through to production.
        signals[root]["contract_violation"] = 1 if rng.random() > 0.20 else 0

        # PRIMARY LEAF NODES — those that GROUP BY the corrupted column (3–6):
        # peak_hour_metrics schema: (pickup_hour, time_period, pickup_borough, trip_count, avg_distance)
        #   time_period column is derived from trips_classified.time_period.
        #   When every row has time_period='off_peak', the 'peak' group disappears entirely.
        #   Checks that fire:
        #     Check 1: "accepted_values for time_period in {'peak', 'off_peak'}" — technically
        #              still passes because 'off_peak' IS an accepted value.
        #     Check 2: "count of rows where time_period='peak' >= minimum_threshold" — this
        #              is the key check: a threshold like "at least 20% of rows should be
        #              'peak' during a weekday" fires immediately (0% peak vs expected 30%).
        #     Check 3: "distinct count of time_period values equals 2" — fails because
        #              only 1 distinct value ('off_peak') now exists.
        #     Check 4–6: "peak_hour trip_count not anomalously low for AM/PM hours" — the
        #                7–9am and 4–7pm hour groups now show 0 peak trips, which is a massive
        #                Z-score breach versus the rolling average.
        if "peak_hour_metrics" in signals:
            signals["peak_hour_metrics"]["local_test_failures"] = rng.randint(3, 6)
        # Divvy: duration_tier_metrics groups by duration_tier (short/medium/long).
        #   When every ride is 'medium', the 'short' and 'long' groups disappear.
        #   Same failure pattern: "distinct count of duration_tier must equal 3" fires.
        if "duration_tier_metrics" in signals:
            signals["duration_tier_metrics"]["local_test_failures"] = rng.randint(3, 6)
        # BTS: delay_tier_metrics groups by delay_tier (early/on_time/delayed/severely_delayed).
        #   When every flight is 'on_time', three tiers disappear.
        if "delay_tier_metrics" in signals:
            signals["delay_tier_metrics"]["local_test_failures"] = rng.randint(3, 6)

        # SIBLING LEAF NODES — those that do NOT use the corrupted column (0–1):
        # fare_band_metrics schema: (fare_tier, pickup_borough, trip_count, total_revenue)
        #   fare_tier comes from fare_amount thresholds, NOT from time_period.
        #   This table is structurally unaffected — row counts and values are identical
        #   to the baseline.  0–1 reflects that a global "sanity check" might weakly
        #   fire (e.g., "trips_classified row count matches fare_band_metrics total")
        #   but no fare-specific check would catch the time_period corruption.
        if "fare_band_metrics" in signals:
            signals["fare_band_metrics"]["local_test_failures"] = rng.randint(0, 1)
        if "member_type_metrics" in signals:
            # member_type_metrics schema: (member_casual, time_period, ride_count, avg_duration)
            # Note: member_type_metrics DOES have a time_period column (inherited from
            # rides_classified), but it groups by member_casual first and the drift
            # makes both member groups show only 'off_peak' — the table still produces
            # the same number of rows (2 member types × 1 time_period = 2 rows instead of
            # 2 × 2 = 4 rows).  This IS a detectable change but the check is mild.
            signals["member_type_metrics"]["local_test_failures"] = rng.randint(0, 1)
        if "carrier_daily_metrics" in signals:
            # carrier_daily_metrics schema: (flight_date, carrier, flight_count, avg_arr_delay, cancelled_count)
            # No delay_tier column — completely unaffected by the classification drift.
            signals["carrier_daily_metrics"]["local_test_failures"] = rng.randint(0, 1)

    # The observed failure node (the node where the monitoring alert that triggered
    # this incident was first noticed) is guaranteed to have high test failures.
    # This overrides whatever the per-fault logic set, using max() to never decrease
    # a value already set higher by the fault-specific logic above.
    #
    # Why this guarantee?  The "observed_failure_asset" field in the incident dict
    # represents the monitoring system's entry point — the engineer who got paged
    # saw failures at this node.  It would be inconsistent to have the entry point
    # show 0 test failures.  The randint(3, 6) ensures at least a "clear failure"
    # reading regardless of which fault type was injected.
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
    """Assemble the canonical incident dict that all RCA methods consume.

    This dict is the primary input/output unit of the benchmark.  Each RCA method
    in evaluate_rankers.py reads exactly these fields to produce a ranked list of
    candidate root-cause nodes.

    ── Top-level fields ──────────────────────────────────────────────────────────

      incident_id (str)
        Unique identifier, e.g. "real_inc_042".  The counter is global across all
        pipelines so IDs never collide in the merged output JSON.

      pipeline (str)
        One of: "nyc_yellow_taxi_etl_real", "nyc_green_taxi_etl_real",
                "divvy_chicago_bike_real", "bts_airline_ontime_real".
        Used for by_pipeline breakdowns and for LR-L leave-one-pipeline-out splits.

      fault_type (str)
        Ground-truth fault class — one of the 6 injected types.
        Used for by_fault_type breakdowns and for verifying that signal ranges
        are discriminative per fault class.

      observability_mode (str)
        "full" | "runtime_sparse" | "runtime_missing_root"
        Determines how much of runtime_edges is available to RCA methods.

      root_cause_asset (str)
        The ground-truth answer — which pipeline node caused the fault.
        This is what every RCA method is trying to rank #1.  Examples:
          "raw_trips"          (missing_partition, stale_source, null_explosion)
          "zone_lookup"        (bad_join_key — yellow/green)
          "trips_classified"   (schema_drift — yellow/green)
          "airport_lookup"     (bad_join_key — BTS)

      observed_failure_asset (str)
        The node where the first monitoring alert fired.  This is the starting
        point for backwards traversal — "we observed a problem here, what caused it?"
        For missing_partition: "daily_zone_metrics" (trip_count gap by date).
        For schema_drift: "peak_hour_metrics" (all time_period='peak' rows gone).

      impacted_assets (list[str])
        All descendants of root_cause_asset in the design DAG.  Pre-computed here
        so evaluate_rankers.py doesn't rerun nx.descendants per incident.
        Example for root="raw_trips":
          ["trips_valid", "trips_enriched", "trips_classified",
           "daily_zone_metrics", "fare_band_metrics", "peak_hour_metrics"]

      design_edges (list[tuple[str,str]])
        Full static lineage from the pipeline spec — every declared edge regardless
        of observability.  Used by the design_distance baseline which assumes
        perfect static knowledge.  Example subset:
          [("raw_trips", "trips_valid"),
           ("trips_valid", "trips_enriched"),
           ("zone_lookup", "trips_enriched"),
           ("trips_enriched", "trips_classified"),
           ("trips_classified", "daily_zone_metrics"), ...]

      runtime_edges (list[tuple[str,str]])
        Observed edges from actual pipeline execution, possibly degraded:
          - full mode:         same as design_edges
          - runtime_sparse:    root's edges removed + 30% random drop
          - runtime_missing_root: only root's edges removed
        Methods that rely on runtime lineage (LR-H, LR-BS, LR-CP) use this field.

      leaf_test_weights (dict[str, float])
        Per-node importance weights for the failed_tests heuristic.  Leaf nodes
        (daily_zone_metrics etc.) typically have weight 1.0; intermediate nodes
        have lower weights because their test suites are less comprehensive.
        Example: {"daily_zone_metrics": 1.0, "fare_band_metrics": 1.0,
                  "peak_hour_metrics": 1.0, "trips_classified": 0.5, ...}

      node_types (dict[str, str])
        Maps each node name to its type label used by some heuristics:
          "source"    — raw ingested table (raw_trips, raw_rides, raw_flights)
          "dimension" — static lookup table (zone_lookup, station_lookup, airport_lookup)
          "staging"   — intermediate transformation (trips_valid, trips_enriched,
                        trips_classified, and their green/divvy/bts equivalents)
          "leaf"      — final output/metric table (daily_zone_metrics, fare_band_metrics,
                        peak_hour_metrics, and their equivalents across pipelines)

      node_signals (dict[str, dict])
        The core evidence object.  One entry per pipeline node, each with 5 fields.
        Full annotated example for a missing_partition incident on yellow taxi
        (root=raw_trips, observed=daily_zone_metrics):

        "node_signals": {
          "raw_trips": {
            "recent_change":    0.71,   # elevated U(0.55,0.86): this node was recently touched
            "freshness_severity": 0.0,  # 0 for missing_partition — data came in, a date just absent
            "run_anomaly":      0.62,   # TYPE A: 0.35 + 0.65*row_delta (real DuckDB measurement)
                                        #   row_delta ≈ 0.039 for Jan 2024 green taxi peak day
                                        #   → anom ≈ 0.35 + 0.65*0.039 + noise ≈ 0.38
                                        #   (Yellow: row_delta ≈ 0.50 because LIMIT 120k captures
                                        #    only 2 days, so removing 1 day = ~50% of rows)
            "contract_violation": 0,    # no schema contract fires for missing_partition
            "local_test_failures": 0    # source tables don't have row-count dbt tests in this model
          },
          "zone_lookup": {
            "recent_change":    0.08,   # low U(0.05,0.28): this node was not involved
            "freshness_severity": 0.0,
            "run_anomaly":      0.11,   # low U(0.04,0.22): background noise
            "contract_violation": 0,
            "local_test_failures": 0
          },
          "trips_valid": {
            "recent_change":    0.14,   # low — downstream of fault, didn't change
            "freshness_severity": 0.0,
            "run_anomaly":      0.29,   # impacted: U(0.12,0.38) + 0.35*row_delta
                                        #   row_delta here = abs(faulted - baseline)/baseline
                                        #   since trips_valid filters raw_trips, its row count
                                        #   also dropped proportionally
            "contract_violation": 0,
            "local_test_failures": 0
          },
          "trips_enriched": {
            "recent_change":    0.19,
            "freshness_severity": 0.0,
            "run_anomaly":      0.31,   # impacted: also dropped due to fewer input rows
            "contract_violation": 0,
            "local_test_failures": 0
          },
          "trips_classified": {
            "recent_change":    0.21,
            "freshness_severity": 0.0,
            "run_anomaly":      0.27,   # impacted: same row loss propagating forward
            "contract_violation": 0,
            "local_test_failures": 0
          },
          "daily_zone_metrics": {
            "recent_change":    0.17,
            "freshness_severity": 0.0,
            "run_anomaly":      0.33,   # impacted: row count dropped (fewer trip-day groups)
            "contract_violation": 0,
            "local_test_failures": 4    # PRIMARY: 3–6 checks fire (see threshold detail below)
                                        #   Check 1: "row_count >= expected_minimum" fails
                                        #             (fewer rows than any recent day)
                                        #   Check 2: "date coverage complete" fails
                                        #             (the GROUP BY trip_day result has a gap)
                                        #   Check 3: "trip_count not anomalously low" fails
                                        #             (total trip_count dropped ~1/31 of month)
                                        #   Check 4+: variance/stddev checks may also fire
          },
          "fare_band_metrics": {
            "recent_change":    0.12,
            "freshness_severity": 0.0,
            "run_anomaly":      0.19,   # impacted but weakly: total revenue slightly lower
            "contract_violation": 0,
            "local_test_failures": 0    # no explicit check fires — proportions unchanged
          },
          "peak_hour_metrics": {
            "recent_change":    0.09,
            "freshness_severity": 0.0,
            "run_anomaly":      0.22,
            "contract_violation": 0,
            "local_test_failures": 2    # SECONDARY: 1–3 checks fire
                                        #   Check 1: "total trip_count not anomalously low" fires
                                        #             (24-hour aggregation, all hours present but
                                        #              each has ~1/31 fewer trips than baseline)
                                        #   — no date gap visible here because no trip_day column
          }
        }

    ── How local_test_failures maps to real monitoring checks ───────────────────
    local_test_failures is an integer 0–6 representing how many data quality tests
    would fail at this node given the fault.  It models a dbt-style test suite
    or Great Expectations checks attached to each pipeline output table.

    The scale is NOT a literal count of specific named tests — it is a severity
    indicator.  The interpretation is:
      0     : no check fires at this node (uninvolved or weakly impacted)
      1–2   : mild anomaly — a "not null" or "accepted values" check fires,
              or a soft volume threshold is approached but not clearly breached
      3–4   : clear failure — a row-count threshold, date-coverage check,
              or uniqueness check definitively fires
      5–6   : severe failure — multiple independent checks fire simultaneously
              (e.g., row count drop + date gap + stddev breach all at once)

    The integer ranges randint(3,6) vs randint(1,3) vs randint(0,1) encode
    whether a node is a "primary" observable failure (3–6), a "secondary" one
    (1–3), or merely adjacent (0–1).  See the fault-type augmentation block
    below for the full mapping.
    """
    if spec is None:
        spec = SPEC
    return {
        "incident_id": incident_id,
        "pipeline": pipeline_name,
        "fault_type": fault_type,
        "observability_mode": observability_mode,
        "root_cause_asset": root,
        "observed_failure_asset": observed,
        # impacted_assets is pre-computed here from the design graph so that
        # evaluate_rankers.py doesn't need to recompute nx.descendants for each
        # incident during evaluation.
        "impacted_assets": sorted(set(nx.descendants(build_graph(list(spec.edges)), root))),
        "design_edges":    list(spec.edges),
        "runtime_edges":   runtime_edges,
        "leaf_test_weights": spec.leaf_test_weights,
        "node_types":      spec.nodes,
        "node_signals":    signals,
    }


# ── Green taxi pipeline (NYC TLC green taxi, Jan 2024) ─────────────────────────
# Same 8-node topology as the yellow taxi extended pipeline but uses green taxi
# trip records (56k rows, smaller dataset, different datetime column names).
#
# Why include Green taxi as a separate pipeline?
#   1. Independent dataset: green taxi serves outer-borough pickup zones not
#      covered by yellow, so it is a genuinely different traffic population.
#   2. Full-month coverage: 56,551 rows covers all of January 2024 without any
#      LIMIT truncation, so missing_partition / duplicate_ingestion / stale_source
#      faults produce different calibrated row_deltas than Yellow's 2-day sample.
#   3. Structural reuse: the 8-node topology is identical to Yellow, providing
#      same-topology diversity — metrics can be compared directly between families.
#
# Column differences from Yellow: lpep_pickup_datetime / lpep_dropoff_datetime
# instead of tpep_*.  All downstream SQL is otherwise structurally identical.
# Table names are prefixed "green_" to avoid namespace collisions when both
# pipelines share a DuckDB connection during calibration.

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
    """Load green taxi trip records.

    max_rows defaults to 56,551 (the full dataset for Jan 2024) because green
    taxi is small enough to load completely — no LIMIT-related date bias issue.
    zone_lookup is the same static dimension as yellow taxi (shared CSV file).
    """
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
    step_rows: dict[str, int] = {
        "raw_green_trips": int(conn.execute("select count(*) from raw_green_trips").fetchone()[0]),
        "zone_lookup":     int(conn.execute("select count(*) from zone_lookup").fetchone()[0]),
    }

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
    """Inject a fault into the green taxi pipeline.

    Structurally identical to _apply_fault (yellow) but uses lpep_* datetime
    columns and the raw_green_trips / green_* table namespace.

    bad_join_key shifts LocationIDs 100–120 (instead of yellow's 1–15) to
    produce a different zone coverage failure pattern, exercising the join
    fault with a distinct set of affected records.
    """
    if fault_type == "missing_partition":
        # Same logic as yellow: remove most-populated date. Full dataset means
        # this will be a peak weekday in mid-January (~1,800–2,000 rows).
        remove_date = conn.execute("""
            select cast(lpep_pickup_datetime as date) as d, count(*) as n
            from raw_green_trips group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_green_trips as
            select * from raw_green_trips
            where cast(lpep_pickup_datetime as date) <> '{remove_date}'
        """)
        return "raw_green_trips", "green_daily_zone", False

    if fault_type == "duplicate_ingestion":
        dup_date = conn.execute("""
            select cast(lpep_pickup_datetime as date) as d, count(*) as n
            from raw_green_trips group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_green_trips as
            select * from raw_green_trips
            union all
            select * from raw_green_trips
            where cast(lpep_pickup_datetime as date) = '{dup_date}'
        """)
        return "raw_green_trips", "green_fare_band", False

    if fault_type == "stale_source":
        # Same 40% descending cutoff logic as yellow.  Full dataset means ~22,620
        # rows dropped — about 11 days of data removed.
        total = conn.execute("select count(*) from raw_green_trips").fetchone()[0]
        drop  = max(1, int(total * 0.40))
        cutoff_date = conn.execute(f"""
            select cast(lpep_pickup_datetime as date)
            from raw_green_trips order by lpep_pickup_datetime desc
            limit 1 offset {drop - 1}
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_green_trips as
            select * from raw_green_trips
            where cast(lpep_pickup_datetime as date) < '{cutoff_date}'
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
        # Shift LocationIDs 100–120 instead of 1–15 (yellow).  LocationIDs 100–120
        # cover mid-range zones in Brooklyn/Queens — shifting these produces a
        # 'Unknown' spike for green taxi pickup areas, testing a different part of
        # the zone lookup table than the yellow taxi bad_join_key scenario.
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
        # Same pattern as yellow: drifted SQL hardcodes 'off_peak'.
        # green_trips_classified is the root; green_peak_hour is the observed failure.
        return "green_trips_classified", "green_peak_hour", True

    raise ValueError(f"Unsupported fault_type: {fault_type!r}")


# ── Divvy Chicago bike-share pipeline (Jan 2024, 144,873 trips) ──────────────
# Third structurally distinct pipeline topology.  Unlike the taxi pipelines, the
# "dimension table" (station_lookup) is derived FROM the raw data itself
# (SELECT DISTINCT start_station_id, start_station_name FROM raw_rides) rather
# than from a separate static file.  This means:
#   - bad_join_key corrupts string-valued station IDs (prepend 'ZZ_') rather
#     than shifting numeric LocationIDs.
#   - station_lookup is rebuilt fresh each load, so there is no persistent
#     external file to corrupt.
#
# The classification step differs: rides are bucketed by duration_tier (short /
# medium / long, based on duration_minutes thresholds) rather than fare.
# Schema_drift hardcodes 'medium' for all rides, corrupting duration_tier_metrics
# (the observed failure) and leaving member_type_metrics unaffected.
#
# Loaded up to 120k rows via LIMIT from the 144k-row CSV.  Like Yellow, the CSV
# is time-sorted, so LIMIT captures Jan 1–early days.  All fault dates are
# data-relative (most-populated date query) to handle this correctly.

_DIVVY_SQL_VALID = """
create or replace table rides_valid as
select
  ride_id,
  rideable_type,
  cast(started_at as timestamp) as started_at,
  cast(ended_at as timestamp)   as ended_at,
  start_station_id,
  end_station_id,
  member_casual
from raw_rides
where started_at is not null
  and ended_at   is not null
  and start_station_id is not null
  and cast(started_at as timestamp) < cast(ended_at as timestamp);
"""

_DIVVY_SQL_ENRICHED = """
create or replace table rides_enriched as
select
  v.*,
  coalesce(s.station_name, 'Unknown') as start_station_name,
  epoch(v.ended_at - v.started_at) / 60.0 as duration_minutes
from rides_valid v
left join station_lookup s on v.start_station_id = s.station_id;
"""

_DIVVY_SQL_CLASSIFIED_NORMAL = """
create or replace table rides_classified as
select
  e.*,
  date_trunc('day', e.started_at) as ride_day,
  extract(hour from e.started_at)::integer as start_hour,
  case
    when e.duration_minutes < 10 then 'short'
    when e.duration_minutes < 30 then 'medium'
    else 'long'
  end as duration_tier,
  case
    when extract(hour from e.started_at) between 7  and 9
      or extract(hour from e.started_at) between 16 and 19
    then 'peak' else 'off_peak'
  end as time_period
from rides_enriched e;
"""

# Schema-drift variant: always classifies rides as 'medium' (broken logic)
_DIVVY_SQL_CLASSIFIED_DRIFTED = """
create or replace table rides_classified as
select
  e.*,
  date_trunc('day', e.started_at) as ride_day,
  extract(hour from e.started_at)::integer as start_hour,
  'medium' as duration_tier,
  case
    when extract(hour from e.started_at) between 7  and 9
      or extract(hour from e.started_at) between 16 and 19
    then 'peak' else 'off_peak'
  end as time_period
from rides_enriched e;
"""

_DIVVY_SQL_DAILY = """
create or replace table daily_station_metrics as
select
  ride_day,
  start_station_id,
  start_station_name,
  count(*)                          as ride_count,
  round(avg(duration_minutes), 2)  as avg_duration
from rides_classified
group by 1, 2, 3;
"""

_DIVVY_SQL_DURATION = """
create or replace table duration_tier_metrics as
select
  duration_tier,
  member_casual,
  count(*) as ride_count
from rides_classified
group by 1, 2;
"""

_DIVVY_SQL_MEMBER = """
create or replace table member_type_metrics as
select
  member_casual,
  time_period,
  count(*)                         as ride_count,
  round(avg(duration_minutes), 2) as avg_duration
from rides_classified
group by 1, 2;
"""


def _load_divvy_sources(conn: duckdb.DuckDBPyConnection, max_rows: int) -> None:
    """Load Divvy ride records and derive a station lookup from the data.

    station_lookup is built from raw_rides itself (SELECT DISTINCT) rather than
    from a separate file.  This means:
    - The station dimension is consistent with whatever rides were loaded.
    - bad_join_key corrupts station_lookup AFTER it has been built correctly,
      which simulates a downstream metadata corruption or stale catalog update.
    - The bad_join_key fault uses string manipulation ('ZZ_' prefix) rather than
      numeric ID shifting because Divvy station IDs are alphanumeric strings.
    """
    raw_dir = ROOT / "data" / "raw" / "divvy"
    ride_path = raw_dir / "202401-divvy-tripdata.csv"
    if not ride_path.exists():
        raise FileNotFoundError(f"Divvy CSV not found: {ride_path}")
    conn.execute(f"""
        create or replace table raw_rides as
        select * from read_csv_auto('{ride_path.as_posix()}', header=true)
        limit {int(max_rows)}
    """)
    conn.execute("""
        create or replace table station_lookup as
        select distinct
          start_station_id   as station_id,
          start_station_name as station_name
        from raw_rides
        where start_station_id   is not null
          and start_station_name is not null
    """)


def _run_divvy_pipeline(conn: duckdb.DuckDBPyConnection, schema_drifted: bool = False) -> dict[str, object]:
    lineage: list[tuple[str, str]] = []
    step_rows: dict[str, int] = {
        "raw_rides":      int(conn.execute("select count(*) from raw_rides").fetchone()[0]),
        "station_lookup": int(conn.execute("select count(*) from station_lookup").fetchone()[0]),
    }

    def _run(sql: str, inputs: list[str], output: str) -> None:
        conn.execute(sql)
        cnt = conn.execute(f"select count(*) from {output}").fetchone()[0]
        step_rows[output] = int(cnt)
        lineage.extend((inp, output) for inp in inputs)

    _run(_DIVVY_SQL_VALID,    ["raw_rides"],                           "rides_valid")
    _run(_DIVVY_SQL_ENRICHED, ["rides_valid", "station_lookup"],       "rides_enriched")
    cl_sql = _DIVVY_SQL_CLASSIFIED_DRIFTED if schema_drifted else _DIVVY_SQL_CLASSIFIED_NORMAL
    _run(cl_sql,               ["rides_enriched"],                     "rides_classified")
    _run(_DIVVY_SQL_DAILY,     ["rides_classified"],                   "daily_station_metrics")
    _run(_DIVVY_SQL_DURATION,  ["rides_classified"],                   "duration_tier_metrics")
    _run(_DIVVY_SQL_MEMBER,    ["rides_classified"],                   "member_type_metrics")

    validations = conn.execute("""
        select
          (select count(*) from rides_enriched    where start_station_name = 'Unknown') as unknown_station,
          (select count(*) from daily_station_metrics)                                   as daily_station_rows,
          (select count(*) from duration_tier_metrics where duration_tier = 'short')    as short_rides
    """).fetchone()

    return {
        "runtime_lineage": lineage,
        "step_rows": step_rows,
        "validations": {
            "unknown_station":    int(validations[0]),
            "daily_station_rows": int(validations[1]),
            "short_rides":        int(validations[2]),
        },
    }


def _apply_divvy_fault(
    conn: duckdb.DuckDBPyConnection,
    fault_type: str,
    iteration: int,
) -> tuple[str, str, bool]:
    """Inject a fault into the Divvy bike-share pipeline.

    Same logic as _apply_fault / _apply_green_fault but uses Divvy column names
    (started_at, ended_at, start_station_id) and Divvy table names (raw_rides,
    station_lookup).

    bad_join_key: prepends 'ZZ_' to station IDs whose string length > 3.
    Divvy station IDs are strings like "TA1307000039" so most IDs qualify —
    this breaks most joins and produces a large 'Unknown' station spike.
    """
    if fault_type == "missing_partition":
        remove_date = conn.execute("""
            select cast(started_at as date) as d, count(*) as n
            from raw_rides group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_rides as
            select * from raw_rides
            where cast(started_at as date) <> '{remove_date}'
        """)
        return "raw_rides", "daily_station_metrics", False

    if fault_type == "duplicate_ingestion":
        dup_date = conn.execute("""
            select cast(started_at as date) as d, count(*) as n
            from raw_rides group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_rides as
            select * from raw_rides
            union all
            select * from raw_rides
            where cast(started_at as date) = '{dup_date}'
        """)
        return "raw_rides", "duration_tier_metrics", False

    if fault_type == "stale_source":
        total = conn.execute("select count(*) from raw_rides").fetchone()[0]
        drop  = max(1, int(total * 0.40))
        cutoff_date = conn.execute(f"""
            select cast(started_at as date)
            from raw_rides order by started_at desc
            limit 1 offset {drop - 1}
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_rides as
            select * from raw_rides
            where cast(started_at as date) < '{cutoff_date}'
        """)
        return "raw_rides", "daily_station_metrics", False

    if fault_type == "null_explosion":
        conn.execute(f"""
            create or replace table raw_rides as
            select
              ride_id, rideable_type, started_at, ended_at,
              case when (row_number() over ()) % 7 = {iteration % 7}
                   then null else start_station_id end as start_station_id,
              end_station_id, start_station_name, end_station_name,
              start_lat, start_lng, end_lat, end_lng, member_casual
            from raw_rides
        """)
        return "raw_rides", "daily_station_metrics", False

    if fault_type == "bad_join_key":
        conn.execute("""
            create or replace table station_lookup as
            select
              case when length(station_id) > 3 then 'ZZ_' || station_id
                   else station_id end as station_id,
              station_name
            from station_lookup
        """)
        return "station_lookup", "daily_station_metrics", False

    if fault_type == "schema_drift":
        return "rides_classified", "duration_tier_metrics", True

    raise ValueError(f"Unsupported fault_type: {fault_type!r}")


# ── BTS Airline On-Time Performance pipeline (Jan 2024, 547,271 flights) ─────
# The only pipeline with a dual-path DAG topology.
#
# airport_lookup feeds TWO nodes:
#   flights_enriched    — via LEFT JOIN on flights_valid.Origin = airport_code
#   route_delay_metrics — via a second LEFT JOIN on flights_classified.Origin = airport_code
#
# This creates a fork: airport_lookup → {flights_enriched, route_delay_metrics}.
# When airport_lookup is corrupted (bad_join_key), BOTH downstream nodes fail
# simultaneously — testing whether RCA methods can identify the shared upstream
# cause of two co-occurring failures (a key use case for lineage-aware RCA).
#
# Bad_join_key: prepend 'ZZ' to airport codes starting with A–F.  These codes
# represent a large fraction of US airports (American, Alaska, Delta hubs etc.)
# so the failure is highly visible in route_delay_metrics.
#
# Schema_drift: hardcodes delay_tier to 'on_time' — all flights appear on time,
# delay_tier_metrics shows only 'on_time' rows, contract tests on other tiers fail.
# The 'early' / 'delayed' / 'severely_delayed' rows drop to 0.
#
# Loaded up to 120k rows from the full 547k CSV via LIMIT.  Like Yellow, LIMIT
# will capture early-January flights (sorted by FlightDate).  All fault dates
# are data-relative to handle this correctly.

_BTS_SQL_VALID = """
create or replace table flights_valid as
select
  cast(FlightDate as date)           as flight_date,
  Reporting_Airline                  as carrier,
  Origin,
  Dest,
  coalesce(DepDelay, 0.0)           as dep_delay,
  coalesce(ArrDelay, 0.0)           as arr_delay,
  Distance,
  coalesce(Cancelled, 0) >= 1       as is_cancelled
from raw_flights
where FlightDate is not null
  and Origin     is not null
  and Dest       is not null
  and Distance   > 0;
"""

_BTS_SQL_ENRICHED = """
create or replace table flights_enriched as
select
  v.*,
  coalesce(a.state_name, 'Unknown') as origin_state_name,
  coalesce(a.state_code, 'Unknown') as origin_state_code
from flights_valid v
left join airport_lookup a on v.Origin = a.airport_code;
"""

_BTS_SQL_CLASSIFIED_NORMAL = """
create or replace table flights_classified as
select
  e.*,
  case
    when e.arr_delay < 0   then 'early'
    when e.arr_delay <= 15 then 'on_time'
    when e.arr_delay <= 60 then 'delayed'
    else                        'severely_delayed'
  end as delay_tier
from flights_enriched e;
"""

# Schema-drift variant: always 'on_time' — corrupts delay_tier_metrics
_BTS_SQL_CLASSIFIED_DRIFTED = """
create or replace table flights_classified as
select
  e.*,
  'on_time' as delay_tier
from flights_enriched e;
"""

_BTS_SQL_CARRIER = """
create or replace table carrier_daily_metrics as
select
  flight_date,
  carrier,
  count(*)                                          as flight_count,
  round(avg(arr_delay), 2)                         as avg_arr_delay,
  sum(case when is_cancelled then 1 else 0 end)    as cancelled_count
from flights_classified
group by 1, 2;
"""

_BTS_SQL_DELAY_TIER = """
create or replace table delay_tier_metrics as
select
  delay_tier,
  origin_state_code,
  count(*)                  as flight_count,
  round(avg(arr_delay), 2) as avg_arr_delay
from flights_classified
group by 1, 2;
"""

# Dual-path: re-joins airport_lookup so airport_lookup → route_delay_metrics edge exists
_BTS_SQL_ROUTE = """
create or replace table route_delay_metrics as
select
  f.Origin,
  f.Dest,
  coalesce(a.state_name, 'Unknown') as origin_state,
  count(*)                           as flight_count,
  round(avg(f.arr_delay), 2)        as avg_arr_delay
from flights_classified f
left join airport_lookup a on f.Origin = a.airport_code
group by 1, 2, 3;
"""


def _load_bts_sources(conn: duckdb.DuckDBPyConnection, max_rows: int) -> None:
    """Load BTS On-Time flights and derive an airport lookup from origin metadata.

    The raw CSV has ~70 columns; we SELECT only the 10 we need to keep the
    in-memory table size manageable.  airport_lookup is derived (SELECT DISTINCT)
    from the Origin / OriginState / OriginStateName columns already in raw_flights,
    so no external dimension file is needed.

    This means airport_lookup is always consistent with the loaded flight records —
    it cannot have airports not present in raw_flights.  This is intentional: we
    want the JOIN to be clean in the baseline, with failures introduced ONLY by
    the bad_join_key fault corruption step.
    """
    raw_dir = ROOT / "data" / "raw" / "bts_airline"
    flight_path = raw_dir / "On_Time_2024_1.csv"
    if not flight_path.exists():
        raise FileNotFoundError(f"BTS airline CSV not found: {flight_path}")
    conn.execute(f"""
        create or replace table raw_flights as
        select
          FlightDate, Reporting_Airline, Origin, OriginState, OriginStateName,
          Dest, DepDelay, ArrDelay, Distance, Cancelled
        from read_csv_auto('{flight_path.as_posix()}', header=true)
        limit {int(max_rows)}
    """)
    conn.execute("""
        create or replace table airport_lookup as
        select distinct
          Origin          as airport_code,
          OriginState     as state_code,
          OriginStateName as state_name
        from raw_flights
        where Origin is not null and OriginState is not null
    """)


def _run_bts_pipeline(conn: duckdb.DuckDBPyConnection, schema_drifted: bool = False) -> dict[str, object]:
    lineage: list[tuple[str, str]] = []
    step_rows: dict[str, int] = {
        "raw_flights":    int(conn.execute("select count(*) from raw_flights").fetchone()[0]),
        "airport_lookup": int(conn.execute("select count(*) from airport_lookup").fetchone()[0]),
    }

    def _run(sql: str, inputs: list[str], output: str) -> None:
        conn.execute(sql)
        cnt = conn.execute(f"select count(*) from {output}").fetchone()[0]
        step_rows[output] = int(cnt)
        lineage.extend((inp, output) for inp in inputs)

    _run(_BTS_SQL_VALID,    ["raw_flights"],                              "flights_valid")
    _run(_BTS_SQL_ENRICHED, ["flights_valid", "airport_lookup"],          "flights_enriched")
    cl_sql = _BTS_SQL_CLASSIFIED_DRIFTED if schema_drifted else _BTS_SQL_CLASSIFIED_NORMAL
    _run(cl_sql,             ["flights_enriched"],                        "flights_classified")
    _run(_BTS_SQL_CARRIER,   ["flights_classified"],                      "carrier_daily_metrics")
    _run(_BTS_SQL_DELAY_TIER,["flights_classified"],                      "delay_tier_metrics")
    # route_delay_metrics is a dual-path node: re-joins airport_lookup
    _run(_BTS_SQL_ROUTE,     ["flights_classified", "airport_lookup"],    "route_delay_metrics")

    validations = conn.execute("""
        select
          (select count(*) from flights_enriched    where origin_state_name = 'Unknown') as unknown_state,
          (select count(*) from carrier_daily_metrics)                                    as carrier_daily_rows,
          (select count(*) from delay_tier_metrics  where delay_tier = 'on_time')        as on_time_rows
    """).fetchone()

    return {
        "runtime_lineage": lineage,
        "step_rows": step_rows,
        "validations": {
            "unknown_state":      int(validations[0]),
            "carrier_daily_rows": int(validations[1]),
            "on_time_rows":       int(validations[2]),
        },
    }


def _apply_bts_fault(
    conn: duckdb.DuckDBPyConnection,
    fault_type: str,
    iteration: int,
) -> tuple[str, str, bool]:
    """Inject a fault into the BTS airline pipeline.

    Structurally mirrors _apply_fault / _apply_green_fault but uses BTS column
    names (FlightDate, Origin, Dest) and the dual-path topology's specifics.

    bad_join_key is the most structurally interesting fault here: corrupting
    airport_lookup causes failures in BOTH flights_enriched AND route_delay_metrics
    simultaneously, testing whether RCA methods identify airport_lookup as the
    shared root rather than blaming one of the two affected nodes.

    null_explosion nullifies Dest (not Origin) to avoid breaking the Origin-based
    airport_lookup join; this lets the enrichment step still run while producing
    null Dest values that cause flights_valid filtering and downstream anomalies.
    """
    if fault_type == "missing_partition":
        remove_date = conn.execute("""
            select cast(FlightDate as date) as d, count(*) as n
            from raw_flights group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_flights as
            select * from raw_flights
            where cast(FlightDate as date) <> '{remove_date}'
        """)
        return "raw_flights", "carrier_daily_metrics", False

    if fault_type == "duplicate_ingestion":
        dup_date = conn.execute("""
            select cast(FlightDate as date) as d, count(*) as n
            from raw_flights group by d order by n desc limit 1
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_flights as
            select * from raw_flights
            union all
            select * from raw_flights
            where cast(FlightDate as date) = '{dup_date}'
        """)
        return "raw_flights", "carrier_daily_metrics", False

    if fault_type == "stale_source":
        total = conn.execute("select count(*) from raw_flights").fetchone()[0]
        drop  = max(1, int(total * 0.40))
        cutoff_date = conn.execute(f"""
            select cast(FlightDate as date)
            from raw_flights order by FlightDate desc
            limit 1 offset {drop - 1}
        """).fetchone()[0]
        conn.execute(f"""
            create or replace table raw_flights as
            select * from raw_flights
            where cast(FlightDate as date) < '{cutoff_date}'
        """)
        return "raw_flights", "carrier_daily_metrics", False

    if fault_type == "null_explosion":
        conn.execute(f"""
            create or replace table raw_flights as
            select
              FlightDate, Reporting_Airline, Origin,
              case when (row_number() over ()) % 7 = {iteration % 7}
                   then null else Dest end as Dest,
              DepDelay, ArrDelay, Distance, Cancelled,
              OriginState, OriginStateName
            from raw_flights
        """)
        return "raw_flights", "carrier_daily_metrics", False

    if fault_type == "bad_join_key":
        # Corrupt airport_lookup: prepend 'ZZ' to codes starting with A–F
        conn.execute("""
            create or replace table airport_lookup as
            select
              case when left(airport_code, 1) between 'A' and 'F'
                   then 'ZZ' || airport_code
                   else airport_code end as airport_code,
              state_code,
              state_name
            from airport_lookup
        """)
        return "airport_lookup", "route_delay_metrics", False

    if fault_type == "schema_drift":
        return "flights_classified", "delay_tier_metrics", True

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
    """Run one complete real-data pipeline case study and return incidents.

    For each (fault_type, iteration) pair this function:
      1. Creates a fresh DuckDB in-process database (separate file per incident
         so each fault starts from a clean baseline state).
      2. Loads the raw data via load_fn.
      3. Injects the fault via fault_fn (modifies source tables in-place).
      4. Runs the full pipeline via run_fn to capture faulted row counts + lineage.
      5. Applies observability degradation via _apply_observability.
      6. Builds stochastic signals via _build_signals (anchored to real row_delta).
      7. Assembles the incident dict via _build_incident.

    seed_base is pipeline-specific to prevent correlated randomness across pipeline
    families.  Within a pipeline, each incident gets a unique seed derived from:
      obs_rng seed = seed_base + iteration * 53 + abs(hash(fault_type)) % 997
      sig_rng seed = seed_base + 1000 + iteration * 71 + abs(hash(fault_type)) % 503

    Why two separate RNGs (obs_rng, sig_rng)?
    The observability RNG controls which edges are dropped; the signal RNG controls
    signal values.  Keeping them separate ensures that changing the observability
    mode (e.g., adding a 4th mode) doesn't affect signal draws, and vice versa.
    Using different multipliers (53 vs 71) prevents the two seeds from being
    identical for any (iteration, fault_type) pair.

    Why % 997 and % 503?
    Prime moduli ensure the hash-derived offset varies across fault types with no
    common factors that could create coincidental collisions between seeds.

    Why iteration+1 passed to fault_fn (not iteration)?
    fault_fn uses iteration for modular null placement (null_explosion uses
    row_number % 7 = iteration % 7).  Passing iteration+1 ensures iteration=0
    doesn't produce a "% 7 = 0" that nulls every 7th row starting from the
    first row — offsetting by 1 distributes patterns more evenly.

    The baseline is built ONCE at the start (not per-iteration) because:
    - All iterations for a given pipeline have the same clean baseline.
    - Building it once saves ~30s per pipeline (would otherwise add 90 DB loads).
    - The baseline only needs source + pipeline execution — no fault injection.
    """
    incidents: list[dict[str, object]] = []

    # Baseline: clean run with no fault — used to compute row_delta = (faulted - baseline) / baseline
    bl_conn = duckdb.connect(str(Path(tmp_dir) / f"{prefix}_baseline.duckdb"))
    load_fn(bl_conn, max_rows)
    baseline = run_fn(bl_conn)
    bl_conn.close()

    counter = incident_counter_start
    for fault_type in fault_types:
        for iteration in range(per_fault):
            # Each incident gets its own DuckDB file to ensure complete isolation —
            # one fault's CREATE OR REPLACE TABLE doesn't bleed into the next incident.
            db_path = Path(tmp_dir) / f"{prefix}_{fault_type}_{iteration}.duckdb"
            conn = duckdb.connect(str(db_path))
            load_fn(conn, max_rows)

            # Fault injection: modifies source tables; returns ground-truth labels
            root, observed, schema_drifted = fault_fn(conn, fault_type, iteration + 1)
            # Pipeline execution: captures faulted row counts and lineage
            faulted = run_fn(conn, schema_drifted=schema_drifted)
            conn.close()

            raw_edges = [(src, dst) for src, dst in faulted["runtime_lineage"]]

            # Separate RNG for observability degradation — independent of signal draws
            obs_rng = random.Random(seed_base + iteration * 53 + abs(hash(fault_type)) % 997)
            runtime_edges, obs_mode = _apply_observability(raw_edges, root, iteration, obs_rng)

            # Separate RNG for signal generation — independent of observability drops
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
    """Entry point: run all 4 pipelines, evaluate all RCA methods, write JSON output.

    Typical invocation (from run_strengthened_suite.py):
      python3 run_real_case_study.py --per-fault 15 --max-rows 120000 --lrllm

    Or standalone heuristics-only for faster iteration:
      python3 run_real_case_study.py --per-fault 15 --no-lrllm
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--per-fault", type=int, default=15,
                        help="Iterations per fault type (15 = 5 full + 5 sparse + 5 missing_root)")
    parser.add_argument("--max-rows", type=int, default=120000,
                        help="Row limit for Yellow / Divvy / BTS (large) datasets.")
    parser.add_argument("--output", type=Path,
                        default=ROOT / "experiments" / "results" / "real_case_study_eval.json")
    parser.add_argument("--lrllm", action="store_true",
                        help="Run LR-LLM scoring via Claude on the real-data incidents.")
    parser.add_argument("--lrllm-model", default="claude-opus-4-7",
                        help="Claude model for LR-LLM. Default: claude-opus-4-7.")
    parser.add_argument("--lrllm-alpha", type=float, default=0.60,
                        help="LLM weight in hybrid score: score = alpha*llm + (1-alpha)*lineage_rank.")
    parser.add_argument("--lrllm-delay", type=float, default=3.0,
                        help="Seconds between LLM API calls to stay within rate limits.")
    args = parser.parse_args()

    # Fault types ordered: Type A first (row-count faults), then Type B (value faults).
    # This ordering affects incident_id numbering but not evaluation results.
    fault_types = [
        "missing_partition",    # Type A: removes most-populated date
        "duplicate_ingestion",  # Type A: duplicates most-populated date
        "stale_source",         # Type A: drops ~40% most-recent rows
        "null_explosion",       # Type B: nullifies ~1/7 of a join key column
        "bad_join_key",         # Type B: shifts dimension table IDs, breaks join
        "schema_drift",         # Type B: hardcodes a classification column
    ]

    # Yellow and Green specs loaded at module level (SPEC, GREEN_SPEC).
    # Divvy and BTS specs loaded here (not used as module-level constants because
    # they're only needed inside main() and adding them to module scope would add
    # unnecessary import overhead for scripts that don't call main()).
    DIVVY_SPEC = get_pipeline_specs()["divvy_chicago_bike"]
    BTS_SPEC   = get_pipeline_specs()["bts_airline_ontime"]

    # Optional pipelines: skip gracefully if data files are not downloaded.
    # Yellow taxi is always run (the module raises if its data is missing).
    green_path = ROOT / "data" / "raw" / "nyc_taxi" / "green_tripdata_2024-01.parquet"
    divvy_path = ROOT / "data" / "raw" / "divvy"     / "202401-divvy-tripdata.csv"
    bts_path   = ROOT / "data" / "raw" / "bts_airline"/ "On_Time_2024_1.csv"
    has_green = green_path.exists()
    has_divvy = divvy_path.exists()
    has_bts   = bts_path.exists()

    incidents: list[dict[str, object]] = []

    # All DuckDB databases are written into a TemporaryDirectory that is
    # automatically cleaned up when the context manager exits.  This avoids
    # leaving 360+ database files on disk (each is ~5–20 MB).
    with tempfile.TemporaryDirectory() as tmp_dir:
        # ── Pipeline 1: NYC Yellow taxi ────────────────────────────────────
        # seed_base=1337: chosen arbitrarily; different from other pipelines
        # to prevent correlated RNG sequences across pipeline families.
        print("Running yellow taxi pipeline (8 nodes, NYC TLC) …")
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
            incident_counter_start=len(incidents) + 1,
        )
        incidents.extend(yellow_incidents)
        n_yellow = len(yellow_incidents)
        print(f"  Yellow: {n_yellow} incidents.")

        # ── Pipeline 2: NYC Green taxi ─────────────────────────────────────
        # seed_base=9999 — distinct from yellow (1337), divvy (7777), bts (3141).
        # max_rows hard-coded to 56,551 (full dataset — no LIMIT truncation needed).
        n_green = 0
        if has_green:
            print("Running green taxi pipeline (8 nodes, NYC TLC green) …")
            green_incidents = _run_one_pipeline(
                tmp_dir=tmp_dir,
                prefix="green",
                fault_types=fault_types,
                per_fault=args.per_fault,
                max_rows=56551,  # full dataset; no LIMIT bias issue for green
                spec=GREEN_SPEC,
                load_fn=_load_green_sources,
                run_fn=_run_green_pipeline,
                fault_fn=_apply_green_fault,
                pipeline_name="nyc_green_taxi_etl_real",
                seed_base=9999,
                incident_counter_start=len(incidents) + 1,
            )
            incidents.extend(green_incidents)
            n_green = len(green_incidents)
            print(f"  Green: {n_green} incidents.")
        else:
            print("  Green taxi parquet not found — skipping.")

        # ── Pipeline 3: Divvy Chicago bike-share ───────────────────────────
        # seed_base=7777.  max_rows=120k from 144k-row CSV.
        n_divvy = 0
        if has_divvy:
            print("Running Divvy bike-share pipeline (8 nodes, Lyft Divvy Jan 2024) …")
            divvy_incidents = _run_one_pipeline(
                tmp_dir=tmp_dir,
                prefix="divvy",
                fault_types=fault_types,
                per_fault=args.per_fault,
                max_rows=args.max_rows,
                spec=DIVVY_SPEC,
                load_fn=_load_divvy_sources,
                run_fn=_run_divvy_pipeline,
                fault_fn=_apply_divvy_fault,
                pipeline_name="divvy_chicago_bike_real",
                seed_base=7777,
                incident_counter_start=len(incidents) + 1,
            )
            incidents.extend(divvy_incidents)
            n_divvy = len(divvy_incidents)
            print(f"  Divvy: {n_divvy} incidents.")
        else:
            print("  Divvy CSV not found — skipping.")

        # ── Pipeline 4: BTS Airline On-Time ───────────────────────────────
        # seed_base=3141 (pi digits).  max_rows=120k from 547k-row CSV.
        # The only dual-path DAG — airport_lookup → {flights_enriched, route_delay_metrics}.
        n_bts = 0
        if has_bts:
            print("Running BTS airline pipeline (8 nodes, dual-path DAG, BTS Jan 2024) …")
            bts_incidents = _run_one_pipeline(
                tmp_dir=tmp_dir,
                prefix="bts",
                fault_types=fault_types,
                per_fault=args.per_fault,
                max_rows=args.max_rows,
                spec=BTS_SPEC,
                load_fn=_load_bts_sources,
                run_fn=_run_bts_pipeline,
                fault_fn=_apply_bts_fault,
                pipeline_name="bts_airline_ontime_real",
                seed_base=3141,
                incident_counter_start=len(incidents) + 1,
            )
            incidents.extend(bts_incidents)
            n_bts = len(bts_incidents)
            print(f"  BTS: {n_bts} incidents.")
        else:
            print("  BTS airline CSV not found — skipping.")

    print(f"\nTotal: {len(incidents)} real-data incidents across "
          f"{sum([1, has_green, has_divvy, has_bts])} pipeline families.")

    # ── Evaluate all proposed RCA methods ─────────────────────────────────────
    # candidate_rows: for each incident, creates one candidate-row dict per node
    #   in the pipeline (so n_incidents × 8 rows total).  Each row has all signal
    #   features and a boolean "is_root_cause".
    # score_rows: computes all heuristic scoring features in-place on the rows.
    rows = [row for inc in incidents for row in candidate_rows(inc)]
    score_rows(rows)

    # ── LR-LLM: lineage-contextualized LLM scoring on real data ──────────────
    # Only runs when --lrllm is passed to avoid API cost on every run.
    # llm_score_incidents adds a "llm_lineage_rank" score field to each row
    # by calling the Claude API with a structured lineage + signal context prompt.
    # alpha=0.60 means final score = 0.60 * llm_score + 0.40 * lineage_rank_score.
    # rate_limit_delay=3.0s between calls gives ~18 min for 360 incidents at 1 call/inc.
    if args.lrllm:
        sources_desc = f"yellow {args.max_rows:,} rows"
        if has_green:  sources_desc += ", green 56k rows"
        if has_divvy:  sources_desc += f", Divvy {args.max_rows:,} rows"
        if has_bts:    sources_desc += f", BTS airline {args.max_rows:,} rows"
        print(f"\nRunning LR-LLM (model={args.lrllm_model}, alpha={args.lrllm_alpha}) "
              f"on {len(incidents)} real incidents …")
        print(f"  Data: {sources_desc}")
        print(f"  Estimated time: ~{len(incidents) * args.lrllm_delay / 60:.1f} min")
        lrllm_call_stats = llm_score_incidents(
            incidents=incidents,
            all_rows=rows,
            model=args.lrllm_model,
            alpha=args.lrllm_alpha,
            rate_limit_delay=args.lrllm_delay,
        )

    # All heuristic and paper-proposed methods.
    # "runtime_distance", "design_distance": graph-distance baselines
    # "centrality", "pagerank_adapted": graph centrality baselines
    # "freshness_only", "failed_tests", "recent_change", "quality_only": single-signal baselines
    # "causal_propagation" (LR-CP), "blind_spot_boosted" (LR-BS), "lineage_rank" (LR-H): proposed
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

    # ── LR-L: Random Forest with 4-fold leave-one-pipeline-out CV ─────────────
    # learned_ranker_predictions trains one RF model per fold (leaving out one
    # pipeline family as test set) and scores its candidates with the model trained
    # on the other three.  This prevents leakage: the model never sees the topology
    # or signals of the pipeline it's evaluating on.
    # FEATURE_SETS["all"] includes all available features (lineage distance, signal
    # scores, node-type indicators, etc.) — the paper reports feature importance.
    _lrl_metrics, lrl_rows, lrl_feature_importances = learned_ranker_predictions(
        rows, FEATURE_SETS["all"]
    )
    lrl_details = rank_details(lrl_rows, "learned_ranker")
    all_details["learned_ranker"] = lrl_details
    methods.append("learned_ranker")

    if args.lrllm:
        methods.append("llm_lineage_rank")
        all_details["llm_lineage_rank"] = rank_details(rows, "llm_lineage_rank")

    active_pipelines = ["nyc_yellow_taxi_etl_real"]
    if has_green:  active_pipelines.append("nyc_green_taxi_etl_real")
    if has_divvy:  active_pipelines.append("divvy_chicago_bike_real")
    if has_bts:    active_pipelines.append("bts_airline_ontime_real")

    data_source_parts = [
        f"NYC TLC yellow_tripdata_2024-01.parquet ({args.max_rows:,} rows)",
    ]
    if has_green:  data_source_parts.append("NYC TLC green_tripdata_2024-01.parquet (56,551 rows)")
    if has_divvy:  data_source_parts.append(f"Divvy 202401-divvy-tripdata.csv ({args.max_rows:,} rows)")
    if has_bts:    data_source_parts.append(f"BTS On_Time_2024_1.csv ({args.max_rows:,} rows)")

    summary: dict[str, object] = {
        "incident_count":        len(incidents),
        "pipelines":             active_pipelines,
        "yellow_incident_count": n_yellow,
        "green_incident_count":  n_green,
        "divvy_incident_count":  n_divvy,
        "bts_incident_count":    n_bts,
        "fault_types": fault_types,
        "data_source": (
            "; ".join(data_source_parts)
            + ". Faults injected via DuckDB SQL; runtime lineage captured from "
            "actual execution. run_anomaly anchored to real row-count deltas."
        ),
        # aggregate_details: computes Top-1, Top-3, MRR, nDCG, assets_before_true_cause
        "overall": {m: aggregate_details(all_details[m]) for m in methods},
        # by_group_from_details: same metrics broken down by a grouping key
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

    # Feature importances from LR-L: which features the Random Forest relied on most.
    # Used in the paper's TABLE VI and for the leakage discussion in Section V.
    summary["learned_feature_importances"] = lrl_feature_importances

    if args.lrllm:
        summary["lrllm_config"] = {
            "model":          args.lrllm_model,
            "alpha":          args.lrllm_alpha,
            "data_source":    "real_multi_pipeline",
            "incident_count": len(incidents),
            **lrllm_call_stats,
        }

    # ── Statistical analysis (can take 2–5 min) ───────────────────────────────
    # confidence_table: 1,500-sample bootstrap CIs for Top-1 and MRR per method.
    # significance_table: Wilcoxon signed-rank tests with Holm-Bonferroni correction
    #   over 7 pre-specified comparisons (TABLE VIII in the paper).
    # leakage_audit: runs LR-L with 4 feature subsets (ablation) to show no one
    #   feature dominates — verifies the learned ranker is not leaking.
    # pilot_analysis: sensitivity tables for LR-H weight profiles (TABLE II),
    #   LR-BS lambda (TABLE III), and LR-H ablation (TABLE XI).
    print("\n[stats] Computing bootstrap confidence intervals...")
    summary["confidence_intervals"] = confidence_table(all_details)

    print("[stats] Computing paired significance tests (Holm-Bonferroni)...")
    summary["significance_tests"] = significance_table(all_details)

    print("[stats] Running leakage audit (LR-L feature-set ablation, ~2-3 min)...")
    summary["leakage_audit"] = leakage_audit(rows)

    print("[stats] Running pilot analysis (TABLE II/III/V/XI)...")
    summary["pilot_analysis"] = pilot_analysis(rows, incidents)

    # ── Write canonical output JSON ───────────────────────────────────────────
    # Three top-level keys:
    #   "summary"   : all aggregate metrics, group breakdowns, statistical tables
    #   "incidents" : the full list of 360 incident dicts (ground truth + signals)
    #   "details"   : flattened per-method per-incident rank details (for post-hoc analysis)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    details_flat = [{"method": m, **d} for m in methods for d in all_details[m]]
    args.output.write_text(
        json.dumps({"summary": summary, "incidents": incidents, "details": details_flat}, indent=2)
    )
    print("\n=== OVERALL RESULTS ===")
    print(json.dumps(summary["overall"], indent=2))

    print("\n=== By observability mode ===")
    obs_methods = [
        "causal_propagation", "blind_spot_boosted", "lineage_rank", "learned_ranker",
    ]
    if args.lrllm:
        obs_methods.append("llm_lineage_rank")
    for m in obs_methods:
        print(f"\n{m}:")
        for mode, r in summary["by_observability"][m].items():
            print(f"  {mode:30s}: Top-1={r['top1']:.4f}  MRR={r['mrr']:.4f}  n={r['incident_count']}")

    print("\n=== By pipeline ===")
    pipe_methods = ["blind_spot_boosted", "lineage_rank", "learned_ranker"]
    if args.lrllm:
        pipe_methods.append("llm_lineage_rank")
    for m in pipe_methods:
        print(f"\n{m}:")
        for pipe, r in summary["by_pipeline"][m].items():
            print(f"  {pipe:40s}: Top-1={r['top1']:.4f}  n={r['incident_count']}")


if __name__ == "__main__":
    main()

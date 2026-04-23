from __future__ import annotations

import argparse
import hashlib
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw" / "nyc_taxi"


def synthetic_trips(seed: int = 99, rows: int = 75000) -> pd.DataFrame:
    random.seed(seed)
    base = datetime(2024, 1, 1)
    payload = []
    for trip_id in range(1, rows + 1):
        pickup = base + timedelta(minutes=random.randint(0, 60 * 24 * 45))
        duration = random.randint(5, 60)
        payload.append(
            {
                "VendorID": random.randint(1, 2),
                "tpep_pickup_datetime": pickup,
                "tpep_dropoff_datetime": pickup + timedelta(minutes=duration),
                "passenger_count": random.randint(1, 4),
                "trip_distance": round(random.uniform(0.5, 18.0), 2),
                "PULocationID": random.randint(1, 50),
                "DOLocationID": random.randint(1, 50),
                "fare_amount": round(random.uniform(7.0, 90.0), 2),
                "total_amount": round(random.uniform(10.0, 110.0), 2),
            }
        )
    return pd.DataFrame(payload)


def synthetic_zone_lookup() -> pd.DataFrame:
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    rows = []
    for location_id in range(1, 51):
        rows.append(
            {
                "LocationID": location_id,
                "Borough": boroughs[(location_id - 1) % len(boroughs)],
                "Zone": f"Zone {location_id}",
                "service_zone": "Yellow Zone" if location_id % 2 else "Boro Zone",
            }
        )
    return pd.DataFrame(rows)


def load_real_tables(conn: duckdb.DuckDBPyConnection, max_rows: int) -> tuple[bool, dict[str, object]]:
    parquet_path = RAW_DIR / "yellow_tripdata_2024-01.parquet"
    zone_path = RAW_DIR / "taxi_zone_lookup.csv"
    if not parquet_path.exists() or not zone_path.exists():
        return False, {}

    conn.execute("drop table if exists taxi_trips_raw")
    conn.execute("drop table if exists taxi_zone_lookup")
    conn.execute(
        f"""
        create table taxi_trips_raw as
        select *
        from read_parquet('{parquet_path.as_posix()}')
        limit {int(max_rows)}
        """
    )
    conn.execute(
        f"""
        create table taxi_zone_lookup as
        select *
        from read_csv_auto('{zone_path.as_posix()}', header=true)
        """
    )
    stats = conn.execute(
        """
        select
          count(*) as rows_loaded,
          count(distinct PULocationID) as pickup_zones,
          min(tpep_pickup_datetime) as min_pickup,
          max(tpep_pickup_datetime) as max_pickup
        from taxi_trips_raw
        """
    ).fetchone()
    return True, {
        "source_mode": "real",
        "rows_loaded": int(stats[0]),
        "distinct_pickup_zones": int(stats[1]),
        "min_pickup_datetime": str(stats[2]),
        "max_pickup_datetime": str(stats[3]),
        "trip_parquet": str(parquet_path),
        "zone_lookup_csv": str(zone_path),
    }


def load_synthetic_tables(conn: duckdb.DuckDBPyConnection) -> dict[str, object]:
    trips = synthetic_trips()
    zones = synthetic_zone_lookup()
    conn.register("trips_df", trips)
    conn.register("zones_df", zones)
    conn.execute("create or replace table taxi_trips_raw as select * from trips_df")
    conn.execute("create or replace table taxi_zone_lookup as select * from zones_df")
    stats = conn.execute(
        """
        select
          count(*) as rows_loaded,
          count(distinct PULocationID) as pickup_zones,
          min(tpep_pickup_datetime) as min_pickup,
          max(tpep_pickup_datetime) as max_pickup
        from taxi_trips_raw
        """
    ).fetchone()
    return {
        "source_mode": "synthetic",
        "rows_loaded": int(stats[0]),
        "distinct_pickup_zones": int(stats[1]),
        "min_pickup_datetime": str(stats[2]),
        "max_pickup_datetime": str(stats[3]),
    }


def lineage_event(step: str, inputs: list[str], outputs: list[str], row_count: int, elapsed: float, sql: str, dataset_mode: str) -> dict[str, object]:
    return {
        "event_time": datetime.utcnow().isoformat() + "Z",
        "step": step,
        "inputs": inputs,
        "outputs": outputs,
        "row_count": int(row_count),
        "elapsed_seconds": round(elapsed, 4),
        "dataset_mode": dataset_mode,
        "sql_hash": hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-mode", choices=["auto", "real", "synthetic"], default="auto")
    parser.add_argument("--max-rows", type=int, default=200000)
    parser.add_argument("--db-path", type=Path, default=ROOT / "data" / "processed" / "nyc_taxi_benchmark.duckdb")
    parser.add_argument("--results-path", type=Path, default=ROOT / "experiments" / "results" / "nyc_taxi_etl_results.json")
    parser.add_argument("--lineage-path", type=Path, default=ROOT / "data" / "lineage" / "nyc_taxi_etl_events.jsonl")
    args = parser.parse_args()

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    args.results_path.parent.mkdir(parents=True, exist_ok=True)
    args.lineage_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(args.db_path))

    real_loaded = False
    source_summary: dict[str, object]
    if args.dataset_mode in {"auto", "real"}:
        real_loaded, source_summary = load_real_tables(conn, args.max_rows)
        if args.dataset_mode == "real" and not real_loaded:
            raise FileNotFoundError("Real NYC taxi data requested, but the parquet/zone files are missing.")
    else:
        source_summary = {}

    if not real_loaded:
        source_summary = load_synthetic_tables(conn)

    dataset_mode = str(source_summary["source_mode"])
    lineage_events: list[dict[str, object]] = []

    steps: list[dict[str, float | int | str | dict[str, object]]] = []
    queries = [
        (
            "trip_enrichment",
            """
            create or replace table taxi_trips_enriched as
            select
              t.VendorID,
              cast(t.tpep_pickup_datetime as timestamp) as pickup_datetime,
              cast(t.tpep_dropoff_datetime as timestamp) as dropoff_datetime,
              t.passenger_count,
              t.trip_distance,
              t.PULocationID as pickup_location_id,
              t.DOLocationID as dropoff_location_id,
              t.fare_amount,
              t.total_amount,
              date_trunc('day', cast(t.tpep_pickup_datetime as timestamp)) as trip_day,
              coalesce(z.Borough, 'Unknown') as pickup_borough,
              coalesce(z.Zone, 'Unknown') as pickup_zone,
              case
                when t.fare_amount < 15 then 'short'
                when t.fare_amount < 40 then 'medium'
                else 'long'
              end as fare_band
            from taxi_trips_raw t
            left join taxi_zone_lookup z
              on t.PULocationID = z.LocationID
            where t.tpep_pickup_datetime is not null;
            select count(*) from taxi_trips_enriched;
            """,
            ["taxi_trips_raw", "taxi_zone_lookup"],
            ["taxi_trips_enriched"],
        ),
        (
            "daily_zone_rollup",
            """
            create or replace table taxi_daily_zone_metrics as
            select
              trip_day,
              pickup_borough,
              pickup_zone,
              count(*) as trip_count,
              round(avg(fare_amount), 2) as avg_fare,
              round(sum(trip_distance), 2) as total_distance
            from taxi_trips_enriched
            group by 1, 2, 3;
            select count(*) from taxi_daily_zone_metrics;
            """,
            ["taxi_trips_enriched"],
            ["taxi_daily_zone_metrics"],
        ),
        (
            "fare_band_rollup",
            """
            create or replace table taxi_fare_band_metrics as
            select
              fare_band,
              pickup_borough,
              count(*) as trip_count,
              round(sum(total_amount), 2) as total_revenue
            from taxi_trips_enriched
            group by 1, 2;
            select count(*) from taxi_fare_band_metrics;
            """,
            ["taxi_trips_enriched"],
            ["taxi_fare_band_metrics"],
        ),
    ]

    for name, query, inputs, outputs in queries:
        start = time.perf_counter()
        row_count = conn.execute(query).fetchone()[0]
        elapsed = time.perf_counter() - start
        steps.append({"name": name, "elapsed_seconds": round(elapsed, 4), "row_count": int(row_count)})
        lineage_events.append(lineage_event(name, inputs, outputs, row_count, elapsed, query, dataset_mode))

    validation = conn.execute(
        """
        select
          (select count(*) from taxi_trips_enriched where pickup_borough = 'Unknown') as unknown_borough_rows,
          (select count(*) from taxi_trips_enriched where fare_band is null) as null_fare_band_rows,
          (select count(*) from taxi_daily_zone_metrics) as daily_zone_rows,
          (select count(*) from taxi_fare_band_metrics) as fare_band_rows
        """
    ).fetchone()

    payload = {
        "source_summary": source_summary,
        "steps": steps,
        "validations": {
            "unknown_borough_rows": int(validation[0]),
            "null_fare_band_rows": int(validation[1]),
            "daily_zone_rows": int(validation[2]),
            "fare_band_rows": int(validation[3]),
        },
        "lineage_path": str(args.lineage_path),
    }

    args.results_path.write_text(json.dumps(payload, indent=2))
    args.lineage_path.write_text("".join(json.dumps(event) + "\n" for event in lineage_events))
    conn.close()


if __name__ == "__main__":
    main()

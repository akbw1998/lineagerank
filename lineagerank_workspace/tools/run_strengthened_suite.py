"""Single entry point for the PipeRCA-Bench real-data evaluation suite.

Runs all heuristic, learned, AND LR-LLM RCA methods strictly over real public
datasets — no synthetic incidents.  Eight structurally distinct pipeline families:

  1. NYC TLC Yellow Taxi  (Jan 2024, 2.96M rows — truncated to --max-rows)
  2. NYC TLC Green Taxi   (Jan 2024, 56,551 rows)
  3. Divvy Chicago Bike   (Jan 2024, 144,873 rides)
  4. BTS Airline On-Time  (Jan 2024, 547,271 flights — dual-path DAG)
  5. NHTSA FARS 2022      (~42k crashes — 3-source fan-in topology)
  6. Chicago Crime 2023   (~264k crimes — diamond parallel-enrichment topology)
  7. EPA AQS PM2.5 2023   (~600k measurements — deep temporal aggregation chain)
  8. NOAA Storm Events 2023 (~100k events — wide 3-leaf fan-out topology)

Each pipeline runs 6 fault types × --per-fault iterations = 90 real incidents.
With 8 pipelines: 720 incidents total, balanced across 3 observability modes.

LR-LLM (Claude Sonnet 4.5 via litellm proxy) runs on every incident by default.
Use --no-lrllm to skip it (e.g. for a quick heuristics-only run).
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


ROOT   = Path(__file__).resolve().parents[1]
PYTHON = Path("/opt/venv/bin/python3")


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def _download_if_missing(data_path: Path, script: Path) -> None:
    if not data_path.exists():
        print(f"Downloading: {data_path.name} …")
        run([str(PYTHON), str(script)])
    else:
        print(f"Already present: {data_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-lrllm", action="store_true",
        help="Skip LR-LLM scoring (runs heuristics + LR-L only). Default: LR-LLM is ON.",
    )
    parser.add_argument(
        "--lrllm-model", default="claude-sonnet-4-5",
        help="Claude model for LR-LLM (default: claude-sonnet-4-5, routes via OpenRouter on proxy).",
    )
    parser.add_argument(
        "--lrllm-alpha", type=float, default=0.60,
        help="LLM weight in hybrid score (default 0.60).",
    )
    parser.add_argument(
        "--per-fault", type=int, default=15,
        help="Iterations per fault type per pipeline (default 15 → 360 total).",
    )
    parser.add_argument(
        "--max-rows", type=int, default=120000,
        help="Row limit for large CSVs/parquets (default 120000).",
    )
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "experiments" / "results" / "real_case_study_eval.json",
    )
    args = parser.parse_args()

    # ── Step 1: Download real datasets if not already present ────────────────
    benchmarks = ROOT / "benchmarks"

    _download_if_missing(
        ROOT / "data" / "raw" / "nyc_taxi" / "yellow_tripdata_2024-01.parquet",
        benchmarks / "nyc_taxi_etl" / "download_real_data.py",
    )
    _download_if_missing(
        ROOT / "data" / "raw" / "divvy" / "202401-divvy-tripdata.csv",
        benchmarks / "divvy_bike_etl" / "download_real_data.py",
    )
    _download_if_missing(
        ROOT / "data" / "raw" / "bts_airline" / "On_Time_2024_1.csv",
        benchmarks / "bts_airline_etl" / "download_real_data.py",
    )
    _download_if_missing(
        ROOT / "data" / "raw" / "nhtsa_fars" / "ACCIDENT.CSV",
        benchmarks / "nhtsa_fars_etl" / "download_real_data.py",
    )
    _download_if_missing(
        ROOT / "data" / "raw" / "chicago_crime" / "crimes_2023.csv",
        benchmarks / "chicago_crime_etl" / "download_real_data.py",
    )
    _download_if_missing(
        ROOT / "data" / "raw" / "epa_aqs" / "daily_88101_2023.csv",
        benchmarks / "epa_aqs_etl" / "download_real_data.py",
    )
    _download_if_missing(
        ROOT / "data" / "raw" / "noaa_storm" / "storm_events_2023.csv",
        benchmarks / "noaa_storm_etl" / "download_real_data.py",
    )

    # ── Step 2: Run all methods over all real pipelines ───────────────────────
    case_study_cmd = [
        str(PYTHON), str(ROOT / "tools" / "run_real_case_study.py"),
        "--per-fault", str(args.per_fault),
        "--max-rows",  str(args.max_rows),
        "--output",    str(args.output),
    ]
    lrllm_enabled = not args.no_lrllm
    if lrllm_enabled:
        case_study_cmd += [
            "--lrllm",
            "--lrllm-model", args.lrllm_model,
            "--lrllm-alpha", str(args.lrllm_alpha),
        ]
    run(case_study_cmd)

    summary = {
        "real_case_study": str(args.output),
        "pipelines": [
            "nyc_yellow_taxi_etl_real",
            "nyc_green_taxi_etl_real",
            "divvy_chicago_bike_real",
            "bts_airline_ontime_real",
            "nhtsa_fars_crash_real",
            "chicago_crime_2023_real",
            "epa_aqs_pm25_real",
            "noaa_storm_events_real",
        ],
        "expected_incidents": args.per_fault * 6 * 8,
        "lrllm": f"{args.lrllm_model} (alpha={args.lrllm_alpha})" if lrllm_enabled else "disabled",
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

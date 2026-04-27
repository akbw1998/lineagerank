"""Single entry point for the PipeRCA-Bench real-data evaluation suite.

Runs all heuristic, learned, AND LR-LLM RCA methods strictly over real public
datasets — no synthetic incidents.  Four structurally distinct pipeline families:

  1. NYC TLC Yellow Taxi (Jan 2024, 2.96M rows — truncated to --max-rows)
  2. NYC TLC Green Taxi  (Jan 2024, 56,551 rows)
  3. Divvy Chicago Bike  (Jan 2024, 144,873 rides)
  4. BTS Airline On-Time (Jan 2024, 547,271 flights — dual-path DAG)

Each pipeline runs 6 fault types × --per-fault iterations = 90 real incidents.
With 4 pipelines: 360 incidents total, balanced across 3 observability modes.

LR-LLM (Claude Opus 4.7 via `claude -p`) runs on every incident by default.
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
        ],
        "expected_incidents": args.per_fault * 6 * 4,
        "lrllm": f"{args.lrllm_model} (alpha={args.lrllm_alpha})" if lrllm_enabled else "disabled",
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

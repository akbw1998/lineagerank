from __future__ import annotations

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "envs" / "benchmark311" / "bin" / "python"


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def main() -> None:
    raw_dir = ROOT / "data" / "raw" / "nyc_taxi"
    trip_path = raw_dir / "yellow_tripdata_2024-01.parquet"
    zone_path = raw_dir / "taxi_zone_lookup.csv"

    if not trip_path.exists() or not zone_path.exists():
        run([str(PYTHON), str(ROOT / "benchmarks" / "nyc_taxi_etl" / "download_real_data.py")])

    run([str(PYTHON), str(ROOT / "tools" / "generate_incidents.py"), "--per-fault", "25", "--seed", "42", "--output", str(ROOT / "data" / "incidents" / "lineagerank_incidents.json")])
    run([str(PYTHON), str(ROOT / "tools" / "evaluate_rankers.py"), "--incidents", str(ROOT / "data" / "incidents" / "lineagerank_incidents.json"), "--output", str(ROOT / "experiments" / "results" / "lineagerank_eval.json")])
    run([str(PYTHON), str(ROOT / "benchmarks" / "nyc_taxi_etl" / "run_benchmark.py"), "--dataset-mode", "real", "--max-rows", "200000"])
    run([str(PYTHON), str(ROOT / "tools" / "run_real_case_study.py"), "--per-fault", "6", "--max-rows", "120000", "--output", str(ROOT / "experiments" / "results" / "real_case_study_eval.json")])

    summary = {
        "synthetic_eval": str(ROOT / "experiments" / "results" / "lineagerank_eval.json"),
        "real_pipeline_run": str(ROOT / "experiments" / "results" / "nyc_taxi_etl_results.json"),
        "real_case_study": str(ROOT / "experiments" / "results" / "real_case_study_eval.json"),
        "runtime_lineage_events": str(ROOT / "data" / "lineage" / "nyc_taxi_etl_events.jsonl"),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import os
import platform
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BENCH_ENV = ROOT / "envs" / "benchmark311" / "bin"
AIRFLOW_ENV = ROOT / "envs" / "airflow311" / "bin"
AIRFLOW_HOME = ROOT / "tools" / "airflow_home"


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(cmd, cwd=cwd or ROOT, env=merged_env, check=True, capture_output=True, text=True)


def main() -> None:
    results_dir = ROOT / "experiments" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, object] = {
        "started_at_epoch": time.time(),
        "system": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "versions": {},
        "benchmarks": {},
    }

    summary["versions"] = {
        "python311": run([str(BENCH_ENV / "python"), "--version"]).stdout.strip(),
        "dbt": run([str(BENCH_ENV / "dbt"), "--version"]).stdout.strip().splitlines()[0],
        "duckdb": run(["/opt/homebrew/bin/duckdb", "--version"]).stdout.strip(),
        "airflow": run(
            [str(AIRFLOW_ENV / "airflow"), "version"],
            env={"AIRFLOW_HOME": str(AIRFLOW_HOME), "AIRFLOW__CORE__LOAD_EXAMPLES": "False"},
        ).stdout.strip(),
        "openlineage_airflow_import": run(
            [str(AIRFLOW_ENV / "python"), "-c", "import openlineage.airflow; print('ok')"],
            env={"AIRFLOW_HOME": str(AIRFLOW_HOME), "AIRFLOW__CORE__LOAD_EXAMPLES": "False"},
        ).stdout.strip(),
    }

    analytics_root = ROOT / "benchmarks" / "analytics_dag"
    analytics_db = ROOT / "data" / "processed" / "analytics_benchmark.duckdb"
    analytics_env = {
        "ANALYTICS_DUCKDB_PATH": str(analytics_db),
        "DBT_PROFILES_DIR": str(analytics_root / "profiles"),
    }

    run([str(BENCH_ENV / "python"), str(analytics_root / "generate_seeds.py")])

    analytics_steps = []
    for name, cmd in [
        ("dbt_seed", [str(BENCH_ENV / "dbt"), "seed", "--project-dir", str(analytics_root), "--profiles-dir", str(analytics_root / "profiles")]),
        ("dbt_build", [str(BENCH_ENV / "dbt"), "build", "--project-dir", str(analytics_root), "--profiles-dir", str(analytics_root / "profiles")]),
    ]:
        start = time.perf_counter()
        result = run(cmd, env=analytics_env)
        analytics_steps.append(
            {
                "name": name,
                "elapsed_seconds": round(time.perf_counter() - start, 4),
                "stdout_tail": result.stdout.strip().splitlines()[-5:],
            }
        )
    summary["benchmarks"]["analytics_dag"] = analytics_steps

    for benchmark_name in ["tpcds_pipeline", "nyc_taxi_etl"]:
        script = ROOT / "benchmarks" / benchmark_name / "run_benchmark.py"
        start = time.perf_counter()
        run([str(BENCH_ENV / "python"), str(script)])
        summary["benchmarks"][benchmark_name] = {
            "elapsed_seconds": round(time.perf_counter() - start, 4),
            "results_file": str(results_dir / f"{benchmark_name}_results.json"),
        }

    summary["finished_at_epoch"] = time.time()
    (results_dir / "benchmark_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

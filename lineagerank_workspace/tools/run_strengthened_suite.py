from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "envs" / "benchmark311" / "bin" / "python"


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Full PipeRCA-Bench strengthened suite: generates incidents, evaluates all "
            "LineageRank methods (including optional LR-LLM), runs the NYC taxi real-data "
            "case study, and writes all result files."
        )
    )
    parser.add_argument(
        "--lrllm-backend", default=None,
        choices=["gemini", "huggingface", "claude_cli"],
        help=(
            "Enable LR-LLM scoring. "
            "gemini: free Gemini 1.5 Flash API (get key at aistudio.google.com); "
            "huggingface: HF Inference API (get token at huggingface.co/settings/tokens); "
            "claude_cli: `claude -p` subprocess. "
            "Omit to run all methods except LR-LLM."
        ),
    )
    parser.add_argument(
        "--lrllm-key", default=None,
        help="API key for gemini or huggingface backends.",
    )
    parser.add_argument(
        "--lrllm-model", default=None,
        help="Model override for LR-LLM backend.",
    )
    parser.add_argument(
        "--lrllm-alpha", type=float, default=0.60,
        help="LLM weight in hybrid score (default 0.60).",
    )
    parser.add_argument(
        "--skip-taxi", action="store_true",
        help="Skip the NYC taxi ETL real-data steps (faster for synthetic-only runs).",
    )
    args = parser.parse_args()

    # ── Step 1: Download real data if needed ─────────────────────────────────
    if not args.skip_taxi:
        raw_dir = ROOT / "data" / "raw" / "nyc_taxi"
        trip_path = raw_dir / "yellow_tripdata_2024-01.parquet"
        zone_path = raw_dir / "taxi_zone_lookup.csv"
        if not trip_path.exists() or not zone_path.exists():
            run([str(PYTHON), str(ROOT / "benchmarks" / "nyc_taxi_etl" / "download_real_data.py")])

    # ── Step 2: Generate PipeRCA-Bench incidents ──────────────────────────────
    run([
        str(PYTHON), str(ROOT / "tools" / "generate_incidents.py"),
        "--per-fault", "25",
        "--seed", "42",
        "--output", str(ROOT / "data" / "incidents" / "lineagerank_incidents.json"),
    ])

    # ── Step 3: Evaluate all rankers (+ LR-LLM if requested) ─────────────────
    eval_cmd = [
        str(PYTHON), str(ROOT / "tools" / "evaluate_rankers.py"),
        "--incidents", str(ROOT / "data" / "incidents" / "lineagerank_incidents.json"),
        "--output",   str(ROOT / "experiments" / "results" / "lineagerank_eval.json"),
    ]
    if args.lrllm_backend:
        eval_cmd += ["--lrllm-backend", args.lrllm_backend]
        if args.lrllm_key:
            eval_cmd += ["--lrllm-key", args.lrllm_key]
        if args.lrllm_model:
            eval_cmd += ["--lrllm-model", args.lrllm_model]
        eval_cmd += ["--lrllm-alpha", str(args.lrllm_alpha)]
    run(eval_cmd)

    # ── Step 4: NYC taxi ETL real-data benchmark ──────────────────────────────
    if not args.skip_taxi:
        run([
            str(PYTHON), str(ROOT / "benchmarks" / "nyc_taxi_etl" / "run_benchmark.py"),
            "--dataset-mode", "real", "--max-rows", "200000",
        ])
        run([
            str(PYTHON), str(ROOT / "tools" / "run_real_case_study.py"),
            "--per-fault", "6", "--max-rows", "120000",
            "--output", str(ROOT / "experiments" / "results" / "real_case_study_eval.json"),
        ])

    summary = {
        "synthetic_eval":        str(ROOT / "experiments" / "results" / "lineagerank_eval.json"),
        "real_pipeline_run":     str(ROOT / "experiments" / "results" / "nyc_taxi_etl_results.json"),
        "real_case_study":       str(ROOT / "experiments" / "results" / "real_case_study_eval.json"),
        "runtime_lineage_events":str(ROOT / "data" / "lineage" / "nyc_taxi_etl_events.jsonl"),
        "lrllm_backend":         args.lrllm_backend or "disabled",
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / "envs" / "benchmark311" / "bin" / "python"


def run(args: list[str]) -> None:
    subprocess.run(args, cwd=ROOT, check=True)


def main() -> None:
    incidents = ROOT / "data" / "incidents" / "lineagerank_incidents.json"
    results = ROOT / "experiments" / "results" / "lineagerank_eval.json"

    run([str(PYTHON), str(ROOT / "tools" / "generate_incidents.py"), "--per-fault", "25", "--seed", "42", "--output", str(incidents)])
    run([str(PYTHON), str(ROOT / "tools" / "evaluate_rankers.py"), "--incidents", str(incidents), "--output", str(results)])

    summary = json.loads(results.read_text())
    print(json.dumps({"incident_count": summary["incident_count"], "overall": summary["overall"]["lineage_rank"]}, indent=2))


if __name__ == "__main__":
    main()

# LineageRank Experiment Commands

## Generate incidents

```bash
cd /Users/rdesai/Research
envs/benchmark311/bin/python tools/generate_incidents.py --per-fault 25 --seed 42
```

## Run RCA evaluation

```bash
envs/benchmark311/bin/python tools/evaluate_rankers.py
```

## Run the full RCA experiment bundle

```bash
envs/benchmark311/bin/python tools/run_rca_experiments.py
```

## Outputs

- `data/incidents/lineagerank_incidents.json`
- `experiments/results/lineagerank_eval.json`

These results are the first LineageRank experiment outputs and are separate from the smoke benchmark validation files.

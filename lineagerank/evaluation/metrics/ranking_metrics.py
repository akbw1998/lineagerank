"""Standard ranking evaluation metrics: Acc@k, MRR, NDCG@k."""

import numpy as np
from typing import List, Tuple, Dict


def accuracy_at_k(ranked: List[Tuple[str, float]], gt: str, k: int) -> float:
    return 1.0 if gt in [n for n, _ in ranked[:k]] else 0.0


def reciprocal_rank(ranked: List[Tuple[str, float]], gt: str) -> float:
    for i, (n, _) in enumerate(ranked):
        if n == gt:
            return 1.0 / (i + 1)
    return 0.0


def ndcg_at_k(ranked: List[Tuple[str, float]], gt: str, k: int) -> float:
    for i, (n, _) in enumerate(ranked[:k]):
        if n == gt:
            return 1.0 / np.log2(i + 2)
    return 0.0


def evaluate(results: List[Dict]) -> Dict[str, float]:
    a1, a3, a5, mrr, ndcg3, ndcg5 = [], [], [], [], [], []
    for r in results:
        ranked, gt = r["ranked"], r["ground_truth"]
        a1.append(accuracy_at_k(ranked, gt, 1))
        a3.append(accuracy_at_k(ranked, gt, 3))
        a5.append(accuracy_at_k(ranked, gt, 5))
        mrr.append(reciprocal_rank(ranked, gt))
        ndcg3.append(ndcg_at_k(ranked, gt, 3))
        ndcg5.append(ndcg_at_k(ranked, gt, 5))
    return {
        "Acc@1": np.mean(a1), "Acc@3": np.mean(a3), "Acc@5": np.mean(a5),
        "MRR": np.mean(mrr), "NDCG@3": np.mean(ndcg3), "NDCG@5": np.mean(ndcg5),
        "n": len(results),
    }


def print_table(all_results: Dict[str, Dict]):
    metrics = ["Acc@1", "Acc@3", "Acc@5", "MRR", "NDCG@3"]
    w = 12
    hdr = f"{'Method':<22}" + "".join(f"{m:>{w}}" for m in metrics)
    print("\n" + "=" * len(hdr))
    print(hdr)
    print("=" * len(hdr))
    for method, res in all_results.items():
        row = f"{method:<22}" + "".join(f"{res.get(m,0):>{w}.3f}" for m in metrics)
        print(row)
    print("=" * len(hdr))

#!/usr/bin/env python3
"""Generate publication-quality figures for the LineageRank paper.

Produces:
  exports/figures/fig1_main_results.png  - Top-1 / MRR bar chart (all methods)
  exports/figures/fig2_observability.png - By-observability breakdown for key methods
  exports/figures/fig3_pipeline_dag.png  - Schematic of the three benchmark pipelines
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "experiments" / "results" / "lineagerank_eval.json"
OUT_DIR = ROOT / "exports" / "figures"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Publication style ─────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 9,
    "axes.titlesize": 9,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "figure.dpi": 200,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.05,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

# Method display names and colors
METHOD_LABELS = {
    "runtime_distance":   "Runtime Dist.",
    "design_distance":    "Design Dist.",
    "centrality":         "Centrality",
    "freshness_only":     "Freshness",
    "failed_tests":       "Failed Tests",
    "recent_change":      "Recent Change",
    "quality_only":       "Quality Only",
    "pagerank_adapted":   "PR-Adapted",
    "causal_propagation": "LR-CP (Causal Prop.)",
    "blind_spot_boosted": "LR-BS (Blind-Spot)",
    "lineage_rank":       "LR-H (Heuristic)",
    "learned_ranker":     "LR-L (Learned RF)",
}

BASELINE_COLOR = "#9ecae1"       # light blue for baselines
LR_CP_COLOR    = "#74c476"       # green for CP
LR_BS_COLOR    = "#fd8d3c"       # orange for BS
LR_H_COLOR     = "#3182bd"       # darker blue for LR-H
LR_L_COLOR     = "#e6550d"       # red-orange for LR-L (learned)

METHOD_COLORS = {
    "runtime_distance":   BASELINE_COLOR,
    "design_distance":    BASELINE_COLOR,
    "centrality":         BASELINE_COLOR,
    "freshness_only":     BASELINE_COLOR,
    "failed_tests":       BASELINE_COLOR,
    "recent_change":      BASELINE_COLOR,
    "quality_only":       BASELINE_COLOR,
    "pagerank_adapted":   "#bcbddc",    # light purple — published adapted baseline
    "causal_propagation": LR_CP_COLOR,
    "blind_spot_boosted": LR_BS_COLOR,
    "lineage_rank":       LR_H_COLOR,
    "learned_ranker":     LR_L_COLOR,
}

ORDERED_METHODS = [
    "runtime_distance", "design_distance", "centrality", "freshness_only",
    "failed_tests", "recent_change", "quality_only", "pagerank_adapted",
    "causal_propagation", "blind_spot_boosted", "lineage_rank", "learned_ranker",
]


# ── Figure 1: Main Results (Top-1 and MRR) ───────────────────────────────────
def fig1_main_results(data: dict) -> None:
    overall = data["overall"]
    ci = data["confidence_intervals"]

    methods = ORDERED_METHODS
    labels = [METHOD_LABELS[m] for m in methods]
    top1_vals = [overall[m]["top1"] for m in methods]
    mrr_vals  = [overall[m]["mrr"]  for m in methods]

    # CI error bars (asymmetric)
    top1_lo = [overall[m]["top1"] - ci[m]["top1"]["ci_low"]  for m in methods]
    top1_hi = [ci[m]["top1"]["ci_high"] - overall[m]["top1"] for m in methods]
    mrr_lo  = [overall[m]["mrr"]  - ci[m]["mrr"]["ci_low"]   for m in methods]
    mrr_hi  = [ci[m]["mrr"]["ci_high"]  - overall[m]["mrr"]  for m in methods]

    colors = [METHOD_COLORS[m] for m in methods]

    x = np.arange(len(methods))
    width = 0.38

    fig, ax = plt.subplots(figsize=(7.0, 3.4))

    bars1 = ax.bar(x - width/2, top1_vals, width, color=colors, alpha=0.92,
                   edgecolor="white", linewidth=0.5, label="Top-1")
    bars2 = ax.bar(x + width/2, mrr_vals,  width, color=colors, alpha=0.55,
                   edgecolor="white", linewidth=0.5, label="MRR", hatch="//")

    # Error bars
    ax.errorbar(x - width/2, top1_vals,
                yerr=[top1_lo, top1_hi],
                fmt="none", color="#333333", capsize=2.5, linewidth=0.9, capthick=0.9)
    ax.errorbar(x + width/2, mrr_vals,
                yerr=[mrr_lo, mrr_hi],
                fmt="none", color="#333333", capsize=2.5, linewidth=0.9, capthick=0.9)

    # Value labels on bars
    for bar, val in zip(bars1, top1_vals):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.015,
                    f"{val:.2f}", ha="center", va="bottom", fontsize=6.5, rotation=90)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=38, ha="right", fontsize=7.5)
    ax.set_ylabel("Score")
    ax.set_ylim(0, 1.15)
    ax.set_title("Fig. 1. Overall RCA Performance on PipeRCA-Bench (450 incidents, 95% bootstrap CI shown)", pad=6)
    ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)

    # Legend
    patch_top1 = mpatches.Patch(color="#aaaaaa", alpha=0.92, label="Top-1 Acc.")
    patch_mrr  = mpatches.Patch(color="#aaaaaa", alpha=0.55, hatch="//", label="MRR")
    legend_colors = [
        mpatches.Patch(color=BASELINE_COLOR, label="Baseline"),
        mpatches.Patch(color=LR_CP_COLOR,    label="LR-CP (new)"),
        mpatches.Patch(color=LR_BS_COLOR,    label="LR-BS (new)"),
        mpatches.Patch(color=LR_H_COLOR,     label="LR-H"),
        mpatches.Patch(color=LR_L_COLOR,     label="LR-L (learned)"),
    ]
    ax.legend(handles=[patch_top1, patch_mrr] + legend_colors,
              loc="upper left", ncol=2, framealpha=0.9, fontsize=7.5,
              handlelength=1.2, handleheight=0.9)

    fig.tight_layout()
    out = OUT_DIR / "fig1_main_results.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved: {out}")


# ── Figure 2: By-Observability Mode ──────────────────────────────────────────
def fig2_observability(data: dict) -> None:
    obs_data = data["by_observability"]
    modes = ["full", "runtime_sparse", "runtime_missing_root"]
    mode_labels = ["Full\nLineage", "Runtime\nSparse (30%)", "Root\nEdges Missing"]

    key_methods = [
        ("quality_only",       "Quality Only",    BASELINE_COLOR, "o-"),
        ("causal_propagation", "LR-CP",           LR_CP_COLOR,    "s-"),
        ("blind_spot_boosted", "LR-BS",           LR_BS_COLOR,    "D-"),
        ("lineage_rank",       "LR-H",            LR_H_COLOR,     "^-"),
        ("learned_ranker",     "LR-L (learned)",  LR_L_COLOR,     "v--"),
    ]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7.0, 3.0), sharey=False)

    for method_key, label, color, style in key_methods:
        top1_vals = [obs_data[method_key][m]["top1"] for m in modes]
        mrr_vals  = [obs_data[method_key][m]["mrr"]  for m in modes]
        ax1.plot(mode_labels, top1_vals, style, color=color, label=label,
                 linewidth=1.5, markersize=5)
        ax2.plot(mode_labels, mrr_vals,  style, color=color, label=label,
                 linewidth=1.5, markersize=5)

    for ax, metric in [(ax1, "Top-1 Accuracy"), (ax2, "MRR")]:
        ax.set_ylabel(metric)
        ax.set_ylim(0, 1.08)
        ax.yaxis.grid(True, alpha=0.3, linewidth=0.5)
        ax.set_axisbelow(True)
        ax.tick_params(axis="x", labelsize=7.5)

    ax1.set_title("(a) Top-1 by Observability Mode")
    ax2.set_title("(b) MRR by Observability Mode")
    ax2.legend(loc="lower left", fontsize=7.5, framealpha=0.9)

    fig.suptitle("Fig. 2. Performance under Three Lineage Observability Conditions", y=1.01, fontsize=9)
    fig.tight_layout()
    out = OUT_DIR / "fig2_observability.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved: {out}")


# ── Figure 3: Pipeline DAG Schematics ────────────────────────────────────────
def fig3_pipeline_dag() -> None:
    fig, axes = plt.subplots(1, 3, figsize=(7.0, 2.8))

    def draw_dag(ax, title, nodes, edges, node_colors):
        import math
        pos = {}
        # Simple hierarchical layout
        for node, (x, y) in nodes.items():
            pos[node] = (x, y)

        ax.set_xlim(-0.2, 2.2)
        ax.set_ylim(-0.3, 2.3)
        ax.set_aspect("equal")
        ax.axis("off")
        ax.set_title(title, fontsize=8, pad=4)

        # Draw edges
        for (src, dst) in edges:
            x0, y0 = pos[src]
            x1, y1 = pos[dst]
            dx, dy = x1 - x0, y1 - y0
            length = math.sqrt(dx*dx + dy*dy)
            # Arrow offset for box boundary (approx 0.25 units)
            offset = 0.22
            ux, uy = dx/length, dy/length
            ax.annotate("", xy=(x1 - ux*offset, y1 - uy*offset),
                        xytext=(x0 + ux*offset, y0 + uy*offset),
                        arrowprops=dict(arrowstyle="-|>", color="#555555",
                                        lw=0.9, mutation_scale=10))

        # Draw nodes
        for node, (x, y) in nodes.items():
            color = node_colors.get(node, "#aaaaaa")
            box = mpatches.FancyBboxPatch((x-0.32, y-0.18), 0.64, 0.36,
                                          boxstyle="round,pad=0.04",
                                          linewidth=0.8, edgecolor="#444444",
                                          facecolor=color)
            ax.add_patch(box)
            ax.text(x, y, node, ha="center", va="center", fontsize=5.5,
                    color="#111111", fontweight="bold" if "mart" in node or "revenue" in node else "normal")

    # Analytics DAG
    a_nodes = {
        "customers":   (0.0, 2.0), "orders":     (1.0, 2.0), "payments":  (2.0, 2.0),
        "stg_cust":    (0.0, 1.2), "stg_ord":    (1.0, 1.2), "stg_pay":   (2.0, 1.2),
        "revenue_mart":(1.0, 0.4),
    }
    a_edges = [
        ("customers","stg_cust"), ("orders","stg_ord"), ("payments","stg_pay"),
        ("stg_cust","revenue_mart"), ("stg_ord","revenue_mart"), ("stg_pay","revenue_mart"),
    ]
    a_colors = {"customers":"#c6dbef","orders":"#c6dbef","payments":"#c6dbef",
                "stg_cust":"#9ecae1","stg_ord":"#9ecae1","stg_pay":"#9ecae1",
                "revenue_mart":"#3182bd"}
    draw_dag(axes[0], "(a) Analytics DAG\n(7 nodes, fan-in join)", a_nodes, a_edges, a_colors)

    # TPC-DS
    t_nodes = {
        "store_sales":(1.0, 2.0), "customers":  (0.0, 2.0),
        "stores":     (1.5, 1.3), "items":      (2.0, 2.0),
        "daily_ss":   (0.3, 0.8), "cust_ltv":   (1.0, 0.3), "cat_region": (1.8, 0.8),
    }
    t_edges = [
        ("store_sales","daily_ss"), ("customers","cust_ltv"), ("store_sales","cust_ltv"),
        ("stores","cat_region"), ("items","cat_region"), ("store_sales","cat_region"),
    ]
    t_colors = {"store_sales":"#c6dbef","customers":"#c6dbef","stores":"#c6dbef","items":"#c6dbef",
                "daily_ss":"#3182bd","cust_ltv":"#3182bd","cat_region":"#3182bd"}
    draw_dag(axes[1], "(b) TPC-DS Pipeline\n(7 nodes, multi-mart)", t_nodes, t_edges, t_colors)

    # NYC Taxi
    n_nodes = {
        "raw_trips":   (0.5, 2.0), "zone_lookup": (1.5, 2.0),
        "enriched":    (1.0, 1.2),
        "daily_zone":  (0.4, 0.3), "fare_band":   (1.6, 0.3),
    }
    n_edges = [
        ("raw_trips","enriched"), ("zone_lookup","enriched"),
        ("enriched","daily_zone"), ("enriched","fare_band"),
    ]
    n_colors = {"raw_trips":"#c6dbef","zone_lookup":"#c6dbef",
                "enriched":"#9ecae1",
                "daily_zone":"#3182bd","fare_band":"#3182bd"}
    draw_dag(axes[2], "(c) NYC Taxi ETL\n(5 nodes, fan-out)", n_nodes, n_edges, n_colors)

    # Legend
    legend_elems = [
        mpatches.Patch(color="#c6dbef", label="Source"),
        mpatches.Patch(color="#9ecae1", label="Staging"),
        mpatches.Patch(color="#3182bd", label="Mart"),
    ]
    fig.legend(handles=legend_elems, loc="lower center", ncol=3, fontsize=7.5,
               framealpha=0.9, bbox_to_anchor=(0.5, -0.02))

    fig.suptitle("Fig. 3. PipeRCA-Bench Pipeline Families (node types color-coded)", fontsize=9, y=1.01)
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    out = OUT_DIR / "fig3_pipeline_dag.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved: {out}")


def fig4_noise_sensitivity() -> None:
    """Fig 4: Noise robustness — Top-1 vs. dropout rate line chart."""
    path = ROOT / "experiments" / "results" / "noise_sensitivity.json"
    sens = json.loads(path.read_text())

    rates  = [int(r.rstrip("%")) for r in sens["dropout_rates"]]
    series = {
        "centrality":         ("Centrality",   "#999999", ":", 0.9),
        "quality_only":       ("Quality Only", "#9ecae1", "--", 0.9),
        "causal_propagation": ("LR-CP",        LR_CP_COLOR, "-.", 1.2),
        "lineage_rank":       ("LR-H",         LR_H_COLOR,  "-",  1.4),
        "blind_spot_boosted": ("LR-BS",        LR_BS_COLOR, "-",  2.0),
    }

    fig, ax = plt.subplots(figsize=(4.2, 2.8))
    for key, (label, color, ls, lw) in series.items():
        vals = [sens["sensitivity"][r][key]["top1"] for r in sens["dropout_rates"]]
        ax.plot(rates, vals, ls, color=color, lw=lw, marker="o",
                markersize=3.5, label=label)

    ax.set_xlabel("Edge Dropout Rate (%)")
    ax.set_ylabel("Top-1 Accuracy")
    ax.set_title("Fig. 4. Noise Robustness vs. Sparse Edge Dropout (450 Incidents)")
    ax.set_xticks(rates)
    ax.set_ylim(0.25, 1.03)
    ax.legend(fontsize=7, loc="lower left")
    ax.yaxis.grid(True, alpha=0.3, lw=0.5)
    ax.set_axisbelow(True)
    fig.tight_layout()
    out = OUT_DIR / "fig4_noise_sensitivity.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved: {out}")


def main() -> None:
    data = json.loads(RESULTS.read_text())
    fig1_main_results(data)
    fig2_observability(data)
    fig3_pipeline_dag()
    fig4_noise_sensitivity()
    print("All figures generated.")


if __name__ == "__main__":
    main()

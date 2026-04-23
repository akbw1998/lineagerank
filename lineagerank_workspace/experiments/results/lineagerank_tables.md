# LineageRank Strengthened Tables

## Table 1. Overall RCA performance on PipeRCA-Bench

| Method | Top-1 | Top-3 | Top-5 | MRR | nDCG | Avg. Assets Before True Cause |
|---|---:|---:|---:|---:|---:|---:|
| Runtime distance | 0.0000 | 0.3178 | 0.7444 | 0.2780 | 0.4506 | 3.2622 |
| Design distance | 0.0000 | 0.5444 | 0.8578 | 0.3310 | 0.4945 | 2.5733 |
| Centrality | 0.2911 | 0.8911 | 0.9644 | 0.5725 | 0.6801 | 1.2800 |
| Freshness only | 0.1667 | 0.6822 | 0.9289 | 0.4615 | 0.5946 | 1.8867 |
| Failed tests | 0.0911 | 0.6400 | 0.8978 | 0.3967 | 0.5448 | 2.2244 |
| Recent change | 0.2244 | 1.0000 | 1.0000 | 0.5493 | 0.6643 | 1.1533 |
| Quality only | 0.3089 | 0.8222 | 0.9844 | 0.5584 | 0.6684 | 1.4156 |
| **LineageRank-H** | **0.6178** | **0.9911** | **1.0000** | **0.7907** | **0.8446** | **0.4956** |
| **LineageRank-L** | **0.8911** | **0.9867** | **0.9911** | **0.9395** | **0.9549** | **0.1667** |

## Table 2. Confidence intervals for the strongest methods

| Method | Top-1 Mean | Top-1 95% CI | MRR Mean | MRR 95% CI | Avg. Assets Before True Cause Mean | Avg. Assets Before True Cause 95% CI |
|---|---:|---|---:|---|---:|---|
| Centrality | 0.2911 | [0.2511, 0.3356] | 0.5725 | [0.5461, 0.5992] | 1.2800 | [1.1644, 1.3889] |
| Quality only | 0.3089 | [0.2667, 0.3489] | 0.5584 | [0.5296, 0.5859] | 1.4156 | [1.3044, 1.5311] |
| **LineageRank-H** | **0.6178** | **[0.5733, 0.6622]** | **0.7907** | **[0.7656, 0.8150]** | **0.4956** | **[0.4333, 0.5622]** |
| **LineageRank-L** | **0.8911** | **[0.8622, 0.9200]** | **0.9395** | **[0.9223, 0.9555]** | **0.1667** | **[0.1133, 0.2289]** |

## Table 3. Pairwise significance tests

| Comparison | Metric | Mean Difference | 95% CI | Bootstrap p-value |
|---|---|---:|---|---:|
| LineageRank-H vs Centrality | Top-1 | 0.3267 | [0.2711, 0.3800] | 0.0000 |
| LineageRank-H vs Centrality | MRR | 0.2183 | [0.1858, 0.2510] | 0.0000 |
| LineageRank-H vs Centrality | Assets Before True Cause | -0.7844 | [-0.9156, -0.6533] | 0.0000 |
| LineageRank-H vs Quality only | Top-1 | 0.3089 | [0.2511, 0.3667] | 0.0000 |
| LineageRank-H vs Quality only | MRR | 0.2324 | [0.1979, 0.2662] | 0.0000 |
| LineageRank-H vs Quality only | Assets Before True Cause | -0.9200 | [-1.0356, -0.8044] | 0.0000 |
| LineageRank-L vs LineageRank-H | Top-1 | 0.2733 | [0.2244, 0.3200] | 0.0000 |
| LineageRank-L vs LineageRank-H | MRR | 0.1488 | [0.1211, 0.1745] | 0.0000 |
| LineageRank-L vs LineageRank-H | Assets Before True Cause | -0.3289 | [-0.4000, -0.2467] | 0.0000 |

## Table 4. Leakage and ablation audit for LineageRank-L

| Variant | Feature Family | Top-1 | Top-3 | MRR | Avg. Assets Before True Cause |
|---|---|---:|---:|---:|---:|
| **All features** | Structure + evidence + priors | **0.8911** | **0.9867** | **0.9395** | **0.1667** |
| No fault prior | Removes fault-prior feature | 0.8689 | 0.9667 | 0.9220 | 0.2467 |
| Structure only | Graph/support features only | 0.6356 | 0.9489 | 0.7794 | 0.6622 |
| Evidence only | Quality/freshness/change features only | 0.8489 | 0.9844 | 0.9160 | 0.2111 |

## Table 5. Top learned feature importances

| Feature | Importance |
|---|---:|
| Run anomaly | 0.3779 |
| Recent change | 0.1899 |
| Fault prior | 0.1033 |
| Blind spot hint | 0.0589 |
| Uncertainty | 0.0522 |
| Runtime proximity | 0.0354 |
| Runtime support | 0.0310 |
| Blast radius | 0.0282 |
| Freshness severity | 0.0280 |
| Dual support | 0.0273 |

## Table 6. Real public-data pipeline run summary

Dataset source:
- Official NYC TLC January 2024 yellow taxi parquet
- Official NYC taxi zone lookup CSV

| Field | Value |
|---|---|
| Dataset mode | Real |
| Rows loaded | 200,000 |
| Distinct pickup zones | 235 |
| Min pickup datetime | 2002-12-31 22:59:39 |
| Max pickup datetime | 2024-01-03 22:46:45 |
| Unknown borough rows after enrichment | 799 |
| Daily zone rollup rows | 653 |
| Fare band rollup rows | 23 |
| Captured runtime lineage events | 3 |

## Table 7. Real-data case study validation

| Method | Top-1 | Top-3 | MRR | Avg. Assets Before True Cause |
|---|---:|---:|---:|---:|
| Centrality | 0.8000 | 1.0000 | 0.9000 | 0.2000 |
| Quality only | 0.4000 | 1.0000 | 0.6667 | 0.8000 |
| **LineageRank-H** | **1.0000** | **1.0000** | **1.0000** | **0.0000** |

Case-study fault coverage:
- Missing partition
- Duplicate ingestion
- Stale source
- Null explosion
- Bad join key against the real taxi zone lookup
